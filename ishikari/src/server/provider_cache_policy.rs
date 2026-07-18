//! Provider `Cache-Control` parsing and shared-cache policy normalization.

use std::{sync::Arc, time::Duration};

use super::cache;

const STYLE_POSITIVE_TTL: Duration = Duration::from_secs(300);
const GLYPH_SPRITE_POSITIVE_TTL: Duration = Duration::from_secs(86400);
/// Upper bound on any upstream-derived freshness or stale window. Ishikari is a
/// shared cache, so a pathological `max-age` must not pin bytes for months.
const MAX_PROVIDER_TTL: Duration = Duration::from_secs(7 * 86400);

/// Effective caching decision for one fetched provider resource, derived from
/// the upstream `Cache-Control` (when present) or the per-resource defaults.
#[derive(Clone)]
pub(super) struct CachePolicy {
    /// When false, the bytes are returned but never retained by this shared
    /// cache (`no-store`, `no-cache`, or `private`).
    pub(super) store: bool,
    /// How long the entry is served without revalidation.
    pub(super) fresh: Duration,
    /// Extra window past `fresh` in which the stale entry is served while a
    /// background revalidation runs (upstream `stale-while-revalidate`).
    pub(super) swr: Duration,
    /// Cache policy emitted to downstream caches. This is normalized and
    /// clamped independently of the local entry's current age; `Age` carries
    /// the time already spent in Ishikari's cache.
    pub(super) response_cache_control: Arc<str>,
    /// Whether the upstream declared an explicit freshness lifetime
    /// (`max-age`/`s-maxage`). When false, `fresh` is Ishikari's invented
    /// default, so a transported `Age`/`Date` must not be charged against it.
    /// This travels with the policy so a background revalidation reuses the
    /// original decision instead of re-parsing a normalized header.
    pub(super) explicit_freshness: bool,
}

impl CachePolicy {
    /// Policy for a body whose upstream did not constrain caching.
    fn defaulted(resource: &'static str) -> Self {
        Self {
            store: true,
            fresh: positive_ttl(resource),
            swr: Duration::ZERO,
            response_cache_control: Arc::from(default_response_cache_control(resource)),
            explicit_freshness: false,
        }
    }
}

/// The subset of `Cache-Control` directives Ishikari acts on.
#[derive(Default)]
struct CacheControl {
    no_store: bool,
    no_cache: bool,
    private: bool,
    must_revalidate: bool,
    proxy_revalidate: bool,
    no_transform: bool,
    immutable: bool,
    max_age: Option<u64>,
    s_maxage: Option<u64>,
    stale_while_revalidate: Option<u64>,
}

fn parse_cache_control(value: &str) -> CacheControl {
    let mut control = CacheControl::default();
    for token in split_directives(value) {
        let token = token.trim();
        let (name, arg) = match token.split_once('=') {
            Some((name, arg)) => (name.trim(), Some(arg.trim().trim_matches('"'))),
            None => (token, None),
        };
        // Invalid delta-seconds are treated as zero. This is conservative and
        // avoids turning a malformed explicit limit into the resource default.
        let seconds = || {
            arg.and_then(|arg| arg.parse::<u128>().ok())
                .map_or(0, |seconds| seconds.min(u64::MAX as u128) as u64)
        };
        match name.to_ascii_lowercase().as_str() {
            "no-store" => control.no_store = true,
            "no-cache" => control.no_cache = true,
            "private" => control.private = true,
            "must-revalidate" => control.must_revalidate = true,
            "proxy-revalidate" => control.proxy_revalidate = true,
            "no-transform" => control.no_transform = true,
            "immutable" => control.immutable = true,
            // Duplicate freshness directives are ambiguous. Keep the most
            // conservative parsed value so reordering fields cannot extend a
            // shared-cache lifetime; a malformed explicit value parses as 0.
            "max-age" => update_min(&mut control.max_age, seconds()),
            "s-maxage" => update_min(&mut control.s_maxage, seconds()),
            "stale-while-revalidate" => update_min(&mut control.stale_while_revalidate, seconds()),
            _ => {}
        }
    }
    control
}

fn update_min(current: &mut Option<u64>, candidate: u64) {
    *current = Some(current.map_or(candidate, |value| value.min(candidate)));
}

/// Splits a `Cache-Control` field on directive boundaries without treating a
/// comma inside a quoted-string argument as a separator (RFC 9110 §5.6.4
/// quoted-string, including `\"` quoted-pairs). Without this, a valid
/// extension such as `x-meta="foo, s-maxage=604800"` would be misread as a
/// real `s-maxage` directive and extend the shared-cache lifetime beyond what
/// the origin declared.
fn split_directives(value: &str) -> Vec<&str> {
    let mut directives = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    let mut escaped = false;
    for (index, byte) in value.bytes().enumerate() {
        if escaped {
            escaped = false;
            continue;
        }
        match byte {
            b'\\' if quoted => escaped = true,
            b'"' => quoted = !quoted,
            b',' if !quoted => {
                directives.push(&value[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    directives.push(&value[start..]);
    directives
}

/// Resolves the effective policy. A shared cache prefers `s-maxage` over
/// `max-age`; `no-store`, `no-cache`, and `private` bypass this shared cache.
/// Revalidation-required responses never use SWR. All windows and the emitted
/// downstream policy are clamped to [`MAX_PROVIDER_TTL`].
pub(super) fn cache_policy(resource: &'static str, upstream: Option<&str>) -> CachePolicy {
    let Some(control) = upstream.map(parse_cache_control) else {
        return CachePolicy::defaulted(resource);
    };
    let response_cache_control = Arc::from(normalized_cache_control(resource, &control));
    let explicit_freshness = control.max_age.is_some() || control.s_maxage.is_some();
    if control.no_store || control.no_cache || control.private {
        return CachePolicy {
            store: false,
            fresh: Duration::ZERO,
            swr: Duration::ZERO,
            response_cache_control,
            explicit_freshness,
        };
    }
    let clamp = |secs: u64| Duration::from_secs(secs).min(MAX_PROVIDER_TTL);
    let fresh = match control.s_maxage.or(control.max_age) {
        Some(secs) => clamp(secs),
        None => positive_ttl(resource),
    };
    let swr = if control.must_revalidate || control.proxy_revalidate {
        Duration::ZERO
    } else {
        control.stale_while_revalidate.map_or(Duration::ZERO, clamp)
    };
    CachePolicy {
        store: true,
        fresh,
        swr,
        response_cache_control,
        explicit_freshness,
    }
}

fn normalized_cache_control(resource: &'static str, control: &CacheControl) -> String {
    if control.no_store {
        return "no-store".to_string();
    }
    if control.no_cache {
        return "no-cache".to_string();
    }

    let clamp = |seconds: u64| seconds.min(MAX_PROVIDER_TTL.as_secs());
    if control.private {
        let max_age = control.max_age.map_or(0, clamp);
        return format!("private, max-age={max_age}");
    }

    let default_fresh = positive_ttl(resource).as_secs();
    let max_age = control.max_age.map(clamp).unwrap_or_else(|| {
        if control.s_maxage.is_some() {
            0
        } else {
            default_fresh
        }
    });
    let s_maxage = control
        .s_maxage
        .map(clamp)
        .or_else(|| control.max_age.map(clamp))
        .unwrap_or(default_fresh);
    let mut directives = vec![
        "public".to_string(),
        format!("max-age={max_age}"),
        format!("s-maxage={s_maxage}"),
    ];
    if !(control.must_revalidate || control.proxy_revalidate)
        && let Some(swr) = control.stale_while_revalidate.map(clamp)
        && swr > 0
    {
        directives.push(format!("stale-while-revalidate={swr}"));
    }
    if control.must_revalidate {
        directives.push("must-revalidate".to_string());
    } else if control.proxy_revalidate {
        directives.push("proxy-revalidate".to_string());
    }
    if control.no_transform {
        directives.push("no-transform".to_string());
    }
    if control.immutable {
        directives.push("immutable".to_string());
    }
    directives.join(", ")
}

fn default_response_cache_control(resource: &'static str) -> &'static str {
    match resource {
        "style" => cache::STYLE,
        "glyph" | "sprite" => cache::GLYPH_SPRITE,
        _ => "no-cache",
    }
}

fn positive_ttl(resource: &'static str) -> Duration {
    match resource {
        "glyph" | "sprite" => GLYPH_SPRITE_POSITIVE_TTL,
        _ => STYLE_POSITIVE_TTL,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::http::HeaderValue;

    use super::{
        MAX_PROVIDER_TTL, cache_policy, normalized_cache_control, parse_cache_control, positive_ttl,
    };

    #[test]
    fn provider_cache_uses_longer_ttl_for_heavy_resources() {
        assert_eq!(positive_ttl("style"), Duration::from_secs(300));
        assert_eq!(positive_ttl("glyph"), Duration::from_secs(86400));
        assert_eq!(positive_ttl("sprite"), Duration::from_secs(86400));
    }

    #[test]
    fn missing_upstream_cache_control_uses_resource_default() {
        let style = cache_policy("style", None);
        assert!(style.store);
        assert_eq!(style.fresh, Duration::from_secs(300));
        assert_eq!(style.swr, Duration::ZERO);
        assert_eq!(
            cache_policy("glyph", None).fresh,
            Duration::from_secs(86400)
        );
    }

    #[test]
    fn shared_cache_prefers_s_maxage_and_honors_swr() {
        let policy = cache_policy(
            "style",
            Some("public, max-age=60, s-maxage=600, stale-while-revalidate=120"),
        );
        assert!(policy.store);
        assert_eq!(policy.fresh, Duration::from_secs(600));
        assert_eq!(policy.swr, Duration::from_secs(120));
    }

    #[test]
    fn max_age_is_used_when_s_maxage_absent() {
        let policy = cache_policy("style", Some("max-age=45"));
        assert_eq!(policy.fresh, Duration::from_secs(45));
        assert_eq!(policy.swr, Duration::ZERO);
    }

    #[test]
    fn no_store_bypasses_the_cache() {
        let policy = cache_policy("glyph", Some("no-store"));
        assert!(!policy.store);
        assert_eq!(policy.fresh, Duration::ZERO);
    }

    #[test]
    fn no_cache_and_private_bypass_the_shared_cache() {
        for directive in ["no-cache", "private", "private, max-age=600"] {
            let policy = cache_policy("style", Some(directive));
            assert!(!policy.store, "{directive} must not enter the shared cache");
            assert_eq!(policy.fresh, Duration::ZERO, "{directive}");
            assert_eq!(policy.swr, Duration::ZERO, "{directive}");
        }
    }

    #[test]
    fn revalidation_directives_disable_stale_serving() {
        for directive in ["must-revalidate", "proxy-revalidate"] {
            let policy = cache_policy(
                "style",
                Some(&format!(
                    "max-age=60, stale-while-revalidate=600, {directive}"
                )),
            );
            assert!(policy.store);
            assert_eq!(policy.fresh, Duration::from_secs(60));
            assert_eq!(policy.swr, Duration::ZERO);
            assert!(
                !policy
                    .response_cache_control
                    .contains("stale-while-revalidate")
            );
        }
    }

    #[test]
    fn duplicate_freshness_directives_use_the_most_conservative_value() {
        for value in ["max-age=0, max-age=604800", "max-age=604800, max-age=0"] {
            let control = parse_cache_control(value);
            assert_eq!(control.max_age, Some(0));
            assert_eq!(cache_policy("style", Some(value)).fresh, Duration::ZERO);
        }

        let control = parse_cache_control(
            "s-maxage=600, s-maxage=30, stale-while-revalidate=120, stale-while-revalidate=10",
        );
        assert_eq!(control.s_maxage, Some(30));
        assert_eq!(control.stale_while_revalidate, Some(10));
    }

    #[test]
    fn upstream_windows_are_clamped_to_the_ceiling() {
        let policy = cache_policy(
            "style",
            Some("max-age=999999999, stale-while-revalidate=999999999"),
        );
        assert_eq!(policy.fresh, MAX_PROVIDER_TTL);
        assert_eq!(policy.swr, MAX_PROVIDER_TTL);
    }

    #[test]
    fn cache_control_parsing_is_case_insensitive_and_tolerant() {
        let control = parse_cache_control("  Public , Max-Age=\"30\" , unknown-directive ");
        assert_eq!(control.max_age, Some(30));
        assert!(!control.no_store);
    }

    #[test]
    fn quoted_directive_arguments_do_not_leak_directives() {
        // A comma inside a quoted extension argument is not a separator; the
        // quoted `s-maxage` fragment must not extend the cache lifetime.
        let control = parse_cache_control("x-meta=\"foo, s-maxage=604800\", max-age=60");
        assert_eq!(control.s_maxage, None);
        assert_eq!(control.max_age, Some(60));
        assert_eq!(
            cache_policy("style", Some("x-meta=\"foo, s-maxage=604800\"")).fresh,
            positive_ttl("style"),
            "quoted fragment must fall back to the resource default"
        );

        // Escaped quotes inside the quoted string do not end it early.
        let control = parse_cache_control("x-meta=\"a\\\", max-age=604800\", max-age=45");
        assert_eq!(control.max_age, Some(45));

        // Freshness inside quotes never counts as explicit freshness.
        assert!(!cache_policy("style", Some("x-meta=\"max-age=100\"")).explicit_freshness);
        assert!(cache_policy("style", Some("max-age=100")).explicit_freshness);
        assert!(!cache_policy("style", Some("stale-while-revalidate=600")).explicit_freshness);
        assert!(!cache_policy("style", None).explicit_freshness);

        // Unbalanced quotes stay conservative: everything after the opening
        // quote is one opaque token, never a directive.
        let control = parse_cache_control("x-meta=\"oops, s-maxage=604800");
        assert_eq!(control.s_maxage, None);
    }

    #[test]
    fn cache_control_parser_tolerates_hostile_inputs() {
        // None of these may panic; each must resolve to a usable policy.
        for input in [
            "",
            "   ",
            ",,,",
            "max-age",
            "max-age=",
            "max-age=abc",
            "max-age=-5",
            "max-age=99999999999999999999999999999999",
            "max-age=\"30",
            "MAX-AGE=30, No-Store",
            "max-age=30, max-age=0",
            &"a=1,".repeat(5_000),
        ] {
            let policy = cache_policy("style", Some(input));
            assert!(
                policy.fresh <= MAX_PROVIDER_TTL,
                "fresh unbounded for {input:?}"
            );
            assert!(
                policy.swr <= MAX_PROVIDER_TTL,
                "swr unbounded for {input:?}"
            );
            let normalized = normalized_cache_control("style", &parse_cache_control(input));
            assert!(
                HeaderValue::from_str(&normalized).is_ok(),
                "unemittable Cache-Control for {input:?}: {normalized:?}"
            );
        }
    }

    #[test]
    fn cache_control_invariants_hold_for_freshness_directives() {
        assert_eq!(
            cache_policy("style", Some("max-age=abc")).fresh,
            Duration::ZERO
        );
        assert_eq!(
            cache_policy("style", Some("s-maxage=99999999999999999999")).fresh,
            MAX_PROVIDER_TTL
        );
        assert_eq!(
            cache_policy("style", Some("max-age=604800, max-age=0")).fresh,
            cache_policy("style", Some("max-age=0, max-age=604800")).fresh
        );
    }
}
