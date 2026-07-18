//! Response validators and conditional-request evaluation (RFC 9110 §13).
//!
//! Validators travel with provider resources: upstream `ETag`/`Last-Modified`
//! pass through verbatim for byte-identical bodies (glyphs, sprites), while
//! derived representations (rewritten style JSON) carry an Ishikari-computed
//! `ETag` instead. Evaluation implements the shared-cache subset: weak `ETag`
//! comparison for `If-None-Match` (which takes precedence) and second-granular
//! `If-Modified-Since`.

use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    body::Body,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::Response,
};

/// Cache validators attached to one provider response body.
#[derive(Clone, Default)]
pub(crate) struct Validators {
    etag: Option<Arc<str>>,
    last_modified: Option<SystemTime>,
}

impl Validators {
    pub(crate) fn new(etag: Option<Arc<str>>, last_modified: Option<SystemTime>) -> Self {
        Self {
            etag,
            last_modified,
        }
    }

    /// Validator set for a derived representation: the upstream `ETag`
    /// identifies the upstream bytes, not the transformed body, so only the
    /// caller-computed `ETag` applies and `Last-Modified` is dropped.
    pub(crate) fn derived_etag(etag: String) -> Self {
        Self {
            etag: Some(Arc::from(etag)),
            last_modified: None,
        }
    }

    /// Validator for a derived, origin/encoding-dependent body (rewritten style
    /// JSON, TileJSON): a strong `ETag` over the exact bytes served. It changes
    /// whenever the body or the rewrite logic changes, and identifies the
    /// transformed representation rather than the upstream bytes.
    pub(crate) fn for_derived_body(body: &[u8]) -> Self {
        use std::hash::Hasher;
        // Seed "ISKR"; the value is opaque, so the choice only needs to be
        // stable across requests, which it is.
        let mut hasher = twox_hash::XxHash64::with_seed(0x49534b52);
        hasher.write(body);
        Self::derived_etag(format!("\"{:016x}\"", hasher.finish()))
    }

    pub(crate) fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }

    pub(crate) fn etag_arc(&self) -> Option<Arc<str>> {
        self.etag.clone()
    }

    pub(crate) fn last_modified(&self) -> Option<SystemTime> {
        self.last_modified
    }

    pub(crate) fn last_modified_http_date(&self) -> Option<String> {
        self.last_modified.map(httpdate::fmt_http_date)
    }

    /// Adds `ETag` / `Last-Modified` to a response. Values that fail header
    /// validation (possible only for a peer-supplied `ETag`) are skipped rather
    /// than failing the response.
    pub(crate) fn apply(&self, headers: &mut HeaderMap) {
        if let Some(etag) = &self.etag
            && let Ok(value) = HeaderValue::from_str(etag)
        {
            headers.insert(header::ETAG, value);
        }
        if let Some(http_date) = self.last_modified_http_date()
            && let Ok(value) = HeaderValue::from_str(&http_date)
        {
            headers.insert(header::LAST_MODIFIED, value);
        }
    }

    /// Whether a conditional GET matches this representation, so a `304 Not
    /// Modified` can be served. `If-None-Match` takes precedence over
    /// `If-Modified-Since` (RFC 9110 §13.2.2).
    pub(crate) fn not_modified(&self, request: &HeaderMap) -> bool {
        if request.contains_key(header::IF_NONE_MATCH) {
            return request
                .get_all(header::IF_NONE_MATCH)
                .iter()
                .filter_map(|value| value.to_str().ok())
                .any(|value| if_none_match_matches(value, self.etag.as_deref()));
        }
        if let Some(if_modified_since) = request.get(header::IF_MODIFIED_SINCE) {
            let Some(last_modified) = self.last_modified else {
                return false;
            };
            let Ok(since) = if_modified_since
                .to_str()
                .map_err(|_| ())
                .and_then(|value| httpdate::parse_http_date(value).map_err(|_| ()))
            else {
                return false;
            };
            // HTTP-dates carry whole seconds; truncate before comparing so a
            // body stored at t.4s is "not modified since" its own emitted date.
            return truncate_to_seconds(last_modified) <= since;
        }
        false
    }

    /// Builds an origin-dependent JSON response with identical validators,
    /// cache policy, and Vary metadata on its `200` and `304` forms.
    pub(crate) fn origin_varying_json_response(
        &self,
        request: &HeaderMap,
        body: impl Into<Body>,
        cache_control: &'static str,
    ) -> Response {
        let mut response = if self.not_modified(request) {
            let mut response = Response::new(Body::empty());
            *response.status_mut() = StatusCode::NOT_MODIFIED;
            response.headers_mut().insert(
                header::CACHE_CONTROL,
                HeaderValue::from_static(cache_control),
            );
            response
        } else {
            super::bytes_response(body, "application/json", Some(cache_control))
        };
        self.apply(response.headers_mut());
        super::apply_origin_vary(response.headers_mut());
        response
    }
}

/// Weak comparison (RFC 9110 §8.8.3.2): `W/` prefixes are ignored on both
/// sides, and `*` matches any existing representation.
fn if_none_match_matches(if_none_match: &str, etag: Option<&str>) -> bool {
    split_entity_tag_list(if_none_match)
        .into_iter()
        .any(|candidate| {
            candidate == "*" || etag.is_some_and(|etag| strip_weak(candidate) == strip_weak(etag))
        })
}

/// Splits an entity-tag list without treating a comma inside an opaque tag as
/// a separator. Although such tags are uncommon, opaque-tag allows commas.
fn split_entity_tag_list(value: &str) -> Vec<&str> {
    let mut values = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    for (index, byte) in value.bytes().enumerate() {
        match byte {
            b'"' => quoted = !quoted,
            b',' if !quoted => {
                values.push(value[start..index].trim());
                start = index + 1;
            }
            _ => {}
        }
    }
    values.push(value[start..].trim());
    values
}

fn strip_weak(etag: &str) -> &str {
    etag.strip_prefix("W/").unwrap_or(etag)
}

fn truncate_to_seconds(time: SystemTime) -> SystemTime {
    match time.duration_since(UNIX_EPOCH) {
        Ok(elapsed) => UNIX_EPOCH + Duration::from_secs(elapsed.as_secs()),
        // Pre-epoch timestamps cannot come from our validators; be conservative.
        Err(_) => time,
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use axum::http::{HeaderMap, HeaderValue, header};

    use super::Validators;

    fn request(name: header::HeaderName, value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(name, HeaderValue::from_str(value).expect("header value"));
        headers
    }

    #[test]
    fn if_none_match_uses_weak_comparison_lists_and_star() {
        let validators = Validators::new(Some("\"abc\"".into()), None);
        assert!(validators.not_modified(&request(header::IF_NONE_MATCH, "\"abc\"")));
        assert!(validators.not_modified(&request(header::IF_NONE_MATCH, "W/\"abc\"")));
        assert!(validators.not_modified(&request(
            header::IF_NONE_MATCH,
            "\"other\", W/\"abc\", \"more\""
        )));
        assert!(validators.not_modified(&request(header::IF_NONE_MATCH, "*")));
        assert!(!validators.not_modified(&request(header::IF_NONE_MATCH, "\"other\"")));

        let no_etag = Validators::new(None, Some(UNIX_EPOCH + Duration::from_secs(1_000)));
        assert!(no_etag.not_modified(&request(header::IF_NONE_MATCH, "*")));
    }

    #[test]
    fn if_none_match_does_not_split_commas_inside_an_opaque_tag() {
        let validators = Validators::new(Some("\"a,b\"".into()), None);
        assert!(validators.not_modified(&request(header::IF_NONE_MATCH, "\"x\", \"a,b\"")));
    }

    #[test]
    fn if_modified_since_compares_at_second_granularity() {
        let stored = UNIX_EPOCH + Duration::from_millis(1_000_000_400);
        let validators = Validators::new(None, Some(stored));
        let http_date = validators
            .last_modified_http_date()
            .expect("last modified date");

        // A client echoing our own emitted date must get a 304 even though the
        // stored instant has sub-second precision.
        assert!(validators.not_modified(&request(header::IF_MODIFIED_SINCE, &http_date)));
        assert!(!validators.not_modified(&request(
            header::IF_MODIFIED_SINCE,
            &httpdate::fmt_http_date(UNIX_EPOCH + Duration::from_secs(999_999))
        )));
    }

    #[test]
    fn if_none_match_takes_precedence_over_if_modified_since() {
        let validators = Validators::new(
            Some("\"abc\"".into()),
            Some(UNIX_EPOCH + Duration::from_secs(1_000_000)),
        );
        let mut headers = request(header::IF_NONE_MATCH, "\"other\"");
        headers.insert(
            header::IF_MODIFIED_SINCE,
            HeaderValue::from_str(&httpdate::fmt_http_date(
                UNIX_EPOCH + Duration::from_secs(2_000_000),
            ))
            .expect("header value"),
        );
        // The date alone would match, but the ETag mismatch wins.
        assert!(!validators.not_modified(&headers));
    }

    #[test]
    fn derived_etag_drops_upstream_last_modified_semantics() {
        let validators = Validators::derived_etag("\"deadbeef\"".to_string());
        assert_eq!(validators.etag(), Some("\"deadbeef\""));
        assert!(validators.last_modified_http_date().is_none());
        let now = httpdate::fmt_http_date(SystemTime::now());
        assert!(!validators.not_modified(&request(header::IF_MODIFIED_SINCE, &now)));
    }

    #[test]
    fn if_none_match_tolerates_hostile_client_lists() {
        let validators = Validators::new(Some("\"abc\"".into()), None);
        // None of these may panic; each is a clean match/non-match decision.
        for (value, expected) in [
            ("", false),
            (",,,", false),
            ("   ", false),
            ("\"", false),      // lone quote
            ("\"abc", false),   // unbalanced quote, not the real tag
            ("W/", false),      // weak prefix only
            ("*", true),        // matches any existing representation
            ("\"x\", *", true), // star anywhere in the list
            ("\"abc\"", true),
            ("  W/\"abc\"  ", true), // surrounding whitespace + weak prefix
        ] {
            assert_eq!(
                validators.not_modified(&request(header::IF_NONE_MATCH, value)),
                expected,
                "If-None-Match {value:?}"
            );
        }

        // A resource with no ETag: only `*` can match; opaque tags never do.
        let no_etag = Validators::new(None, None);
        assert!(no_etag.not_modified(&request(header::IF_NONE_MATCH, "*")));
        assert!(!no_etag.not_modified(&request(header::IF_NONE_MATCH, "\"abc\"")));

        // A served ETag always matches itself (reflexive), including a comma
        // inside the opaque tag.
        let comma = Validators::new(Some("\"a,b\"".into()), None);
        assert!(comma.not_modified(&request(header::IF_NONE_MATCH, "\"a,b\"")));
    }

    #[test]
    fn derived_body_etag_is_strong_stable_and_body_sensitive() {
        let a = Validators::for_derived_body(b"one");
        let b = Validators::for_derived_body(b"one");
        let c = Validators::for_derived_body(b"two");
        let etag = a.etag().expect("etag");
        assert!(
            etag.starts_with('"') && etag.ends_with('"'),
            "strong ETag: {etag:?}"
        );
        assert!(!etag.starts_with("W/"));
        assert_eq!(etag, b.etag().unwrap(), "same body → same ETag");
        assert_ne!(etag, c.etag().unwrap(), "different body → different ETag");
        assert!(a.last_modified_http_date().is_none());
        // Round-trips as a conditional match against itself.
        assert!(a.not_modified(&request(header::IF_NONE_MATCH, etag)));
    }

    #[test]
    fn if_modified_since_tolerates_unparseable_dates() {
        let validators = Validators::new(None, Some(UNIX_EPOCH + Duration::from_secs(1_000)));
        for value in ["", "not a date", "0", "Mon, 99 Xyz 9999 99:99:99 GMT"] {
            // An unparseable If-Modified-Since is ignored (serve 200), not a panic.
            assert!(!validators.not_modified(&request(header::IF_MODIFIED_SINCE, value)));
        }
    }
}
