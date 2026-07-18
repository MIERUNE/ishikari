//! Provider-resource template configuration.

use url::Url;

#[derive(Clone, Debug, Default)]
pub struct ProviderConfig {
    style_templates: ResourceTemplates,
    glyph_url_template: Option<String>,
    sprite_templates: ResourceTemplates,
}

impl ProviderConfig {
    pub fn new(
        style_templates: Option<String>,
        glyph_url_template: Option<String>,
        sprite_templates: Option<String>,
    ) -> Result<Self, String> {
        Ok(Self {
            style_templates: style_templates
                .map(|raw| ResourceTemplates::parse(&raw, "STYLE_TEMPLATES"))
                .transpose()?
                .unwrap_or_default(),
            glyph_url_template: glyph_url_template
                .map(|template| validate_template(&template, &["{fontstack}", "{range}"]))
                .transpose()?,
            sprite_templates: sprite_templates
                .map(|raw| ResourceTemplates::parse(&raw, "SPRITE_TEMPLATES"))
                .transpose()?
                .unwrap_or_default(),
        })
    }

    pub(crate) fn resolve_style_url(&self, style_key: &str) -> Option<String> {
        self.style_templates.resolve(style_key)
    }

    pub(crate) fn resolve_glyph_url(&self, fontstack: &str, range: &str) -> Option<String> {
        let template = self.glyph_url_template.as_ref()?;
        Some(
            template
                .replace("{fontstack}", &path_percent_encode(fontstack))
                .replace("{range}", range),
        )
    }

    pub(crate) fn resolve_sprite_url(&self, style_key: &str, suffix: &str) -> Option<String> {
        self.sprite_templates
            .resolve(style_key)
            .map(|base| format!("{base}{suffix}"))
    }

    /// Whether a glyph upstream is configured, so glyph URLs can be served here.
    pub(crate) fn has_glyph_provider(&self) -> bool {
        self.glyph_url_template.is_some()
    }

    /// Whether a sprite upstream resolves for this style key, so its sprite URL
    /// can be served here.
    pub(crate) fn has_sprite_provider(&self, style_key: &str) -> bool {
        self.sprite_templates.resolve(style_key).is_some()
    }
}

#[derive(Clone, Debug, Default)]
struct ResourceTemplates {
    namespaces: Vec<(String, String)>,
    default: Option<String>,
}

impl ResourceTemplates {
    fn parse(raw: &str, config_name: &str) -> Result<Self, String> {
        let mut out = Self::default();

        for entry in raw.split(';') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }

            let (key, template, is_default) = match entry.split_once('=') {
                Some((key, value)) if is_namespace_key(key.trim()) => {
                    let key = key.trim();
                    (key, value.trim(), key == "default")
                }
                _ => ("default", entry, true),
            };
            // `{style_id}` is optional: a template without it resolves to a fixed
            // URL for every key, which is the common case for a shared sprite (one
            // sprite sheet across all styles). When present it is substituted in
            // `resolve`.
            let template = validate_template(template, &[])?;

            if is_default {
                if out.default.replace(template).is_some() {
                    return Err(format!("{config_name} has multiple default templates"));
                }
            } else if out.namespaces.iter().any(|(namespace, _)| namespace == key) {
                return Err(format!("{config_name} has duplicate namespace {key:?}"));
            } else {
                out.namespaces.push((key.to_string(), template));
            }
        }

        if out.namespaces.is_empty() && out.default.is_none() {
            return Err(format!("{config_name} must define at least one template"));
        }
        Ok(out)
    }

    fn resolve(&self, style_key: &str) -> Option<String> {
        if let Some((namespace, rest)) = style_key.split_once('/')
            && let Some((_, template)) = self
                .namespaces
                .iter()
                .find(|(candidate, _)| candidate == namespace)
        {
            return Some(template.replace("{style_id}", &path_percent_encode_segments(rest)));
        }
        self.default.as_ref().map(|template| {
            template.replace("{style_id}", &path_percent_encode_segments(style_key))
        })
    }
}

fn validate_template(template: &str, placeholders: &[&str]) -> Result<String, String> {
    if template.is_empty() {
        return Err("template must not be empty".to_string());
    }
    for placeholder in placeholders {
        if !template.contains(placeholder) {
            return Err(format!("template must contain {placeholder}"));
        }
    }
    let sample = template
        .replace("{style_id}", "sample")
        .replace("{fontstack}", "Noto%20Sans")
        .replace("{range}", "0-255");
    let parsed = Url::parse(&sample).map_err(|error| format!("template URL invalid: {error}"))?;
    match parsed.scheme() {
        // `gs`/`s3` are read through object_store (authenticated, e.g. Workload
        // Identity); `http(s)` through object_store's anonymous HTTP backend.
        "http" | "https" | "gs" | "s3" => Ok(template.to_string()),
        scheme => Err(format!("template URL scheme {scheme:?} is not supported")),
    }
}

fn is_namespace_key(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

pub(crate) fn path_percent_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~' | b',') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// Stable HRW routing key for a provider upstream. The URL is digested rather
/// than embedded: routing keys are logged verbatim (`routing_key`) on forward
/// warnings, and a provider URL can carry a signed query string or API key
/// that must never reach logs. Routing only needs key stability.
pub(crate) fn provider_routing_key(kind: &str, upstream_url: &str) -> String {
    use std::hash::Hasher;
    let mut hasher = twox_hash::XxHash64::with_seed(0x726f7574); // "rout"
    hasher.write(upstream_url.as_bytes());
    format!("{kind}:{:016x}", hasher.finish())
}

pub(crate) fn path_percent_encode_segments(value: &str) -> String {
    value
        .split('/')
        .map(path_percent_encode)
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::{ProviderConfig, ResourceTemplates};

    #[test]
    fn style_templates_strip_matched_namespace() {
        let templates = ResourceTemplates::parse(
            "carto=https://styles.example/carto/{style_id}/style.json;default=https://styles.example/{style_id}/style.json",
            "STYLE_TEMPLATES",
        )
        .unwrap();

        assert_eq!(
            templates.resolve("carto/voyager light").as_deref(),
            Some("https://styles.example/carto/voyager%20light/style.json")
        );
        assert_eq!(
            templates.resolve("demo/basic").as_deref(),
            Some("https://styles.example/demo/basic/style.json")
        );
        assert_eq!(
            templates.resolve("demo/basic?x=1#frag").as_deref(),
            Some("https://styles.example/demo/basic%3Fx%3D1%23frag/style.json")
        );
    }

    #[test]
    fn glyph_template_percent_encodes_fontstack() {
        let config = ProviderConfig::new(
            None,
            Some("https://glyphs.example/{fontstack}/{range}.pbf".to_string()),
            None,
        )
        .unwrap();

        assert_eq!(
            config
                .resolve_glyph_url("Noto Sans JP,Arial", "0-255")
                .as_deref(),
            Some("https://glyphs.example/Noto%20Sans%20JP,Arial/0-255.pbf")
        );
    }

    #[test]
    fn sprite_templates_append_requested_suffix() {
        let config = ProviderConfig::new(
            None,
            None,
            Some("carto=https://sprites.example/{style_id}/sprite".to_string()),
        )
        .unwrap();

        assert_eq!(
            config
                .resolve_sprite_url("carto/voyager", "@2x.png")
                .as_deref(),
            Some("https://sprites.example/voyager/sprite@2x.png")
        );
    }

    #[test]
    fn accepts_object_store_schemes() {
        // `gs://` (and `s3://`) templates are read via object_store with ambient
        // credentials, so they must validate alongside `http(s)`.
        let config = ProviderConfig::new(
            Some("gs://bucket/styles/{style_id}/style.json".to_string()),
            Some("gs://bucket/fonts/{fontstack}/{range}.pbf".to_string()),
            Some("s3://bucket/sprite/sprite".to_string()),
        )
        .unwrap();

        assert_eq!(
            config.resolve_style_url("hokkaido").as_deref(),
            Some("gs://bucket/styles/hokkaido/style.json")
        );
        assert_eq!(
            config.resolve_sprite_url("hokkaido", "@2x.png").as_deref(),
            Some("s3://bucket/sprite/sprite@2x.png")
        );
    }

    #[test]
    fn routing_keys_never_embed_the_upstream_url() {
        let url = "https://cdn.example/style.json?X-Signature=SECRETTOKEN";
        let key = super::provider_routing_key("style", url);
        assert!(key.starts_with("style:"), "{key}");
        assert!(!key.contains("SECRETTOKEN"), "{key}");
        assert!(!key.contains("cdn.example"), "{key}");
        // Stable for routing, distinct per URL.
        assert_eq!(key, super::provider_routing_key("style", url));
        assert_ne!(
            key,
            super::provider_routing_key("style", "https://cdn.example/other.json")
        );
    }
}
