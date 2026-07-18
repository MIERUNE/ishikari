//! Shared registry of object stores.
//!
//! Object stores (and their reqwest connection pools and credential caches) are
//! built once per scheme + authority (bucket/host) and reused across every
//! tileset and provider read to that backend, instead of being rebuilt per
//! request. A single registry is shared by the tile storage layer and the
//! provider fetch layer, so e.g. tiles and styles in the same bucket share one
//! store.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::{Result, anyhow};
use object_store::{ObjectStore, parse_url_opts, path::Path as ObjectPath};
use url::Url;

/// Caches object stores keyed by scheme + authority, including credential
/// identity. The key deliberately has no `Debug`/`Display` implementation so
/// URL userinfo cannot accidentally enter diagnostics.
#[derive(Default)]
pub struct ObjectStoreRegistry {
    stores: Mutex<HashMap<StoreKey, Arc<dyn ObjectStore>>>,
}

#[derive(Eq, Hash, PartialEq)]
struct StoreKey {
    scheme: String,
    authority: String,
}

impl ObjectStoreRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Resolves a URL to a reused object store plus the object path within it.
    /// The store is built once per scheme + authority and cached; the path is
    /// derived from the URL so different prefixes on one bucket share a store.
    pub fn resolve(&self, url: &Url) -> Result<(Arc<dyn ObjectStore>, ObjectPath)> {
        let key = store_key(url);
        let store = {
            let mut stores = self.stores.lock().expect("object store registry poisoned");
            if let Some(store) = stores.get(&key) {
                store.clone()
            } else {
                // The HTTP backend refuses plain-text HTTP by default, but
                // `http://` is an accepted provider-template scheme (local and
                // dev upstreams). The URL scheme already states the intent, so
                // enable it here instead of requiring an ALLOW_HTTP env var.
                let allow_http = (url.scheme() == "http")
                    .then_some(("allow_http".to_string(), "true".to_string()));
                let options = std::env::vars().chain(allow_http);
                let safe_url = diagnostic_url(url);
                let (store, _path) = parse_url_opts(url, options).map_err(|_| {
                    // `object_store` errors can contain the generated signed HTTP
                    // request URL. Do not retain that source in an error chain.
                    anyhow!("failed to parse object store URL {safe_url}")
                })?;
                let store: Arc<dyn ObjectStore> = store.into();
                stores.insert(key, store.clone());
                store
            }
        };
        let path = ObjectPath::from_url_path(url.path()).map_err(|_| {
            // Keep diagnostics useful without retaining query credentials,
            // userinfo, or a potentially secret-bearing source error.
            anyhow!("invalid object path in URL {}", diagnostic_url(url))
        })?;
        Ok((store, path))
    }
}

/// Identifies the object store backing a URL by scheme + authority
/// (host/port) and credential identity, independent of the object path.
///
/// A store built from an `http(s)` URL with userinfo bakes in those Basic Auth
/// credentials, so two sources on the same host under different credentials
/// (e.g. `alice:t@host` vs `bob:t@host`) must not share a store — reuse would
/// fetch one tenant's data under another's identity. Userinfo is folded in as a
/// part of the private key. This avoids both cross-credential reuse and the
/// collision risk of treating a short, non-cryptographic digest as identity.
fn store_key(url: &Url) -> StoreKey {
    StoreKey {
        scheme: url.scheme().to_string(),
        authority: url.authority().to_string(),
    }
}

/// Scheme + host + port, with no userinfo, path, query, or fragment. Safe to
/// log and stable per backend endpoint.
fn scheme_and_authority(url: &Url) -> String {
    format!(
        "{}://{}",
        url.scheme(),
        &url[url::Position::BeforeHost..url::Position::AfterPort]
    )
}

/// URL form safe for configuration diagnostics. Signed query parameters and
/// URL userinfo are credentials and must never enter public errors or logs.
fn diagnostic_url(url: &Url) -> String {
    let mut safe = url.clone();
    let _ = safe.set_password(None);
    let _ = safe.set_username("");
    safe.set_query(None);
    safe.set_fragment(None);
    safe.to_string()
}

/// Redacts a `namespace=url;…;default=url` source list (the `TILESET_SOURCES`
/// and provider-template form) for logging: each URL keeps only its scheme and
/// authority, dropping signed query strings, userinfo, and object paths so
/// startup logs never carry storage credentials. Namespaces are preserved.
pub fn redact_source_list(sources: &str) -> String {
    sources
        .split(';')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            let (prefix, raw_url) = match entry.split_once('=') {
                Some((namespace, url)) if is_namespace_key(namespace.trim()) => {
                    (format!("{}=", namespace.trim()), url.trim())
                }
                _ => (String::new(), entry),
            };
            let safe = Url::parse(raw_url)
                .map(|url| scheme_and_authority(&url))
                .unwrap_or_else(|_| "<unparseable-url>".to_string());
            format!("{prefix}{safe}")
        })
        .collect::<Vec<_>>()
        .join(";")
}

fn is_namespace_key(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{ObjectStoreRegistry, diagnostic_url, redact_source_list, store_key};
    use url::Url;

    #[test]
    fn store_key_is_path_and_query_independent() {
        let a = Url::parse("gs://bucket/styles/x/style.json").unwrap();
        let b = Url::parse("gs://bucket/japan.pmtiles?signature=abc").unwrap();
        // Same bucket + credentials, different paths/queries -> one store.
        // `StoreKey` has no `Debug` (so userinfo cannot leak), so compare with
        // `==` rather than `assert_eq!`.
        assert!(store_key(&a) == store_key(&b));
        assert_eq!(store_key(&a).scheme, "gs");
        assert_eq!(store_key(&a).authority, "bucket");

        let other = Url::parse("gs://other-bucket/x").unwrap();
        assert!(store_key(&a) != store_key(&other));
    }

    #[test]
    fn store_key_separates_distinct_credentials_on_one_host() {
        // Basic Auth is baked into an HTTP store, so different userinfo on the
        // same host must not reuse the first tenant's credentials.
        let alice = Url::parse("https://alice:secret@host.example/a").unwrap();
        let bob = Url::parse("https://bob:secret@host.example/a").unwrap();
        let alice_other_pw = Url::parse("https://alice:other@host.example/a").unwrap();
        let anon = Url::parse("https://host.example/a").unwrap();

        assert!(store_key(&alice) != store_key(&bob));
        assert!(store_key(&alice) != store_key(&alice_other_pw));
        assert!(store_key(&alice) != store_key(&anon));
        // Same credentials, different path -> shared.
        let alice_b = Url::parse("https://alice:secret@host.example/b").unwrap();
        assert!(store_key(&alice) == store_key(&alice_b));
    }

    #[test]
    fn registry_never_reuses_an_http_client_across_credentials() {
        let registry = ObjectStoreRegistry::new();
        let alice_a = Url::parse("https://alice:secret@host.example/a").unwrap();
        let alice_b = Url::parse("https://alice:secret@host.example/b").unwrap();
        let bob = Url::parse("https://bob:secret@host.example/a").unwrap();

        let (alice_store, _) = registry.resolve(&alice_a).expect("Alice store");
        let (alice_store_b, _) = registry.resolve(&alice_b).expect("reused Alice store");
        let (bob_store, _) = registry.resolve(&bob).expect("Bob store");

        assert!(Arc::ptr_eq(&alice_store, &alice_store_b));
        assert!(!Arc::ptr_eq(&alice_store, &bob_store));
    }

    #[test]
    fn diagnostic_urls_remove_query_fragment_and_userinfo() {
        let secret = "SECRETTOKEN";
        let url = Url::parse(&format!(
            "https://signed-user:signed-password@host.example/a/b?X-Signature={secret}#private"
        ))
        .unwrap();
        let diagnostic = diagnostic_url(&url);

        assert_eq!(diagnostic, "https://host.example/a/b");
        for sensitive in [
            secret,
            "signed-user",
            "signed-password",
            "X-Signature",
            "private",
        ] {
            assert!(!diagnostic.contains(sensitive), "{diagnostic}");
        }
    }

    #[test]
    fn source_list_diagnostics_never_retain_url_credentials_or_paths() {
        let diagnostic = redact_source_list(
            "regional=https://user:password@tiles.example/private/prefix?signature=SECRET;\
             https://default.example/private?token=DEFAULT_SECRET",
        );

        assert_eq!(
            diagnostic,
            "regional=https://tiles.example;https://default.example"
        );
        for sensitive in [
            "user",
            "password",
            "private",
            "signature",
            "SECRET",
            "token",
            "DEFAULT_SECRET",
        ] {
            assert!(!diagnostic.contains(sensitive), "{diagnostic}");
        }
    }
}
