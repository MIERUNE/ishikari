//! Cross-hop request correlation id (`X-Request-Id`).
//!
//! An inbound id is accepted from the client, otherwise a fallback is generated.
//! The id is held in a task-local for the duration of the request so it can be
//! attached to tracing spans and forwarded to peers without threading it
//! through every function signature.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Canonical lowercase header name.
pub const HEADER: &str = "x-request-id";

/// Maximum accepted length of a client-supplied id.
pub const MAX_LEN: usize = 200;

tokio::task_local! {
    /// The request id in scope for the current task.
    pub static REQUEST_ID: String;
}

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generates a process-unique fallback request id.
pub fn generate() -> String {
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos() as u64);
    format!("ish-{nanos:x}-{seq:x}")
}

/// Accepts a client-supplied id if it is non-empty and reasonably sized,
/// otherwise generates a fallback.
pub fn accept_or_generate(candidate: Option<&str>) -> String {
    match candidate {
        Some(value) if is_valid(value) => value.to_owned(),
        _ => generate(),
    }
}

fn is_valid(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_LEN
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'!' | b'#'
                        | b'$'
                        | b'%'
                        | b'&'
                        | b'\''
                        | b'*'
                        | b'+'
                        | b'-'
                        | b'.'
                        | b'^'
                        | b'_'
                        | b'`'
                        | b'|'
                        | b'~'
                )
        })
}

/// Returns the request id in scope for the current task, if any.
pub fn current() -> Option<String> {
    REQUEST_ID.try_with(Clone::clone).ok()
}

#[cfg(test)]
mod tests {
    use super::{MAX_LEN, accept_or_generate};

    #[test]
    fn accepts_http_token_request_ids() {
        assert_eq!(accept_or_generate(Some("trace_01.a-b")), "trace_01.a-b");
    }

    #[test]
    fn replaces_unsafe_request_ids() {
        for value in ["contains space", "contains\ttab", "non-ascii-あ"] {
            assert!(accept_or_generate(Some(value)).starts_with("ish-"));
        }
        assert!(accept_or_generate(Some(&"a".repeat(MAX_LEN + 1))).starts_with("ish-"));
    }
}
