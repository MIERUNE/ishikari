use std::{
    collections::{HashMap, HashSet},
    io::{BufRead, Read, Write},
    ops::Range,
};

use anyhow::{Context, Result, bail};

use crate::workload::TraceEntry;

/// Hard bounds on imported traces. The byte budget includes blank lines and
/// line terminators, and the line limit is enforced before an unbounded string
/// allocation can occur.
const MAX_TRACE_LINE_BYTES: usize = 4 * 1024;
const MAX_TRACE_BYTES: u64 = 256 * 1024 * 1024;
const MAX_TRACE_ENTRIES: usize = 2_000_000;
const MAX_TRACE_USERS: usize = 10_000;
/// The generated viewport workload emits at most nine requests per batch;
/// imported traces get headroom but not unbounded per-batch concurrency.
const MAX_VIEWPORT_BATCH_REQUESTS: usize = 64;

#[derive(Clone, Copy)]
struct TraceLimits {
    line_bytes: usize,
    total_bytes: u64,
    entries: usize,
}

impl Default for TraceLimits {
    fn default() -> Self {
        Self {
            line_bytes: MAX_TRACE_LINE_BYTES,
            total_bytes: MAX_TRACE_BYTES,
            entries: MAX_TRACE_ENTRIES,
        }
    }
}

/// Reads and validates a JSONL trace.
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

pub(crate) fn fnv1a64(bytes: &[u8]) -> u64 {
    fnv1a64_continue(FNV_OFFSET_BASIS, bytes)
}

fn fnv1a64_continue(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

pub(crate) fn format_fnv1a64(hash: u64) -> String {
    format!("fnv1a64:{hash:016x}")
}

/// Byte count and FNV-1a 64 hash of the exact bytes consumed while parsing a
/// trace. Computed in the single bounded parse pass so a report's request count
/// and its fingerprint always describe the same file version — reopening the
/// file to hash it separately would let a concurrent replacement describe
/// different bytes than were executed.
#[derive(Debug, Clone, Copy)]
pub struct TraceDigest {
    pub bytes: u64,
    pub fnv1a64: u64,
}

pub fn read_trace(reader: impl BufRead) -> Result<Vec<TraceEntry>> {
    Ok(read_trace_with_limits(reader, TraceLimits::default())?.0)
}

/// Parses a trace and returns its content digest from the same pass.
pub fn read_trace_with_digest(reader: impl BufRead) -> Result<(Vec<TraceEntry>, TraceDigest)> {
    read_trace_with_limits(reader, TraceLimits::default())
}

fn read_trace_with_limits(
    mut reader: impl BufRead,
    limits: TraceLimits,
) -> Result<(Vec<TraceEntry>, TraceDigest)> {
    let read_limit = limits
        .line_bytes
        .checked_add(2)
        .context("trace line limit overflow")?;
    let mut entries = Vec::new();
    let mut line = Vec::with_capacity(read_limit.min(8 * 1024));
    let mut total_bytes = 0_u64;
    let mut hash = FNV_OFFSET_BASIS;
    let mut line_number = 0_usize;

    loop {
        line.clear();
        let bytes_read = (&mut reader)
            .take(read_limit as u64)
            .read_until(b'\n', &mut line)
            .with_context(|| format!("read trace line {}", line_number + 1))?;
        if bytes_read == 0 {
            break;
        }
        line_number += 1;
        total_bytes = total_bytes
            .checked_add(bytes_read as u64)
            .context("trace byte count overflow")?;
        if total_bytes > limits.total_bytes {
            bail!("trace exceeds {} raw bytes", limits.total_bytes);
        }
        // Hash every raw byte, including terminators and whitespace-only lines,
        // so the digest matches a whole-file FNV-1a over the exact input.
        hash = fnv1a64_continue(hash, &line);

        let mut payload_end = line.len();
        if line.last() == Some(&b'\n') {
            payload_end -= 1;
            if payload_end > 0 && line[payload_end - 1] == b'\r' {
                payload_end -= 1;
            }
        }
        if payload_end > limits.line_bytes {
            bail!(
                "trace line {line_number} exceeds {} bytes",
                limits.line_bytes
            );
        }
        let payload = &line[..payload_end];
        if payload.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        if entries.len() >= limits.entries {
            bail!("trace exceeds {} entries", limits.entries);
        }
        let entry = serde_json::from_slice(payload)
            .with_context(|| format!("parse trace line {line_number}"))?;
        entries.push(entry);
    }
    viewport_batch_ranges(&entries)?;
    Ok((
        entries,
        TraceDigest {
            bytes: total_bytes,
            fnv1a64: hash,
        },
    ))
}

/// Writes one trace entry in JSONL format.
pub fn write_trace_entry(writer: &mut impl Write, entry: &TraceEntry) -> Result<()> {
    serde_json::to_writer(&mut *writer, entry).context("serialize trace entry")?;
    writer.write_all(b"\n").context("write trace newline")
}

/// Returns the contiguous ranges that form viewport batches.
///
/// A batch is identified by `(step, user)`. Ordinals must start at zero and be
/// contiguous, and a batch may not reappear later in the trace.
pub fn viewport_batch_ranges(entries: &[TraceEntry]) -> Result<Vec<Range<usize>>> {
    let mut ranges = Vec::new();
    let mut seen = HashSet::new();
    let mut last_step_by_user = HashMap::new();
    let mut start = 0;

    while start < entries.len() {
        let key = (entries[start].step, entries[start].user);
        if !seen.insert(key) {
            bail!(
                "trace batch step={} user={} is not contiguous",
                key.0,
                key.1
            );
        }
        if let Some(previous_step) = last_step_by_user.insert(key.1, key.0)
            && key.0 <= previous_step
        {
            bail!(
                "trace user={} step={} does not follow previous step={}",
                key.1,
                key.0,
                previous_step
            );
        }
        if last_step_by_user.len() > MAX_TRACE_USERS {
            bail!("trace exceeds {MAX_TRACE_USERS} distinct users");
        }

        let mut end = start;
        while end < entries.len() && (entries[end].step, entries[end].user) == key {
            let expected_ordinal = end - start;
            if entries[end].ordinal != expected_ordinal {
                bail!(
                    "trace batch step={} user={} has ordinal {}, expected {}",
                    key.0,
                    key.1,
                    entries[end].ordinal,
                    expected_ordinal
                );
            }
            end += 1;
        }
        if end - start > MAX_VIEWPORT_BATCH_REQUESTS {
            bail!(
                "trace batch step={} user={} has {} requests; the maximum is {MAX_VIEWPORT_BATCH_REQUESTS}",
                key.0,
                key.1,
                end - start
            );
        }
        ranges.push(start..end);
        start = end;
    }

    Ok(ranges)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::{
        FNV_OFFSET_BASIS, FNV_PRIME, TraceLimits, read_trace, read_trace_with_digest,
        read_trace_with_limits, viewport_batch_ranges, write_trace_entry,
    };
    use crate::workload::TraceEntry;

    #[test]
    fn digest_matches_a_whole_file_fnv_over_the_same_pass() {
        let bytes = b"{\"step\":1,\"user\":0,\"ordinal\":0,\"tileset\":\"japan\",\"z\":10,\"x\":900,\"y\":400,\"entry_node\":0}\n";
        let (entries, digest) =
            read_trace_with_digest(Cursor::new(&bytes[..])).expect("read with digest");
        assert_eq!(entries.len(), 1);
        assert_eq!(digest.bytes, bytes.len() as u64);
        let mut expected = FNV_OFFSET_BASIS;
        for &byte in bytes.iter() {
            expected ^= u64::from(byte);
            expected = expected.wrapping_mul(FNV_PRIME);
        }
        assert_eq!(digest.fnv1a64, expected);
    }

    fn entry(step: u64, user: usize, ordinal: usize) -> TraceEntry {
        TraceEntry {
            step,
            user,
            ordinal,
            tileset: "japan".to_string(),
            z: 10,
            x: 900,
            y: 400,
            entry_node: Some(0),
        }
    }

    #[test]
    fn jsonl_round_trip_preserves_batches() {
        let expected = vec![entry(0, 0, 0), entry(0, 0, 1), entry(0, 1, 0)];
        let mut bytes = Vec::new();
        for entry in &expected {
            write_trace_entry(&mut bytes, entry).expect("write entry");
        }

        let actual = read_trace(Cursor::new(bytes)).expect("read trace");

        assert_eq!(actual, expected);
        assert_eq!(
            viewport_batch_ranges(&actual).expect("ranges"),
            [0..2, 2..3]
        );
    }

    #[test]
    fn rejects_non_contiguous_batch() {
        let entries = vec![entry(0, 0, 0), entry(0, 1, 0), entry(0, 0, 0)];

        let error = viewport_batch_ranges(&entries).expect_err("reopened batch must fail");

        assert!(error.to_string().contains("is not contiguous"));
    }

    #[test]
    fn rejects_missing_ordinal() {
        let entries = vec![entry(0, 0, 0), entry(0, 0, 2)];

        let error = viewport_batch_ranges(&entries).expect_err("ordinal gap must fail");

        assert!(error.to_string().contains("has ordinal 2, expected 1"));
    }

    #[test]
    fn rejects_steps_that_go_backwards_for_one_user() {
        let entries = vec![entry(2, 0, 0), entry(1, 1, 0), entry(1, 0, 0)];

        let error = viewport_batch_ranges(&entries).expect_err("backward step must fail");

        assert!(
            error
                .to_string()
                .contains("does not follow previous step=2")
        );
    }

    #[test]
    fn non_timed_trace_validation_accepts_sparse_steps() {
        let entries = vec![entry(0, 0, 0), entry(u64::MAX - 1, 0, 0)];
        assert_eq!(
            viewport_batch_ranges(&entries).expect("sparse ordered steps"),
            [0..1, 1..2]
        );

        let entries = vec![entry(u64::MAX - 1, 0, 0)];
        let ranges = viewport_batch_ranges(&entries).expect("large initial step");
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], 0..1);
    }

    #[test]
    fn rejects_oversized_viewport_batches() {
        let entries: Vec<_> = (0..super::MAX_VIEWPORT_BATCH_REQUESTS + 1)
            .map(|ordinal| entry(0, 0, ordinal))
            .collect();
        let error = viewport_batch_ranges(&entries).expect_err("oversized batch must fail");
        assert!(error.to_string().contains("the maximum is"), "{error}");
    }

    #[test]
    fn rejects_oversized_trace_lines_before_reading_without_bound() {
        let limits = TraceLimits {
            line_bytes: 32,
            total_bytes: 1_024,
            entries: 10,
        };
        let input = vec![b'x'; 1_024];
        let mut cursor = Cursor::new(input);
        let error = read_trace_with_limits(&mut cursor, limits)
            .expect_err("oversized unterminated line must fail");
        assert!(error.to_string().contains("trace line 1 exceeds 32 bytes"));
        assert!(cursor.position() <= 34);
    }

    #[test]
    fn raw_trace_budget_counts_blank_lines() {
        let limits = TraceLimits {
            line_bytes: 32,
            total_bytes: 5,
            entries: 10,
        };
        let error = read_trace_with_limits(Cursor::new(b"\n\n\n\n\n\n"), limits)
            .expect_err("blank lines must consume the raw byte budget");
        assert!(error.to_string().contains("exceeds 5 raw bytes"));
    }

    #[test]
    fn exact_line_limit_accepts_lf_and_crlf_but_rejects_one_more_byte() {
        let limits = TraceLimits {
            line_bytes: 3,
            total_bytes: 32,
            entries: 10,
        };
        let whitespace = |bytes: &'static [u8]| {
            read_trace_with_limits(Cursor::new(bytes), limits)
                .expect("bounded blank line")
                .0
        };
        assert!(whitespace(b"   \n").is_empty());
        assert!(whitespace(b"   \r\n").is_empty());

        let error = read_trace_with_limits(Cursor::new(b"    \n"), limits)
            .expect_err("payload beyond the line limit must fail");
        assert!(error.to_string().contains("trace line 1 exceeds 3 bytes"));
    }
}
