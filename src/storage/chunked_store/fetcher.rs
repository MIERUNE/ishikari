//! Object-store fetch implementation for chunked reads.

use std::{
    ops::Range,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use object_store::{
    Error as ObjectStoreError, ObjectStore, ObjectStoreExt, parse_url_opts,
    path::Path as ObjectPath,
};
use thiserror::Error;
use tracing::debug;
use url::Url;

use crate::interned::TilesetId;

const BACKEND_FETCH_TIMEOUT: Duration = Duration::from_secs(10);

/// Errors produced while fetching raw backend chunks.
#[derive(Clone, Debug, Error)]
pub enum ChunkFetchError {
    #[error("object not found")]
    NotFound,
    #[error("{0}")]
    Message(String),
}

#[derive(Clone)]
pub struct ChunkFetcher {
    object_store: Arc<dyn ObjectStore>,
    base_path: ObjectPath,
    chunk_size: u64,
    debug_fetch_delay: Duration,
    received_bytes: Arc<AtomicU64>,
}

impl ChunkFetcher {
    pub fn new(data_url: String, chunk_size: u64, debug_fetch_delay_ms: u64) -> Result<Self> {
        let url = normalize_data_url(&data_url)?;
        let (object_store, base_path) = parse_url_opts(&url, std::env::vars())
            .with_context(|| format!("failed to parse object store URL {url}"))?;
        let object_store: Arc<dyn ObjectStore> = object_store.into();

        Ok(Self {
            object_store,
            base_path,
            chunk_size,
            debug_fetch_delay: Duration::from_millis(debug_fetch_delay_ms),
            received_bytes: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn chunk_size(&self) -> u64 {
        self.chunk_size
    }

    pub fn received_bytes(&self) -> u64 {
        self.received_bytes.load(Ordering::Relaxed)
    }

    pub async fn fetch_chunk_group(
        &self,
        tileset_id: &TilesetId,
        chunk_range: Range<u64>,
        archive_len: u64,
    ) -> std::result::Result<Bytes, ChunkFetchError> {
        if chunk_range.start >= chunk_range.end {
            return Ok(Bytes::new());
        }

        let path = self.object_path(tileset_id);
        let fetch_started_at = std::time::Instant::now();
        let start_chunk = chunk_range.start;
        let end_chunk = chunk_range.end;
        let range_start = start_chunk * self.chunk_size;
        let range_end = (end_chunk * self.chunk_size).min(archive_len);
        debug!(
            tileset_id = %tileset_id,
            start_chunk = start_chunk,
            end_chunk = end_chunk,
            prefetched_chunks = end_chunk - start_chunk,
            prefetched_bytes = range_end - range_start,
            "fetching backend chunks"
        );

        if !self.debug_fetch_delay.is_zero() {
            tokio::time::sleep(self.debug_fetch_delay).await;
        }

        let bytes = tokio::time::timeout(
            BACKEND_FETCH_TIMEOUT,
            self.object_store.get_range(&path, range_start..range_end),
        )
        .await
        .map_err(|error| {
            ChunkFetchError::Message(format!(
                "timed out fetching chunk range from object store: path={path} range={range_start}..{range_end}: {error}"
            ))
        })?
        .map_err(ChunkFetchError::from)?;
        let expected_len = (range_end - range_start) as usize;
        if bytes.len() != expected_len {
            return Err(ChunkFetchError::Message(format!(
                "short range read from object store: path={path} range={range_start}..{range_end} expected_bytes={expected_len} actual_bytes={}",
                bytes.len()
            )));
        }
        self.received_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        debug!(
            tileset_id = %tileset_id,
            start_chunk = start_chunk,
            end_chunk = end_chunk - 1,
            backend_fetched_bytes = bytes.len(),
            duration_ms = fetch_started_at.elapsed().as_millis() as u64,
            "fetched backend chunk bytes"
        );

        Ok(bytes)
    }

    fn object_path(&self, tileset_id: &TilesetId) -> ObjectPath {
        self.base_path.clone().join(format!("{tileset_id}.pmtiles"))
    }
}

impl From<ObjectStoreError> for ChunkFetchError {
    fn from(error: ObjectStoreError) -> Self {
        if matches!(error, ObjectStoreError::NotFound { .. }) {
            return Self::NotFound;
        }
        Self::Message(format!(
            "failed to fetch chunk range from object store: {error}"
        ))
    }
}

fn normalize_data_url(data_url: &str) -> Result<Url> {
    if let Ok(url) = Url::parse(data_url) {
        return Ok(url);
    }

    let path = std::fs::canonicalize(PathBuf::from(data_url))
        .with_context(|| format!("failed to resolve local data path {data_url}"))?;
    Url::from_directory_path(path)
        .map_err(|_| anyhow!("failed to convert local path to file:// URL"))
}
