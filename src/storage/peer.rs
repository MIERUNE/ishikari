//! Peer routing and internal HTTP transport.

use bytes::Bytes;
use reqwest::{Client, StatusCode};
use thiserror::Error;

use crate::{
    cluster::{
        membership::{Membership, Peer},
        router::HrwRouter,
    },
    interned_str::TilesetId,
};

/// Peer-backed internal transport for routed PMTiles resources.
#[derive(Clone)]
pub(crate) struct PeerBackend {
    self_node_id: String,
    membership: Membership,
    router: HrwRouter,
    http_client: Client,
}

/// Errors returned while fetching internal resources from a peer.
#[derive(Debug, Error)]
pub(crate) enum PeerFetchError {
    #[error("peer resource not found")]
    NotFound,
    #[error("{0}")]
    Retryable(String),
    #[error("{0}")]
    Fatal(String),
}

impl PeerBackend {
    /// Creates the peer backend used for internal PMTiles forwarding.
    pub fn new(
        self_node_id: String,
        membership: Membership,
        router: HrwRouter,
        http_client: Client,
    ) -> Self {
        Self {
            self_node_id,
            membership,
            router,
            http_client,
        }
    }

    /// Returns the routed candidate peers for a tileset.
    pub async fn route_tileset(&self, tileset_id: &TilesetId) -> Vec<Peer> {
        let peers = self.membership.peers().await;
        self.router
            .route_tileset(&peers, tileset_id.as_ref(), |peer| peer.id.as_str())
            .into_iter()
            .cloned()
            .collect()
    }

    /// Returns the routed candidate peers for a tile request.
    pub async fn route_tile(&self, tileset_id: &TilesetId, tile_id: u64) -> Vec<Peer> {
        let peers = self.membership.peers().await;
        self.router
            .route_tile(&peers, tileset_id.as_ref(), tile_id, |peer| {
                peer.id.as_str()
            })
            .into_iter()
            .cloned()
            .collect()
    }

    /// Returns whether the given peer is the local node.
    pub fn is_self(&self, peer: &Peer) -> bool {
        peer.id == self.self_node_id
    }

    /// Fetches routed archive bootstrap bytes from a peer.
    pub async fn fetch_archive_index_bytes(
        &self,
        peer: &Peer,
        tileset_id: &TilesetId,
    ) -> Result<Option<Bytes>, PeerFetchError> {
        let path = format!("/_internal/pmtiles/{tileset_id}/index");
        match self.fetch_internal_bytes(peer, &path).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(PeerFetchError::NotFound) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Fetches routed metadata bytes from a peer.
    pub async fn fetch_metadata_bytes(
        &self,
        peer: &Peer,
        tileset_id: &TilesetId,
    ) -> Result<Option<Bytes>, PeerFetchError> {
        let path = format!("/_internal/pmtiles/{tileset_id}/metadata");
        match self.fetch_internal_bytes(peer, &path).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(PeerFetchError::NotFound) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Fetches routed leaf bytes from a peer.
    pub async fn fetch_leaf_bytes(
        &self,
        peer: &Peer,
        tileset_id: &TilesetId,
        offset: u64,
        length: usize,
    ) -> Result<Option<Bytes>, PeerFetchError> {
        let path = format!("/_internal/pmtiles/{tileset_id}/leaf/{offset}/{length}");
        match self.fetch_internal_bytes(peer, &path).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(PeerFetchError::NotFound) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Fetches tile bytes from a peer over the internal tile endpoint.
    pub async fn fetch_tile_bytes(
        &self,
        peer: &Peer,
        tileset_id: &TilesetId,
        tile_id: u64,
    ) -> Result<Bytes, PeerFetchError> {
        let path = format!("/_internal/tiles/{tileset_id}/{tile_id}");
        self.fetch_internal_bytes(peer, &path).await
    }

    /// Issues an internal GET request to a peer and returns the response body.
    async fn fetch_internal_bytes(
        &self,
        peer: &Peer,
        path: &str,
    ) -> Result<Bytes, PeerFetchError> {
        let url = format!("http://{}{}", peer.addr, path);
        let response = self.http_client.get(url).send().await.map_err(|error| {
            if error.is_connect() || error.is_timeout() {
                PeerFetchError::Retryable(error.to_string())
            } else {
                PeerFetchError::Fatal(error.to_string())
            }
        })?;

        let status = response.status();
        if status == StatusCode::NOT_FOUND {
            return Err(PeerFetchError::NotFound);
        }
        if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
            return Err(PeerFetchError::Retryable(format!(
                "peer returned {status}"
            )));
        }
        if !status.is_success() {
            return Err(PeerFetchError::Fatal(format!(
                "peer returned unexpected status {status}"
            )));
        }

        response
            .bytes()
            .await
            .map_err(|error| PeerFetchError::Fatal(error.to_string()))
    }
}
