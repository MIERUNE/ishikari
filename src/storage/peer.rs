//! Peer routing and internal HTTP transport.

use anyhow::Result;
use bytes::Bytes;
use reqwest::{Client, StatusCode};
use thiserror::Error;
use tracing::{debug, warn};

use crate::{
    interned::TilesetId,
    membership::{Membership, Peer},
    pmtiles::BootstrapTransfer,
};

use super::routing::{HrwRouter, ScoredPeer};

/// Peer-backed internal transport for routed resources.
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

impl PeerFetchError {
    fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable(_))
    }
}

impl PeerBackend {
    /// Creates the peer backend used for internal forwarding.
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
    pub async fn route_tileset(&self, tileset_id: &TilesetId) -> Vec<ScoredPeer> {
        let peers = self.membership.peers().await;
        self.router.route_tileset(peers, tileset_id.as_ref())
    }

    /// Returns the routed candidate peers for a tile request.
    pub async fn route_tile(&self, tileset_id: &TilesetId, tile_id: u64) -> Vec<ScoredPeer> {
        let peers = self.membership.peers().await;
        self.router.route_tile(peers, tileset_id.as_ref(), tile_id)
    }

    /// Returns whether the given peer is the local node.
    pub fn is_self(&self, peer: &Peer) -> bool {
        peer.id == self.self_node_id
    }

    /// Routes a bootstrap request across candidate peers, returning the first successful result.
    pub async fn route_bootstrap(
        &self,
        tileset_id: &TilesetId,
        include_metadata: bool,
    ) -> Result<Option<BootstrapTransfer>> {
        let path = if include_metadata {
            format!("/_internal/pmtiles/{tileset_id}/bootstrap?metadata=true")
        } else {
            format!("/_internal/pmtiles/{tileset_id}/bootstrap")
        };
        let result = self
            .route_fetch_optional(tileset_id, &path, "bootstrap")
            .await?;
        match result {
            Some(bytes) => {
                let transfer = decode_bootstrap_wire(bytes, include_metadata)?;
                Ok(Some(transfer))
            }
            None => Ok(None),
        }
    }

    /// Routes a leaf request across candidate peers, returning the first successful result.
    pub async fn route_leaf(
        &self,
        tileset_id: &TilesetId,
        offset: u64,
        length: usize,
    ) -> Result<Option<Bytes>> {
        let path = format!("/_internal/pmtiles/{tileset_id}/leaf/{offset}/{length}");
        self.route_fetch_optional(tileset_id, &path, "leaf").await
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

    /// Routes a request across tileset candidate peers, returning `None` to signal local fallback.
    async fn route_fetch_optional(
        &self,
        tileset_id: &TilesetId,
        path: &str,
        kind: &str,
    ) -> Result<Option<Bytes>> {
        let candidates = self.route_tileset(tileset_id).await;

        if candidates.is_empty()
            || candidates
                .first()
                .is_some_and(|peer| self.is_self(&peer.peer))
        {
            debug!(tileset_id = %tileset_id, kind = kind, "using local read");
            return Ok(None);
        }

        for peer in candidates {
            if self.is_self(&peer.peer) {
                debug!(
                    tileset_id = %tileset_id,
                    peer_id = %peer.peer.id,
                    kind = kind,
                    "reached local node; falling back local"
                );
                return Ok(None);
            }

            debug!(
                tileset_id = %tileset_id,
                peer_id = %peer.peer.id,
                kind = kind,
                "forwarding request to peer"
            );
            match self.fetch_internal_bytes(&peer.peer, path).await {
                Ok(bytes) => {
                    debug!(
                        tileset_id = %tileset_id,
                        peer_id = %peer.peer.id,
                        kind = kind,
                        body_len = bytes.len(),
                        "received bytes from peer"
                    );
                    return Ok(Some(bytes));
                }
                Err(PeerFetchError::NotFound) => {
                    debug!(
                        tileset_id = %tileset_id,
                        peer_id = %peer.peer.id,
                        kind = kind,
                        "peer reported missing"
                    );
                    return Ok(None);
                }
                Err(error) if error.is_retryable() => {
                    warn!(
                        tileset_id = %tileset_id,
                        peer_id = %peer.peer.id,
                        kind = kind,
                        error = %error,
                        "forward failed; trying next candidate"
                    );
                    continue;
                }
                Err(error) => {
                    warn!(
                        tileset_id = %tileset_id,
                        peer_id = %peer.peer.id,
                        kind = kind,
                        error = %error,
                        "forward failed; falling back local"
                    );
                    return Ok(None);
                }
            }
        }

        debug!(
            tileset_id = %tileset_id,
            kind = kind,
            "all forwards failed; falling back local"
        );
        Ok(None)
    }

    /// Issues an internal GET request to a peer and returns the response body.
    async fn fetch_internal_bytes(&self, peer: &Peer, path: &str) -> Result<Bytes, PeerFetchError> {
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
            return Err(PeerFetchError::Retryable(format!("peer returned {status}")));
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

/// Decodes the bootstrap wire format received from a peer.
///
/// Without metadata: raw bootstrap bytes.
/// With metadata: `[8 bytes: bootstrap_len as u64 LE][bootstrap][metadata]`.
fn decode_bootstrap_wire(body: Bytes, include_metadata: bool) -> Result<BootstrapTransfer> {
    if !include_metadata {
        return Ok(BootstrapTransfer {
            bootstrap: body,
            metadata: None,
        });
    }
    anyhow::ensure!(body.len() >= 8, "bootstrap transfer too short");
    let bootstrap_len = u64::from_le_bytes(body[..8].try_into().unwrap()) as usize;
    anyhow::ensure!(
        body.len() >= 8 + bootstrap_len,
        "bootstrap transfer truncated"
    );
    let bootstrap = body.slice(8..8 + bootstrap_len);
    let metadata = if body.len() > 8 + bootstrap_len {
        Some(body.slice(8 + bootstrap_len..))
    } else {
        None
    };
    Ok(BootstrapTransfer {
        bootstrap,
        metadata,
    })
}
