//! HRW-based placement of tile groups onto cluster members.

use std::{cmp::Ordering, collections::BinaryHeap, hash::Hasher};

use twox_hash::XxHash64;

use crate::membership::Peer;

/// HRW placement over cluster peers for a given tileset and tile-locality group.
#[derive(Clone)]
pub struct HrwRouter {
    candidate_count: usize,
    tile_group_size: u64,
}

#[derive(Eq, PartialEq)]
pub struct ScoredPeer {
    pub score: u64,
    pub peer: Peer,
}

impl Ord for ScoredPeer {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .score
            .cmp(&self.score)
            .then_with(|| self.peer.id.cmp(&other.peer.id))
    }
}

impl PartialOrd for ScoredPeer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl HrwRouter {
    /// Creates a router with the given candidate count and tile-group size.
    pub fn new(candidate_count: usize, tile_group_size: u64) -> Self {
        Self {
            candidate_count: candidate_count.max(1),
            tile_group_size: tile_group_size.max(1),
        }
    }

    /// Returns candidate peers for the tile-locality group.
    pub fn route_tile(&self, peers: Vec<Peer>, tileset_id: &str, tile_id: u64) -> Vec<ScoredPeer> {
        let tile_group_id = tile_id / self.tile_group_size;
        let mut top_peers =
            BinaryHeap::with_capacity(self.candidate_count.saturating_add(1).min(peers.len()));

        for peer in peers {
            let candidate = ScoredPeer {
                score: hrw_weight(tileset_id, tile_group_id, &peer.id),
                peer,
            };
            top_peers.push(candidate);
            if top_peers.len() > self.candidate_count {
                top_peers.pop();
            }
        }

        let routed = top_peers.into_sorted_vec();
        debug_assert!(routed.len() <= self.candidate_count);
        debug_assert!(routed.windows(2).all(|pair| pair[0].score >= pair[1].score));
        routed
    }

    /// Returns candidate peers for tileset-wide metadata endpoints.
    pub fn route_tileset(&self, peers: Vec<Peer>, tileset_id: &str) -> Vec<ScoredPeer> {
        self.route_tile(peers, tileset_id, 0)
    }
}

/// Computes the rendezvous-hash score for a node and tile-locality group.
fn hrw_weight(tileset_id: &str, tile_group_id: u64, node_id: &str) -> u64 {
    let mut hasher = XxHash64::default();
    hasher.write(tileset_id.as_bytes());
    hasher.write_u8(0xff);
    hasher.write_u64(tile_group_id);
    hasher.write_u8(0xfe);
    hasher.write(node_id.as_bytes());
    hasher.finish()
}
