//! HRW-based placement of tile groups onto cluster members.

use std::hash::Hasher;

use twox_hash::XxHash64;

/// HRW placement over cluster peers for a given tileset and chunk-locality group.
#[derive(Clone)]
pub struct HrwRouter {
    candidate_count: usize,
    tile_group_size: u64,
}

impl HrwRouter {
    /// Creates a router with the given candidate count and tile-group size.
    pub fn new(candidate_count: usize, tile_group_size: u64) -> Self {
        Self {
            candidate_count: candidate_count.max(1),
            tile_group_size: tile_group_size.max(1),
        }
    }

    /// Returns candidate peers for the chunk-locality group derived from a tile request.
    pub fn route_tile<'a, T>(
        &self,
        nodes: &'a [T],
        tileset_id: &str,
        tile_id: u64,
        node_id: impl Fn(&'a T) -> &'a str,
    ) -> Vec<&'a T> {
        let tile_group_id = tile_id / self.tile_group_size;
        let mut weighted = nodes
            .iter()
            .map(|node| {
                let id = node_id(node);
                let weight = hrw_weight(tileset_id, tile_group_id, id);
                (weight, id, node)
            })
            .collect::<Vec<_>>();

        weighted.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(right.1)));

        weighted
            .into_iter()
            .take(self.candidate_count)
            .map(|(_, _, node)| node)
            .collect()
    }

    /// Returns candidate peers for tileset-wide metadata endpoints.
    pub fn route_tileset<'a, T>(
        &self,
        nodes: &'a [T],
        tileset_id: &str,
        node_id: impl Fn(&'a T) -> &'a str,
    ) -> Vec<&'a T> {
        self.route_tile(nodes, tileset_id, 0, node_id)
    }
}

/// Computes the rendezvous-hash score for a node and chunk-locality group.
fn hrw_weight(tileset_id: &str, tile_group_id: u64, node_id: &str) -> u64 {
    let mut hasher = XxHash64::default();
    hasher.write(tileset_id.as_bytes());
    hasher.write_u8(0xff);
    hasher.write_u64(tile_group_id);
    hasher.write_u8(0xfe);
    hasher.write(node_id.as_bytes());
    hasher.finish()
}
