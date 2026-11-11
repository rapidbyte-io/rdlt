//! What a simulated read serves next, and where it checkpoints, as its stream's draw and the
//! operator leave its source.

use rdlt_connector::{Partition, ReadMode};

use super::SimCursor;
use super::plans::PlannedRead;
use crate::workload::SimStream;
use crate::world::World;

/// What a read serves next: as which read, up to which of its partition's rows, and where it
/// checkpoints.
#[derive(Clone, Copy)]
pub(super) struct Serving<'a> {
    pub(super) reading: &'a PlannedRead<'a>,
    pub(super) available: usize,
    pub(super) marks: Marks,
}

/// Where a read checkpoints.
#[derive(Clone, Copy, Debug)]
pub(super) struct Marks {
    /// Whether after its last batch.
    pub(super) last: bool,
    /// Whether anywhere at all.
    pub(super) any: bool,
}

impl Marks {
    /// Where a read of `partition` of `stream`, the world's stream `index`, checkpoints: after its
    /// last batch where the stream does or its source was mended to; and nowhere where it does not,
    /// in an incremental read of a partition that never ends.
    pub(super) fn of(
        stream: &SimStream,
        world: &World,
        index: usize,
        partition: &Partition,
    ) -> Self {
        let last = stream.final_checkpoint || world.mended(index);
        let incremental = stream.reads[world.phase()] == ReadMode::Incremental;
        Self {
            last,
            any: last || !(incremental && partition.is_unbounded()),
        }
    }
}

/// The cursor at `next` of a source pressing as `world` says.
pub(super) fn at(world: &World, next: usize) -> SimCursor {
    SimCursor {
        next: next as u64,
        pad: "p".repeat(world.pressure().pad),
    }
}
