//! What a simulated read serves next, where it checkpoints, as its stream's draw and the
//! operator leave its source, and what it ended on.

use std::ops::Range;

use rdlt_connector::{Checkpointing, Partition, ReadMode};

use super::SimCursor;
use super::plans::PlannedRead;
use crate::workload::{End, SimStream};
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
    /// last batch where the stream does or its source was mended to; and where it does not,
    /// nowhere in an incremental read of a partition that never ends of a stream that checkpoints
    /// on demand.
    pub(super) fn of(
        stream: &SimStream,
        world: &World,
        index: usize,
        partition: &Partition,
    ) -> Self {
        let last = stream.final_checkpoint || world.mended(index);
        let incremental = stream.reads[world.phase()] == ReadMode::Incremental;
        let on_demand = stream.checkpointing == Checkpointing::OnDemand;
        Self {
            last,
            any: last || !(incremental && partition.is_unbounded() && on_demand),
        }
    }

    /// Whether a read may checkpoint before the row at `next` of its partition's `held` rows:
    /// after its last only where it checkpoints there.
    pub(super) fn at(self, next: usize, held: usize) -> bool {
        self.any && (self.last || next < held)
    }
}

/// The cursor at `next` of a source pressing as `world` says.
pub(super) fn at(world: &World, next: usize) -> SimCursor {
    SimCursor {
        next: next as u64,
        pad: "p".repeat(world.pressure().pad),
    }
}

/// Notes, in `world`, what the read `reading` of stream `stream` ended on, where it sent the rows
/// at the offsets `sent`: a checkpoint after them, rows after its last checkpoint, or no
/// checkpoint at all.
pub(super) fn ended(world: &World, stream: usize, reading: &PlannedRead<'_>, sent: Range<usize>) {
    if sent.is_empty() {
        return;
    }
    let end = match reading.checkpointed() {
        Some(at) if at == sent.end as u64 => End::Checkpoint,
        Some(_) => End::Tail,
        None => End::Unmarked,
    };
    world.ended(stream, end);
}
