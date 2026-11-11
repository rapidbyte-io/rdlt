//! How hard a source presses on the engine's memory budget, as a world draws it.

use crate::rng::SplitMix64;

/// How hard a source presses on the engine's memory budget: the bytes each Arrow batch it pushes
/// keeps alive beside its rows, the bytes each cursor carries beside its offset, and the spaces a
/// JSON push carries beside its records in a world of fat values.
///
/// Each is drawn against the limits the engine admits within, so a push or a cursor is never
/// refused, and several of them fill their share of the budget: pushes then wait for room as
/// lowering releases it, and cursors for a commit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Pressure {
    /// Bytes: the allocation each Arrow batch keeps alive, of which its rows take the first.
    pub(crate) ballast: usize,
    /// Bytes: what each cursor carries beside its offset.
    pub(crate) pad: usize,
    /// Bytes: the spaces a JSON push carries beside its records where it holds a spaced row,
    /// in a world of fat values.
    pub(crate) spaces: usize,
}

impl Pressure {
    /// The pressure drawn from `rng` for an engine admitting within `limits`, of a budget whose
    /// cursors may take `cursors` bytes, reading `partitions` in all: in three worlds of four a
    /// push of a fifth to nine tenths of a frame, and cursors of half to all a cursor may be, the
    /// cursors of every partition together within a quarter of the state an open may answer.
    pub(crate) fn draw(
        rng: &mut SplitMix64,
        limits: &rdlt_wire::Limits,
        cursors: u64,
        partitions: u64,
    ) -> Self {
        let within = |limit: u64, low: u64, high: u64, chance: u64, rng: &mut SplitMix64| {
            let low = limit / low;
            let bytes = rng
                .chance(chance)
                .then(|| low + rng.below((limit / high).saturating_sub(low).max(1)));
            usize::try_from(bytes.unwrap_or(0)).unwrap_or(0)
        };
        let frame = limits.frame_bytes.saturating_mul(9) / 10;
        let state = limits.state_bytes / 4 / partitions.saturating_add(1);
        Self {
            ballast: within(frame, 5, 1, 750, rng),
            // A cursor carries its offset and its field names beside its padding.
            pad: within(
                cursors
                    .min(limits.cursor_bytes)
                    .min(state)
                    .saturating_sub(64),
                2,
                1,
                750,
                rng,
            ),
            spaces: 0,
        }
    }
}
