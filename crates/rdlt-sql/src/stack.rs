//! Stack for the work a query drives.
//!
//! The work on its text runs on a segment of its own, sized for the deepest tree text of its
//! length can build. Planning, and the walk that bounds what a query makes, see only a query
//! admitted flat in its types and at most [`SQL_DEPTH`](crate::SQL_DEPTH) deep in its
//! expressions, and grow the thread's stack as they recurse, deeper than a thread's stack holds
//! in unoptimized builds at the depth limit.

#[cfg(test)]
mod tests;

/// Stack the work on a query's text may take for each byte of it, 4 KiB: parsing it, measuring
/// it, checking it, and dropping what is refused.
///
/// The deepest trees text builds are nested types, which sqlparser parses a frame a level,
/// outside its recursion limit, and drops alike, so the stack they take grows with the text.
/// Of every type the dialect nests, `TABLE(a ` takes the most: at 1 KiB, 8 KiB and 64 KiB of
/// text, 647 KiB, 4.8 MiB and 38.0 MiB built as the dev profile builds, sqlparser optimised,
/// and 751 KiB, 5.6 MiB and 44.5 MiB built for coverage, 0.73 KiB a byte at most; 64 KiB takes
/// 36.1 MiB as the release profile builds. `Nested(a ` takes 655 KiB, 4.8 MiB and 38.5 MiB for
/// coverage, `Tuple(` 431 KiB, 3.0 MiB and 23.5 MiB, and each other at most 13.3 MiB at 64 KiB
/// on the dev profile. Four KiB a byte is more than five times the most any takes at each
/// length; only the pages the work touches are used.
const PER_BYTE: usize = 4 << 10;

/// Stack the work on the shortest text is given, 1 MiB.
const LEAST: usize = 1 << 20;

/// Stack the work on the longest text is given, 256 MiB: [`PER_BYTE`] for each of
/// [`SQL_BYTES`](crate::SQL_BYTES).
const MOST: usize = 256 << 20;

const _: () = assert!(MOST == crate::SQL_BYTES * PER_BYTE);

/// Stack planning a query is sure of before it starts, 4 MiB.
const STACK: usize = 4 << 20;

/// Stack added where planning must grow it, 8 MiB.
const SEGMENT: usize = 8 << 20;

/// Stack a level of a walk over a query may use before it grows the stack, 128 KiB.
const RED_ZONE: usize = 128 << 10;

/// Stack a level adds where it grows it, 1 MiB.
const LEVEL: usize = 1 << 20;

/// Runs `work`, on a query's text of `bytes`, on a stack of its own, [`PER_BYTE`] for each of
/// them, at least [`LEAST`] and at most [`MOST`].
pub(crate) fn text<T>(bytes: usize, work: impl FnOnce() -> T) -> T {
    stacker::grow(segment(bytes), work)
}

/// The stack work on a text of `bytes` runs on.
fn segment(bytes: usize) -> usize {
    bytes.saturating_mul(PER_BYTE).clamp(LEAST, MOST)
}

/// Runs `work`, planning a query, with at least [`STACK`] of stack.
pub(crate) fn grown<T>(work: impl FnOnce() -> T) -> T {
    stacker::maybe_grow(STACK, SEGMENT, work)
}

/// Runs `level`, one level of a walk over a query's expressions, on more stack when little is
/// left.
pub(crate) fn nest<T>(level: impl FnOnce() -> T) -> T {
    stacker::maybe_grow(RED_ZONE, LEVEL, level)
}
