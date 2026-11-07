//! Stack for the work a query drives.
//!
//! The work on its text runs on a segment of its own, sized for the deepest tree text of
//! [`SQL_BYTES`](crate::SQL_BYTES) can build. Planning, and the walk that bounds what a query
//! makes, see only a query admitted flat in its types and at most
//! [`SQL_DEPTH`](crate::SQL_DEPTH) deep in its expressions, and grow the thread's stack as they
//! recurse, deeper than a thread's stack holds in unoptimized builds at the depth limit.

/// Stack the work on a query's text runs on, 256 MiB of its own: parsing it, measuring it,
/// checking it, and dropping what is refused.
///
/// The deepest trees 64 KiB of text builds are nested types, which sqlparser parses a frame a
/// level, outside its recursion limit, and drops alike. Of every type the dialect nests, 64 KiB
/// of `TABLE(a ` takes the most: 38.1 MiB built as the dev profile builds, sqlparser optimised,
/// 36.1 MiB as the release profile does, and 45.1 MiB built for coverage. `Nested(a ` takes
/// 33.6, 31.6 and 39.1 MiB, `Tuple(` 15.9, 13.9 and 23.6 MiB, and each other at most 13.4 MiB
/// on the dev profile. 256 MiB leaves more than five times the most any takes; only the pages
/// the work touches are used.
const TEXT: usize = 256 << 20;

/// Stack planning a query is sure of before it starts, 4 MiB.
const STACK: usize = 4 << 20;

/// Stack added where planning must grow it, 8 MiB.
const SEGMENT: usize = 8 << 20;

/// Stack a level of a walk over a query may use before it grows the stack, 128 KiB.
const RED_ZONE: usize = 128 << 10;

/// Stack a level adds where it grows it, 1 MiB.
const LEVEL: usize = 1 << 20;

/// Runs `work`, on a query's text, on a stack of [`TEXT`] of its own.
pub(crate) fn text<T>(work: impl FnOnce() -> T) -> T {
    stacker::grow(TEXT, work)
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
