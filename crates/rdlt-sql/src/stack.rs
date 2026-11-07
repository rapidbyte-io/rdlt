//! Stack for the work on a query's text, which runs on a segment of its own, sized for the
//! deepest tree text of [`SQL_BYTES`](crate::SQL_BYTES) can build.

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

/// Runs `work`, on a query's text, on a stack of [`TEXT`] of its own.
pub(crate) fn text<T>(work: impl FnOnce() -> T) -> T {
    stacker::grow(TEXT, work)
}
