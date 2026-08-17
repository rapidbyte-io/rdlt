//! The ONE statement of the T7E coverage rule's attribution mappings.
//!
//! The rule has two halves that must agree forever: the loader's commit
//! gate (which tables' rows does this stream's checkpoint cover?) and
//! recovery's replay filter (which recorded segments does this
//! checkpoint cover?). Both reduce every question to the same two
//! mappings — a stream owns the root table its name normalizes to, and
//! a written table resolves to its root along recorded parent links —
//! so both call HERE. Split implementations drifted apart would let the
//! writer commit under one coverage rule and the scan replay under
//! another: segments silently dropped (rows lost until re-extraction)
//! or recovery refused outright.

use rdlt_core::id::{StreamName, TableName};
use rdlt_core::schema::IdentRules;

use crate::naming;

/// The destination root table a stream owns: `normalize_ident` under the
/// destination's rules. `runtime::validate` proves this mapping
/// injective across a run's streams before any session opens; the run
/// wiring, the loader's checkpoint arm, and the scan's stream↔root join
/// all build on exactly this call.
pub(crate) fn root_table(stream: &StreamName, rules: IdentRules) -> TableName {
    TableName::new(naming::normalize_ident(stream.as_str(), rules))
}

/// Walk recorded parent links from `start` to its root. `parent_of`
/// answers one hop — `Ok(None)` means the table IS a root, `Ok(Some)` a
/// recorded parent, `Err` the caller's own refusal (the scan errs on a
/// table with no recorded schema; the loader has no failing case).
/// Bounded by `links` hops: more would mean a cycle, which no shred
/// produces — `Ok(None)` in the outer `Option` then, for the caller to
/// name.
pub(crate) fn walk_to_root<E>(
    start: &TableName,
    links: usize,
    mut parent_of: impl FnMut(&TableName) -> Result<Option<TableName>, E>,
) -> Result<Option<TableName>, E> {
    let mut current = start.clone();
    for _ in 0..=links {
        match parent_of(&current)? {
            None => return Ok(Some(current)),
            Some(parent) => current = parent,
        }
    }
    Ok(None)
}
