//! Lineage: the ONE statement of how tables attribute to streams. The
//! loader's commit gate (which tables' rows does this stream's checkpoint
//! cover?) and recovery's replay filter (which recorded segments does this
//! checkpoint cover?) must agree forever, and both reduce to the same two
//! mappings — a stream owns the root table its name normalizes to, and a
//! written table resolves to its root along recorded parent links — so both
//! call HERE. Split implementations drifted apart would let the writer commit
//! under one coverage rule and the scan replay under another: segments
//! silently dropped (rows lost until re-extraction) or recovery refused
//! outright.

use std::collections::BTreeMap;

use rdlt_core::id::{StreamName, TableName};
use rdlt_core::schema::IdentRules;

use crate::naming;

/// The destination root table a stream owns: `normalize_ident` under the
/// destination's rules. Stream validation proves this mapping injective
/// across a run's streams before any session opens; the run wiring, the
/// loader's checkpoint arm, and the scan's stream↔root join all build on
/// exactly this call.
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

/// The memoized chain walker: each table's recorded ancestor chain — the
/// table itself first, its root last — resolved ONCE per owner and read by
/// every consumer after. No invalidation exists because none is needed:
/// parent links are append-only, and a table's own delta (link included)
/// precedes its first batch, so by the time a table is first resolved its
/// chain is complete and no later delta ever re-parents a table that already
/// wrote.
#[derive(Default)]
pub(crate) struct Chain {
    chains: BTreeMap<TableName, Vec<TableName>>,
}

impl Chain {
    /// `table`'s recorded chain, resolved through [`walk_to_root`] on a
    /// miss with the caller's `parent_of` and `links` bound. `Ok(None)` is
    /// the walk's own verdict for a chain that does not terminate — left
    /// unmemoized, for the caller to name; `Err` is `parent_of`'s refusal.
    pub(crate) fn resolve<E>(
        &mut self,
        table: &TableName,
        links: usize,
        mut parent_of: impl FnMut(&TableName) -> Result<Option<TableName>, E>,
    ) -> Result<Option<&[TableName]>, E> {
        if !self.chains.contains_key(table) {
            let mut path: Vec<TableName> = Vec::new();
            let root = walk_to_root(table, links, |current| {
                path.push(current.clone());
                parent_of(current)
            })?;
            if root.is_none() {
                return Ok(None);
            }
            self.chains.insert(table.clone(), path);
        }
        Ok(self.chains.get(table).map(Vec::as_slice))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The walker memoizes a resolved chain (table first, root last), asks
    /// `parent_of` nothing on a hit, leaves a non-terminating chain
    /// unmemoized, and passes the caller's refusal through.
    #[test]
    fn the_chain_memoizes_terminating_walks_only() {
        let parents: BTreeMap<TableName, TableName> = [
            (TableName::new("a__b__c"), TableName::new("a__b")),
            (TableName::new("a__b"), TableName::new("a")),
            (TableName::new("loop"), TableName::new("loop")),
        ]
        .into_iter()
        .collect();
        let mut chain = Chain::default();
        let mut hops = 0usize;
        let resolved = chain
            .resolve(&TableName::new("a__b__c"), parents.len(), |current| {
                hops += 1;
                Ok::<_, String>(parents.get(current).cloned())
            })
            .expect("no refusal")
            .expect("terminates")
            .to_vec();
        assert_eq!(
            resolved,
            vec![
                TableName::new("a__b__c"),
                TableName::new("a__b"),
                TableName::new("a")
            ]
        );
        assert_eq!(hops, 3);
        chain
            .resolve(&TableName::new("a__b__c"), parents.len(), |_| {
                hops += 1;
                Ok::<_, String>(None)
            })
            .expect("no refusal")
            .expect("memoized");
        assert_eq!(hops, 3, "a hit asks parent_of nothing");

        assert!(
            chain
                .resolve(&TableName::new("loop"), parents.len(), |current| {
                    Ok::<_, String>(parents.get(current).cloned())
                })
                .expect("no refusal")
                .is_none(),
            "a cycle is the walk's None"
        );
        assert_eq!(
            chain.resolve(&TableName::new("orphan"), parents.len(), |current| {
                Err(format!("no schema for `{current}`"))
            }),
            Err("no schema for `orphan`".to_owned())
        );
    }
}
