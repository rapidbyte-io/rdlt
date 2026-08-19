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

/// The memoized chain walker: each table's recorded ancestor chain — the
/// table itself first, its root last — resolved ONCE per owner and read by
/// every consumer after. No invalidation exists because none is needed:
/// parent links are append-only, and a table's own delta (link included)
/// precedes its first batch, so by the time a table is first resolved its
/// chain is complete and no later delta ever re-parents a table any resolve
/// has already WALKED as anyone's ancestor — the suffix sharing below leans
/// on that wider form, since a walked node's memoized tail is read by every
/// LATER chain that descends through it, not just the query that walked it.
/// ENFORCED, not merely relied on: the loader refuses a re-parenting delta
/// typed at its recording seat, and the scan resolves against a map frozen
/// before any resolve runs.
///
/// EVERY node a walk visits is memoized, its tail SHARED with its parent's
/// link ([`Link`] is a persistent list), and a walk stops at the first
/// memoized ancestor — so each parent edge is traversed once ever and
/// resolving all N tables of a manifest costs O(N log N) total, where
/// memoizing only the queried table left a hostile manifest of linear
/// chains quadratic (N ≈ millions of recorded deltas priced recovery in
/// hours). Memory stays O(N): one link and one root name per table.
#[derive(Default)]
pub(crate) struct Chain {
    links: BTreeMap<TableName, std::sync::Arc<Link>>,
    /// How many `parent_of` calls resolution has made — the meter the
    /// complexity pins read (a wall-clock bound would flake; this is
    /// structural).
    hops: u64,
}

/// One memoized table: its name, its chain's root, and the shared tail.
/// `Arc` rather than `Rc` because a [`Chain`] rides inside the loader,
/// which lives in a spawned task.
pub(crate) struct Link {
    table: TableName,
    root: TableName,
    parent: Option<std::sync::Arc<Link>>,
}

impl Link {
    /// The chain's last hop — the root the stream↔table joins key on.
    pub(crate) fn root(&self) -> &TableName {
        &self.root
    }

    /// The chain, table first, root last.
    pub(crate) fn iter(&self) -> Links<'_> {
        Links(Some(self))
    }
}

/// The compiler's automatic drop would recurse link-by-link down the
/// tail — a stack overflow on exactly the deep chains the memoization
/// exists for — so the tail is unlinked ITERATIVELY: take the parent
/// out first (leaving nothing for the automatic field drop to recurse
/// into), then walk down, stopping at any link another holder still
/// shares (that holder unlinks onward when it drops).
impl Drop for Link {
    fn drop(&mut self) {
        let mut next = self.parent.take();
        while let Some(parent) = next {
            match std::sync::Arc::try_unwrap(parent) {
                Ok(mut link) => next = link.parent.take(),
                Err(_shared) => break,
            }
        }
    }
}

/// Iterator over a resolved chain's table names, table first, root last.
pub(crate) struct Links<'a>(Option<&'a Link>);

impl<'a> Iterator for Links<'a> {
    type Item = &'a TableName;
    fn next(&mut self) -> Option<&'a TableName> {
        let link = self.0?;
        self.0 = link.parent.as_deref();
        Some(&link.table)
    }
}

impl Chain {
    /// `table`'s recorded chain, walked with the caller's `parent_of` on a
    /// miss until the root, a memoized ancestor (whose tail the new links
    /// then share), or the `links` hop bound. `parent_of` answers one hop
    /// — `Ok(None)` means the table IS a root, `Ok(Some)` a recorded
    /// parent, `Err` the caller's own refusal (the scan errs on a table
    /// with no recorded schema; the loader has no failing case). More
    /// than `links` hops would mean a cycle, which no shred produces —
    /// `Ok(None)` in the outer `Option` then, for the caller to name,
    /// with nothing memoized; `Err` memoizes nothing either.
    pub(crate) fn resolve<E>(
        &mut self,
        table: &TableName,
        links: usize,
        mut parent_of: impl FnMut(&TableName) -> Result<Option<TableName>, E>,
    ) -> Result<Option<&Link>, E> {
        if !self.links.contains_key(table) {
            // The fresh prefix, queried table first; `tail` is what it
            // hangs off — a memoized ancestor's link, or nothing at the
            // root.
            let mut visited: Vec<TableName> = Vec::new();
            let mut tail: Option<std::sync::Arc<Link>> = None;
            let mut terminated = false;
            let mut current = table.clone();
            for _ in 0..=links {
                if let Some(known) = self.links.get(&current) {
                    tail = Some(std::sync::Arc::clone(known));
                    terminated = true;
                    break;
                }
                self.hops += 1;
                match parent_of(&current)? {
                    None => {
                        visited.push(current);
                        terminated = true;
                        break;
                    }
                    Some(parent) => {
                        visited.push(current);
                        current = parent;
                    }
                }
            }
            if !terminated {
                return Ok(None);
            }
            for table in visited.into_iter().rev() {
                let root = match &tail {
                    Some(parent) => parent.root.clone(),
                    None => table.clone(),
                };
                let link = std::sync::Arc::new(Link {
                    table: table.clone(),
                    root,
                    parent: tail.take(),
                });
                tail = Some(std::sync::Arc::clone(&link));
                self.links.insert(table, link);
            }
        }
        Ok(self.links.get(table).map(std::sync::Arc::as_ref))
    }

    /// Total `parent_of` calls made across every resolve — the
    /// structural cost the complexity pins bound.
    #[cfg(test)]
    pub(crate) fn hops(&self) -> u64 {
        self.hops
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// THE COMPLEXITY PIN: resolving every table of one K-deep linear
    /// chain costs O(K) `parent_of` calls TOTAL, shallowest-first — the
    /// order that punishes single-table memoization hardest (each
    /// resolve would re-walk everything below it: (K+1)(K+2)/2 calls,
    /// 501,501 at K=1,000). Suffix memoization makes each edge's walk
    /// happen once ever: a resolve stops at the first memoized
    /// ancestor.
    #[test]
    fn resolving_every_table_of_a_linear_chain_costs_linear_hops() {
        const K: usize = 1_000;
        let parents: BTreeMap<TableName, TableName> = (1..=K)
            .map(|i| {
                (
                    TableName::new(format!("t{i}")),
                    TableName::new(format!("t{}", i - 1)),
                )
            })
            .collect();
        let mut chain = Chain::default();
        let mut hops = 0usize;
        for i in 0..=K {
            chain
                .resolve(&TableName::new(format!("t{i}")), K, |current| {
                    hops += 1;
                    Ok::<_, String>(parents.get(current).cloned())
                })
                .expect("no refusal")
                .expect("a linear chain terminates");
        }
        assert!(
            hops <= 2 * (K + 1),
            "the total walk stays linear in the chain, not quadratic: {hops} hops for {K} tables"
        );
    }

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
        let resolved: Vec<TableName> = chain
            .resolve(&TableName::new("a__b__c"), parents.len(), |current| {
                hops += 1;
                Ok::<_, String>(parents.get(current).cloned())
            })
            .expect("no refusal")
            .expect("terminates")
            .iter()
            .cloned()
            .collect();
        assert_eq!(
            resolved,
            vec![
                TableName::new("a__b__c"),
                TableName::new("a__b"),
                TableName::new("a")
            ]
        );
        assert_eq!(hops, 3);
        assert_eq!(
            chain
                .resolve(
                    &TableName::new("a__b__c"),
                    parents.len(),
                    |_| Ok::<_, String>(None)
                )
                .expect("no refusal")
                .expect("memoized")
                .root(),
            &TableName::new("a"),
            "the root rides the link, O(1) to read"
        );
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
        let Err(refusal) = chain.resolve(&TableName::new("orphan"), parents.len(), |current| {
            Err::<Option<TableName>, _>(format!("no schema for `{current}`"))
        }) else {
            panic!("the caller's refusal passes through");
        };
        assert_eq!(refusal, "no schema for `orphan`");
    }
}
