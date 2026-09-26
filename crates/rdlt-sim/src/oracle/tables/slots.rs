//! The rows a table holds, as slots: any of a slot's rows, as often as the slot says.

use std::collections::BTreeMap;

use super::super::expected::{self, Expected};
use super::super::rows::Group;

/// Rows a table holds `count` times in all, any of `rows` each time.
pub(super) struct Slot<'a> {
    pub(super) rows: Vec<&'a Expected>,
    pub(super) count: usize,
    /// Whether the table may hold the slot's rows fewer times than `count`.
    pub(super) at_most: bool,
}

impl<'a> Slot<'a> {
    /// A slot for each of `groups`, holding the rows of `modeled` each group's rows are.
    pub(super) fn of(groups: &[Group], modeled: &'a [Expected]) -> Vec<Self> {
        let by_ident: BTreeMap<&str, &Expected> = modeled
            .iter()
            .map(|row| (row.ident.as_str(), row))
            .collect();
        groups
            .iter()
            .map(|group| Slot {
                rows: group
                    .rows
                    .iter()
                    .filter_map(|row| by_ident.get(expected::ident(row).as_str()).copied())
                    .collect(),
                count: group.count,
                at_most: group.at_most,
            })
            .collect()
    }

    /// How the table's rows, `held` so often by identity, miscount the slot, if they do: more
    /// often than it says, or, unless it or the whole table is `at_most`, less often.
    pub(super) fn miscounted(
        &self,
        held: &BTreeMap<String, usize>,
        at_most: bool,
    ) -> Option<String> {
        let at_most = at_most || self.at_most;
        let idents: Vec<&str> = self.rows.iter().map(|row| row.ident.as_str()).collect();
        let found: usize = idents
            .iter()
            .map(|ident| held.get(*ident).copied().unwrap_or(0))
            .sum();
        (found > self.count || (found < self.count && !at_most))
            .then(|| format!("rows {idents:?} are held {found} times, not {}", self.count))
    }

    /// A slot for each of `rows`, held as often as it appears.
    pub(super) fn each(rows: &'a [Expected]) -> Vec<Self> {
        let mut counts: BTreeMap<&str, (&Expected, usize)> = BTreeMap::new();
        for row in rows {
            counts.entry(&row.ident).or_insert((row, 0)).1 += 1;
        }
        counts
            .into_values()
            .map(|(row, count)| Slot {
                rows: vec![row],
                count,
                at_most: false,
            })
            .collect()
    }
}
