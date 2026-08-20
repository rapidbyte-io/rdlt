//! The keyed-vector slot lookup shared by the shred's per-key seats
//! (column observation, struct-field observation, the child-table
//! memo). Entries stay a `Vec` in first-seen order — order is part of
//! schema identity — and a key→slot map rides BESIDE the vector, built
//! lazily the moment the set outgrows a small linear prelude: the
//! arena parser's own duplicate-scan shape, shared. Small sets stay
//! linear (a handful of compares beats hashing); past the prelude a
//! lookup is O(1), where a bare linear find priced a wide table's push
//! at O(keys × entries) string compares of pure CPU inside
//! `spawn_blocking` — uninterruptible for minutes on one legal
//! byte-budgeted batch.

/// How many entries stay linear before the map is built — the same
/// crossover the arena parser's duplicate scan uses.
const LINEAR_PRELUDE: usize = 16;

/// A lazy key→slot index over a caller-owned `Vec<(String, T)>`. The
/// caller keeps the vector; this keeps the map coherent with it through
/// the three mutations the seats perform: lookup, append, and the cold
/// rebuild after a rollback removes entries.
#[derive(Debug, Clone, Default)]
pub(crate) struct SlotIndex {
    /// key → entry position; `None` while the set is inside the prelude.
    /// Held INLINE rather than behind a box. Boxing shrinks
    /// `ColumnState` — cloned per push for the rollback snapshot — and
    /// was tried and measured: worth about a tenth of a percent of the
    /// nested-shred instruction count, in the good direction. It is not
    /// taken because the workspace's lint refuses a boxed map as an
    /// extra indirection, and a tenth of a percent does not buy an
    /// exemption. Recorded so the next reader knows the trade was
    /// measured rather than assumed.
    index: Option<std::collections::HashMap<String, usize>>,
    /// Structural cost meter: key comparisons made by linear scans plus
    /// one per map lookup — what the complexity pins bound (wall-clock
    /// would flake). Test-only so the hot path carries no accounting.
    #[cfg(test)]
    probes: u64,
}

impl SlotIndex {
    /// The slot holding `key`, if any. `entries` must be the vector this
    /// index has seen every append of.
    pub(crate) fn slot_of<T>(&mut self, entries: &[(String, T)], key: &str) -> Option<usize> {
        if let Some(index) = &self.index {
            let found = index.get(key).copied();
            self.probe(1);
            return found;
        }
        let mut scanned = 0;
        let found = entries.iter().position(|(k, _)| {
            scanned += 1;
            k == key
        });
        self.probe(scanned);
        found
    }

    /// Note an append (the caller just pushed onto `entries`): extends a
    /// live index, or builds one the moment the set outgrows the prelude.
    pub(crate) fn grew<T>(&mut self, entries: &[(String, T)]) {
        match &mut self.index {
            Some(index) => {
                let last = entries.len() - 1;
                index.insert(entries[last].0.clone(), last);
            }
            None if entries.len() > LINEAR_PRELUDE => {
                self.index = Some(
                    entries
                        .iter()
                        .enumerate()
                        .map(|(slot, (key, _))| (key.clone(), slot))
                        .collect(),
                );
            }
            None => {}
        }
    }

    /// Re-derive after the caller removed entries (the policy-rollback
    /// path): removal shifts every later slot, so a live index is rebuilt
    /// wholesale. Cold path — rollbacks run only under Discard* policy
    /// enforcement.
    pub(crate) fn rebuilt<T>(&mut self, entries: &[(String, T)]) {
        if self.index.is_some() {
            self.index = Some(
                entries
                    .iter()
                    .enumerate()
                    .map(|(slot, (key, _))| (key.clone(), slot))
                    .collect(),
            );
        }
    }

    #[cfg(test)]
    fn probe(&mut self, comparisons: usize) {
        self.probes += comparisons as u64;
    }

    #[cfg(not(test))]
    fn probe(&mut self, _comparisons: usize) {}

    /// Total comparisons this index's lookups have cost — the meter the
    /// complexity pins read.
    #[cfg(test)]
    pub(crate) fn probes(&self) -> u64 {
        self.probes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The index stays coherent through the three mutations: appends
    /// inside the prelude stay linear, the map arrives when the set
    /// outgrows it, and a rebuild after removal re-points every slot.
    #[test]
    fn slots_survive_append_growth_and_rebuild() {
        let mut entries: Vec<(String, u32)> = Vec::new();
        let mut slots = SlotIndex::default();
        for n in 0..40u32 {
            entries.push((format!("k{n}"), n));
            slots.grew(&entries);
        }
        assert_eq!(slots.slot_of(&entries, "k39"), Some(39));
        assert_eq!(slots.slot_of(&entries, "missing"), None);
        entries.retain(|(_, n)| n % 2 == 0);
        slots.rebuilt(&entries);
        assert_eq!(slots.slot_of(&entries, "k38"), Some(19));
        assert_eq!(slots.slot_of(&entries, "k39"), None);
    }

    /// THE COMPLEXITY PIN for the shared shape: W lookups against W
    /// entries cost O(W) comparisons total once the index is live — a
    /// bare linear find costs W²/2 and this meter is what keeps the
    /// map load-bearing rather than decorative.
    #[test]
    fn wide_lookups_cost_linear_not_quadratic_probes() {
        const W: usize = 512;
        let mut entries: Vec<(String, usize)> = Vec::new();
        let mut slots = SlotIndex::default();
        for n in 0..W {
            entries.push((format!("k{n}"), n));
            slots.grew(&entries);
        }
        for n in 0..W {
            assert_eq!(slots.slot_of(&entries, &format!("k{n}")), Some(n));
        }
        let probes = slots.probes();
        assert!(
            probes <= (W as u64) + (LINEAR_PRELUDE as u64).pow(2),
            "W lookups stay linear in W, not quadratic: {probes} probes for {W} lookups"
        );
    }
}
