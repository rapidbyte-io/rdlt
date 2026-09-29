//! What a change stream's table remembers of the rows it removed outright, so that a change
//! sequenced before their removal, arriving again later, never brings them back.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

/// A table's tombstones: each key a hard delete removed, with its sequence, and the sequence of
/// the latest hard truncate, before which every row is gone; sequences as the texts they compare
/// by.
#[derive(Clone, Debug, Default)]
pub(crate) struct Tombstones {
    by_key: BTreeMap<String, String>,
    bound: Option<String>,
}

impl Tombstones {
    /// Whether a change of `key`, none for a truncate, sequenced at `seq` may apply: it is not
    /// sequenced before the bound, nor at or before its key's tombstone.
    pub(crate) fn admits(&self, key: Option<&str>, seq: &str) -> bool {
        let bounded = self.bound.as_deref().is_some_and(|bound| seq < bound);
        let buried = key
            .and_then(|key| self.by_key.get(key))
            .is_some_and(|stone| stone.as_str() >= seq);
        !bounded && !buried
    }

    /// Records that a hard delete at `seq` removed `key`.
    pub(crate) fn bury(&mut self, key: String, seq: String) {
        self.by_key.insert(key, seq);
    }

    /// Forgets `key`'s tombstone: a row sequenced after it holds the key now.
    pub(crate) fn lift(&mut self, key: &str) {
        self.by_key.remove(key);
    }

    /// Records a hard truncate at `seq`: the bound rises to it, covering the tombstones before it.
    pub(crate) fn raise(&mut self, seq: String) {
        if self.bound.as_ref().is_some_and(|bound| *bound >= seq) {
            return;
        }
        self.by_key.retain(|_, stone| *stone >= seq);
        self.bound = Some(seq);
    }
}
