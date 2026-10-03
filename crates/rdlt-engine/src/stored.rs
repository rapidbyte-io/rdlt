//! The state a destination stores for a pipeline, as the messages that carry it measure it: each
//! record's bytes, and the limit a commit keeps them, and its own request, within.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use rdlt_connector::wire::{commit_bytes, record_bytes};
use rdlt_connector::{CommitMeta, StateChange, StateKey, StateRecord, StreamName};

use crate::config::EngineConfig;
use crate::error::Error;
use crate::limits::{CHILD_TABLES_EXCEEDED, STATE_BYTES_EXCEEDED, STATE_ENVELOPE};

/// The bytes each stored record takes in an open's answer, and their sum.
#[derive(Debug)]
pub(crate) struct Stored {
    records: BTreeMap<String, u64>,
    total: u64,
    limits: StateLimits,
}

/// Bytes: what the stored state may take as an open's answer carries it, and what a commit's
/// request may take.
#[derive(Clone, Copy, Debug)]
pub(crate) struct StateLimits {
    pub(crate) stored: u64,
    pub(crate) request: u64,
}

impl StateLimits {
    /// The limits `config` holds state to.
    pub(crate) fn of(config: &EngineConfig) -> Self {
        let message = config.growth().state_bytes().get();
        Self {
            stored: config.state_limit(),
            request: message.saturating_sub(STATE_ENVELOPE),
        }
    }
}

impl Stored {
    /// The state `records` store, held to `limits`.
    pub(crate) fn of(records: &[StateRecord], limits: StateLimits) -> Self {
        let records: BTreeMap<String, u64> = records
            .iter()
            .map(|record| (record.key.clone(), record_bytes(record)))
            .collect();
        let total = records.values().copied().fold(0, u64::saturating_add);
        Self {
            records,
            total,
            limits,
        }
    }

    /// Bytes: what the stored records take in an open's answer.
    #[cfg(test)]
    pub(crate) fn total(&self) -> u64 {
        self.total
    }

    /// Admits the commit `meta`: the state it leaves, as an open's answer carries it, within its
    /// limit or no more than what is stored, and its own request within its limit.
    ///
    /// State stored under a larger limit, before the memory was lowered, or by a replayed commit
    /// logged under one, is past the limit: a commit that does not grow it lands, so a pipeline
    /// whose limit fell keeps loading. The receipt, which every commit replaces, is not counted
    /// there: it takes a byte more whenever one of its numbers gains a digit.
    ///
    /// The records of child tables state does not record yet, `born` by their keys with their
    /// streams, are blamed first: where the state would fit without them, the commit is refused
    /// for its stream's child tables.
    ///
    /// # Errors
    ///
    /// A `Schema` error coded `child_tables_exceeded` where only the new child tables' records
    /// take state past its limit, else a `Config` error coded `state_bytes_exceeded` where the
    /// commit would grow state past its limit or send a request past its: nothing of the commit
    /// is logged, acknowledged or sent.
    pub(crate) fn admit(
        &self,
        meta: &CommitMeta,
        born: &[(StreamName, String)],
    ) -> Result<(), Error> {
        let left = self.after(&meta.state_delta);
        let request = commit_bytes(meta);
        let StateLimits {
            stored,
            request: sent,
        } = self.limits;
        let fits = request <= sent;
        if fits && self.fits(&meta.state_delta) {
            return Ok(());
        }
        let unborn: Vec<StateChange> = meta
            .state_delta
            .iter()
            .filter(|change| match change {
                StateChange::Put(record) => !born.iter().any(|(_, key)| *key == record.key),
                StateChange::Delete(_) => true,
            })
            .cloned()
            .collect();
        if let Some((stream, _)) = born.first()
            && fits
            && self.fits(&unborn)
        {
            return Err(Error::schema(format!(
                "stream {stream}: its new child tables would leave {left} bytes of state, where \
                 {stored} may be stored"
            ))
            .with_code(CHILD_TABLES_EXCEEDED)
            .with_stream(stream));
        }
        Err(Error::config(format!(
            "a commit would leave {left} bytes of state, where {stored} may be stored, and send \
             {request} in its request, where {sent} may be: raise the memory or the state limit, \
             or reset the streams whose positions or tables grew it"
        ))
        .with_code(STATE_BYTES_EXCEEDED))
    }

    /// Notes that the changes `delta` landed.
    pub(crate) fn apply(&mut self, delta: &[StateChange]) {
        self.total = self.after(delta);
        for change in delta {
            match change {
                StateChange::Put(record) => {
                    self.records
                        .insert(record.key.clone(), record_bytes(record));
                }
                StateChange::Delete(key) => {
                    self.records.remove(key);
                }
            }
        }
    }

    /// Whether the state `delta` leaves fits its limit, or is no larger than what is stored.
    fn fits(&self, delta: &[StateChange]) -> bool {
        self.excess(delta) == 0
    }

    /// Bytes: by how much the state `delta` leaves passes what may stand, its limit or, where
    /// state is past it already, what is stored.
    fn excess(&self, delta: &[StateChange]) -> u64 {
        let past = self.after(delta).saturating_sub(self.limits.stored);
        past.min(self.growth(delta))
    }

    /// How many of the stored records at `keys`, deleted in order with `meta`, `meta` needs deleted
    /// for the state it leaves to fit: none where it fits as it is, `None` where deleting every
    /// one of them would not do.
    pub(crate) fn relief(&self, meta: &CommitMeta, keys: &[&str]) -> Option<usize> {
        let mut excess = self.excess(&meta.state_delta);
        for (deleted, key) in keys.iter().enumerate() {
            if excess == 0 {
                return Some(deleted);
            }
            let freed = self.records.get(*key).copied().unwrap_or(0);
            excess = excess.saturating_sub(freed);
        }
        (excess == 0).then_some(keys.len())
    }

    /// Bytes: what the stored records take once `delta` lands.
    fn after(&self, delta: &[StateChange]) -> u64 {
        changed(delta)
            .iter()
            .fold(self.total, |total, (key, bytes)| {
                let before = self.records.get(*key).copied().unwrap_or(0);
                total.saturating_sub(before).saturating_add(*bytes)
            })
    }

    /// Bytes: by how much `delta` leaves the records other than the receipt taking more than
    /// they take.
    fn growth(&self, delta: &[StateChange]) -> u64 {
        let receipt = StateKey::Receipt.encode();
        let (more, less) = changed(delta)
            .into_iter()
            .filter(|(key, _)| *key != receipt)
            .fold((0_u64, 0_u64), |(more, less), (key, bytes)| {
                let before = self.records.get(key).copied().unwrap_or(0);
                (
                    more.saturating_add(bytes.saturating_sub(before)),
                    less.saturating_add(before.saturating_sub(bytes)),
                )
            });
        more.saturating_sub(less)
    }
}

/// Bytes: what each record `delta` changes takes once it lands, nothing where it is deleted.
fn changed(delta: &[StateChange]) -> BTreeMap<&str, u64> {
    let mut changed = BTreeMap::new();
    for change in delta {
        let (key, bytes) = match change {
            StateChange::Put(record) => (record.key.as_str(), record_bytes(record)),
            StateChange::Delete(key) => (key.as_str(), 0),
        };
        changed.insert(key, bytes);
    }
    changed
}
