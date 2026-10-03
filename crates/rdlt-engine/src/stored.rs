//! The state a destination stores for a pipeline, as the messages that carry it measure it: each
//! record's bytes, and the limit a commit keeps them, and its own request, within.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use rdlt_connector::wire::{commit_bytes, record_bytes};
use rdlt_connector::{CommitMeta, StateChange, StateKey, StateRecord};

use crate::config::EngineConfig;
use crate::error::Error;
use crate::limits::{STATE_BYTES_EXCEEDED, STATE_ENVELOPE};

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
    /// # Errors
    ///
    /// A `Config` error coded `state_bytes_exceeded` where the commit would grow state past its
    /// limit or send a request past its: nothing of the commit is logged, acknowledged or sent.
    pub(crate) fn admit(&self, meta: &CommitMeta) -> Result<(), Error> {
        let left = self.after(&meta.state_delta);
        let request = commit_bytes(meta);
        let StateLimits {
            stored,
            request: sent,
        } = self.limits;
        if (left <= stored || !self.grows(&meta.state_delta)) && request <= sent {
            return Ok(());
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

    /// Bytes: what the stored records take once `delta` lands.
    fn after(&self, delta: &[StateChange]) -> u64 {
        changed(delta)
            .iter()
            .fold(self.total, |total, (key, bytes)| {
                let before = self.records.get(*key).copied().unwrap_or(0);
                total.saturating_sub(before).saturating_add(*bytes)
            })
    }

    /// Whether `delta` leaves the records other than the receipt taking more than they take.
    fn grows(&self, delta: &[StateChange]) -> bool {
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
        more > less
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
