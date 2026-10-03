//! The state a destination stores for a pipeline, as the messages that carry it measure it: each
//! record's bytes, and the limit a commit keeps them, and its own request, within.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use rdlt_connector::wire::{commit_bytes, record_bytes};
use rdlt_connector::{CommitMeta, StateChange, StateRecord};

use crate::error::Error;
use crate::limits::{STATE_BYTES_EXCEEDED, STATE_ENVELOPE};

/// The bytes each stored record takes in an open's answer, and their sum.
#[derive(Debug)]
pub(crate) struct Stored {
    records: BTreeMap<String, u64>,
    total: u64,
    /// Bytes: what one message carrying state may take.
    limit: u64,
}

impl Stored {
    /// The state `records` store, held to messages of `limit` bytes.
    pub(crate) fn of(records: &[StateRecord], limit: u64) -> Self {
        let records: BTreeMap<String, u64> = records
            .iter()
            .map(|record| (record.key.clone(), record_bytes(record)))
            .collect();
        let total = records.values().copied().fold(0, u64::saturating_add);
        Self {
            records,
            total,
            limit,
        }
    }

    /// Bytes: what the stored records take in an open's answer.
    #[cfg(test)]
    pub(crate) fn total(&self) -> u64 {
        self.total
    }

    /// Admits the commit `meta`: the state it leaves carried by an open's answer, and its own
    /// request, each within the limit beside what else such a message holds.
    ///
    /// # Errors
    ///
    /// A `Config` error coded `state_bytes_exceeded` where either would pass it: nothing of the
    /// commit is logged, acknowledged or sent.
    pub(crate) fn admit(&self, meta: &CommitMeta) -> Result<(), Error> {
        let room = self.limit.saturating_sub(STATE_ENVELOPE);
        let left = self.after(&meta.state_delta);
        let request = commit_bytes(meta);
        if left <= room && request <= room {
            return Ok(());
        }
        Err(Error::config(format!(
            "a commit would leave {left} bytes of state and send {request} in its request, \
             where a message carrying state takes {} at most: raise the state limit at both \
             ends, or reset the streams whose positions or tables grew it",
            self.limit
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
        let mut changed: BTreeMap<&str, u64> = BTreeMap::new();
        for change in delta {
            let (key, bytes) = match change {
                StateChange::Put(record) => (record.key.as_str(), record_bytes(record)),
                StateChange::Delete(key) => (key.as_str(), 0),
            };
            changed.insert(key, bytes);
        }
        changed.iter().fold(self.total, |total, (key, bytes)| {
            let before = self.records.get(*key).copied().unwrap_or(0);
            total.saturating_sub(before).saturating_add(*bytes)
        })
    }
}
