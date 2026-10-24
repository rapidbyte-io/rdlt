//! The state a destination stores for a pipeline, as the messages that carry it hold it decoded:
//! each record's bytes, and the limit a commit keeps them, and its own request, within.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use rdlt_connector::wire::{answer_bytes, commit_bytes, record_bytes};
use rdlt_connector::{CommitMeta, StateChange, StateKey, StateRecord, StreamName};

use crate::config::EngineConfig;
use crate::error::{Error, codes};

#[cfg(test)]
thread_local! {
    /// How many records and requests this thread measured for commits, which tests read.
    pub(crate) static MEASURES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Bytes each record a commit changes takes once it lands, by key, its last change counting;
/// `None` where it is deleted.
type Changed = BTreeMap<String, Option<u64>>;

/// The bytes each stored record adds to an open's answer decoded, and what the answer holds.
#[derive(Debug)]
pub(crate) struct Stored {
    records: BTreeMap<String, u64>,
    total: u64,
    limits: StateLimits,
}

/// Bytes: what the stored state may hold decoded in an open's answer, and what a commit's
/// request may hold.
#[derive(Clone, Copy, Debug)]
pub(crate) struct StateLimits {
    pub(crate) stored: u64,
    pub(crate) request: u64,
}

impl StateLimits {
    /// The limits `config` holds state to: both the state limit it advertises.
    pub(crate) fn of(config: &EngineConfig) -> Self {
        let limit = config.state_limit();
        Self {
            stored: limit,
            request: limit,
        }
    }
}

/// A commit's state changes and its request as its admission measures them, each once.
#[derive(Debug)]
pub(crate) struct Measured {
    changed: Changed,
    /// Bytes: what the commit's request holds decoded.
    request: u64,
}

impl Measured {
    /// The state changes of `meta`, and its request, measured.
    pub(crate) fn of(meta: &CommitMeta) -> Self {
        let mut changed = Changed::new();
        for change in &meta.state_delta {
            match change {
                StateChange::Put(record) => {
                    changed.insert(record.key.clone(), Some(measured(record)));
                }
                StateChange::Delete(key) => {
                    changed.insert(key.clone(), None);
                }
            }
        }
        Self {
            changed,
            request: request(meta),
        }
    }

    /// These changes with `deleted` deleted after them, and the request of `meta`, which
    /// carries those deletes.
    pub(crate) fn deleting(mut self, meta: &CommitMeta, deleted: Vec<String>) -> Self {
        for key in deleted {
            self.changed.insert(key, None);
        }
        self.request = request(meta);
        self
    }
}

impl Stored {
    /// The state `records` store, held to `limits`.
    pub(crate) fn of(records: &[StateRecord], limits: StateLimits) -> Self {
        let records: BTreeMap<String, u64> = records
            .iter()
            .map(|record| (record.key.clone(), record_bytes(record)))
            .collect();
        let total = records
            .values()
            .copied()
            .fold(answer_bytes(&[]), u64::saturating_add);
        Self {
            records,
            total,
            limits,
        }
    }

    /// Bytes: what an open's answer carrying the stored records holds decoded.
    #[cfg(test)]
    pub(crate) fn total(&self) -> u64 {
        self.total
    }

    /// Admits the commit `measured` measures: the state it leaves, as an open's answer carries
    /// it, within its limit or no more than what is stored, and its own request within its
    /// limit.
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
        measured: &Measured,
        born: &[(StreamName, String)],
    ) -> Result<(), Error> {
        let changed = &measured.changed;
        let (left, request) = (self.after(changed), measured.request);
        let StateLimits {
            stored,
            request: sent,
        } = self.limits;
        let fits = request <= sent;
        if fits && self.fits(changed) {
            return Ok(());
        }
        // A new child table's record has nothing stored under its key: without it, its key is
        // unchanged.
        let unborn: Changed = changed
            .iter()
            .filter(|(key, bytes)| bytes.is_none() || !born.iter().any(|(_, born)| born == *key))
            .map(|(key, bytes)| (key.clone(), *bytes))
            .collect();
        if let Some((stream, _)) = born.first()
            && fits
            && self.fits(&unborn)
        {
            return Err(Error::schema(format!(
                "stream {stream}: its new child tables would leave {left} bytes of state, where \
                 {stored} may be stored"
            ))
            .with_code(codes::CHILD_TABLES_EXCEEDED)
            .with_stream(stream));
        }
        Err(Error::config(format!(
            "a commit would leave {left} bytes of state, where {stored} may be stored, and send \
             {request} in its request, where {sent} may be: raise the memory or the state limit, \
             or reset the streams whose positions or tables grew it"
        ))
        .with_code(codes::STATE_BYTES_EXCEEDED))
    }

    /// Notes that the commit `measured` measures landed.
    pub(crate) fn apply(&mut self, measured: Measured) {
        self.total = self.after(&measured.changed);
        for (key, bytes) in measured.changed {
            match bytes {
                Some(bytes) => {
                    self.records.insert(key, bytes);
                }
                None => {
                    self.records.remove(&key);
                }
            }
        }
    }

    /// How many of the stored records at `keys`, deleted in order with the commit `measured`
    /// measures, it needs deleted for the state it leaves to fit: none where it fits as it is,
    /// `None` where deleting every one of them would not do.
    pub(crate) fn relief(&self, measured: &Measured, keys: &[&str]) -> Option<usize> {
        let mut excess = self.excess(&measured.changed);
        for (deleted, key) in keys.iter().enumerate() {
            if excess == 0 {
                return Some(deleted);
            }
            let freed = self.records.get(*key).copied().unwrap_or(0);
            excess = excess.saturating_sub(freed);
        }
        (excess == 0).then_some(keys.len())
    }

    /// Whether the state `changed` leaves fits its limit, or is no larger than what is stored.
    fn fits(&self, changed: &Changed) -> bool {
        self.excess(changed) == 0
    }

    /// Bytes: by how much the state `changed` leaves passes what may stand, its limit or, where
    /// state is past it already, what is stored.
    fn excess(&self, changed: &Changed) -> u64 {
        let past = self.after(changed).saturating_sub(self.limits.stored);
        past.min(self.growth(changed))
    }

    /// Bytes: what the stored records take once `changed` lands.
    fn after(&self, changed: &Changed) -> u64 {
        changed.iter().fold(self.total, |total, (key, bytes)| {
            let before = self.records.get(key).copied().unwrap_or(0);
            total
                .saturating_sub(before)
                .saturating_add(bytes.unwrap_or(0))
        })
    }

    /// Bytes: by how much `changed` leaves the records other than the receipt taking more than
    /// they take.
    fn growth(&self, changed: &Changed) -> u64 {
        let receipt = StateKey::Receipt.encode();
        let (more, less) = changed.iter().filter(|(key, _)| **key != receipt).fold(
            (0_u64, 0_u64),
            |(more, less), (key, bytes)| {
                let (before, bytes) = (
                    self.records.get(key).copied().unwrap_or(0),
                    bytes.unwrap_or(0),
                );
                (
                    more.saturating_add(bytes.saturating_sub(before)),
                    less.saturating_add(before.saturating_sub(bytes)),
                )
            },
        );
        more.saturating_sub(less)
    }
}

/// Bytes: what `record` adds to an open's answer decoded.
fn measured(record: &StateRecord) -> u64 {
    #[cfg(test)]
    MEASURES.with(|measures| measures.set(measures.get() + 1));
    record_bytes(record)
}

/// Bytes: what the request carrying `meta` holds decoded.
fn request(meta: &CommitMeta) -> u64 {
    #[cfg(test)]
    MEASURES.with(|measures| measures.set(measures.get() + 1));
    commit_bytes(meta)
}
