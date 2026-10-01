//! Pipeline state and how it is stored: as keyed records the destination keeps opaque.

mod error;
mod names;
#[cfg(test)]
mod tests;
mod value_text;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::commit::Receipt;
use crate::cursor::Cursor;
use crate::id::{Epoch, GenerationId, PartitionId, SchemaVersion, StreamName, TablePath};
use crate::schema::TableSchema;

pub use error::StateError;
pub use names::{NameConflict, NameMap};

/// The state value format this crate writes and reads.
const STATE_VERSION: u16 = 1;

/// One stored state record, opaque to the destination that keeps it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateRecord {
    /// The record's key; a commit's `Put` replaces the record with the same key.
    pub key: String,
    /// The record's value; base64 text where the record is written as JSON, a third longer than
    /// its bytes, where a number a byte would be four times them.
    #[serde(with = "value_text")]
    pub value: Bytes,
}

/// A change a commit applies to the stored state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateChange {
    /// Insert or replace the record with this key.
    Put(StateRecord),
    /// Remove the record with this key, if present.
    Delete(String),
}

/// Where a partition stands.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionState {
    /// Resume from this cursor.
    Cursor(Cursor),
    /// The partition is fully read.
    Done,
}

/// Which state record an entry is.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateKey {
    /// The fencing epoch.
    Epoch,
    /// A stream's phase.
    Phase(StreamName),
    /// A partition's position.
    Partition(StreamName, PartitionId),
    /// A stream's full read in progress.
    Generation(StreamName),
    /// A stream's recently completed full reads.
    Completed(StreamName),
    /// The epoch of a stream's last reset.
    Reset(StreamName),
    /// A table's schema.
    Schema(TablePath),
    /// A table's name map.
    Names(TablePath),
    /// Who made a table's sequences.
    Sequences(TablePath),
    /// The last commit's receipt.
    Receipt,
}

impl StateKey {
    /// The record key.
    #[expect(
        clippy::missing_panics_doc,
        reason = "state keys always serialize to JSON"
    )]
    pub fn encode(&self) -> String {
        serde_json::to_string(self).expect("state keys serialize to JSON")
    }

    /// The key a record key names; only the exact text [`StateKey::encode`] writes is accepted, so
    /// one key cannot hide under two record keys.
    pub fn parse(key: &str) -> Result<Self, StateError> {
        let malformed = || StateError::MalformedKey {
            key: key.to_owned(),
        };
        let parsed: Self = serde_json::from_str(key).map_err(|_| malformed())?;
        if parsed.encode() == key {
            Ok(parsed)
        } else {
            Err(malformed())
        }
    }
}

/// One piece of pipeline state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateEntry {
    /// The fencing epoch.
    Epoch(Epoch),
    /// A stream's phase, owned by the source.
    Phase {
        /// The stream.
        stream: StreamName,
        /// The phase number.
        phase: u16,
    },
    /// A partition's position.
    Partition {
        /// The stream.
        stream: StreamName,
        /// The partition.
        partition: PartitionId,
        /// Where it stands.
        state: PartitionState,
    },
    /// A stream's full read in progress; a replace stream fills the read's generation.
    Generation {
        /// The stream.
        stream: StreamName,
        /// The generation.
        generation: GenerationId,
    },
    /// A stream's recently completed full reads.
    Completed {
        /// The stream.
        stream: StreamName,
        /// The generations of the completed reads, oldest first.
        generations: Vec<GenerationId>,
    },
    /// A stream's last reset: nothing logged by a session older than it applies to the stream.
    Reset {
        /// The stream.
        stream: StreamName,
        /// The epoch of the session that reset it.
        epoch: Epoch,
    },
    /// A table's schema.
    Schema {
        /// The table.
        table: TablePath,
        /// The schema's version.
        version: SchemaVersion,
        /// The schema.
        schema: TableSchema,
        /// The columns of 64-bit integers every stored value of which a 64-bit float holds
        /// exactly; a record without them holds none such.
        #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
        exact: BTreeSet<Arc<str>>,
    },
    /// A table's destination identifier and its columns' identifiers.
    Names {
        /// The table.
        table: TablePath,
        /// The table's identifier.
        physical: Arc<str>,
        /// The columns' identifiers.
        names: NameMap,
    },
    /// Who made the sequences of the rows a table holds, and whether it keeps every version of
    /// each key.
    Sequences {
        /// The table.
        table: TablePath,
        /// Who made them.
        sequences: Sequences,
        /// Whether the table is a history table; records written before tables kept
        /// history hold no flag, as no table did.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        history: bool,
    },
    /// The last commit's receipt.
    Receipt(Receipt),
}

/// Who made the `_rdlt_seq` values a table's rows hold, which says whether a later load may
/// compare them.
///
/// A change stream's merge compares each row's sequence with the stored row's across commits, so
/// the stored sequences must be positions of the same source; the engine's own, which only order
/// rows within one commit, compare as nothing of the kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Sequences {
    /// The engine, as it writes every stream that does not merge changes.
    Engine,
    /// A change stream's source: its positions.
    Source,
}

#[derive(Serialize, Deserialize)]
struct VersionedEntry {
    v: u16,
    entry: StateEntry,
}

impl StateEntry {
    /// The key this entry is stored under.
    pub fn key(&self) -> StateKey {
        match self {
            Self::Epoch(_) => StateKey::Epoch,
            Self::Phase { stream, .. } => StateKey::Phase(stream.clone()),
            Self::Partition {
                stream, partition, ..
            } => StateKey::Partition(stream.clone(), partition.clone()),
            Self::Generation { stream, .. } => StateKey::Generation(stream.clone()),
            Self::Completed { stream, .. } => StateKey::Completed(stream.clone()),
            Self::Reset { stream, .. } => StateKey::Reset(stream.clone()),
            Self::Schema { table, .. } => StateKey::Schema(table.clone()),
            Self::Names { table, .. } => StateKey::Names(table.clone()),
            Self::Sequences { table, .. } => StateKey::Sequences(table.clone()),
            Self::Receipt(_) => StateKey::Receipt,
        }
    }

    /// The record that stores this entry.
    #[expect(
        clippy::missing_panics_doc,
        reason = "state entries always serialize to JSON"
    )]
    pub fn to_record(&self) -> StateRecord {
        let value = serde_json::to_vec(&VersionedEntry {
            v: STATE_VERSION,
            entry: self.clone(),
        })
        .expect("state entries serialize to JSON");
        StateRecord {
            key: self.key().encode(),
            value: Bytes::from(value),
        }
    }

    /// The entry a record stores.
    pub fn from_record(record: &StateRecord) -> Result<Self, StateError> {
        let key = StateKey::parse(&record.key)?;
        let malformed = |reason: String| StateError::MalformedValue {
            key: record.key.clone(),
            reason,
        };
        let version: VersionOnly =
            serde_json::from_slice(&record.value).map_err(|error| malformed(error.to_string()))?;
        if version.v != STATE_VERSION {
            return Err(StateError::UnsupportedVersion {
                key: record.key.clone(),
                version: version.v,
            });
        }
        let versioned: VersionedEntry =
            serde_json::from_slice(&record.value).map_err(|error| malformed(error.to_string()))?;
        if versioned.entry.key() != key {
            return Err(StateError::KeyMismatch {
                key: record.key.clone(),
            });
        }
        Ok(versioned.entry)
    }
}

#[derive(Deserialize)]
struct VersionOnly {
    v: u16,
}

/// A stream's committed position.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamState {
    /// The phase, owned by the source (for example snapshot, then changes).
    pub phase: u16,
    /// Each partition's position.
    pub partitions: BTreeMap<PartitionId, PartitionState>,
    /// The full read in progress; a replace stream fills its generation.
    pub generation: Option<GenerationId>,
    /// The generations of recently completed full reads, oldest first, so a retry of a run that
    /// completed one does not read the stream again, even after a newer run completed another.
    pub completed: Vec<GenerationId>,
}

/// A table's committed schema and names.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableState {
    /// The versioned schema, once one is committed: columns by identifier, with their logical
    /// types.
    pub schema: Option<(SchemaVersion, TableSchema)>,
    /// The table's identifier, once one is committed.
    pub physical: Option<Arc<str>>,
    /// The columns' identifiers.
    pub names: NameMap,
    /// Who made the sequences of the rows the table holds, once a load recorded it.
    pub sequences: Option<Sequences>,
    /// Whether the table keeps every version of each key, as its sequences' record says.
    pub history: bool,
    /// The columns of 64-bit integers every stored value of which a 64-bit float holds exactly.
    pub exact: BTreeSet<Arc<str>>,
}

/// Everything a pipeline has committed: epoch, cursors, schemas, names and the last receipt.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PipelineState {
    /// The fencing epoch recorded in state.
    ///
    /// Destinations keep their live epoch outside the records, so the engine fences with
    /// [`OpenedSession::epoch`](crate::OpenedSession::epoch) instead.
    pub epoch: Epoch,
    /// Each stream's position.
    pub streams: BTreeMap<StreamName, StreamState>,
    /// Each table's schema and names.
    pub tables: BTreeMap<TablePath, TableState>,
    /// The epoch of the session that last reset each stream reset: what a session older than it
    /// logged never applies to the stream.
    pub resets: BTreeMap<StreamName, Epoch>,
    /// The last commit's receipt.
    pub last_receipt: Option<Receipt>,
}

impl PipelineState {
    /// Rebuilds state from stored records.
    pub fn from_records(records: &[StateRecord]) -> Result<Self, StateError> {
        let mut state = Self::default();
        for record in records {
            state.put(StateEntry::from_record(record)?);
        }
        Ok(state)
    }

    /// The records that store this state.
    pub fn to_records(&self) -> Vec<StateRecord> {
        let mut entries = vec![StateEntry::Epoch(self.epoch)];
        for (name, stream) in &self.streams {
            entries.push(StateEntry::Phase {
                stream: name.clone(),
                phase: stream.phase,
            });
            for (partition, state) in &stream.partitions {
                entries.push(StateEntry::Partition {
                    stream: name.clone(),
                    partition: partition.clone(),
                    state: state.clone(),
                });
            }
            if let Some(generation) = stream.generation {
                entries.push(StateEntry::Generation {
                    stream: name.clone(),
                    generation,
                });
            }
            if !stream.completed.is_empty() {
                entries.push(StateEntry::Completed {
                    stream: name.clone(),
                    generations: stream.completed.clone(),
                });
            }
        }
        for (stream, epoch) in &self.resets {
            entries.push(StateEntry::Reset {
                stream: stream.clone(),
                epoch: *epoch,
            });
        }
        for (path, table) in &self.tables {
            if let Some((version, schema)) = &table.schema {
                entries.push(StateEntry::Schema {
                    table: path.clone(),
                    version: *version,
                    schema: schema.clone(),
                    exact: table.exact.clone(),
                });
            }
            if let Some(physical) = &table.physical {
                entries.push(StateEntry::Names {
                    table: path.clone(),
                    physical: Arc::clone(physical),
                    names: table.names.clone(),
                });
            }
            if let Some(sequences) = table.sequences {
                entries.push(StateEntry::Sequences {
                    table: path.clone(),
                    sequences,
                    history: table.history,
                });
            }
        }
        if let Some(receipt) = &self.last_receipt {
            entries.push(StateEntry::Receipt(receipt.clone()));
        }
        entries.iter().map(StateEntry::to_record).collect()
    }

    /// Applies one committed change.
    pub fn apply(&mut self, change: &StateChange) -> Result<(), StateError> {
        match change {
            StateChange::Put(record) => self.put(StateEntry::from_record(record)?),
            StateChange::Delete(key) => self.delete(&StateKey::parse(key)?),
        }
        Ok(())
    }

    fn put(&mut self, entry: StateEntry) {
        match entry {
            StateEntry::Epoch(epoch) => self.epoch = epoch,
            StateEntry::Phase { stream, phase } => {
                self.streams.entry(stream).or_default().phase = phase;
            }
            StateEntry::Partition {
                stream,
                partition,
                state,
            } => {
                self.streams
                    .entry(stream)
                    .or_default()
                    .partitions
                    .insert(partition, state);
            }
            StateEntry::Generation { stream, generation } => {
                self.streams.entry(stream).or_default().generation = Some(generation);
            }
            StateEntry::Completed {
                stream,
                generations,
            } => {
                self.streams.entry(stream).or_default().completed = generations;
            }
            StateEntry::Reset { stream, epoch } => {
                self.resets.insert(stream, epoch);
            }
            StateEntry::Schema {
                table,
                version,
                schema,
                exact,
            } => {
                let state = self.tables.entry(table).or_default();
                state.schema = Some((version, schema));
                state.exact = exact;
            }
            StateEntry::Names {
                table,
                physical,
                names,
            } => {
                let state = self.tables.entry(table).or_default();
                state.physical = Some(physical);
                state.names = names;
            }
            StateEntry::Sequences {
                table,
                sequences,
                history,
            } => {
                let state = self.tables.entry(table).or_default();
                state.sequences = Some(sequences);
                state.history = history;
            }
            StateEntry::Receipt(receipt) => self.last_receipt = Some(receipt),
        }
    }

    fn delete(&mut self, key: &StateKey) {
        match key {
            StateKey::Epoch => self.epoch = Epoch::default(),
            StateKey::Phase(stream) => {
                if let Some(state) = self.streams.get_mut(stream) {
                    state.phase = 0;
                }
            }
            StateKey::Partition(stream, partition) => {
                if let Some(state) = self.streams.get_mut(stream) {
                    state.partitions.remove(partition);
                }
            }
            StateKey::Generation(stream) => {
                if let Some(state) = self.streams.get_mut(stream) {
                    state.generation = None;
                }
            }
            StateKey::Completed(stream) => {
                if let Some(state) = self.streams.get_mut(stream) {
                    state.completed.clear();
                }
            }
            StateKey::Reset(stream) => {
                self.resets.remove(stream);
            }
            StateKey::Schema(table) => {
                if let Some(state) = self.tables.get_mut(table) {
                    state.schema = None;
                    state.exact.clear();
                }
            }
            StateKey::Names(table) => {
                if let Some(state) = self.tables.get_mut(table) {
                    state.physical = None;
                    state.names = NameMap::default();
                }
            }
            StateKey::Sequences(table) => {
                if let Some(state) = self.tables.get_mut(table) {
                    state.sequences = None;
                    state.history = false;
                }
            }
            StateKey::Receipt => self.last_receipt = None,
        }
    }
}
