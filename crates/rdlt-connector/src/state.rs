//! Pipeline state and how it is stored: as keyed records the destination keeps opaque.

mod error;
mod key;
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
use crate::id::{Epoch, GenerationId, LoadId, PartitionId, SchemaVersion, StreamName, TablePath};
use crate::schema::{ColumnPath, TableSchema};

pub use error::StateError;
pub use key::StateKey;
pub use names::{NameConflict, NameMap};

/// The state value format this crate writes and reads.
const STATE_VERSION: u16 = 2;

/// One stored state record, opaque to the destination that keeps it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

/// One piece of pipeline state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
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
        /// The load whose commit recorded it there.
        load: LoadId,
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
        /// exactly.
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
    /// Who made the sequences of the rows a table holds, whether it keeps every version of each
    /// key, the key its rows merge by, and the column its versions begin at.
    Sequences {
        /// The table.
        table: TablePath,
        /// Who made them.
        sequences: Sequences,
        /// Whether the table is a history table.
        history: bool,
        /// The columns its rows were merged by; none for a table never merged.
        key: Vec<ColumnPath>,
        /// The column a history table's versions begin at; none where they begin as their rows
        /// arrive, and for a table that keeps no history.
        #[serde(deserialize_with = "Option::deserialize")]
        change_time: Option<ColumnPath>,
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
#[serde(deny_unknown_fields)]
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
            key: error::shown(&record.key),
            reason: error::shown(&reason),
        };
        let version: VersionOnly =
            serde_json::from_slice(&record.value).map_err(|error| malformed(error.to_string()))?;
        if version.v != STATE_VERSION {
            return Err(StateError::UnsupportedVersion {
                key: error::shown(&record.key),
                version: version.v,
            });
        }
        let versioned: VersionedEntry =
            serde_json::from_slice(&record.value).map_err(|error| malformed(error.to_string()))?;
        if versioned.entry.key() != key {
            return Err(StateError::KeyMismatch {
                key: error::shown(&record.key),
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
    /// The columns the table's rows were merged by, as its sequences' record says; none for a
    /// table never merged.
    pub key: Vec<ColumnPath>,
    /// The column a history table's versions begin at, as its sequences' record says.
    pub change_time: Option<ColumnPath>,
    /// The columns of 64-bit integers every stored value of which a 64-bit float holds exactly.
    pub exact: BTreeSet<Arc<str>>,
}

impl TableState {
    /// Pushes to `entries` the entries that record this table, at `path`.
    fn record(&self, path: &TablePath, entries: &mut Vec<StateEntry>) {
        if let Some((version, schema)) = &self.schema {
            entries.push(StateEntry::Schema {
                table: path.clone(),
                version: *version,
                schema: schema.clone(),
                exact: self.exact.clone(),
            });
        }
        if let Some(physical) = &self.physical {
            entries.push(StateEntry::Names {
                table: path.clone(),
                physical: Arc::clone(physical),
                names: self.names.clone(),
            });
        }
        if let Some(sequences) = self.sequences {
            entries.push(StateEntry::Sequences {
                table: path.clone(),
                sequences,
                history: self.history,
                key: self.key.clone(),
                change_time: self.change_time.clone(),
            });
        }
    }
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
    /// The load whose commit recorded each partition's position, by stream and partition.
    pub recorded_by: BTreeMap<(StreamName, PartitionId), LoadId>,
}

impl PipelineState {
    /// Rebuilds state from stored records, one a key.
    pub fn from_records(records: &[StateRecord]) -> Result<Self, StateError> {
        let mut state = Self::default();
        let mut keys = BTreeSet::new();
        for record in records {
            if !keys.insert(record.key.as_str()) {
                return Err(StateError::Repeated {
                    key: error::shown(&record.key),
                });
            }
            state.put(StateEntry::from_record(record)?);
        }
        Ok(state)
    }

    /// The entries of `stream`'s partitions, named `name`; a position no load is known to have
    /// recorded, as in a state built by hand, records as the earliest load's.
    fn positions<'a>(
        &'a self,
        name: &'a StreamName,
        stream: &'a StreamState,
    ) -> impl Iterator<Item = StateEntry> + 'a {
        stream.partitions.iter().map(move |(partition, state)| {
            let load = self
                .recorded_by
                .get(&(name.clone(), partition.clone()))
                .copied()
                .unwrap_or_else(|| LoadId::from_parts(std::time::UNIX_EPOCH, 0));
            StateEntry::Partition {
                stream: name.clone(),
                partition: partition.clone(),
                state: state.clone(),
                load,
            }
        })
    }

    /// The records that store this state.
    pub fn to_records(&self) -> Vec<StateRecord> {
        let mut entries = vec![StateEntry::Epoch(self.epoch)];
        for (name, stream) in &self.streams {
            entries.push(StateEntry::Phase {
                stream: name.clone(),
                phase: stream.phase,
            });
            entries.extend(self.positions(name, stream));
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
            table.record(path, &mut entries);
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
                load,
            } => {
                let positions = &mut self.streams.entry(stream.clone()).or_default().partitions;
                positions.insert(partition.clone(), state);
                self.recorded_by.insert((stream, partition), load);
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
                (state.schema, state.exact) = (Some((version, schema)), exact);
            }
            StateEntry::Names {
                table,
                physical,
                names,
            } => {
                let state = self.tables.entry(table).or_default();
                (state.physical, state.names) = (Some(physical), names);
            }
            StateEntry::Sequences {
                table,
                sequences,
                history,
                key,
                change_time,
            } => {
                let state = self.tables.entry(table).or_default();
                (state.sequences, state.history) = (Some(sequences), history);
                (state.key, state.change_time) = (key, change_time);
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
                self.recorded_by
                    .remove(&(stream.clone(), partition.clone()));
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
                    state.key.clear();
                    state.change_time = None;
                }
            }
            StateKey::Receipt => self.last_receipt = None,
        }
    }
}
