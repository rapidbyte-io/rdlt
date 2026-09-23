//! Pipeline state and how it is stored: as keyed records the destination keeps opaque.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::commit::Receipt;
use crate::cursor::Cursor;
use crate::id::{Epoch, GenerationId, PartitionId, SchemaVersion, StreamName, TablePath};
use crate::schema::{ColumnPath, TableSchema};

/// The state value format this crate writes and reads.
const STATE_VERSION: u16 = 1;

/// One stored state record, opaque to the destination that keeps it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateRecord {
    /// The record's key; a commit's `Put` replaces the record with the same key.
    pub key: String,
    /// The record's value.
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

/// Source paths mapped to destination identifiers; a mapping, once added, never changes.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "Vec<(ColumnPath, String)>", into = "Vec<(ColumnPath, String)>")]
pub struct NameMap(BTreeMap<ColumnPath, Arc<str>>);

/// An attempt to remap a source path that already has a destination identifier.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{path} is already mapped to {existing:?}")]
pub struct NameConflict {
    /// The source path.
    pub path: ColumnPath,
    /// Its existing identifier.
    pub existing: String,
}

impl NameMap {
    /// The identifier for `path`.
    pub fn get(&self, path: &ColumnPath) -> Option<&str> {
        self.0.get(path).map(AsRef::as_ref)
    }

    /// Maps `path` to `name`; mapping a path again to the same name is a no-op.
    pub fn insert(
        &mut self,
        path: ColumnPath,
        name: impl Into<Arc<str>>,
    ) -> Result<(), NameConflict> {
        let name = name.into();
        match self.0.get(&path) {
            Some(existing) if *existing != name => Err(NameConflict {
                path,
                existing: existing.to_string(),
            }),
            Some(_) => Ok(()),
            None => {
                self.0.insert(path, name);
                Ok(())
            }
        }
    }

    /// The mappings, ordered by path.
    pub fn iter(&self) -> impl Iterator<Item = (&ColumnPath, &str)> {
        self.0.iter().map(|(path, name)| (path, name.as_ref()))
    }

    /// The number of mappings.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether there are no mappings.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<Vec<(ColumnPath, String)>> for NameMap {
    fn from(pairs: Vec<(ColumnPath, String)>) -> Self {
        Self(
            pairs
                .into_iter()
                .map(|(path, name)| (path, Arc::from(name)))
                .collect(),
        )
    }
}

impl From<NameMap> for Vec<(ColumnPath, String)> {
    fn from(names: NameMap) -> Self {
        names
            .0
            .into_iter()
            .map(|(path, name)| (path, name.to_string()))
            .collect()
    }
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
    /// A replace stream's generation in progress.
    Generation(StreamName),
    /// A table's schema.
    Schema(TablePath),
    /// A table's name map.
    Names(TablePath),
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

    /// The key a record key names.
    pub fn parse(key: &str) -> Result<Self, StateError> {
        serde_json::from_str(key).map_err(|_| StateError::MalformedKey {
            key: key.to_owned(),
        })
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
    /// A replace stream's generation in progress.
    Generation {
        /// The stream.
        stream: StreamName,
        /// The generation.
        generation: GenerationId,
    },
    /// A table's schema.
    Schema {
        /// The table.
        table: TablePath,
        /// The schema's version.
        version: SchemaVersion,
        /// The schema.
        schema: TableSchema,
    },
    /// A table's name map.
    Names {
        /// The table.
        table: TablePath,
        /// The mappings.
        names: NameMap,
    },
    /// The last commit's receipt.
    Receipt(Receipt),
}

#[derive(Serialize, Deserialize)]
struct VersionedEntry {
    v: u16,
    entry: StateEntry,
}

/// A state record that cannot be read.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum StateError {
    /// The key is not a state key.
    #[error("state key {key:?} is malformed")]
    MalformedKey {
        /// The key.
        key: String,
    },
    /// The value is not a state value.
    #[error("state value for {key:?} is malformed: {reason}")]
    MalformedValue {
        /// The key.
        key: String,
        /// What is wrong.
        reason: String,
    },
    /// The value was written by a newer format.
    #[error("state value for {key:?} is format {version}; this build reads format 1")]
    UnsupportedVersion {
        /// The key.
        key: String,
        /// The format found.
        version: u16,
    },
    /// The value belongs to a different key.
    #[error("state value stored under {key:?} belongs to another key")]
    KeyMismatch {
        /// The key.
        key: String,
    },
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
            Self::Schema { table, .. } => StateKey::Schema(table.clone()),
            Self::Names { table, .. } => StateKey::Names(table.clone()),
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
    /// The replace generation in progress.
    pub generation: Option<GenerationId>,
}

/// A table's committed schema and names.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableState {
    /// The versioned schema, once one is committed.
    pub schema: Option<(SchemaVersion, TableSchema)>,
    /// Source paths mapped to destination identifiers.
    pub names: NameMap,
}

/// Everything a pipeline has committed: epoch, cursors, schemas, names and the last receipt.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PipelineState {
    /// The fencing epoch.
    pub epoch: Epoch,
    /// Each stream's position.
    pub streams: BTreeMap<StreamName, StreamState>,
    /// Each table's schema and names.
    pub tables: BTreeMap<TablePath, TableState>,
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
        }
        for (path, table) in &self.tables {
            if let Some((version, schema)) = &table.schema {
                entries.push(StateEntry::Schema {
                    table: path.clone(),
                    version: *version,
                    schema: schema.clone(),
                });
            }
            entries.push(StateEntry::Names {
                table: path.clone(),
                names: table.names.clone(),
            });
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
            StateEntry::Schema {
                table,
                version,
                schema,
            } => {
                self.tables.entry(table).or_default().schema = Some((version, schema));
            }
            StateEntry::Names { table, names } => {
                self.tables.entry(table).or_default().names = names;
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
            StateKey::Schema(table) => {
                if let Some(state) = self.tables.get_mut(table) {
                    state.schema = None;
                }
            }
            StateKey::Names(table) => {
                if let Some(state) = self.tables.get_mut(table) {
                    state.names = NameMap::default();
                }
            }
            StateKey::Receipt => self.last_receipt = None,
        }
    }
}
