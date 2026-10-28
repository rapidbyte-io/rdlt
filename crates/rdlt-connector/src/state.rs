//! Pipeline state and how it is stored: as keyed records the destination keeps opaque.

mod applied;
mod error;
mod key;
mod names;
#[cfg(test)]
mod tests;
mod value_json;

use std::collections::{BTreeMap, BTreeSet};
use std::io;
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
const STATE_VERSION: u16 = 4;

/// One stored state record, opaque to the destination that keeps it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateRecord {
    /// The record's key; a commit's `Put` replaces the record with the same key.
    pub key: String,
    /// The record's value: JSON text, which a record written as JSON holds as it is.
    #[serde(with = "value_json")]
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
    /// A stream's last reset: a full read a session older than it began is over.
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
    /// key, the key its rows merge by, the column its versions begin at, and how its root rows
    /// are identified.
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
        /// How the table's root rows are identified.
        identity: Identity,
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

/// How a table's root rows are identified, which says whether a merge may replace their
/// children.
///
/// A merge replaces each root's children by the root's id, the hash of its key; a load that does
/// not merge identifies its normalized rows by their positions, which no key reproduces, so a
/// merge into a table holding such roots would leave their children behind.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Identity {
    /// By the hash of their merge key, where a load gave them an id.
    #[default]
    Key,
    /// Some by their positions: a normalized load that does not merge wrote them.
    Position,
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
        let writing = Writing {
            v: STATE_VERSION,
            entry: self,
        };
        // State holds a record as long as the entry stands: it is written once, into a buffer
        // of its length.
        let mut counted = Counted(0);
        serde_json::to_writer(&mut counted, &writing).expect("state entries serialize to JSON");
        let mut value = Vec::with_capacity(counted.0);
        serde_json::to_writer(&mut value, &writing).expect("state entries serialize to JSON");
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
        let read: Reading =
            serde_json::from_slice(&record.value).map_err(|error| malformed(error.to_string()))?;
        let entry = match read {
            Reading::Entry(entry) => entry,
            Reading::Other(version) => {
                return Err(StateError::UnsupportedVersion {
                    key: error::shown(&record.key),
                    version,
                });
            }
        };
        if entry.key() != key {
            return Err(StateError::KeyMismatch {
                key: error::shown(&record.key),
            });
        }
        Ok(entry)
    }
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
    /// How the table's root rows are identified, as its sequences' record says.
    pub identity: Identity,
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
    /// The epoch of the session that last reset each stream reset: a full read a session older
    /// than it began is over.
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

    /// Applies one committed change.
    pub fn apply(&mut self, change: &StateChange) -> Result<(), StateError> {
        match change {
            StateChange::Put(record) => self.put(StateEntry::from_record(record)?),
            StateChange::Delete(key) => self.delete(&StateKey::parse(key)?),
        }
        Ok(())
    }
}

/// A state value as this crate writes it: its format, then its entry.
#[derive(Serialize)]
struct Writing<'a> {
    v: u16,
    entry: &'a StateEntry,
}

/// A state value as it is read, its format first: its entry, or the other format it is of,
/// whose entry is passed over unread.
enum Reading {
    Entry(StateEntry),
    Other(u16),
}

/// A member of a state value.
#[derive(Deserialize, PartialEq)]
#[serde(field_identifier, rename_all = "lowercase")]
enum Member {
    V,
    Entry,
}

impl<'de> Deserialize<'de> for Reading {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(ReadingVisitor)
    }
}

/// Reads a state value's members in the order they are written.
struct ReadingVisitor;

impl<'de> serde::de::Visitor<'de> for ReadingVisitor {
    type Value = Reading;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a state value: its format, then its entry")
    }

    fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<Reading, A::Error> {
        use serde::de::Error as _;
        if map.next_key::<Member>()? != Some(Member::V) {
            return Err(A::Error::custom("a state value names its format first"));
        }
        let version: u16 = map.next_value()?;
        if map.next_key::<Member>()? != Some(Member::Entry) {
            return Err(A::Error::missing_field("entry"));
        }
        let read = if version == STATE_VERSION {
            Reading::Entry(map.next_value()?)
        } else {
            map.next_value::<serde::de::IgnoredAny>()?;
            Reading::Other(version)
        };
        if map.next_key::<Member>()?.is_some() {
            return Err(A::Error::custom(
                "a state value holds its format and its entry alone",
            ));
        }
        Ok(read)
    }
}

/// A writer that keeps only how many bytes it was given.
struct Counted(usize);

impl io::Write for Counted {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len());
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
