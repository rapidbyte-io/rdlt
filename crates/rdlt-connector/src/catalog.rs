//! What a source offers: its streams and how each can be read.

#[cfg(test)]
mod tests;

use serde::{Deserialize, Serialize};

use crate::id::StreamName;
use crate::schema::{ColumnPath, TableSchema};

/// How a stream can be read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadMode {
    /// Every record, every run.
    Full,
    /// Records past a cursor field's last committed value.
    Incremental,
    /// A snapshot, then changes.
    Cdc,
}

/// Whether the source splits a stream into several partitions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Partitioning {
    /// One partition.
    #[default]
    Single,
    /// Partitions planned by the source for each run.
    Planned,
}

/// When a stream's partitions emit checkpoints.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Checkpointing {
    /// Only where the source chooses; the engine never asks.
    #[default]
    Natural,
    /// Also at the next safe point after the engine asks.
    OnDemand,
}

/// One stream a source offers, and how it can be read.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamSpec {
    name: StreamName,
    schema: Option<TableSchema>,
    primary_key: Option<Vec<ColumnPath>>,
    cursor_fields: Vec<ColumnPath>,
    read_modes: Vec<ReadMode>,
    partitioning: Partitioning,
    checkpointing: Checkpointing,
    replayable: bool,
    change_time: Option<ColumnPath>,
}

impl StreamSpec {
    /// A stream read in full, as one partition, with natural checkpoints and replayable reads.
    pub fn new(name: StreamName) -> Self {
        Self {
            name,
            schema: None,
            primary_key: None,
            cursor_fields: Vec::new(),
            read_modes: vec![ReadMode::Full],
            partitioning: Partitioning::Single,
            checkpointing: Checkpointing::Natural,
            replayable: true,
            change_time: None,
        }
    }

    /// Declares the schema; without one, records are JSON and their schema is inferred.
    #[must_use]
    pub fn with_schema(mut self, schema: TableSchema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Declares the primary key.
    #[must_use]
    pub fn with_primary_key<C: Into<ColumnPath>>(
        mut self,
        columns: impl IntoIterator<Item = C>,
    ) -> Self {
        self.primary_key = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Adds a candidate cursor field for incremental reads.
    #[must_use]
    pub fn with_cursor_field(mut self, column: impl Into<ColumnPath>) -> Self {
        self.cursor_fields.push(column.into());
        self
    }

    /// Replaces the supported read modes.
    #[must_use]
    pub fn with_read_modes(mut self, modes: impl IntoIterator<Item = ReadMode>) -> Self {
        self.read_modes = modes.into_iter().collect();
        self
    }

    /// Sets how the stream is partitioned.
    #[must_use]
    pub fn with_partitioning(mut self, partitioning: Partitioning) -> Self {
        self.partitioning = partitioning;
        self
    }

    /// Sets when the stream checkpoints.
    #[must_use]
    pub fn with_checkpointing(mut self, checkpointing: Checkpointing) -> Self {
        self.checkpointing = checkpointing;
        self
    }

    /// Declares whether the stream can be re-read from any committed cursor.
    #[must_use]
    pub fn with_replayable(mut self, replayable: bool) -> Self {
        self.replayable = replayable;
        self
    }

    /// Declares the column holding each change's source commit time.
    #[must_use]
    pub fn with_change_time(mut self, column: impl Into<ColumnPath>) -> Self {
        self.change_time = Some(column.into());
        self
    }

    /// The stream's name.
    pub fn name(&self) -> &StreamName {
        &self.name
    }

    /// The declared schema, if any.
    pub fn schema(&self) -> Option<&TableSchema> {
        self.schema.as_ref()
    }

    /// The primary key, if declared.
    pub fn primary_key(&self) -> Option<&[ColumnPath]> {
        self.primary_key.as_deref()
    }

    /// Candidate cursor fields.
    pub fn cursor_fields(&self) -> &[ColumnPath] {
        &self.cursor_fields
    }

    /// The read modes the stream supports, in the order declared.
    pub fn read_modes(&self) -> &[ReadMode] {
        &self.read_modes
    }

    /// Whether the stream supports `mode`.
    pub fn supports(&self, mode: ReadMode) -> bool {
        self.read_modes.contains(&mode)
    }

    /// How the stream is partitioned.
    pub fn partitioning(&self) -> Partitioning {
        self.partitioning
    }

    /// When the stream checkpoints.
    pub fn checkpointing(&self) -> Checkpointing {
        self.checkpointing
    }

    /// Whether the stream can be re-read from any committed cursor.
    pub fn is_replayable(&self) -> bool {
        self.replayable
    }

    /// The column holding each change's source commit time.
    pub fn change_time(&self) -> Option<&ColumnPath> {
        self.change_time.as_ref()
    }
}

/// The streams a source offers, with distinct names.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<StreamSpec>", into = "Vec<StreamSpec>")]
pub struct Catalog(Vec<StreamSpec>);

/// Two streams of one catalog share a name.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("stream {0} appears more than once in the catalog")]
pub struct DuplicateStream(pub StreamName);

impl Catalog {
    /// A catalog of `streams`, which must have distinct names.
    pub fn new(streams: Vec<StreamSpec>) -> Result<Self, DuplicateStream> {
        for (index, stream) in streams.iter().enumerate() {
            if streams[..index]
                .iter()
                .any(|earlier| earlier.name() == stream.name())
            {
                return Err(DuplicateStream(stream.name().clone()));
            }
        }
        Ok(Self(streams))
    }

    /// The stream called `name`.
    pub fn get(&self, name: &StreamName) -> Option<&StreamSpec> {
        self.0.iter().find(|stream| stream.name() == name)
    }

    /// The streams, in the order the source listed them.
    pub fn iter(&self) -> impl Iterator<Item = &StreamSpec> {
        self.0.iter()
    }

    /// The number of streams.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the catalog is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl TryFrom<Vec<StreamSpec>> for Catalog {
    type Error = DuplicateStream;

    fn try_from(streams: Vec<StreamSpec>) -> Result<Self, Self::Error> {
        Self::new(streams)
    }
}

impl From<Catalog> for Vec<StreamSpec> {
    fn from(catalog: Catalog) -> Self {
        catalog.0
    }
}
