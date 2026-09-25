//! What a run loads: the pipeline, its streams and how each one is read and written.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};

use rdlt_connector::{ColumnPath, LogicalType, PipelineId, ReadMode, StreamName};

use crate::error::Error;
use crate::policy::{Nested, SchemaSettings};

/// How a stream's rows reach its table.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteMode {
    /// Insert every row.
    Append,
    /// Fill a hidden generation of the table and swap it in once the stream is fully read.
    Replace,
    /// Upsert by key: a row replaces the table's row with the same key.
    Merge,
}

/// One stream of a [`PipelinePlan`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamPlan {
    name: StreamName,
    read: ReadMode,
    write: WriteMode,
    key: Option<Vec<ColumnPath>>,
    schema: SchemaSettings,
    columns: BTreeMap<ColumnPath, SchemaSettings>,
    hints: BTreeMap<ColumnPath, LogicalType>,
}

impl StreamPlan {
    /// A stream read in full and appended.
    pub fn new(name: StreamName) -> Self {
        Self {
            name,
            read: ReadMode::Full,
            write: WriteMode::Append,
            key: None,
            schema: SchemaSettings::default(),
            columns: BTreeMap::new(),
            hints: BTreeMap::new(),
        }
    }

    /// Sets how the stream is read.
    #[must_use]
    pub fn read(mut self, mode: ReadMode) -> Self {
        self.read = mode;
        self
    }

    /// Sets how the stream is written.
    #[must_use]
    pub fn write(mut self, mode: WriteMode) -> Self {
        self.write = mode;
        self
    }

    /// Sets the key a merge matches rows by, instead of the stream's primary key.
    #[must_use]
    pub fn key<C: Into<ColumnPath>>(mut self, columns: impl IntoIterator<Item = C>) -> Self {
        self.key = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Sets the stream's schema settings, which its tables and columns inherit.
    #[must_use]
    pub fn schema(mut self, settings: SchemaSettings) -> Self {
        self.schema = settings;
        self
    }

    /// Sets one column's schema settings.
    #[must_use]
    pub fn column(mut self, column: impl Into<ColumnPath>, settings: SchemaSettings) -> Self {
        self.columns.insert(column.into(), settings);
        self
    }

    /// Fixes a column's type: values that do not fit it are incompatible changes, which the schema
    /// policy handles.
    #[must_use]
    pub fn hint(mut self, column: impl Into<ColumnPath>, logical_type: LogicalType) -> Self {
        self.hints.insert(column.into(), logical_type);
        self
    }

    /// The stream.
    pub fn name(&self) -> &StreamName {
        &self.name
    }

    /// The merge key the plan sets, if any.
    pub fn merge_key(&self) -> Option<&[ColumnPath]> {
        self.key.as_deref()
    }

    /// The stream's schema settings.
    pub fn schema_settings(&self) -> &SchemaSettings {
        &self.schema
    }

    /// The schema settings of `column`, if the plan sets any.
    pub fn column_settings(&self, column: &ColumnPath) -> Option<&SchemaSettings> {
        self.columns.get(column)
    }

    /// The same stream without its column settings and hints, which name columns of its own
    /// table: its child tables' settings.
    pub(crate) fn without_columns(&self) -> Self {
        Self {
            columns: BTreeMap::new(),
            hints: BTreeMap::new(),
            key: None,
            ..self.clone()
        }
    }

    /// Every column the plan sets schema settings for, with them.
    pub(crate) fn columns(&self) -> impl Iterator<Item = (&ColumnPath, &SchemaSettings)> {
        self.columns.iter()
    }

    /// The type hinted for `column`, if any.
    pub fn hinted(&self, column: &ColumnPath) -> Option<&LogicalType> {
        self.hints.get(column)
    }

    /// How the stream is read.
    pub fn read_mode(&self) -> ReadMode {
        self.read
    }

    /// How the stream is written.
    pub fn write_mode(&self) -> WriteMode {
        self.write
    }
}

/// A pipeline and the streams one run of it loads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PipelinePlan {
    pipeline: PipelineId,
    streams: Vec<StreamPlan>,
    schema: SchemaSettings,
}

impl PipelinePlan {
    /// Validates `streams` of `pipeline`: at least one stream, distinct names and tables, supported
    /// combinations of read and write modes (`full` with `append`, `replace` or `merge`,
    /// `incremental` with `append` or `merge`), a key only on merge streams, and settings, hints
    /// and keys naming top-level columns.
    pub fn new(
        pipeline: PipelineId,
        streams: impl IntoIterator<Item = StreamPlan>,
    ) -> Result<Self, Error> {
        let streams: Vec<StreamPlan> = streams.into_iter().collect();
        if streams.is_empty() {
            return Err(
                Error::config(format!("pipeline {pipeline} selects no streams"))
                    .with_code("plan_empty"),
            );
        }
        let mut names = BTreeSet::new();
        let mut tables = BTreeSet::new();
        for stream in &streams {
            if !names.insert(&stream.name) {
                return Err(
                    Error::config(format!("stream {} is selected twice", stream.name))
                        .with_code("plan_duplicate_stream")
                        .with_stream(&stream.name),
                );
            }
            // Each stream loads the table named after it, so two names that display alike
            // would write into one table.
            if !tables.insert(stream.name.to_string()) {
                return Err(Error::config(format!(
                    "stream {} would load the same table as another selected stream",
                    stream.name
                ))
                .with_code("plan_table_collision")
                .with_stream(&stream.name));
            }
            check_modes(stream)?;
            check_columns(stream)?;
        }
        Ok(Self {
            pipeline,
            streams,
            schema: SchemaSettings::default(),
        })
    }

    /// Sets the pipeline's schema settings, which every stream inherits.
    #[must_use]
    pub fn schema(mut self, settings: SchemaSettings) -> Self {
        self.schema = settings;
        self
    }

    /// The pipeline's schema settings.
    pub fn schema_settings(&self) -> &SchemaSettings {
        &self.schema
    }

    /// The pipeline.
    pub fn pipeline(&self) -> &PipelineId {
        &self.pipeline
    }

    /// The selected streams.
    pub fn streams(&self) -> &[StreamPlan] {
        &self.streams
    }
}

fn check_modes(stream: &StreamPlan) -> Result<(), Error> {
    let refuse = |code: &str, reason: &str| {
        Err(Error::config(format!("stream {}: {reason}", stream.name))
            .with_code(code)
            .with_stream(&stream.name))
    };
    if stream.key.is_some() && stream.write != WriteMode::Merge {
        return refuse(
            "plan_key_unused",
            "a key is set, but only merge streams match rows by key",
        );
    }
    match (stream.read, stream.write) {
        (ReadMode::Full, WriteMode::Append | WriteMode::Replace | WriteMode::Merge)
        | (ReadMode::Incremental, WriteMode::Append | WriteMode::Merge) => Ok(()),
        (ReadMode::Incremental, WriteMode::Replace) => refuse(
            "plan_mode_invalid",
            "replace needs a full read, since the new generation replaces every row",
        ),
        _ => refuse(
            "plan_mode_unsupported",
            "change data capture is not supported yet",
        ),
    }
}

/// Refuses column settings, hints and keys that name nested columns, which only top-level
/// columns may have until nested columns can be normalized, and null hints.
fn check_columns(stream: &StreamPlan) -> Result<(), Error> {
    let refuse = |code: &str, reason: String| {
        Err(Error::config(format!("stream {}: {reason}", stream.name))
            .with_code(code)
            .with_stream(&stream.name))
    };
    let named = stream
        .columns
        .keys()
        .chain(stream.hints.keys())
        .chain(stream.key.iter().flatten());
    for column in named {
        if column.segments().count() > 1 {
            return refuse(
                "plan_column_nested",
                format!(
                    "column {column} is nested; settings, hints and keys name top-level columns"
                ),
            );
        }
    }
    if let Some((column, _)) = stream
        .columns
        .iter()
        .find(|(_, settings)| matches!(settings.nested_setting(), Some(Nested::Normalize { .. })))
    {
        return refuse(
            "plan_nested_normalize_column",
            format!("column {column} is set to normalize; only pipelines and streams normalize"),
        );
    }
    if let Some((column, _)) = stream
        .hints
        .iter()
        .find(|(_, hint)| **hint == LogicalType::Null)
    {
        return refuse(
            "plan_hint_invalid",
            format!("column {column} is hinted as null"),
        );
    }
    if stream.key.as_ref().is_some_and(Vec::is_empty) {
        return refuse("plan_key_empty", "the merge key names no column".to_owned());
    }
    Ok(())
}
