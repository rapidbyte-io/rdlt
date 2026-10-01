//! What a run loads: the pipeline, its streams and how each one is read and written.

mod retention;
#[cfg(test)]
mod tests;
mod until;

use std::collections::{BTreeMap, BTreeSet};

use rdlt_connector::{ColumnPath, LogicalType, PipelineId, ReadMode, StreamName};

use crate::error::Error;
use crate::policy::{Nested, SchemaSettings};

pub use retention::RetentionLoss;
pub use until::Until;

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
    /// Keep every version of each key: a row that changes its key's data closes the
    /// key's current version and becomes the current one.
    History,
}

/// What a change stream's deletes do to its merge or history table.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DeleteMode {
    /// Remove the row.
    #[default]
    Hard,
    /// Keep the row with its last values, and record when it was deleted in `_rdlt_deleted_at`;
    /// a history table closes the key's version and keeps a deleted one.
    Soft,
    /// Drop delete rows; the report counts them.
    Ignore,
}

/// What a change stream's truncates do to its merge or history table.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OnTruncate {
    /// Remove every row the source truncated, as the stream's deletes remove rows (hard unless
    /// deletes are soft), in the commit that carries the truncate.
    #[default]
    Apply,
    /// Drop truncates; the report counts them.
    Ignore,
}

/// One stream of a [`PipelinePlan`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamPlan {
    name: StreamName,
    read: ReadMode,
    write: WriteMode,
    key: Option<Vec<ColumnPath>>,
    deletes: Option<DeleteMode>,
    truncates: Option<OnTruncate>,
    schema: SchemaSettings,
    columns: BTreeMap<ColumnPath, SchemaSettings>,
    hints: BTreeMap<ColumnPath, LogicalType>,
    retention: RetentionLoss,
}

impl StreamPlan {
    /// A stream read in full and appended.
    pub fn new(name: StreamName) -> Self {
        Self {
            name,
            read: ReadMode::Full,
            write: WriteMode::Append,
            key: None,
            deletes: None,
            truncates: None,
            schema: SchemaSettings::default(),
            columns: BTreeMap::new(),
            hints: BTreeMap::new(),
            retention: RetentionLoss::default(),
        }
    }

    /// Sets what the stream does when its source's retention dropped where a read would resume
    /// (default: the run fails).
    #[must_use]
    pub fn on_retention_loss(mut self, retention: RetentionLoss) -> Self {
        self.retention = retention;
        self
    }

    /// What the stream does when its source's retention dropped where a read would resume.
    pub fn retention_loss(&self) -> RetentionLoss {
        self.retention
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

    /// Sets what a change stream's deletes do; hard by default.
    ///
    /// Only change streams merged by key (`cdc` read, `merge` write) take it.
    #[must_use]
    pub fn deletes(mut self, mode: DeleteMode) -> Self {
        self.deletes = Some(mode);
        self
    }

    /// Sets what a change stream's truncates do; applied by default.
    ///
    /// Only change streams merged by key take it.
    #[must_use]
    pub fn on_truncate(mut self, mode: OnTruncate) -> Self {
        self.truncates = Some(mode);
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
    ///
    /// A normalized stream stores a hinted column whole.
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

    /// The same stream without its hints, which name columns of its own table: its child
    /// tables' settings.
    pub(crate) fn without_hints(&self) -> Self {
        Self {
            hints: BTreeMap::new(),
            ..self.clone()
        }
    }

    /// Every column the plan hints a type for.
    pub(crate) fn hinted_columns(&self) -> impl Iterator<Item = &ColumnPath> {
        self.hints.keys()
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

    /// What the stream's deletes do.
    pub fn delete_mode(&self) -> DeleteMode {
        self.deletes.unwrap_or_default()
    }

    /// What the stream's truncates do.
    pub fn truncate_mode(&self) -> OnTruncate {
        self.truncates.unwrap_or_default()
    }

    /// Whether the stream matches rows by key: merged, or kept as history.
    pub(crate) fn keyed(&self) -> bool {
        matches!(self.write, WriteMode::Merge | WriteMode::History)
    }

    /// Whether the stream keeps every version of each key.
    pub(crate) fn keeps_history(&self) -> bool {
        self.write == WriteMode::History
    }

    /// Whether the stream is read as changes and matched by key, so its deletes and truncates
    /// change its table.
    pub(crate) fn merges_changes(&self) -> bool {
        self.read == ReadMode::Cdc && self.keyed()
    }
}

/// A pipeline and the streams one run of it loads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PipelinePlan {
    pipeline: PipelineId,
    streams: Vec<StreamPlan>,
    schema: SchemaSettings,
    wal: bool,
    until: Until,
}

impl PipelinePlan {
    /// Validates `streams` of `pipeline`: at least one stream, distinct names and tables, supported
    /// combinations of read and write modes (`full` with `append`, `replace` or `merge`,
    /// `incremental` and `cdc` with `append` or `merge`), a key only on merge streams, delete and
    /// truncate modes only on `cdc` streams merged by key, and settings, hints and keys naming
    /// top-level columns.
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
            wal: false,
            until: Until::default(),
        })
    }

    /// Sets the pipeline's schema settings, which every stream inherits.
    #[must_use]
    pub fn schema(mut self, settings: SchemaSettings) -> Self {
        self.schema = settings;
        self
    }

    /// Keeps a write-ahead log of what each load writes, where `enabled`, as a stream whose source
    /// cannot read again what it acknowledged does whatever this says (spec §15.6).
    #[must_use]
    pub fn with_wal(mut self, enabled: bool) -> Self {
        self.wal = enabled;
        self
    }

    /// Reads as `until` says: until the source has caught up (the default), forever, or for a
    /// while (spec §9.6).
    #[must_use]
    pub fn with_until(mut self, until: Until) -> Self {
        self.until = until;
        self
    }

    /// How long a run of the pipeline reads.
    pub fn until(&self) -> Until {
        self.until
    }

    /// Whether a run follows its source or reads changes, so it commits as a stream does.
    pub(crate) fn commits_as_stream(&self) -> bool {
        self.until.follows()
            || self
                .streams
                .iter()
                .any(|stream| stream.read_mode() == ReadMode::Cdc)
    }

    /// Whether the pipeline asks for a write-ahead log.
    pub fn logs_ahead(&self) -> bool {
        self.wal
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
    if stream.key.is_some() && !stream.keyed() {
        return refuse(
            "plan_key_unused",
            "a key is set, but only merge and history streams match rows by key",
        );
    }
    if (stream.deletes.is_some() || stream.truncates.is_some()) && !stream.merges_changes() {
        return refuse(
            "plan_deletes_unused",
            "a delete or truncate mode is set, but only change streams matched by key apply them",
        );
    }
    match (stream.read, stream.write) {
        (ReadMode::Incremental | ReadMode::Cdc, WriteMode::Replace) => refuse(
            "plan_mode_invalid",
            "replace needs a full read, since the new generation replaces every row",
        ),
        _ => Ok(()),
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
