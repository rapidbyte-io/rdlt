//! What a Python stream says of itself and its plans, and a Python source of its catalog.

use rdlt_connector::{
    Checkpointing, ConnectorError, Cursor, Partition, PartitionId, PartitionPlan, ReadMode, Result,
    ResultExt as _, StreamName, StreamSpec, TableSchema,
};
use serde::Deserialize;
use serde_json::value::RawValue;

use super::PyCursor;

/// What a Python stream says of itself, as its `spec` answers it.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "the JSON a stream says itself in, each flag a property of its own"
)]
pub(super) struct Declared {
    pub(super) spec: DeclaredSpec,
    /// Whether its plan is its own, `plan=` or `partitions=`, rather than one partition.
    pub(super) planned: bool,
    /// Whether its plan is told where each partition stands.
    pub(super) stated: bool,
    /// Whether the stream is told which of its cursors are committed.
    pub(super) committed: bool,
    /// Whether the stream tells where it stands outside the engine.
    pub(super) acknowledged: bool,
    /// The format of the stream's cursors.
    pub(super) cursor_version: u16,
}

/// A stream's spec, as Python says it: a stream's own, or one of a catalog its source discovers.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DeclaredSpec {
    name: String,
    namespace: Option<String>,
    schema: Option<TableSchema>,
    primary_key: Option<Vec<String>>,
    read_modes: Vec<ReadMode>,
    checkpointing: Checkpointing,
    replayable: bool,
    change_time: Option<String>,
}

impl DeclaredSpec {
    /// The spec, its name checked as the Rust SDK checks one.
    pub(super) fn spec(self) -> Result<StreamSpec> {
        let name = match &self.namespace {
            Some(namespace) => StreamName::with_namespace(namespace, &self.name),
            None => StreamName::new(&self.name),
        };
        let shown = match &self.namespace {
            Some(namespace) => format!("{namespace}.{}", self.name),
            None => self.name.clone(),
        };
        let name = name.config(format!("stream name {shown:?}"))?;
        let mut spec = StreamSpec::new(name)
            .with_read_modes(self.read_modes)
            .with_checkpointing(self.checkpointing)
            .with_replayable(self.replayable);
        if let Some(key) = &self.primary_key {
            spec = spec.with_primary_key(key.iter().map(String::as_str));
        }
        if let Some(schema) = self.schema {
            spec = spec.with_schema(schema);
        }
        if let Some(column) = &self.change_time {
            spec = spec.with_change_time(column.as_str());
        }
        Ok(spec)
    }
}

/// `answered`, JSON a Python stream or source said, as `T`.
pub(super) fn read_as<T: serde::de::DeserializeOwned>(answered: &str, what: &str) -> Result<T> {
    serde_json::from_str(answered).map_err(|error| {
        ConnectorError::internal(format!("{what} in JSON rdlt cannot read: {error}"))
    })
}

/// A plan as a Python stream answers it: its phase, each partition's id and whether it never
/// ends, and each start's partition and cursor as JSON.
pub(super) type Planned = (Option<u16>, Vec<(String, bool)>, Vec<(String, String)>);

/// The plan `planned` says of `stream`, each start's cursor in the stream's format `version`.
pub(super) fn plan_of(
    planned: Planned,
    stream: &StreamName,
    version: u16,
) -> Result<PartitionPlan> {
    let (phase, partitions, starts) = planned;
    let partitions = partitions
        .iter()
        .map(|(key, unbounded)| {
            let partition = Partition::new(partition(key)?);
            Ok(if *unbounded {
                partition.unbounded()
            } else {
                partition
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut plan = PartitionPlan::new(partitions);
    plan.phase = phase;
    for (key, cursor) in starts {
        let raw = RawValue::from_string(cursor).map_err(|error| {
            ConnectorError::internal(format!(
                "stream {stream}: where partition {key:?} starts is not JSON: {error}"
            ))
        })?;
        let cursor = Cursor::encode(version, &PyCursor(Some(raw)))?;
        plan.starts.insert(partition(&key)?, cursor);
    }
    Ok(plan)
}

/// The partition `key` names, as a Python plan names it.
fn partition(key: &str) -> Result<PartitionId> {
    PartitionId::parse(key).config(format!("partition {key:?}"))
}
