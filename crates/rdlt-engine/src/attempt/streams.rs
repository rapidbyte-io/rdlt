//! Planning an attempt's streams: checking them against the catalog and the destination,
//! preparing their tables, and choosing the partitions to read.

use std::collections::BTreeSet;
use std::sync::Arc;

use rdlt_connector::{
    Capabilities, Catalog, Checkpointing, ColumnKey, ColumnPath, Cursor, GenerationId, Partition,
    PartitionState, PipelineState, ReadMode, SchemaVersion, StreamName, StreamSpec, StreamState,
    TablePath, TableRef,
};

use super::{Planned, RunContext};
use crate::coordinator::{Cycle, StreamRun};
use crate::error::{Error, Side};
use crate::naming::Naming;
use crate::normalize::{self, Shape};
use crate::plan::{StreamPlan, WriteMode};
use crate::policy::Nested;
use crate::table::{Incoming, LineageColumns, MetaNames, Model, Resolver, Settings, Tables};

/// What planning an attempt's streams needs besides the stream itself.
pub(super) struct Planning<'a> {
    pub(super) context: &'a RunContext,
    pub(super) catalog: &'a Catalog,
    pub(super) state: &'a PipelineState,
    pub(super) naming: Naming,
    pub(super) capabilities: Arc<Capabilities>,
}

impl Planning<'_> {
    /// Checks `plan` against the catalog, the destination and committed state, prepares its
    /// table, and plans its partitions.
    ///
    /// The table keeps its committed identifier and names; a new one gets a free identifier. A
    /// declared schema is resolved like a batch, so it creates the table or changes it under the
    /// stream's policy before anything is read.
    pub(super) async fn stream(
        &mut self,
        plan: &StreamPlan,
        tables: &Tables,
    ) -> Result<Planned, Error> {
        let name = plan.name();
        let spec = check_stream(self.context, plan, self.catalog)?;
        let read = read(self.context, plan, self.state.streams.get(name));
        let generation = match (plan.write_mode(), &read) {
            (WriteMode::Replace, Read::Cycle(cycle, _)) => Some(cycle.generation),
            _ => None,
        };
        let shape = normalized(self.context, plan, spec);
        let (resolver, table, model) =
            self.table(plan, spec, generation, tables, shape.is_some())?;
        let index = tables.add_normalized(resolver, &table, model, shape.clone());
        if let Some(declared) = spec.schema() {
            let incoming = match &shape {
                Some(shape) => {
                    tables.declare_children(index, normalize::declared_arrays(declared, shape));
                    normalize::root_columns(declared, shape)?
                }
                None => Incoming::from(declared.clone()),
            };
            tables.fit(index, &incoming).await?;
        }
        tables.create_generation(index).await?;
        tables.add_lineage(index).await?;
        for child in tables.recorded_children(index) {
            tables.child(index, &child).await?;
        }
        let (cycle, partitions) = match read {
            Read::Incremental(state) => (None, partitions(self.context, name, &state).await?),
            Read::Cycle(cycle, state) => {
                (Some(cycle), partitions(self.context, name, &state).await?)
            }
            Read::Completed => (None, Vec::new()),
        };
        Ok(Planned {
            stream: StreamRun {
                name: name.clone(),
                write: plan.write_mode(),
                table: index,
                cycle,
                remaining: partitions.len(),
                stopped: false,
            },
            on_demand: spec.checkpointing() == Checkpointing::OnDemand,
            partitions,
        })
    }

    /// The stream's table as committed, or with a free identifier when new, and the resolver of
    /// its batches; a merge stream's key columns are named up front, so writers learn the key.
    fn table(
        &self,
        plan: &StreamPlan,
        spec: &StreamSpec,
        generation: Option<GenerationId>,
        tables: &Tables,
        normalized: bool,
    ) -> Result<(Resolver, TableRef, Model), Error> {
        let name = plan.name();
        let key = merge_key(plan, spec)?;
        let path = TablePath::new([name.to_string()]).map_err(|error| {
            Error::internal(format!("stream {name} has no valid table path: {error}"))
        })?;
        let mut model = Model::from_state(tables.recorded_table(&path))?;
        let physical = tables.name(&path, &self.naming)?;
        let lineage = if normalized {
            LineageColumns::Root
        } else {
            LineageColumns::None
        };
        let meta = MetaNames::assign(&self.naming, !key.is_empty(), lineage)?;
        let keys: BTreeSet<ColumnKey> = key.iter().cloned().map(ColumnKey::Source).collect();
        self.naming
            .assign_columns(&mut model.names, &keys, &meta.all())?;
        let table = TableRef {
            path,
            name: physical,
            version: SchemaVersion(model.version),
            generation,
            merge: None,
        };
        let resolver = Resolver {
            stream: name.clone(),
            settings: Settings {
                pipeline: *self.context.plan.schema_settings(),
                stream: plan.clone(),
                key,
            },
            capabilities: Arc::clone(&self.capabilities),
            naming: self.naming.clone(),
            meta,
            root: None,
        };
        Ok((resolver, table, model))
    }
}

/// How `plan`'s stream normalizes, if its settings or the pipeline's say it does.
///
/// Its rows are identified by the merge key or the source's primary key where there is one.
fn normalized(context: &RunContext, plan: &StreamPlan, spec: &StreamSpec) -> Option<Shape> {
    let pipeline = context.plan.schema_settings();
    let nested = plan
        .schema_settings()
        .nested_setting()
        .or_else(|| pipeline.nested_setting());
    let Some(Nested::Normalize { max_depth }) = nested else {
        return None;
    };
    let whole = plan
        .columns()
        .filter(|(_, settings)| {
            matches!(
                settings.nested_setting(),
                Some(Nested::Native | Nested::Json)
            )
        })
        .filter_map(|(column, _)| column.segments().next().map(Arc::from))
        .collect();
    let key = plan
        .merge_key()
        .or_else(|| spec.primary_key())
        .unwrap_or_default()
        .iter()
        .filter_map(|column| column.segments().next().map(Arc::from))
        .collect();
    Some(Shape {
        max_depth,
        whole,
        key,
    })
}

/// The stream's catalog entry, once the source can read it as planned and the destination can
/// write it as planned.
fn check_stream<'a>(
    context: &RunContext,
    plan: &StreamPlan,
    catalog: &'a Catalog,
) -> Result<&'a StreamSpec, Error> {
    let name = plan.name();
    let refuse = |code: &str, detail: &str| {
        Error::config(format!("stream {name}: {detail}"))
            .with_code(code)
            .with_stream(name)
    };
    let spec = catalog.get(name).ok_or_else(|| {
        refuse(
            "stream_not_found",
            "the source's catalog has no such stream",
        )
    })?;
    if !spec.supports(plan.read_mode()) {
        let detail = format!("the source cannot read it as {:?}", plan.read_mode());
        return Err(refuse("read_mode_unsupported", &detail));
    }
    let modes = context.destination.capabilities().write_modes;
    let writable = match plan.write_mode() {
        WriteMode::Append => modes.append,
        WriteMode::Replace => modes.replace,
        WriteMode::Merge => modes.merge,
    };
    if !writable {
        let detail = format!("the destination cannot write {:?}", plan.write_mode());
        return Err(refuse("write_mode_unsupported", &detail));
    }
    Ok(spec)
}

/// The columns a merge stream matches rows by: the plan's key, or else the stream's primary key;
/// none for other streams.
pub(super) fn merge_key(plan: &StreamPlan, spec: &StreamSpec) -> Result<Vec<ColumnPath>, Error> {
    if plan.write_mode() != WriteMode::Merge {
        return Ok(Vec::new());
    }
    let name = plan.name();
    let key = plan
        .merge_key()
        .or_else(|| spec.primary_key())
        .ok_or_else(|| {
            Error::config(format!(
                "stream {name}: merging needs a key, and neither the plan nor the source's \
                 catalog names one"
            ))
            .with_code("merge_key_missing")
            .with_stream(name)
        })?;
    if key.iter().any(|column| column.segments().count() > 1) {
        return Err(Error::config(format!(
            "stream {name}: the merge key names a nested column"
        ))
        .with_code("plan_column_nested")
        .with_stream(name));
    }
    Ok(key.to_vec())
}

/// How an attempt reads a stream.
enum Read {
    /// From its committed partitions.
    Incremental(StreamState),
    /// As one full read, planned from the given state.
    Cycle(Cycle, StreamState),
    /// Not at all: this run already completed the stream's full read.
    Completed,
}

/// How an attempt reads `plan`, given its committed state.
///
/// A full read resumes the read state records as in progress, whichever run started it. Without
/// one, it starts a new read from the beginning, whose first commit deletes the previous read's
/// partition entries, unless this run already completed a full read of the stream.
fn read(context: &RunContext, plan: &StreamPlan, committed: Option<&StreamState>) -> Read {
    let committed = committed.cloned().unwrap_or_default();
    if plan.read_mode() != ReadMode::Full {
        return Read::Incremental(committed);
    }
    let mut cycles = context.cycles.lock();
    if let Some(generation) = committed.generation {
        cycles.insert(plan.name().clone(), generation);
        let cycle = Cycle {
            generation,
            recorded: true,
            stale: Vec::new(),
            finished: false,
            completed: committed.completed.clone(),
        };
        return Read::Cycle(cycle, committed);
    }
    let generation = *cycles
        .entry(plan.name().clone())
        .or_insert_with(|| GenerationId(context.env.random()));
    if committed.completed.contains(&generation) {
        return Read::Completed;
    }
    let cycle = Cycle {
        generation,
        recorded: false,
        stale: committed.partitions.keys().cloned().collect(),
        finished: false,
        completed: committed.completed.clone(),
    };
    Read::Cycle(cycle, StreamState::default())
}

/// The partitions of `name` still to read, with their committed cursors.
async fn partitions(
    context: &RunContext,
    name: &StreamName,
    state: &StreamState,
) -> Result<Vec<(Partition, Option<Cursor>)>, Error> {
    let planned = context.source.plan(name, state).await.map_err(|error| {
        Error::connector(Side::Source, format!("planning stream {name}"), error).with_stream(name)
    })?;
    Ok(planned
        .into_iter()
        .filter_map(|partition| match state.partitions.get(partition.id()) {
            Some(PartitionState::Done) => None,
            Some(PartitionState::Cursor(cursor)) => Some((partition, Some(cursor.clone()))),
            None => Some((partition, None)),
        })
        .collect())
}
