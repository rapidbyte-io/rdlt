//! Planning an attempt's streams: checking them against the catalog and the destination,
//! preparing their tables, and choosing the partitions to read.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use rdlt_connector::{
    Capabilities, Catalog, Checkpointing, ColumnKey, ColumnPath, Cursor, GenerationId, Partition,
    PartitionId, PartitionState, PipelineState, ReadMode, SchemaVersion, StreamSpec, StreamState,
    TablePath, TableRef,
};

use super::check::check_stream;
use super::{Planned, RunContext, sequences};
use crate::coordinator::{Begun, Cycle, Phases, StreamRun, Template};
use crate::error::{Error, ErrorKind, Side};
use crate::naming::Naming;
use crate::normalize::{self, Shape};
use crate::partition::ChangeMode;
use crate::plan::{DeleteMode, RetentionLoss, StreamPlan, WriteMode};
use crate::policy::Nested;
use crate::table::{
    ChangeLayout, Incoming, LineageColumns, MetaNames, Model, Resolver, Settings, Tables,
};

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
        if shape.is_some() && plan.read_mode() == ReadMode::Cdc {
            return Err(Error::config(format!(
                "stream {name}: a change stream cannot normalize yet"
            ))
            .with_code("normalize_changes_unsupported")
            .with_stream(name));
        }
        let (resolver, table, model) =
            self.table(plan, spec, generation, tables, shape.is_some())?;
        // Refused before the table changes, and recorded by the attempt's first commit.
        let sequences = sequences::to_record(plan, tables.recorded_table(&table.path))?
            .map(|sequences| (table.path.clone(), sequences));
        let index = tables.add_normalized(resolver, &table, model, shape.clone());
        if let Some(declared) = spec.schema() {
            let incoming = match &shape {
                Some(shape) => {
                    tables.declare_children(index, normalize::declared_arrays(declared, shape));
                    normalize::root_columns(declared, shape)?
                }
                None => Incoming::declared(declared.clone()),
            };
            tables.fit(index, &incoming).await?;
        }
        tables.create_generation(index).await?;
        tables.add_meta_columns(index).await?;
        for child in tables.recorded_children(index) {
            tables.child(index, &child).await?;
        }
        let (cycle, partitioned) = match read {
            Read::Incremental(state) => (None, partitions(self.context, plan, &state).await?),
            Read::Cycle(cycle, state) => {
                (Some(cycle), partitions(self.context, plan, &state).await?)
            }
            Read::Completed => (None, Partitioned::default()),
        };
        let on_demand = spec.checkpointing() == Checkpointing::OnDemand;
        let partial_updates = self.capabilities.partial_updates;
        let follow = self.context.plan.until().follows();
        let mut planned = planned(
            plan,
            index,
            (on_demand, partial_updates, follow),
            cycle,
            partitioned,
        );
        planned.stream.sequences = sequences;
        planned.stream.replayable = spec.is_replayable();
        Ok(planned)
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
        let meta = MetaNames::assign_changes(&self.naming, !key.is_empty(), lineage, layout(plan))?;
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
                owner: None,
            },
            capabilities: Arc::clone(&self.capabilities),
            naming: self.naming.clone(),
            meta,
            root: None,
        };
        Ok((resolver, table, model))
    }
}

/// `plan`'s stream, its table at `index`, ready to load `partitioned`, checkpointing on demand,
/// changing unchanged columns and following its source as `(on_demand, partial_updates,
/// follow)` say.
///
/// A change stream reads in phases, and an incremental stream of a following run tracks its
/// partitions as one does, so it is planned again as it reads.
fn planned(
    plan: &StreamPlan,
    index: usize,
    (on_demand, partial_updates, follow): (bool, bool, bool),
    cycle: Option<Cycle>,
    partitioned: Partitioned,
) -> Planned {
    let cdc = plan.read_mode() == ReadMode::Cdc;
    let reset_retention = plan.retention_loss() == RetentionLoss::Reset;
    let changes = cdc.then(|| ChangeMode {
        merge: plan.write_mode() == WriteMode::Merge,
        deletes: plan.delete_mode(),
        truncates: plan.truncate_mode(),
        partial_updates,
    });
    // Only change streams read in phases (spec §9.4).
    let tracked = cdc || (follow && plan.read_mode() == ReadMode::Incremental);
    let phases = tracked.then(|| Phases {
        phase: partitioned.phase,
        reading: Vec::new(),
        committed: partitioned.committed,
        begun: partitioned.begun,
        settled: false,
        template: Template {
            table: index,
            on_demand,
            changes,
            reset_retention,
        },
    });
    Planned {
        stream: StreamRun {
            name: plan.name().clone(),
            write: plan.write_mode(),
            table: index,
            cycle,
            remaining: partitioned.partitions.len(),
            stopped: false,
            phases,
            sequences: None,
            replayable: true,
        },
        on_demand,
        changes,
        follow: follow && tracked,
        reset_retention,
        partitions: partitioned.partitions,
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
        .map(|(column, _)| column)
        .chain(plan.hinted_columns())
        .filter_map(|column| column.segments().next().map(Arc::from))
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

/// How a change stream's table holds its changes: merged by key for a merge stream, as a log
/// otherwise; `None` for a stream not read as changes.
fn layout(plan: &StreamPlan) -> Option<ChangeLayout> {
    match (plan.read_mode(), plan.write_mode()) {
        (ReadMode::Cdc, WriteMode::Merge) => Some(ChangeLayout::Merge {
            soft: plan.delete_mode() == DeleteMode::Soft,
        }),
        (ReadMode::Cdc, _) => Some(ChangeLayout::Log),
        _ => None,
    }
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

/// The partitions of a stream still to read, and its phase.
#[derive(Debug, Default)]
struct Partitioned {
    /// The phase they belong to.
    phase: u16,
    /// The committed positions of the phase's partitions.
    committed: BTreeMap<PartitionId, PartitionState>,
    /// When the plan begins a new phase, the entries of the phases before it and where its
    /// partitions start.
    begun: Option<Begun>,
    /// The partitions to read, each from its committed position or its start.
    partitions: Vec<(Partition, Option<Cursor>)>,
}

/// The partitions of `plan`'s stream still to read, with their committed cursors; a plan
/// beginning a new phase starts its partitions where it says.
///
/// Only a change stream reads in phases: a plan beginning one for any other stream is
/// `phase_unexpected`, a Source error, since the stream would start over from its plan every run.
async fn partitions(
    context: &RunContext,
    plan: &StreamPlan,
    state: &StreamState,
) -> Result<Partitioned, Error> {
    let name = plan.name();
    let planned = context.source.plan(name, state).await.map_err(|error| {
        Error::connector(Side::Source, format!("planning stream {name}"), error).with_stream(name)
    })?;
    if let Some(phase) = planned.phase.filter(|phase| *phase != state.phase) {
        if plan.read_mode() != ReadMode::Cdc {
            return Err(Error::new(
                ErrorKind::Source,
                format!("stream {name}: the source planned phase {phase} of a stream not read as changes"),
            )
            .with_code("phase_unexpected")
            .with_stream(name));
        }
        let begun = Begun {
            stale: state.partitions.keys().cloned().collect(),
            starts: planned.starts.clone(),
        };
        let partitions = planned
            .partitions
            .into_iter()
            .map(|partition| {
                let start = planned.starts.get(partition.id()).cloned();
                (partition, start)
            })
            .collect();
        return Ok(Partitioned {
            phase,
            committed: BTreeMap::new(),
            begun: Some(begun),
            partitions,
        });
    }
    let partitions = planned
        .partitions
        .into_iter()
        .filter_map(|partition| match state.partitions.get(partition.id()) {
            Some(PartitionState::Done) => None,
            Some(PartitionState::Cursor(cursor)) => Some((partition, Some(cursor.clone()))),
            None => Some((partition, None)),
        })
        .collect();
    Ok(Partitioned {
        phase: state.phase,
        committed: state.partitions.clone(),
        begun: None,
        partitions,
    })
}
