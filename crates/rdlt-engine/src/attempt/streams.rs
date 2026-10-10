//! Planning an attempt's streams: checking them against the catalog and the destination,
//! preparing their tables, and choosing the partitions to read.

mod reads;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use rdlt_connector::{
    Capabilities, Catalog, Checkpointing, ColumnKey, ColumnPath, GenerationId, PipelineState,
    ReadMode, SchemaVersion, StreamSpec, TablePath, TableRef,
};

use self::reads::{Partitioned, Read, Sourced, read, sourced};
use super::check::check_stream;
use super::{Planned, RunContext, sequences, together};
use crate::coordinator::{Cycle, Phases, StreamRun, Template};
use crate::error::Error;
use crate::limits::PLANS_AT_ONCE;
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

impl<'a> Planning<'a> {
    /// Plans `plans`' streams: each checked in plan order against the catalog and the
    /// destination, then their partitions planned by the source all together, [`PLANS_AT_ONCE`]
    /// at most at once, then, in plan order, each one's table prepared and its partitions taken.
    ///
    /// The source plans no stream the checks refuse, nor any after it: the streams before it are
    /// planned and prepared, and its refusal is returned unless one of them fails. The first
    /// error in plan order is returned, whichever failed first, and no stream after it changes a
    /// table. Each stream's read is chosen in plan order before any plan starts, as a new full
    /// read draws its generation.
    pub(super) async fn streams(
        &mut self,
        plans: &[StreamPlan],
        tables: &Tables,
    ) -> Result<Vec<Planned>, Error> {
        let mut accepted = Vec::with_capacity(plans.len());
        let mut refused = None;
        for plan in plans {
            match checked(self.context, plan, self.catalog) {
                Ok(checked) => accepted.push(checked),
                Err(error) => {
                    refused = Some(error);
                    break;
                }
            }
        }
        let plans = &plans[..accepted.len()];
        let reads: Vec<Read> = plans
            .iter()
            .map(|plan| {
                let name = plan.name();
                let reset = self.state.resets.get(name).copied();
                read(self.context, plan, self.state.streams.get(name), reset)
            })
            .collect();
        let context = self.context;
        let sourcing = plans
            .iter()
            .zip(reads)
            .map(|(plan, read)| sourced(context, plan, read))
            .collect();
        let sourced = together::in_order(sourcing, PLANS_AT_ONCE).await;
        let mut planned = Vec::with_capacity(plans.len());
        for ((plan, checked), sourced) in plans.iter().zip(accepted).zip(sourced) {
            planned.push(self.stream(plan, checked, tables, sourced).await?);
        }
        refused.map_or(Ok(planned), Err)
    }

    /// Prepares the table of `plan`, as `checked` accepted it, and takes the partitions
    /// `sourced` planned.
    ///
    /// The table keeps its committed identifier and names; a new one gets a free identifier. A
    /// declared schema is resolved like a batch, so it creates the table or changes it under the
    /// stream's policy before anything is read.
    async fn stream(
        &mut self,
        plan: &StreamPlan,
        checked: Checked<'a>,
        tables: &Tables,
        sourced: Sourced,
    ) -> Result<Planned, Error> {
        let Checked { spec, shape, key } = checked;
        let generation = match (plan.write_mode(), &sourced.cycle) {
            (WriteMode::Replace, Some(cycle)) => Some(cycle.generation),
            _ => None,
        };
        let (resolver, table, model) =
            self.table(plan, spec, &key, generation, tables, shape.is_some())?;
        // Refused before the table changes, and recorded by the attempt's first commit.
        let recorded = tables.recorded_table(&table.path);
        let sequences = sequences::to_record(plan, recorded, &key, spec.change_time())?
            .map(|keying| (table.path.clone(), keying));
        let index = tables.add_normalized(resolver, &table, model, shape.clone())?;
        let mut children = Vec::new();
        if let Some(declared) = spec.schema() {
            let incoming = match &shape {
                Some(shape) => {
                    let made = normalize::declared::children(declared, shape)?;
                    children = tables.declared_children(index, made)?;
                    normalize::declared::root_columns(declared, shape)?
                }
                None => Incoming::declared(declared.clone()),
            };
            tables.fit(index, &incoming).await?;
        }
        tables.create_generation(index).await?;
        tables.add_meta_columns(index).await?;
        for (path, incoming) in children {
            let child = tables.child(index, &path).await?;
            tables.fit(child, &incoming).await?;
        }
        for child in tables.recorded_children(index) {
            tables.child(index, &child).await?;
        }
        let (cycle, partitioned) = (sourced.cycle, sourced.partitioned?);
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
        key: &[ColumnPath],
        generation: Option<GenerationId>,
        tables: &Tables,
        normalized: bool,
    ) -> Result<(Resolver, TableRef, Model), Error> {
        let name = plan.name();
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
        let meta = MetaNames::assign_changes(&self.naming, !key.is_empty(), lineage, layout(plan));
        let meta = if plan.keeps_history() {
            meta.with_history(&self.naming, change_time(spec))
        } else {
            meta
        };
        let keys: BTreeSet<ColumnKey> = key.iter().cloned().map(ColumnKey::Source).collect();
        self.naming
            .assign_columns(&mut model.names, &keys)
            .map_err(|error| error.with_stream(name))?;
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
                key: key.to_vec(),
                owner: None,
            },
            capabilities: Arc::clone(&self.capabilities),
            naming: self.naming.clone(),
            meta,
            root: None,
            columns: self.context.config.limits().schema_columns,
            unwidened: BTreeSet::new(),
        };
        Ok((resolver, table, model))
    }
}

/// A stream the checks accept: its catalog entry, how it normalizes, and the columns it merges by.
struct Checked<'a> {
    spec: &'a StreamSpec,
    shape: Option<Shape>,
    key: Vec<ColumnPath>,
}

/// Checks `plan`'s settings against the catalog and the destination, none of which a stream's
/// table work changes.
fn checked<'a>(
    context: &RunContext,
    plan: &StreamPlan,
    catalog: &'a Catalog,
) -> Result<Checked<'a>, Error> {
    let spec = check_stream(context, plan, catalog)?;
    let shape = normalized(context, plan, spec);
    unnormalizable(plan, shape.is_some())?;
    let key = merge_key(plan, spec)?;
    Ok(Checked { spec, shape, key })
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
    // A history table's hashes need whole rows, which an update leaving columns unchanged lacks.
    let changes = cdc.then(|| ChangeMode {
        merge: plan.keyed(),
        deletes: plan.delete_mode(),
        truncates: plan.truncate_mode(),
        partial_updates: partial_updates && !plan.keeps_history(),
    });
    // Only change streams read in phases.
    let tracked = cdc || (follow && plan.read_mode() == ReadMode::Incremental);
    let phases = tracked.then(|| Phases {
        phase: partitioned.phase,
        reading: BTreeMap::new(),
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
            named: partitioned.named,
            replayable: true,
        },
        on_demand,
        changes,
        follow: follow && tracked,
        reset_retention,
        partitions: partitioned.partitions,
    }
}

/// Refuses `plan`'s stream where it is `normalized` but cannot normalize yet: a change stream, or
/// a history stream, whose child rows would need versions of their own.
fn unnormalizable(plan: &StreamPlan, normalized: bool) -> Result<(), Error> {
    if !normalized || (plan.read_mode() != ReadMode::Cdc && !plan.keeps_history()) {
        return Ok(());
    }
    let (code, what) = if plan.keeps_history() {
        ("history_normalize_unsupported", "a history stream")
    } else {
        ("normalize_changes_unsupported", "a change stream")
    };
    let name = plan.name();
    Err(
        Error::config(format!("stream {name}: {what} cannot normalize yet"))
            .with_code(code)
            .with_stream(name),
    )
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

/// How a change stream's table holds its changes: merged by key for a merge or history stream,
/// as a log otherwise; `None` for a stream not read as changes.
fn layout(plan: &StreamPlan) -> Option<ChangeLayout> {
    match (plan.read_mode(), plan.write_mode()) {
        (ReadMode::Cdc, WriteMode::Merge | WriteMode::History) => Some(ChangeLayout::Merge {
            soft: plan.delete_mode() == DeleteMode::Soft,
        }),
        (ReadMode::Cdc, _) => Some(ChangeLayout::Log),
        _ => None,
    }
}

/// The column a history stream's versions begin at, its source's change time: a top-level
/// column, as the stream's check makes it.
fn change_time(spec: &StreamSpec) -> Option<Arc<str>> {
    spec.change_time()
        .and_then(|column| column.segments().next())
        .map(Arc::from)
}

/// The columns a merge or history stream matches rows by: the plan's key, or else the stream's
/// primary key; none for other streams.
pub(super) fn merge_key(plan: &StreamPlan, spec: &StreamSpec) -> Result<Vec<ColumnPath>, Error> {
    if !plan.keyed() {
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
    // A plan's key names a column, as the plan checks; a catalog's may name none.
    if key.is_empty() {
        return Err(Error::config(format!(
            "stream {name}: merging needs a key, and the source's catalog names a key of no column"
        ))
        .with_code("merge_key_missing")
        .with_stream(name));
    }
    if key.iter().any(|column| column.segments().count() > 1) {
        return Err(Error::config(format!(
            "stream {name}: the merge key names a nested column"
        ))
        .with_code("plan_column_nested")
        .with_stream(name));
    }
    Ok(key.to_vec())
}
