//! The tables of one attempt: their current views, the schema changes partitions make, and what
//! the next commit records about them.

mod children;
#[cfg(test)]
mod tests;

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use rdlt_connector::{
    ConnectorError, PipelineState, SchemaVersion, StateChange, StateEntry, TableChange, TablePath,
    TableRef, TableState,
};

use super::model::Model;
use super::resolve::{Change, Incoming, Resolution, Resolver, Route};
use super::session::SharedSession;
use super::{LoweringPlan, TableView};
use crate::error::{Error, Side};
use crate::naming::Naming;
use crate::normalize::Shape;

pub(crate) use children::Admission;

/// How many times a table's change is named around columns that attempts which never committed
/// left behind before the conflict fails the run.
pub(crate) const CONFLICT_RETRIES: usize = 4;

/// Lowering plans kept per table: one per incoming schema its partitions send, for its current
/// view.
const PLANS: usize = 8;

/// One table and its resolver.
#[derive(Debug)]
struct Slot {
    resolver: Resolver,
    current: Mutex<Arc<TableView>>,
    /// Held while a schema change is worked out and applied, so changes to one table never race.
    evolving: tokio::sync::Mutex<()>,
    /// The schema version state records.
    recorded: Mutex<u32>,
    /// Plans for the current view, the newest last.
    plans: Mutex<Vec<Arc<LoweringPlan>>>,
    /// How the stream normalizes, for a normalized stream's own table.
    shape: Option<Arc<Shape>>,
}

/// Child tables' indexes by their stream's table and their path below it.
type Children = BTreeMap<(usize, Vec<Arc<str>>), usize>;

/// Every table of an attempt: each stream's own, and the child tables of normalized streams,
/// added as their first rows arrive.
#[derive(Debug)]
pub(crate) struct Tables {
    session: Arc<SharedSession>,
    slots: RwLock<Vec<Arc<Slot>>>,
    /// Child tables by their stream's table and their path below it.
    children: Mutex<Children>,
    /// The arrays below each normalized stream's table that its declared schema holds, by that
    /// table and their path below it.
    declared: Mutex<BTreeSet<(usize, Vec<Arc<str>>)>>,
    /// Held while a child table is added, so each is added once.
    adding: tokio::sync::Mutex<()>,
    /// The tables state records, whose names and schemas tables keep.
    committed: BTreeMap<TablePath, TableState>,
    /// Table identifiers taken: committed ones, then those this attempt assigns.
    taken: Mutex<BTreeSet<String>>,
}

/// What a commit records about the tables, and the versions it records.
#[derive(Debug, Default)]
pub(crate) struct TablesDelta {
    pub(crate) changes: Vec<StateChange>,
    pub(crate) versions: Vec<(usize, u32)>,
}

impl Tables {
    /// The tables of an attempt over `session`, where state records no table yet.
    pub(crate) fn new(session: Arc<SharedSession>) -> Self {
        Self {
            session,
            slots: RwLock::new(Vec::new()),
            children: Mutex::new(BTreeMap::new()),
            declared: Mutex::new(BTreeSet::new()),
            adding: tokio::sync::Mutex::new(()),
            committed: BTreeMap::new(),
            taken: Mutex::new(BTreeSet::new()),
        }
    }

    /// The same tables, over the tables `state` records.
    #[must_use]
    pub(crate) fn committed(mut self, state: &PipelineState) -> Self {
        self.taken = Mutex::new(
            state
                .tables
                .values()
                .filter_map(|table| table.physical.as_deref().map(str::to_owned))
                .collect(),
        );
        self.committed = state.tables.clone();
        self
    }

    /// The session the tables change through.
    pub(crate) fn session(&self) -> &Arc<SharedSession> {
        &self.session
    }

    /// The table at `path` as state records it, if it does.
    pub(crate) fn recorded_table(&self, path: &TablePath) -> Option<&TableState> {
        self.committed.get(path)
    }

    /// The identifier of the table at `path`: the committed one, or a free one under `naming`.
    pub(crate) fn name(&self, path: &TablePath, naming: &Naming) -> Result<Arc<str>, Error> {
        let mut taken = self.taken.lock();
        let physical = match self
            .committed
            .get(path)
            .and_then(|table| table.physical.clone())
        {
            Some(physical) => physical,
            None => naming.table(path, &taken)?.into(),
        };
        taken.insert(physical.to_string());
        Ok(physical)
    }

    /// Adds the table `table` at its committed `model`; returns its index.
    pub(crate) fn add(&self, resolver: Resolver, table: &TableRef, model: Model) -> usize {
        self.add_normalized(resolver, table, model, None)
    }

    /// Adds the table `table` at its committed `model`, whose stream normalizes as `shape`, if
    /// it does; returns its index.
    pub(crate) fn add_normalized(
        &self,
        resolver: Resolver,
        table: &TableRef,
        model: Model,
        shape: Option<Shape>,
    ) -> usize {
        let recorded = model.version;
        let view = TableView::new(table, model, &resolver);
        let mut slots = self.slots.write();
        slots.push(Arc::new(Slot {
            resolver,
            current: Mutex::new(Arc::new(view)),
            evolving: tokio::sync::Mutex::new(()),
            recorded: Mutex::new(recorded),
            plans: Mutex::new(Vec::new()),
            shape: shape.map(Arc::new),
        }));
        slots.len() - 1
    }

    fn slot(&self, table: usize) -> Arc<Slot> {
        Arc::clone(&self.slots.read()[table])
    }

    /// The current view of `table`.
    pub(crate) fn view(&self, table: usize) -> Arc<TableView> {
        Arc::clone(&self.slot(table).current.lock())
    }

    /// How the stream whose table is `table` normalizes, if it does.
    pub(crate) fn shape(&self, table: usize) -> Option<Arc<Shape>> {
        self.slot(table).shape.clone()
    }

    /// The plan lowering batches of `incoming` into `table`: the plan made for the table's current
    /// view and `incoming`, or a new one once the schema changes `incoming` needs are applied.
    pub(crate) async fn plan(
        &self,
        table: usize,
        incoming: impl Into<Incoming>,
    ) -> Result<Arc<LoweringPlan>, Error> {
        let incoming = incoming.into();
        let slot = self.slot(table);
        let view = self.view(table);
        let planned = |plans: &[Arc<LoweringPlan>], view: &Arc<TableView>| {
            plans
                .iter()
                .find(|plan| Arc::ptr_eq(plan.view(), view) && *plan.incoming() == incoming)
                .cloned()
        };
        if let Some(plan) = planned(&slot.plans.lock(), &view) {
            return Ok(plan);
        }
        let (view, routes) = self.fit(table, &incoming).await?;
        let mut plans = slot.plans.lock();
        if let Some(plan) = planned(&plans, &view) {
            return Ok(plan);
        }
        let stream = slot.resolver.stream.clone();
        let plan = Arc::new(LoweringPlan::new(
            stream,
            Arc::clone(&view),
            incoming,
            routes,
        ));
        plans.retain(|plan| Arc::ptr_eq(plan.view(), &view));
        if plans.len() == PLANS {
            plans.remove(0);
        }
        plans.push(Arc::clone(&plan));
        Ok(plan)
    }

    /// How columns of `incoming` fit `table`, after applying the schema changes they need.
    ///
    /// Batches that fit need no lock. A change is worked out again under the table's lock against
    /// the latest view, since another partition may have changed the table meanwhile, and applied
    /// through the session before any batch is written under it.
    pub(crate) async fn fit(
        &self,
        table: usize,
        incoming: &Incoming,
    ) -> Result<(Arc<TableView>, Vec<Route>), Error> {
        let slot = self.slot(table);
        let view = self.view(table);
        let unchanged = |resolution: &Resolution, view: &TableView| {
            resolution.model.version == view.model.version
        };
        let resolution = slot.resolver.resolve(&view.model, incoming)?;
        if unchanged(&resolution, &view) {
            return Ok((view, resolution.routes));
        }
        let _evolving = slot.evolving.lock().await;
        let view = self.view(table);
        let resolution = slot.resolver.resolve(&view.model, incoming)?;
        if unchanged(&resolution, &view) {
            return Ok((view, resolution.routes));
        }
        let (mut resolver, mut resolution, mut retries) =
            (Cow::Borrowed(&slot.resolver), resolution, 0);
        let next = loop {
            match self.evolve(table, &view, resolution, &resolver).await? {
                Ok(next) => break next,
                Err(error) if retries == CONFLICT_RETRIES => return Err(self.refused(table, error)),
                Err(_conflict) => {
                    resolver = Cow::Owned(slot.resolver.hashing(retries as u64));
                    resolution = resolver.resolve(&view.model, incoming)?;
                    retries += 1;
                }
            }
        };
        *slot.current.lock() = Arc::clone(&next.0);
        Ok(next)
    }

    /// Applies what `resolution` changes in `view`; the destination's `schema_conflict`, from
    /// columns an attempt that never committed left behind, is returned for the caller to name
    /// around.
    async fn evolve(
        &self,
        table: usize,
        view: &TableView,
        resolution: Resolution,
        resolver: &Resolver,
    ) -> Result<Result<(Arc<TableView>, Vec<Route>), ConnectorError>, Error> {
        let next = Arc::new(TableView::new(&view.table, resolution.model, resolver));
        let changes = table_changes(view, &next, &resolution.changes);
        match self.session.apply_schema(&changes).await? {
            Ok(()) => Ok(Ok((next, resolution.routes))),
            Err(error) if error.code() == Some("schema_conflict") => Ok(Err(error)),
            Err(error) => Err(self.refused(table, error)),
        }
    }

    /// Applies `changes` to `table` through the session.
    async fn apply(&self, table: usize, changes: &[TableChange]) -> Result<(), Error> {
        self.session
            .apply_schema(changes)
            .await?
            .map_err(|error| self.refused(table, error))
    }

    /// The error for the destination refusing a change to `table`.
    fn refused(&self, table: usize, error: ConnectorError) -> Error {
        let slot = self.slot(table);
        let stream = &slot.resolver.stream;
        let context = format!("changing the table of stream {stream}");
        Error::connector(Side::Destination, context, error).with_stream(stream)
    }

    /// Adds the lineage column of a stream's table created before the stream normalized; a table
    /// that already has it changes nothing, as a generation created with it does.
    pub(crate) async fn add_lineage(&self, table: usize) -> Result<(), Error> {
        let view = self.view(table);
        let Some(id) = &view.meta.id else {
            return Ok(());
        };
        if !view.model.created() {
            return Ok(());
        }
        let changes: Vec<TableChange> = view.physical[view.model.columns.len()..]
            .iter()
            .filter(|field| field.name() == id.as_ref())
            .map(|field| TableChange::AddColumn {
                table: view.table.clone(),
                field: field.clone(),
            })
            .collect();
        self.apply(table, &changes).await
    }

    /// Creates `table`'s generation, a replace stream's hidden copy, with the table's columns.
    pub(crate) async fn create_generation(&self, table: usize) -> Result<(), Error> {
        let view = self.view(table);
        if !view.model.created() || view.table.generation.is_none() {
            return Ok(());
        }
        let create = TableChange::Create {
            table: view.table.clone(),
            schema: view.physical_schema(),
        };
        self.apply(table, &[create]).await
    }

    /// The schema and names of every table changed since state last recorded it.
    pub(crate) fn delta(&self) -> TablesDelta {
        let mut delta = TablesDelta::default();
        let slots = self.slots.read().clone();
        for (index, slot) in slots.iter().enumerate() {
            let view = self.view(index);
            if view.model.version <= *slot.recorded.lock() {
                continue;
            }
            let path = view.table.path.clone();
            let schema = StateEntry::Schema {
                table: path.clone(),
                version: SchemaVersion(view.model.version),
                schema: view.model.schema(),
            };
            let names = StateEntry::Names {
                table: path,
                physical: Arc::clone(&view.table.name),
                names: view.model.names.clone(),
            };
            delta.changes.push(StateChange::Put(schema.to_record()));
            delta.changes.push(StateChange::Put(names.to_record()));
            delta.versions.push((index, view.model.version));
        }
        delta
    }

    /// Notes that state now records `versions`.
    pub(crate) fn recorded(&self, versions: &[(usize, u32)]) {
        for (index, version) in versions {
            let slot = self.slot(*index);
            let mut recorded = slot.recorded.lock();
            *recorded = (*recorded).max(*version);
        }
    }
}

/// The destination changes that take `before` to `after`: the whole table for its first version,
/// then each added column and each widening that changes how the destination stores a column.
fn table_changes(before: &TableView, after: &TableView, changes: &[Change]) -> Vec<TableChange> {
    let table = after.table.clone();
    if !before.model.created() {
        return vec![TableChange::Create {
            table,
            schema: after.physical_schema(),
        }];
    }
    changes
        .iter()
        .filter_map(|change| match change {
            Change::Add { key } => {
                let (index, _) = after.model.column(key)?;
                Some(TableChange::AddColumn {
                    table: table.clone(),
                    field: after.physical[index].clone(),
                })
            }
            Change::Widen { column, .. } => {
                let (from, to) = (&before.lowered[*column], &after.lowered[*column]);
                (from != to).then(|| TableChange::Widen {
                    table: table.clone(),
                    column: after.model.columns[*column].name().into(),
                    from: from.clone(),
                    to: to.clone(),
                })
            }
        })
        .collect()
}
