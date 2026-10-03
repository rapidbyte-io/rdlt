//! The tables of one attempt: their current views, the schema changes partitions make, and what
//! the next commit records about them.

mod children;
mod records;
#[cfg(test)]
mod tests;

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use rdlt_connector::{
    ColumnKey, ConnectorError, Field, PipelineState, StateChange, StreamName, TableChange,
    TablePath, TableRef, TableState,
};

use super::model::Model;
use super::resolve::{Change, Incoming, Resolution, Resolver, Route};
use super::session::SharedSession;
use super::{LoweringPlan, TableView};
use crate::config::GrowthLimits;
use crate::error::{Error, ErrorKind, Side};
use crate::naming::Naming;
use crate::normalize::Shape;

pub(crate) use children::Admission;

/// How many times a table's change is named around columns that attempts which never committed
/// left behind before the conflict fails the run.
pub(crate) const CONFLICT_RETRIES: usize = 4;

/// A schema change the destination refused as `schema_conflict`, and the column it refused to
/// widen, where it refused a widen.
type Conflict = (Option<ColumnKey>, ConnectorError);

/// Lowering plans kept per table: one per incoming schema its partitions send, for its current
/// view.
const PLANS: usize = 8;

/// One table and its resolver.
#[derive(Debug)]
struct Slot {
    /// Resolves the table's changes; it keeps from widening, for the rest of the attempt, the
    /// columns whose widening the destination refused.
    resolver: Mutex<Arc<Resolver>>,
    current: Mutex<Arc<TableView>>,
    /// Held while a schema change is worked out and applied, so changes to one table never race.
    evolving: tokio::sync::Mutex<()>,
    /// The model revision state records.
    recorded: Mutex<u32>,
    /// Plans for the current view, the newest last.
    plans: Mutex<Vec<Arc<LoweringPlan>>>,
    /// How the stream normalizes, for a normalized stream's own table.
    shape: Option<Arc<Shape>>,
    /// What the next commit records of the table, reserved since its last change.
    records: Mutex<records::Held>,
}

/// Keeps `plan` among `plans`, the plans of a table whose view is `current`, dropping those of
/// earlier views and, once there are [`PLANS`], the oldest; a plan of a view already superseded
/// is used but not kept.
fn cache(plans: &mut Vec<Arc<LoweringPlan>>, plan: Arc<LoweringPlan>, current: &Arc<TableView>) {
    if !Arc::ptr_eq(plan.view(), current) {
        return;
    }
    plans.retain(|kept| Arc::ptr_eq(kept.view(), current));
    if plans.len() == PLANS {
        plans.remove(0);
    }
    plans.push(plan);
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
    /// Where the records of each table's change are reserved, if anywhere.
    charge: std::sync::OnceLock<records::Charge>,
    /// Tables: the most child tables a stream's table may have.
    children_limit: usize,
}

/// What a commit records about the tables, and the model revisions it records.
#[derive(Debug, Default)]
pub(crate) struct TablesDelta {
    pub(crate) changes: Vec<StateChange>,
    pub(crate) revisions: Vec<(usize, u32)>,
    /// The keys of the records of child tables state does not record yet, with their streams.
    pub(crate) born: Vec<(StreamName, String)>,
    /// Bytes: what the changes take in a commit's frame, which their schema changes reserved.
    pub(crate) prepaid: u64,
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
            charge: std::sync::OnceLock::new(),
            children_limit: GrowthLimits::default().child_tables().get(),
        }
    }

    /// The same tables, each stream's table holding at most `children_limit` child tables.
    #[must_use]
    pub(crate) fn growing(self, children_limit: usize) -> Self {
        Self {
            children_limit,
            ..self
        }
    }

    /// The same tables, over the tables `state` records.
    ///
    /// # Errors
    ///
    /// `state_invalid`, a Destination error, where two tables are recorded under one identifier:
    /// writes and drops of one would reach the other.
    pub(crate) fn committed(mut self, state: &PipelineState) -> Result<Self, Error> {
        if let Some(name) = shared(state).first() {
            return Err(Error::new(
                ErrorKind::Destination,
                format!("reading pipeline state: two tables are recorded under the name {name}"),
            )
            .with_code("state_invalid"));
        }
        self.taken = Mutex::new(
            state
                .tables
                .values()
                .filter_map(|table| table.physical.as_deref().map(str::to_owned))
                .collect(),
        );
        self.committed = state.tables.clone();
        Ok(self)
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
    ///
    /// # Errors
    ///
    /// As [`TableView::new`].
    pub(crate) fn add(
        &self,
        resolver: Resolver,
        table: &TableRef,
        model: Model,
    ) -> Result<usize, Error> {
        self.add_normalized(resolver, table, model, None)
    }

    /// Adds the table `table` at its committed `model`, whose stream normalizes as `shape`, if
    /// it does; returns its index.
    ///
    /// # Errors
    ///
    /// As [`TableView::new`].
    pub(crate) fn add_normalized(
        &self,
        resolver: Resolver,
        table: &TableRef,
        model: Model,
        shape: Option<Shape>,
    ) -> Result<usize, Error> {
        let recorded = model.revision;
        let view = TableView::new(table, model, &resolver)?;
        let mut slots = self.slots.write();
        slots.push(Arc::new(Slot {
            resolver: Mutex::new(Arc::new(resolver)),
            current: Mutex::new(Arc::new(view)),
            evolving: tokio::sync::Mutex::new(()),
            recorded: Mutex::new(recorded),
            plans: Mutex::new(Vec::new()),
            shape: shape.map(Arc::new),
            records: Mutex::new(None),
        }));
        Ok(slots.len() - 1)
    }

    fn slot(&self, table: usize) -> Arc<Slot> {
        Arc::clone(&self.slots.read()[table])
    }

    /// The resolver of `table`.
    fn resolver(&self, table: usize) -> Arc<Resolver> {
        Arc::clone(&self.slot(table).resolver.lock())
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
        incoming: Incoming,
    ) -> Result<Arc<LoweringPlan>, Error> {
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
        let stream = slot.resolver.lock().stream.clone();
        let plan = Arc::new(LoweringPlan::new(
            stream,
            Arc::clone(&view),
            incoming,
            routes,
        ));
        let current = self.view(table);
        cache(&mut plans, Arc::clone(&plan), &current);
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
            resolution.model.revision == view.model.revision
        };
        let resolution = self.resolver(table).resolve(&view.model, incoming)?;
        if unchanged(&resolution, &view) {
            return Ok((view, resolution.routes));
        }
        let _evolving = slot.evolving.lock().await;
        let view = self.view(table);
        let kept = self.resolver(table);
        let resolution = kept.resolve(&view.model, incoming)?;
        if unchanged(&resolution, &view) {
            return Ok((view, resolution.routes));
        }
        let (mut resolver, mut resolution, mut retries) = (Cow::Borrowed(&*kept), resolution, 0);
        let mut blamed = Vec::new();
        let next = loop {
            match self.evolve(table, &view, resolution, &resolver).await? {
                Ok(next) => break next,
                // A widen the destination refuses keeps its column as it is, for the attempt.
                Err((Some(column), _)) if !resolver.unwidened.contains(&column) => {
                    resolver = Cow::Owned(resolver.unwidening([column.clone()]));
                    blamed.push(column);
                }
                Err((_, error)) if retries == CONFLICT_RETRIES => {
                    return Err(self.refused(table, error));
                }
                // Else a name an attempt that never committed left behind: new columns are
                // named around it.
                Err(_) => {
                    resolver = Cow::Owned(resolver.hashing(retries as u64));
                    retries += 1;
                }
            }
            resolution = resolver.resolve(&view.model, incoming)?;
        };
        if !blamed.is_empty() {
            *slot.resolver.lock() = Arc::new(kept.unwidening(blamed));
        }
        *slot.current.lock() = Arc::clone(&next.0);
        Ok(next)
    }

    /// Applies what `resolution` changes in `view`; the destination's `schema_conflict` is
    /// returned for the caller to resolve around, with the column it refused to widen, if it
    /// refused a widen.
    async fn evolve(
        &self,
        table: usize,
        view: &TableView,
        resolution: Resolution,
        resolver: &Resolver,
    ) -> Result<Result<(Arc<TableView>, Vec<Route>), Conflict>, Error> {
        let next = Arc::new(TableView::new(&view.table, resolution.model, resolver)?);
        // What the commit will record of the change is reserved before the destination sees it.
        self.reserve_records(table, &next).await?;
        let changes = table_changes(view, &next, &resolution.changes);
        match self.session.apply_each(&changes).await? {
            Ok(()) => Ok(Ok((next, resolution.routes))),
            Err((index, error)) if error.code() == Some("schema_conflict") => {
                let widened = match changes.get(index) {
                    Some(TableChange::Widen { column, .. }) => next.model.names.owner(column),
                    _ => None,
                };
                Ok(Err((widened.cloned(), error)))
            }
            Err((_, error)) => Err(self.refused(table, error)),
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
        let resolver = self.resolver(table);
        let stream = &resolver.stream;
        let context = format!("changing the table of stream {stream}");
        Error::connector(Side::Destination, context, error).with_stream(stream)
    }

    /// Adds the metadata columns a stream's table created before the stream merged, normalized or
    /// soft-deleted lacks: its sequence, lineage and deleted-at columns, nullable, since the rows
    /// it holds have none.
    ///
    /// A table that already has them changes nothing, as a generation created with them does.
    pub(crate) async fn add_meta_columns(&self, table: usize) -> Result<(), Error> {
        let view = self.view(table);
        if !view.model.created() {
            return Ok(());
        }
        let deleted_at = view
            .meta
            .changes
            .as_ref()
            .and_then(|changes| changes.deleted_at.as_deref());
        let added = [
            view.meta.seq.as_deref(),
            view.meta.id.as_deref(),
            deleted_at,
        ];
        let changes: Vec<TableChange> = view.physical[view.model.columns.len()..]
            .iter()
            .filter(|field| added.contains(&Some(field.name())))
            .map(|field| TableChange::AddColumn {
                table: view.table.clone(),
                field: Field::new(field.name(), field.logical_type().clone(), true),
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
        let children: BTreeSet<usize> = self.children.lock().values().copied().collect();
        for (index, slot) in slots.iter().enumerate() {
            let view = self.view(index);
            if view.model.revision <= *slot.recorded.lock() {
                continue;
            }
            let records = records::records(&view);
            if children.contains(&index) && !self.committed.contains_key(&view.table.path) {
                let stream = &slot.resolver.lock().stream;
                delta
                    .born
                    .extend(records.iter().filter_map(|change| match change {
                        StateChange::Put(record) => Some((stream.clone(), record.key.clone())),
                        StateChange::Delete(_) => None,
                    }));
            }
            delta.changes.extend(records);
            delta.revisions.push((index, view.model.revision));
        }
        delta.prepaid = records::recorded_bytes(&delta.changes);
        delta
    }

    /// Notes that state now records the model `revisions`.
    pub(crate) fn recorded(&self, revisions: &[(usize, u32)]) {
        for (index, revision) in revisions {
            let slot = self.slot(*index);
            let mut recorded = slot.recorded.lock();
            *recorded = (*recorded).max(*revision);
            drop(recorded);
            self.release_records(*index, *revision);
        }
    }
}

/// The identifiers `state` records for more than one table.
pub(crate) fn shared(state: &PipelineState) -> BTreeSet<&str> {
    let mut recorded = BTreeSet::new();
    state
        .tables
        .values()
        .filter_map(|table| table.physical.as_deref())
        .filter(|physical| !recorded.insert(*physical))
        .collect()
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
    let positions = after.model.positions();
    changes
        .iter()
        .filter_map(|change| match change {
            Change::Add { key } => {
                let index = *positions.get(after.model.names.get(key)?)?;
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
