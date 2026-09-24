//! The tables of one attempt: their current views, the schema changes partitions make, and what
//! the next commit records about them.

#[cfg(test)]
mod tests;

use std::borrow::Cow;
use std::sync::Arc;

use parking_lot::Mutex;
use rdlt_connector::{
    CommitMeta, ConnectorError, DestinationSession, DestinationWriter, Receipt, SchemaVersion,
    StateChange, StateEntry, TableChange, TableRef, TableSchema,
};

use super::model::Model;
use super::resolve::{Change, Resolution, Resolver, Route};
use super::{LoweringPlan, TableView};
use crate::error::{Error, Side};

/// How many times a table's change is named around columns that attempts which never committed
/// left behind before the conflict fails the run.
pub(crate) const CONFLICT_RETRIES: usize = 4;

/// Lowering plans kept per table: one per incoming schema its partitions send, for its current
/// view.
const PLANS: usize = 8;

/// The destination session, shared by the coordinator's commits and the partitions' schema
/// changes until the coordinator closes it.
pub(crate) struct SharedSession(tokio::sync::Mutex<Option<Box<dyn DestinationSession>>>);

impl SharedSession {
    pub(crate) fn new(session: Box<dyn DestinationSession>) -> Arc<Self> {
        Arc::new(Self(tokio::sync::Mutex::new(Some(session))))
    }

    /// Applies `changes` in order, stopping at the first the destination refuses.
    ///
    /// Each call returns the destination's own result inside the error for a closed session.
    pub(crate) async fn apply_schema(
        &self,
        changes: &[TableChange],
    ) -> Result<rdlt_connector::Result<()>, Error> {
        let mut session = self.0.lock().await;
        let session = session.as_mut().ok_or_else(closed)?;
        for change in changes {
            if let Err(error) = session.apply_schema(change).await {
                return Ok(Err(error));
            }
        }
        Ok(Ok(()))
    }

    /// A writer for `table`.
    pub(crate) async fn writer(
        &self,
        table: &TableRef,
    ) -> Result<rdlt_connector::Result<Box<dyn DestinationWriter>>, Error> {
        let mut session = self.0.lock().await;
        Ok(session.as_mut().ok_or_else(closed)?.writer(table).await)
    }

    /// Commits `meta`.
    pub(crate) async fn commit(
        &self,
        meta: &CommitMeta,
    ) -> Result<rdlt_connector::Result<Receipt>, Error> {
        let mut session = self.0.lock().await;
        Ok(session.as_mut().ok_or_else(closed)?.commit(meta).await)
    }

    /// Closes the session; later calls find it closed.
    pub(crate) async fn close(&self) -> Result<(), Error> {
        let Some(session) = self.0.lock().await.take() else {
            return Ok(());
        };
        session
            .close()
            .await
            .map_err(|error| Error::connector(Side::Destination, "closing the session", error))
    }
}

fn closed() -> Error {
    Error::internal("the destination session is already closed")
}

impl std::fmt::Debug for SharedSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedSession").finish_non_exhaustive()
    }
}

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
}

/// Every table of an attempt.
#[derive(Debug)]
pub(crate) struct Tables {
    session: Arc<SharedSession>,
    slots: Vec<Slot>,
}

/// What a commit records about the tables, and the versions it records.
#[derive(Debug, Default)]
pub(crate) struct TablesDelta {
    pub(crate) changes: Vec<StateChange>,
    pub(crate) versions: Vec<(usize, u32)>,
}

impl Tables {
    pub(crate) fn new(session: Arc<SharedSession>) -> Self {
        Self {
            session,
            slots: Vec::new(),
        }
    }

    /// The session the tables change through.
    pub(crate) fn session(&self) -> &Arc<SharedSession> {
        &self.session
    }

    /// Adds the table `table` at its committed `model`; returns its index.
    pub(crate) fn add(&mut self, resolver: Resolver, table: &TableRef, model: Model) -> usize {
        let recorded = model.version;
        let view = TableView::new(table, model, &resolver);
        self.slots.push(Slot {
            resolver,
            current: Mutex::new(Arc::new(view)),
            evolving: tokio::sync::Mutex::new(()),
            recorded: Mutex::new(recorded),
            plans: Mutex::new(Vec::new()),
        });
        self.slots.len() - 1
    }

    /// The current view of `table`.
    pub(crate) fn view(&self, table: usize) -> Arc<TableView> {
        Arc::clone(&self.slots[table].current.lock())
    }

    /// The plan lowering batches of `incoming` into `table`: the plan made for the table's current
    /// view and `incoming`, or a new one once the schema changes `incoming` needs are applied.
    pub(crate) async fn plan(
        &self,
        table: usize,
        incoming: TableSchema,
    ) -> Result<Arc<LoweringPlan>, Error> {
        let slot = &self.slots[table];
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
        incoming: &TableSchema,
    ) -> Result<(Arc<TableView>, Vec<Route>), Error> {
        let slot = &self.slots[table];
        let view = self.view(table);
        let resolution = slot.resolver.resolve(&view.model, incoming)?;
        if resolution.changes.is_empty() {
            return Ok((view, resolution.routes));
        }
        let _evolving = slot.evolving.lock().await;
        let view = self.view(table);
        let resolution = slot.resolver.resolve(&view.model, incoming)?;
        if resolution.changes.is_empty() {
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
        let stream = &self.slots[table].resolver.stream;
        let context = format!("changing the table of stream {stream}");
        Error::connector(Side::Destination, context, error).with_stream(stream)
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
        for (index, slot) in self.slots.iter().enumerate() {
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
            let mut recorded = self.slots[*index].recorded.lock();
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
