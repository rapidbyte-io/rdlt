//! Lowering plans: how batches of one incoming schema become rows of one table view, worked out
//! once and applied to every batch — discards, exact conversions and lowering, a merge table's
//! compaction, then the metadata columns, a merge table's sequence column among them.

mod changes;
mod constants;
mod costs;
#[cfg(test)]
mod differential;
mod handed;
mod history;
mod kept;
mod keys;
mod merge;
mod model;
mod prepared;
#[cfg(test)]
mod reference;
mod split;
mod unheld;
#[cfg(test)]
pub(crate) use split::lowered as split_lowered;

use std::sync::Arc;
use std::time::SystemTime;

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch};
use arrow_schema::FieldRef;
use arrow_select::filter::FilterBuilder;
use parking_lot::Mutex;
use rdlt_connector::{Epoch, Field, LogicalType, SegmentId, StreamName};

use super::TableView;
use super::convert::text;
use super::lower::{ID_TYPE, IDX_TYPE};
use super::resolve::{Incoming, Rest, Route};
use crate::error::Error;
use crate::normalize::{Lineage, Received};
use crate::policy::SchemaPolicy;
pub(crate) use changes::{ChangeRows, aligned};
use constants::Constants;
use costs::Storing;
pub(crate) use kept::filtered;
use kept::kept_by;
use keys::keyed;
pub(crate) use keys::{key_values, with_columns};
use merge::{positions, sequence};
pub(crate) use prepared::Prepared;
use split::{Fitted, Splits};
#[cfg(test)]
pub(crate) use unheld::{Check, nulled_at, refused};

/// What the metadata columns of a batch hold.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Stamp {
    /// When the load started.
    pub(crate) loaded_at: SystemTime,
    /// When the batch was received: where a history stream's versions begin without a change
    /// time, so a run that follows its source for days keeps each version's span.
    pub(crate) received_at: SystemTime,
    /// The epoch of the session writing the batch.
    pub(crate) epoch: Epoch,
    /// The batch's segment, and the position of its first row among the rows written to it.
    pub(crate) segment: SegmentId,
    pub(crate) first_row: u64,
}

/// Where one of the view's columns takes its values from.
#[derive(Clone, Debug)]
enum Source {
    /// The incoming column at this position, of this type.
    Incoming(usize, LogicalType),
    /// The values of the incoming column of JSON at this position that the column holds, read
    /// into its type.
    Read(usize),
    /// The values of the incoming column of JSON at this position that its own column does not
    /// hold.
    Rest(usize),
    /// Nowhere: the batch has no values for the column.
    Nulls,
}

/// How batches of `incoming` become rows of `view`'s table, worked out once: where each column
/// goes, what each policy decides, and where change flags and change times sit.
#[derive(Debug)]
pub(crate) struct LoweringPlan {
    stream: StreamName,
    view: Arc<TableView>,
    incoming: Incoming,
    routes: Vec<Route>,
    /// Where each of the view's columns takes its values from, in the view's order.
    sources: Vec<Source>,
    /// Where each incoming column is written: the position of the table's column it goes to,
    /// if any.
    written: Vec<Option<usize>>,
    /// The incoming column holding a history table's change times, where the table names one
    /// and the batches carry it.
    change_time: Option<usize>,
    /// The field of each of the view's columns a history table's versions hash, in the view's
    /// order; empty for another table.
    hashed: Vec<Option<FieldRef>>,
    /// The incoming columns whose values the schema policy discards.
    discarded: Vec<usize>,
    /// The incoming columns of JSON whose values their own columns hold in part.
    splits: Splits,
    /// The metadata columns holding one value per load, built once and sliced per batch.
    constants: Mutex<Option<Constants>>,
    /// How the table stores each column.
    storing: Storing,
    /// Each incoming column's schema policy, which decides what becomes of a value its column
    /// cannot hold.
    policies: Vec<SchemaPolicy>,
    /// Whether the plan hands every incoming column over as it arrives.
    handed: bool,
}

impl Source {
    /// Whether the column holds nothing but nulls: the batch lacks it, or sends it typed null.
    fn is_null(&self) -> bool {
        match self {
            Self::Incoming(_, from) => *from == LogicalType::Null,
            Self::Read(_) | Self::Rest(_) => false,
            Self::Nulls => true,
        }
    }
}

impl LoweringPlan {
    /// The plan lowering batches of `incoming`, which `routes` sends to `view`'s columns.
    pub(crate) fn new(
        stream: StreamName,
        view: Arc<TableView>,
        incoming: Incoming,
        routes: Vec<Route>,
    ) -> Self {
        let mut sources = vec![Source::Nulls; view.model.columns.len()];
        let mut discarded = Vec::new();
        for (index, (route, field)) in routes
            .iter()
            .zip(incoming.schema.fields().iter())
            .enumerate()
        {
            match route {
                Route::Column(column) => {
                    sources[*column] = Source::Incoming(index, field.logical_type().clone());
                }
                Route::Split { own, rest } => {
                    sources[*own] = Source::Read(index);
                    if let Rest::Column(column) = rest {
                        sources[*column] = Source::Rest(index);
                    }
                }
                Route::DiscardValues => discarded.push(index),
                Route::DiscardRows | Route::Skip => {}
            }
        }
        let types: Vec<LogicalType> = view
            .model
            .columns
            .iter()
            .map(|column| column.logical_type().clone())
            .collect();
        let written = changes::written(&sources, incoming.schema.fields().len());
        let change_time = history::change_time(&view, &incoming);
        let hashed = history::hashed(&view, &sources, change_time);
        let mut plan = Self {
            splits: Splits::of(&routes, &types),
            storing: Storing::new(&view, &sources, routes.len()),
            stream,
            view,
            incoming,
            routes,
            sources,
            written,
            change_time,
            hashed,
            discarded,
            constants: Mutex::new(None),
            policies: Vec::new(),
            handed: false,
        };
        plan.handed = plan.hands_over();
        plan
    }

    /// The same plan, its incoming columns following `policies`, in their order; a column with
    /// none refuses a value it cannot hold with its batch.
    pub(crate) fn with_policies(mut self, policies: Vec<SchemaPolicy>) -> Self {
        self.policies = policies;
        self.handed = self.hands_over();
        self
    }

    /// The table view the plan lowers into.
    pub(crate) fn view(&self) -> &Arc<TableView> {
        &self.view
    }

    /// Whether the schema policy drops some of the rows the plan lowers: those holding a value of
    /// a change it discards, or a value their column cannot hold.
    ///
    /// Only a column whose policy discards rows routes values to be dropped with their rows.
    pub(crate) fn drops_rows(&self) -> bool {
        self.policies.contains(&SchemaPolicy::DiscardRow)
    }

    /// Which rows of `batch` the schema policy keeps, where it drops some.
    ///
    /// # Errors
    ///
    /// Where a value of a column of JSON its own column holds in part cannot be read.
    pub(crate) fn kept(
        &self,
        batch: &RecordBatch,
    ) -> Result<Option<BooleanArray>, arrow_schema::ArrowError> {
        let fitted = self.splits.fit(batch, true)?;
        let split = both(
            self.splits.kept(&fitted, batch.num_rows()),
            self.held(batch)?.0,
        );
        Ok(kept_by(batch, &self.routes, split))
    }

    /// The incoming columns the plan lowers.
    pub(crate) fn incoming(&self) -> &Incoming {
        &self.incoming
    }

    /// `batch`, whose columns are the plan's incoming ones, as rows of its table.
    ///
    /// Every column goes where the plan's routes send it, converted exactly and lowered to how
    /// the destination stores it; a merge table's batch then keeps the rows its merge keeps,
    /// and the metadata columns follow for those rows, `lineage` last for the tables of
    /// normalized streams. Each step that keeps some rows lets go of what it copied them from,
    /// a column at a time, where nothing else holds it.
    pub(crate) fn prepare(
        &self,
        batch: RecordBatch,
        lineage: Option<&Lineage>,
        stamp: &Stamp,
        changes: Option<ChangeRows>,
    ) -> Result<Prepared, Error> {
        let stream = &self.stream;
        let view = &self.view;
        let failed = |error: arrow_schema::ArrowError| {
            Error::internal(format!("stream {stream}: preparing a batch")).with_source(error)
        };
        let (batch, mut kept, discarded_rows, fitted, mut converted) =
            self.kept_rows(batch).map_err(failed)?;
        let (batch, nulled, left) = self.nulled(batch).map_err(failed)?;
        converted.extend(left);
        let mut changes = match (changes, &kept) {
            (Some(changes), Some(kept)) => Some(changes.filter(kept).map_err(failed)?),
            (changes, _) => changes,
        };
        if batch.num_rows() == 0 {
            return Ok(Prepared {
                batch: RecordBatch::new_empty(Arc::clone(&view.schema)),
                view: Arc::clone(view),
                discarded_rows,
                discarded_values: 0,
            });
        }
        let discarded_values = self.discarded_values(&batch, &fitted) + nulled;
        let (batch, zeroed) = keyed(stream, view, batch, &self.sources, changes.as_ref())?;
        // A key column changed in a zero holds no longer what it converted to.
        converted.forget(zeroed);
        let (mut columns, held) = self.model_columns(&batch, &fitted, &converted)?;
        // Only a history table hashes its rows from the batch, and it keeps every row: any other
        // lets the batch go, so a merge's compaction lets each column go as it copies it.
        let (rows, batch) = (batch.num_rows(), held.is_some().then_some(batch));
        drop((fitted, converted));
        self.merged(&mut columns, &mut kept, &mut changes)
            .map_err(failed)?;
        let rows = columns.first().map_or(rows, Array::len);
        let (constants, loaded_at) = self.constants(stamp, rows).map_err(failed)?;
        columns.extend(constants);
        if view.meta.seq.is_some() {
            let seq = match &changes {
                Some(changes) => self
                    .source_sequence(changes, columns.len())
                    .map_err(failed)?,
                None => self.sequence(lineage, kept.as_ref(), stamp, rows)?,
            };
            columns.push(seq);
        }
        if let Some(changes) = &changes {
            self.stored_changes(changes, loaded_at, &mut columns)
                .map_err(failed)?;
        }
        if let (Some(held), Some(batch)) = (held, &batch) {
            let history = self.history(batch, (&held, columns.len()), stamp, changes.as_ref())?;
            columns.extend(history);
        }
        let first = columns.len();
        columns.extend(self.lineage(lineage, kept.as_ref(), stamp, first)?);
        columns.extend(self.directives(changes.as_ref()));
        let prepared = RecordBatch::try_new(Arc::clone(&view.schema), columns).map_err(failed)?;
        Ok(Prepared {
            batch: prepared,
            view: Arc::clone(view),
            discarded_rows,
            discarded_values,
        })
    }

    /// How many values of `batch` the schema policy discards: of the columns it discards, and of
    /// its columns of JSON, which `fitted` read, those their own columns do not hold.
    fn discarded_values(&self, batch: &RecordBatch, fitted: &Fitted) -> u64 {
        let columns = self.discarded.iter().map(|index| {
            let column = batch.column(*index);
            (column.len() - column.logical_null_count()) as u64
        });
        columns.sum::<u64>() + self.splits.discarded(batch, fitted)
    }

    /// The columns that only direct a change stream's merge, after its stored ones: its op, and
    /// its unchanged flags but in a history table, whose hashes need whole rows.
    fn directives(&self, changes: Option<&ChangeRows>) -> Vec<ArrayRef> {
        let view = &self.view;
        let (Some(names), Some(changes)) = (&view.meta.changes, changes) else {
            return Vec::new();
        };
        if names.stored {
            return Vec::new();
        }
        let mut columns: Vec<ArrayRef> = vec![Arc::new(changes.op.clone())];
        if view.meta.history.is_none() {
            columns.push(changes.unchanged_over(&self.written));
        }
        columns
    }

    /// The rows of `batch` the schema policy keeps, which those are where it drops some, how many
    /// it drops, which values of its columns of JSON their own columns hold, and what the
    /// columns the policies decide converted to, each of the kept rows.
    fn kept_rows(&self, batch: RecordBatch) -> Result<Kept, arrow_schema::ArrowError> {
        let fitted = self.splits.fit(&batch, false)?;
        let (held, mut converted) = self.held(&batch)?;
        let split = both(self.splits.kept(&fitted, batch.num_rows()), held);
        let Some(keep) = kept_by(&batch, &self.routes, split) else {
            return Ok((batch, None, 0, fitted, converted));
        };
        let rows = batch.num_rows();
        let own = converted.release(&batch);
        let kept_batch = filtered(batch, &keep)?;
        converted.filter(&own, &kept_batch, &keep)?;
        let fitted = fitted.filtered(&keep)?;
        let discarded_rows = (rows - kept_batch.num_rows()) as u64;
        Ok((kept_batch, Some(keep), discarded_rows, fitted, converted))
    }

    /// The error for a value of a column of JSON that `column` holds in part that cannot be read,
    /// `error` kept as its cause.
    fn unread(&self, column: &Field, error: arrow_schema::ArrowError) -> Error {
        Error::internal(format!(
            "stream {}: column {}: reading the values of JSON text it holds",
            self.stream,
            column.name()
        ))
        .with_source(error)
    }

    /// The sequence column of `rows` rows of a merge table, which the rows `kept` keeps: a row's
    /// sequence is its position among the rows received, and a normalized stream's row's is its
    /// root row's, from `lineage`, so a child row follows its root's merge.
    fn sequence(
        &self,
        lineage: Option<&Lineage>,
        kept: Option<&BooleanArray>,
        stamp: &Stamp,
        rows: usize,
    ) -> Result<ArrayRef, Error> {
        let stream = &self.stream;
        let failed = |error: arrow_schema::ArrowError| {
            Error::internal(format!("stream {stream}: sequencing a batch")).with_source(error)
        };
        let positions = match lineage {
            Some(lineage) => Some(kept_rows(lineage.root_row(), kept).map_err(failed)?),
            None => kept.map(positions),
        };
        sequence(&self.view, stamp, rows, positions.as_ref()).map_err(failed)
    }

    /// The lineage columns of the plan's table, lowered, from `lineage`, the rows `kept` keeps
    /// and the batch's `stamp`; the first is the table's column at `first`.
    fn lineage(
        &self,
        lineage: Option<&Lineage>,
        kept: Option<&BooleanArray>,
        stamp: &Stamp,
        first: usize,
    ) -> Result<Vec<ArrayRef>, Error> {
        let stream = &self.stream;
        lineage_columns(&self.view, stream, (lineage, kept), stamp)?
            .into_iter()
            .enumerate()
            .map(|(index, (array, logical))| {
                let lowered = self.view.physical[first + index].logical_type();
                lower_array(&array, &logical, lowered).map_err(|error| {
                    Error::internal(format!("stream {stream}: lowering lineage: {error}"))
                })
            })
            .collect()
    }

    /// The load epoch and load start columns for `rows` rows, and the load start in microseconds
    /// since the epoch: slices of arrays built once, and built again only for a batch more than
    /// the arrays hold, whose keys take a byte a row.
    fn constants(
        &self,
        stamp: &Stamp,
        rows: usize,
    ) -> Result<([ArrayRef; 2], i64), arrow_schema::ArrowError> {
        let mut constants = self.constants.lock();
        if !constants
            .as_ref()
            .is_some_and(|built| built.fits(stamp, rows))
        {
            *constants = Some(Constants::new(&self.view, stamp, rows)?);
        }
        let built = constants.as_ref().expect("the constants were just built");
        let columns = built.columns.clone().map(|column| column.slice(0, rows));
        Ok((columns, built.micros))
    }
}

/// The lineage columns of `view`'s rows and their types, built from `lineage` of the rows `kept`
/// keeps where the schema policy dropped some, received as `stamp` says; none for a stream that
/// does not normalize.
fn lineage_columns(
    view: &TableView,
    stream: &StreamName,
    (lineage, kept): (Option<&Lineage>, Option<&BooleanArray>),
    stamp: &Stamp,
) -> Result<Vec<(ArrayRef, LogicalType)>, Error> {
    if view.meta.id.is_none() {
        return Ok(Vec::new());
    }
    let missing =
        |what: &str| Error::internal(format!("stream {stream}: a {what} batch has no lineage"));
    let failed = |error: arrow_schema::ArrowError| {
        Error::internal(format!("stream {stream}: building lineage: {error}"))
    };
    let mut lineage = lineage
        .ok_or_else(|| missing("normalized table's"))?
        .clone();
    if let Some(kept) = kept {
        lineage = lineage
            .filter(&FilterBuilder::new(kept).build())
            .map_err(failed)?;
    }
    let received = Received {
        epoch: stamp.epoch,
        segment: stamp.segment,
        first_row: stamp.first_row,
    };
    let (id, parent) = lineage.columns(&received).map_err(failed)?;
    let mut columns = vec![(id, ID_TYPE)];
    if view.meta.parent.is_some() {
        let [parent, root, idx] = parent.ok_or_else(|| missing("child table's"))?;
        columns.extend([(parent, ID_TYPE), (root, ID_TYPE), (idx, IDX_TYPE)]);
    }
    Ok(columns)
}

/// The rows both `left` and `right` keep, where either keeps fewer than every row; neither mask
/// holds a null.
fn both(left: Option<BooleanArray>, right: Option<BooleanArray>) -> Option<BooleanArray> {
    match (left, right) {
        (Some(left), Some(right)) => Some(BooleanArray::new(left.values() & right.values(), None)),
        (left, right) => left.or(right),
    }
}

/// `array`, of the rows `kept` keeps where the schema policy dropped some.
fn kept_rows(
    array: &ArrayRef,
    kept: Option<&BooleanArray>,
) -> Result<ArrayRef, arrow_schema::ArrowError> {
    match kept {
        Some(kept) => arrow_select::filter::filter(array.as_ref(), kept),
        None => Ok(Arc::clone(array)),
    }
}

/// The rows of a batch a schema policy keeps, which those are where it drops some, how many it
/// drops, which values of its columns of JSON their own columns hold, and the columns the
/// policies decide, converted.
type Kept = (
    RecordBatch,
    Option<BooleanArray>,
    u64,
    Fitted,
    unheld::Converted,
);

/// `array` of `logical` as the destination stores it: as it is, or as its text, which for nested
/// values and JSON is JSON.
fn lower_array(
    array: &ArrayRef,
    logical: &LogicalType,
    lowered: &LogicalType,
) -> Result<ArrayRef, arrow_schema::ArrowError> {
    if lowered == logical {
        Ok(Arc::clone(array))
    } else {
        text(array, logical)
    }
}
