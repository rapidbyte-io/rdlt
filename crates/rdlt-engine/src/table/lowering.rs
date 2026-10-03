//! Lowering plans (spec §7.3): how batches of one incoming schema become rows of one table view,
//! worked out once and applied to every batch — discards, exact conversions, lowering, metadata
//! columns and, for merge tables, the sequence column and compaction.

mod changes;
mod constants;
mod costs;
#[cfg(test)]
mod differential;
mod history;
mod kept;
mod merge;
mod prepared;
#[cfg(test)]
mod reference;
mod split;
#[cfg(test)]
pub(crate) use split::lowered as split_lowered;

use std::sync::Arc;
use std::time::SystemTime;

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, new_null_array};
use parking_lot::Mutex;
use rdlt_connector::{Field, LoadId, LogicalType, SegmentId, StreamName};

use super::TableView;
use super::convert::{convert, text};
use super::lower::{ID_TYPE, IDX_TYPE};
use super::resolve::{Incoming, Rest, Route};
use crate::error::Error;
use crate::normalize::Lineage;
pub(crate) use changes::{ChangeRows, data_ordinals};
use constants::Constants;
use kept::{discard_rows, kept_by};
use merge::{check_key, positions, sequence};
pub(crate) use prepared::Prepared;
use split::{Fitted, Splits};

/// What the metadata columns of a batch hold.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Stamp {
    pub(crate) load_id: LoadId,
    /// When the load started.
    pub(crate) loaded_at: SystemTime,
    /// When the batch was received: where a history stream's versions begin without a change
    /// time, so a run that follows its source for days keeps each version's span.
    pub(crate) received_at: SystemTime,
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

/// How batches of `incoming` become rows of `view`'s table, worked out once.
#[derive(Debug)]
pub(crate) struct LoweringPlan {
    stream: StreamName,
    view: Arc<TableView>,
    incoming: Incoming,
    routes: Vec<Route>,
    /// Where each of the view's columns takes its values from, in the view's order.
    sources: Vec<Source>,
    /// The incoming columns whose values the schema policy discards.
    discarded: Vec<usize>,
    /// The incoming columns of JSON whose values their own columns hold in part.
    splits: Splits,
    /// The metadata columns holding one value per load, built once and sliced per batch.
    constants: Mutex<Option<Constants>>,
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
        Self {
            splits: Splits::of(&routes, &types),
            stream,
            view,
            incoming,
            routes,
            sources,
            discarded,
            constants: Mutex::new(None),
        }
    }

    /// The table view the plan lowers into.
    pub(crate) fn view(&self) -> &Arc<TableView> {
        &self.view
    }

    /// Whether the schema policy drops some of the rows the plan lowers: those holding a value of
    /// a change it discards.
    pub(crate) fn drops_rows(&self) -> bool {
        self.routes.iter().any(|route| route.drops_rows())
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
        let split = self.splits.kept(&fitted, batch.num_rows());
        Ok(kept_by(batch, &self.routes, split))
    }

    /// The incoming columns the plan lowers.
    pub(crate) fn incoming(&self) -> &Incoming {
        &self.incoming
    }

    /// `batch`, whose columns are the plan's incoming ones, as rows of its table.
    ///
    /// Every column goes where the plan's routes send it, converted exactly and lowered to how
    /// the destination stores it, and the metadata columns follow, `lineage` last for the tables
    /// of normalized streams. A merge table's batch keeps only the last row of each key.
    pub(crate) fn prepare(
        &self,
        batch: &RecordBatch,
        lineage: Option<&Lineage>,
        stamp: &Stamp,
        changes: Option<&ChangeRows>,
    ) -> Result<Prepared, Error> {
        let stream = &self.stream;
        let view = &self.view;
        let failed = |error: arrow_schema::ArrowError| {
            Error::internal(format!("stream {stream}: preparing a batch: {error}"))
        };
        let (batch, kept, discarded_rows, fitted) = self.kept_rows(batch).map_err(failed)?;
        let changes = match (changes, &kept) {
            (Some(changes), Some(kept)) => Some(changes.filter(kept).map_err(failed)?),
            (changes, _) => changes.cloned(),
        };
        if batch.num_rows() == 0 {
            return Ok(Prepared {
                batch: RecordBatch::new_empty(Arc::clone(&view.schema)),
                view: Arc::clone(view),
                discarded_rows,
                discarded_values: 0,
            });
        }
        let discarded_values = self
            .discarded
            .iter()
            .map(|index| {
                let column = batch.column(*index);
                (column.len() - column.logical_null_count()) as u64
            })
            .sum::<u64>()
            + self.splits.discarded(&batch, &fitted);
        check_key(stream, view, &batch, &self.sources, changes.as_ref())?;
        let rows = batch.num_rows();
        let mut columns = self.model_columns(&batch, &fitted)?;
        columns.extend(self.constants(stamp, rows).map_err(failed)?);
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
            self.stored_changes(changes, stamp, &mut columns)
                .map_err(failed)?;
        }
        columns.extend(self.history(&batch, &columns, stamp, changes.as_ref())?);
        let first = columns.len();
        columns.extend(self.lineage(lineage, kept.as_ref(), first)?);
        columns.extend(self.directives(changes.as_ref()));
        let prepared = RecordBatch::try_new(Arc::clone(&view.schema), columns).map_err(failed)?;
        let prepared = self.compacted(prepared).map_err(failed)?;
        Ok(Prepared {
            batch: prepared,
            view: Arc::clone(view),
            discarded_rows,
            discarded_values,
        })
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
            columns.push(changes.unchanged_over(&self.written_ordinals()));
        }
        columns
    }

    /// The model's columns of `batch`, each from where the plan routes it, converted and lowered
    /// as its column stores it; a column the batch lacks is null.
    fn model_columns(&self, batch: &RecordBatch, fitted: &Fitted) -> Result<Vec<ArrayRef>, Error> {
        let view = &self.view;
        let mut columns = Vec::with_capacity(view.physical.len());
        for ((column, lowered), source) in view
            .model
            .columns
            .iter()
            .zip(&view.lowered)
            .zip(&self.sources)
        {
            let array = match source {
                Source::Incoming(index, from) if !source.is_null() => {
                    store(&self.stream, batch.column(*index), from, column, lowered)?
                }
                Source::Read(index) => {
                    let own = fitted.own(*index, column.logical_type());
                    let own = own.map_err(|error| self.unread(column, &error))?;
                    store(&self.stream, &own, column.logical_type(), column, lowered)?
                }
                Source::Rest(index) => {
                    let rest = fitted.rest(*index);
                    let rest = rest.map_err(|error| self.unread(column, &error))?;
                    store(&self.stream, &rest, &LogicalType::Json, column, lowered)?
                }
                // Nulls are built as the destination stores them, never as the wider type the
                // column holds them in.
                _ => new_null_array(&lowered.to_arrow(), batch.num_rows()),
            };
            columns.push(array);
        }
        Ok(columns)
    }

    /// The rows of `batch` the schema policy keeps, which those are where it drops some, how many
    /// it drops, and which values of its columns of JSON their own columns hold, of those kept.
    fn kept_rows(
        &self,
        batch: &RecordBatch,
    ) -> Result<(RecordBatch, Option<BooleanArray>, u64, Fitted), arrow_schema::ArrowError> {
        let fitted = self.splits.fit(batch, false)?;
        let split = self.splits.kept(&fitted, batch.num_rows());
        let (batch, kept, discarded_rows) = discard_rows(batch, &self.routes, split)?;
        let fitted = match &kept {
            Some(kept) => fitted.filtered(kept)?,
            None => fitted,
        };
        Ok((batch, kept, discarded_rows, fitted))
    }

    /// The error for a value of a column of JSON that `column` holds in part that cannot be read.
    fn unread(&self, column: &Field, error: &arrow_schema::ArrowError) -> Error {
        Error::internal(format!(
            "stream {}: column {}: reading the values of JSON text it holds: {error}",
            self.stream,
            column.name()
        ))
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
            Error::internal(format!("stream {stream}: sequencing a batch: {error}"))
        };
        let positions = match lineage {
            Some(lineage) => Some(kept_rows(&lineage.root_row, kept).map_err(failed)?),
            None => kept.map(positions),
        };
        sequence(&self.view, stamp, rows, positions.as_ref()).map_err(failed)
    }

    /// The lineage columns of the plan's table, lowered, from `lineage` and the rows `kept`
    /// keeps; the first is the table's column at `first`.
    fn lineage(
        &self,
        lineage: Option<&Lineage>,
        kept: Option<&BooleanArray>,
        first: usize,
    ) -> Result<Vec<ArrayRef>, Error> {
        let stream = &self.stream;
        lineage_columns(&self.view, stream, lineage, kept)?
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

    /// The load id and load start columns for `rows` rows: slices of arrays built once, and
    /// built again only for a batch more than the arrays hold, whose keys take a byte a row.
    fn constants(
        &self,
        stamp: &Stamp,
        rows: usize,
    ) -> Result<[ArrayRef; 2], arrow_schema::ArrowError> {
        let mut constants = self.constants.lock();
        let fits = constants.as_ref().is_some_and(|built| {
            built.load_id == stamp.load_id
                && built.loaded_at == stamp.loaded_at
                && built.rows >= rows
        });
        if !fits {
            *constants = Some(Constants::new(&self.view, stamp, rows)?);
        }
        let built = constants.as_ref().expect("the constants were just built");
        Ok(built.columns.clone().map(|column| column.slice(0, rows)))
    }
}

/// `array`, of type `from`, as `column` holds it and `lowered` stores it; a value the column
/// cannot hold fails the batch.
fn store(
    stream: &StreamName,
    array: &ArrayRef,
    from: &LogicalType,
    column: &Field,
    lowered: &LogicalType,
) -> Result<ArrayRef, Error> {
    convert(array, from, column.logical_type())
        .and_then(|array| lower_array(&array, column.logical_type(), lowered))
        .map_err(|error| {
            let detail = format!(
                "stream {stream}: column {} cannot hold a value of the batch: {error}",
                column.name()
            );
            Error::schema(detail)
                .with_code("value_unrepresentable")
                .with_stream(stream)
        })
}

/// The lineage columns of `view`'s rows and their types: `lineage`, the rows `kept` keeps where
/// the schema policy dropped some; none for a stream that does not normalize.
fn lineage_columns(
    view: &TableView,
    stream: &StreamName,
    lineage: Option<&Lineage>,
    kept: Option<&BooleanArray>,
) -> Result<Vec<(ArrayRef, LogicalType)>, Error> {
    if view.meta.id.is_none() {
        return Ok(Vec::new());
    }
    let missing =
        |what: &str| Error::internal(format!("stream {stream}: a {what} batch has no lineage"));
    let lineage = lineage.ok_or_else(|| missing("normalized table's"))?;
    let mut columns = vec![(Arc::clone(&lineage.id), ID_TYPE)];
    if view.meta.parent.is_some() {
        let parent = lineage
            .parent
            .as_ref()
            .ok_or_else(|| missing("child table's"))?;
        columns.push((Arc::clone(&parent.id), ID_TYPE));
        columns.push((Arc::clone(&parent.root), ID_TYPE));
        columns.push((Arc::clone(&parent.idx), IDX_TYPE));
    }
    let Some(kept) = kept else {
        return Ok(columns);
    };
    columns
        .into_iter()
        .map(|(array, logical)| {
            let array = arrow_select::filter::filter(array.as_ref(), kept).map_err(|error| {
                Error::internal(format!("stream {stream}: filtering lineage: {error}"))
            })?;
            Ok((array, logical))
        })
        .collect()
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

/// `array` of `logical` as the destination stores it: as it is, or as its text, which for
/// nested values and JSON is JSON (spec §8.7).
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
