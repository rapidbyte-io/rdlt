//! Lowering plans (spec §7.3): how batches of one incoming schema become rows of one table view,
//! worked out once and applied to every batch — discards, exact conversions, lowering, metadata
//! columns and, for merge tables, the sequence column and compaction.

mod merge;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::builder::FixedSizeBinaryBuilder;
use arrow_array::types::Int8Type;
use arrow_array::{
    Array, ArrayRef, BooleanArray, DictionaryArray, Int8Array, RecordBatch,
    TimestampMicrosecondArray, new_null_array,
};
use parking_lot::Mutex;
use rdlt_connector::{Field, LoadId, LogicalType, SegmentId, StreamName};

use super::TableView;
use super::convert::{convert, text};
use super::lower::{ID_TYPE, IDX_TYPE, LOAD_ID_TYPE, loaded_at_type};
use super::resolve::{Incoming, Route};
use crate::error::Error;
use crate::normalize::Lineage;
use merge::{check_key, compact, positions, sequence};

/// What the metadata columns of a batch hold.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Stamp {
    pub(crate) load_id: LoadId,
    /// When the load started.
    pub(crate) loaded_at: SystemTime,
    /// The batch's segment, and the position of its first row among the rows written to it.
    pub(crate) segment: SegmentId,
    pub(crate) first_row: u64,
}

/// A batch ready for its table, and what the schema policy discarded from it.
#[derive(Debug)]
pub(crate) struct Prepared {
    pub(crate) batch: RecordBatch,
    /// Rows dropped because they carried a discarded change.
    pub(crate) discarded_rows: u64,
    /// Values nulled because they carried a discarded change.
    pub(crate) discarded_values: u64,
    /// Which of the batch's rows the schema policy kept, where it dropped some.
    pub(crate) kept: Option<BooleanArray>,
}

/// Where one of the view's columns takes its values from.
#[derive(Clone, Debug)]
enum Source {
    /// The incoming column at this position, of this type.
    Incoming(usize, LogicalType),
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
    /// The metadata columns holding one value per load, built once and sliced per batch.
    constants: Mutex<Option<Constants>>,
}

/// The load id and load start as dictionary arrays of one value, for up to `rows` rows.
#[derive(Debug)]
struct Constants {
    load_id: LoadId,
    loaded_at: SystemTime,
    rows: usize,
    columns: [ArrayRef; 2],
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
                Route::DiscardValues => discarded.push(index),
                Route::DiscardRows | Route::Skip => {}
            }
        }
        Self {
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
    ) -> Result<Prepared, Error> {
        let stream = &self.stream;
        let view = &self.view;
        let failed = |error: arrow_schema::ArrowError| {
            Error::internal(format!("stream {stream}: preparing a batch: {error}"))
        };
        let (batch, kept, discarded_rows) = discard_rows(batch, &self.routes).map_err(failed)?;
        if batch.num_rows() == 0 {
            return Ok(Prepared {
                batch: RecordBatch::new_empty(Arc::clone(&view.schema)),
                discarded_rows,
                discarded_values: 0,
                kept,
            });
        }
        let discarded_values = self
            .discarded
            .iter()
            .map(|index| {
                let column = batch.column(*index);
                (column.len() - column.null_count()) as u64
            })
            .sum();
        check_key(stream, view, &batch, &self.sources)?;
        let rows = batch.num_rows();
        let mut columns = Vec::with_capacity(view.physical.len());
        for ((column, lowered), source) in view
            .model
            .columns
            .iter()
            .zip(&view.lowered)
            .zip(&self.sources)
        {
            let array = match source {
                Source::Incoming(index, from) => {
                    store(stream, batch.column(*index), from, column, lowered)?
                }
                Source::Nulls => new_null_array(&lowered.to_arrow(), rows),
            };
            columns.push(array);
        }
        columns.extend(self.constants(stamp, rows).map_err(failed)?);
        if view.meta.seq.is_some() {
            columns.push(self.sequence(lineage, kept.as_ref(), stamp, rows)?);
        }
        let first = columns.len();
        columns.extend(self.lineage(lineage, kept.as_ref(), first)?);
        let prepared = RecordBatch::try_new(Arc::clone(&view.schema), columns).map_err(failed)?;
        let prepared = if view.compacts() {
            compact(&prepared, &view.key).map_err(failed)?
        } else {
            prepared
        };
        Ok(Prepared {
            batch: prepared,
            discarded_rows,
            discarded_values,
            kept,
        })
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
    /// built again only for a batch more than the arrays hold or less than half of it.
    fn constants(
        &self,
        stamp: &Stamp,
        rows: usize,
    ) -> Result<[ArrayRef; 2], arrow_schema::ArrowError> {
        let mut constants = self.constants.lock();
        let fits = constants.as_ref().is_some_and(|built| {
            built.load_id == stamp.load_id
                && built.loaded_at == stamp.loaded_at
                && (rows..=rows.saturating_mul(2)).contains(&built.rows)
        });
        if !fits {
            *constants = Some(Constants::new(&self.view, stamp, rows)?);
        }
        let built = constants.as_ref().expect("the constants were just built");
        Ok(built.columns.clone().map(|column| column.slice(0, rows)))
    }
}

impl Constants {
    /// The constant metadata columns of `view` for `rows` rows of the load `stamp` names.
    fn new(view: &TableView, stamp: &Stamp, rows: usize) -> Result<Self, arrow_schema::ArrowError> {
        let lowered = |index: usize| view.physical[view.model.columns.len() + index].logical_type();
        let mut load_ids = FixedSizeBinaryBuilder::with_capacity(1, 16);
        load_ids.append_value(stamp.load_id.as_bytes())?;
        let load_id: ArrayRef = Arc::new(load_ids.finish());
        let micros = stamp
            .loaded_at
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| {
                i64::try_from(since.as_micros()).unwrap_or(i64::MAX)
            });
        let loaded_at: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(vec![micros]).with_timezone("UTC"));
        let constant = |value: ArrayRef| -> Result<ArrayRef, arrow_schema::ArrowError> {
            let keys = Int8Array::from(vec![0; rows]);
            Ok(Arc::new(DictionaryArray::<Int8Type>::try_new(keys, value)?))
        };
        Ok(Self {
            load_id: stamp.load_id,
            loaded_at: stamp.loaded_at,
            rows,
            columns: [
                constant(lower_array(&load_id, &LOAD_ID_TYPE, lowered(0))?)?,
                constant(lower_array(&loaded_at, &loaded_at_type(), lowered(1))?)?,
            ],
        })
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

/// `batch` without the rows holding a value in a column routed to [`Route::DiscardRows`], and
/// how many rows were dropped.
fn discard_rows(
    batch: &RecordBatch,
    routes: &[Route],
) -> Result<(RecordBatch, Option<BooleanArray>, u64), arrow_schema::ArrowError> {
    let discarding: Vec<&ArrayRef> = routes
        .iter()
        .enumerate()
        .filter(|(_, route)| **route == Route::DiscardRows)
        .map(|(index, _)| batch.column(index))
        .collect();
    if discarding
        .iter()
        .all(|column| column.null_count() == column.len())
    {
        return Ok((batch.clone(), None, 0));
    }
    let keep: BooleanArray = (0..batch.num_rows())
        .map(|row| Some(discarding.iter().all(|column| column.is_null(row))))
        .collect();
    let kept = arrow_select::filter::filter_record_batch(batch, &keep)?;
    let dropped = (batch.num_rows() - kept.num_rows()) as u64;
    Ok((kept, Some(keep), dropped))
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
