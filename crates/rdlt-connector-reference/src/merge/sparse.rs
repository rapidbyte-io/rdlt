//! A merge's rows by the columns they hold: a column a row never had costs the row nothing.
//!
//! A table gains columns while it holds rows written before them. Each batch a merge reads, and
//! each it gives back, carries only the columns one of its rows holds a value in, under the
//! table's types, and a merged row is found by the rows its cells come from. Rows that hold the
//! same columns are given back together, so a table is as many batches as its rows have shapes,
//! and a row of many columns among many rows of few costs its own cells alone.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, UInt32Array, new_null_array};
use arrow_schema::{ArrowError, DataType, Schema, SchemaRef};

use super::refused::{VALUE_UNHOLDABLE, refused};
use super::retype::retyped;

/// A row of a source batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct At {
    pub(super) source: usize,
    pub(super) row: usize,
}

/// Arrays of nulls by type and length, made once and shared by every column that needs one.
#[derive(Debug, Default)]
pub(super) struct Nulls(HashMap<(DataType, usize), ArrayRef>);

impl Nulls {
    /// `rows` nulls of `kind`.
    pub(super) fn of(&mut self, kind: &DataType, rows: usize) -> ArrayRef {
        let nulls = self
            .0
            .entry((kind.clone(), rows))
            .or_insert_with(|| new_null_array(kind, rows));
        Arc::clone(nulls)
    }
}

/// One batch a merge reads: the columns one of its rows holds a value in, by their place in
/// the table's schema, each under the table's type.
#[derive(Debug)]
struct Source {
    rows: usize,
    /// The held columns, by ascending place in the schema.
    held: Vec<(usize, ArrayRef)>,
}

impl Source {
    fn column(&self, column: usize) -> Option<&ArrayRef> {
        let found = self.held.binary_search_by_key(&column, |(held, _)| *held);
        found.ok().map(|index| &self.held[index].1)
    }
}

/// The batches a merge reads under one schema, the table's.
#[derive(Debug)]
pub(super) struct Sources {
    schema: SchemaRef,
    list: Vec<Source>,
}

impl Sources {
    /// No batch yet, under `schema`.
    pub(super) fn new(schema: &SchemaRef) -> Self {
        Self {
            schema: Arc::clone(schema),
            list: Vec::new(),
        }
    }

    /// Adds `batch`, whose columns are found by name and converted to the schema's types where
    /// that keeps every value, and returns its place among the sources.
    ///
    /// A column the batch lacks, or holds no value in, is not held: its rows cost nothing for
    /// it. A value the schema's type cannot hold fails.
    pub(super) fn add(&mut self, batch: &RecordBatch) -> Result<usize, ArrowError> {
        let mut held = Vec::new();
        for (column, field) in self.schema.fields().iter().enumerate() {
            let Some(values) = batch.column_by_name(field.name()) else {
                continue;
            };
            if values.null_count() == values.len() {
                continue;
            }
            held.push((column, retyped(values, field.data_type())?));
        }
        self.list.push(Source {
            rows: batch.num_rows(),
            held,
        });
        Ok(self.list.len() - 1)
    }

    /// Adds a source of `rows` rows that holds `held` alone, the column at each place given as
    /// it is: for values a merge computes of its rows, which no batch carries.
    pub(super) fn add_held(&mut self, rows: usize, mut held: Vec<(usize, ArrayRef)>) -> usize {
        held.sort_by_key(|(column, _)| *column);
        self.list.push(Source { rows, held });
        self.list.len() - 1
    }

    pub(super) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// How many batches were added.
    pub(super) fn len(&self) -> usize {
        self.list.len()
    }

    /// How many rows the source at `source` has.
    pub(super) fn rows(&self, source: usize) -> usize {
        self.list[source].rows
    }

    /// The column at `column` of the source at `source`, where it holds one.
    pub(super) fn column(&self, source: usize, column: usize) -> Option<&ArrayRef> {
        self.list[source].column(column)
    }

    /// The column at `column` of the source at `source`, nulls where it holds none: for the few
    /// columns a merge reads of every row, as its key and its sequence.
    pub(super) fn dense(&self, source: usize, column: usize, nulls: &mut Nulls) -> ArrayRef {
        match self.column(source, column) {
            Some(values) => Arc::clone(values),
            None => nulls.of(self.schema.field(column).data_type(), self.rows(source)),
        }
    }

    /// Whether the cell at `column` of the row `at` is null.
    pub(super) fn is_null(&self, at: At, column: usize) -> bool {
        self.column(at.source, column)
            .is_none_or(|values| values.is_null(at.row))
    }
}

/// Where a merged row's cells come from.
#[derive(Clone, Copy, Debug)]
pub(super) struct Pick<'a> {
    /// The cells no entry of `over` names.
    pub(super) base: Base<'a>,
    /// Columns whose cell comes from another row, or is null, by ascending column.
    pub(super) over: &'a [(usize, Option<At>)],
}

/// Where the cells of a merged row come from but for those set apart.
#[derive(Clone, Copy, Debug)]
pub(super) enum Base<'a> {
    /// Every cell is that of one source row.
    Row(At),
    /// Each cell from its own source row, or null, in the schema's order.
    Cells(&'a [Option<At>]),
}

impl Pick<'_> {
    /// Whether `over` names `column`.
    fn sets_apart(&self, column: usize) -> bool {
        self.over
            .binary_search_by_key(&column, |(over, _)| *over)
            .is_ok()
    }

    /// The row the cell at `column` comes from; none for a null.
    fn cell(&self, column: usize) -> Option<At> {
        if let Ok(index) = self.over.binary_search_by_key(&column, |(over, _)| *over) {
            return self.over[index].1;
        }
        match self.base {
            Base::Row(at) => Some(at),
            Base::Cells(cells) => cells.get(column).copied().flatten(),
        }
    }

    /// The row every cell comes from, where one row gives them all.
    fn whole(&self) -> Option<At> {
        match self.base {
            Base::Row(at) if self.over.is_empty() => Some(at),
            _ => None,
        }
    }
}

/// The rows `picks` name as batches, each of the columns its rows hold, in the schema's order
/// and under its types; rows holding the same columns share a batch.
pub(super) fn assemble<'a>(
    sources: &Sources,
    picks: impl IntoIterator<Item = Pick<'a>>,
) -> Result<Vec<RecordBatch>, ArrowError> {
    // Rows that are one source row whole, by source, in the order given; the others by the
    // columns they hold.
    let mut whole: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    let mut composed: BTreeMap<Vec<usize>, Vec<Pick<'a>>> = BTreeMap::new();
    for pick in picks {
        match pick.whole() {
            Some(at) => whole.entry(at.source).or_default().push(at.row),
            None => composed
                .entry(shape(sources, &pick))
                .or_default()
                .push(pick),
        }
    }
    let mut shaped: BTreeMap<Vec<usize>, Vec<RecordBatch>> = BTreeMap::new();
    for (source, rows) in whole {
        let held = &sources.list[source].held;
        let columns: Vec<usize> = held.iter().map(|(column, _)| *column).collect();
        let batch = taken(sources, source, &rows)?;
        shaped.entry(columns).or_default().push(batch);
    }
    for (columns, picks) in composed {
        let batch = composed_batch(sources, &columns, &picks)?;
        shaped.entry(columns).or_default().push(batch);
    }
    shaped
        .into_values()
        .filter(|batches| batches.iter().any(|batch| batch.num_rows() != 0))
        .map(|batches| match &batches[..] {
            [one] => Ok(one.clone()),
            many => arrow_select::concat::concat_batches(&many[0].schema(), many),
        })
        .collect()
}

/// The columns `pick`'s row holds: those one of its cells comes from a source that holds it.
fn shape(sources: &Sources, pick: &Pick<'_>) -> Vec<usize> {
    let held = |column: usize, at: Option<At>| {
        at.is_some_and(|at| sources.column(at.source, column).is_some())
    };
    let mut columns: Vec<usize> = match pick.base {
        // The base row's own columns, but for those another row gives.
        Base::Row(at) => sources.list[at.source]
            .held
            .iter()
            .map(|(column, _)| *column)
            .filter(|column| !pick.sets_apart(*column))
            .collect(),
        Base::Cells(cells) => (0..cells.len())
            .filter(|column| !pick.sets_apart(*column) && held(*column, cells[*column]))
            .collect(),
    };
    let apart = pick.over.iter().filter(|(column, at)| held(*column, *at));
    columns.extend(apart.map(|(column, _)| *column));
    columns.sort_unstable();
    columns
}

/// The schema of a batch holding `columns` of `schema`.
fn held_schema(schema: &SchemaRef, columns: &[usize]) -> SchemaRef {
    let fields: Vec<_> = columns
        .iter()
        .map(|column| Arc::clone(&schema.fields()[*column]))
        .collect();
    Arc::new(Schema::new(fields))
}

fn batch(
    schema: SchemaRef,
    columns: Vec<ArrayRef>,
    rows: usize,
) -> Result<RecordBatch, ArrowError> {
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    RecordBatch::try_new_with_options(schema, columns, &options)
}

/// A row's place as an index Arrow takes rows by.
fn index(row: usize) -> Result<u32, ArrowError> {
    u32::try_from(row).map_err(|_| refused(VALUE_UNHOLDABLE, "a batch holds too many rows"))
}

/// The rows `rows` of the source at `source`, in that order, as a batch of the columns it
/// holds; the source's own arrays where the rows are all of them in order.
fn taken(sources: &Sources, source: usize, rows: &[usize]) -> Result<RecordBatch, ArrowError> {
    let from = &sources.list[source];
    let columns: Vec<usize> = from.held.iter().map(|(column, _)| *column).collect();
    let schema = held_schema(&sources.schema, &columns);
    let all = rows.len() == from.rows && rows.iter().enumerate().all(|(place, row)| place == *row);
    if all {
        let arrays = from.held.iter().map(|(_, values)| Arc::clone(values));
        return batch(schema, arrays.collect(), rows.len());
    }
    let indices: UInt32Array = rows
        .iter()
        .map(|row| index(*row).map(Some))
        .collect::<Result<_, _>>()?;
    let arrays = from
        .held
        .iter()
        .map(|(_, values)| arrow_select::take::take(values.as_ref(), &indices, None))
        .collect::<Result<Vec<_>, _>>()?;
    batch(schema, arrays, rows.len())
}

/// The rows `picks` name, which hold `columns`, as a batch of those columns: each cell taken
/// from the row it comes from.
fn composed_batch(
    sources: &Sources,
    columns: &[usize],
    picks: &[Pick<'_>],
) -> Result<RecordBatch, ArrowError> {
    let mut arrays = Vec::with_capacity(columns.len());
    for column in columns {
        let null = new_null_array(sources.schema.field(*column).data_type(), 1);
        // The sources the column's cells come from, each once, the null last.
        let mut places: HashMap<usize, usize> = HashMap::new();
        let mut values: Vec<&dyn Array> = Vec::new();
        let mut cells: Vec<Option<(usize, usize)>> = Vec::with_capacity(picks.len());
        for pick in picks {
            let cell = pick.cell(*column).and_then(|at| {
                let held = sources.column(at.source, *column)?;
                let place = *places.entry(at.source).or_insert_with(|| {
                    values.push(held.as_ref());
                    values.len() - 1
                });
                Some((place, at.row))
            });
            cells.push(cell);
        }
        let absent = values.len();
        values.push(null.as_ref());
        let indices: Vec<(usize, usize)> = cells
            .into_iter()
            .map(|cell| cell.unwrap_or((absent, 0)))
            .collect();
        arrays.push(arrow_select::interleave::interleave(&values, &indices)?);
    }
    batch(held_schema(&sources.schema, columns), arrays, picks.len())
}

/// The cells `batches`, each of the columns it holds, lack of the columns of `schema`: the rows
/// of each batch times the columns it does not hold, which a destination writing every column
/// of every row makes.
pub(super) fn absent(schema: &SchemaRef, batches: &[RecordBatch]) -> u64 {
    let columns = u64::try_from(schema.fields().len()).unwrap_or(u64::MAX);
    batches.iter().fold(0_u64, |absent, batch| {
        let held = schema
            .fields()
            .iter()
            .filter(|field| batch.column_by_name(field.name()).is_some())
            .count();
        let lacking = columns.saturating_sub(u64::try_from(held).unwrap_or(u64::MAX));
        let rows = u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
        absent.saturating_add(rows.saturating_mul(lacking))
    })
}

/// `batches`, each of the columns it holds, as batches of every column of `schema`, at its
/// types: a column a batch does not hold is nulls, one array for every such column of its type
/// and length.
pub(super) fn every_column(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    nulls: &mut Nulls,
) -> Result<Vec<RecordBatch>, ArrowError> {
    batches
        .iter()
        .map(|sparse| {
            let rows = sparse.num_rows();
            let columns = schema
                .fields()
                .iter()
                .map(|field| match sparse.column_by_name(field.name()) {
                    // A batch kept since before its column widened holds the type it had.
                    Some(column) => retyped(column, field.data_type()),
                    None => Ok(nulls.of(field.data_type(), rows)),
                })
                .collect::<Result<Vec<_>, _>>()?;
            batch(Arc::clone(schema), columns, rows)
        })
        .collect()
}
