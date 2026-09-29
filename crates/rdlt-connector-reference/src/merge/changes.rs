//! Merging a change stream's rows: each row applies in sequence order, only when its sequence is
//! greater than the published row's, as an insert, update, delete or truncate (spec §9.3, §9.4).

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int8Type;
use arrow_array::{Array, ArrayRef, RecordBatch, new_null_array};
use arrow_row::{RowConverter, Rows};
use arrow_schema::{ArrowError, DataType, Schema, SchemaRef};
use rdlt_connector::{ChangeColumns, ChangeOp, Deletion, MergeKey};

use super::tombstones::{self, Tombstones};
use super::{align, concat, converter, key_columns};

/// Where one cell of a merged row comes from.
#[derive(Clone, Copy, Debug)]
enum Cell {
    /// The published row at this index.
    Published(usize),
    /// Row `.1` of incoming batch `.0`.
    Incoming(usize, usize),
    /// Nowhere: the cell is null.
    Null,
}

/// A merged row: its sequence and where each of its cells comes from, in the schema's order.
struct Merged {
    seq: Vec<u8>,
    cells: Vec<Cell>,
}

/// The rows of a table, by key, in the order they were first published.
struct Table {
    rows: Vec<Option<Merged>>,
    by_key: BTreeMap<Vec<u8>, usize>,
}

impl Table {
    /// The table holding `published`, its rows keyed as `converter` encodes `key`.
    fn load(
        published: &RecordBatch,
        converter: &RowConverter,
        key: &MergeKey,
    ) -> Result<Self, ArrowError> {
        let mut table = Self {
            rows: Vec::new(),
            by_key: BTreeMap::new(),
        };
        let keys = converter.convert_columns(&key_columns(published, key)?)?;
        let seqs = binary(published, &key.seq)?;
        let seqs = seqs.as_binary::<i32>();
        for row in 0..published.num_rows() {
            let merged = Merged {
                seq: seqs.value(row).to_vec(),
                cells: vec![Cell::Published(row); published.num_columns()],
            };
            table.put(keys.row(row).as_ref().to_vec(), merged);
        }
        Ok(table)
    }

    fn get(&self, key: &[u8]) -> Option<&Merged> {
        self.by_key
            .get(key)
            .and_then(|index| self.rows[*index].as_ref())
    }

    fn put(&mut self, key: Vec<u8>, merged: Merged) {
        if let Some(index) = self.by_key.get(&key) {
            self.rows[*index] = Some(merged);
        } else {
            self.by_key.insert(key, self.rows.len());
            self.rows.push(Some(merged));
        }
    }

    /// Removes every row sequenced before `change`, a truncate, or marks it deleted.
    fn truncate(&mut self, change: &Change, columns: &Columns, sources: &Sources<'_>) {
        let cell = Cell::Incoming(change.batch, change.row);
        for merged in &mut self.rows {
            if merged
                .as_ref()
                .is_none_or(|merged| merged.seq >= change.seq)
            {
                continue;
            }
            match (columns.at, merged.as_mut()) {
                (Some(at), Some(kept)) => {
                    mark_deleted(kept, change, cell, columns.seq, at, sources);
                }
                _ => *merged = None,
            }
        }
        let rows = &self.rows;
        self.by_key.retain(|_, index| rows[*index].is_some());
    }

    /// Applies `change` to the row with `key`, when it is sequenced past it: an insert or
    /// update keeping the columns `flags` names, or a delete; returns what it did.
    fn apply(
        &mut self,
        key: Vec<u8>,
        change: Change,
        flags: &[usize],
        columns: &Columns,
        sources: &Sources<'_>,
    ) -> Applied {
        let current = self.get(&key);
        if current.is_some_and(|current| current.seq >= change.seq) {
            return Applied::Nothing;
        }
        let cell = Cell::Incoming(change.batch, change.row);
        if change.op == ChangeOp::Delete {
            return match (columns.at, current) {
                (None, _) => {
                    self.remove(&key);
                    Applied::Removed
                }
                (Some(at), Some(current)) => {
                    let mut kept = Merged {
                        seq: current.seq.clone(),
                        cells: current.cells.clone(),
                    };
                    mark_deleted(&mut kept, &change, cell, columns.seq, at, sources);
                    self.put(key, kept);
                    Applied::Nothing
                }
                (Some(_), None) => Applied::Nothing,
            };
        }
        let cells = (0..columns.count)
            .map(|column| match (flags.contains(&column), current) {
                (false, _) => cell,
                (true, Some(current)) => current.cells[column],
                (true, None) => Cell::Null,
            })
            .collect();
        self.put(
            key,
            Merged {
                seq: change.seq,
                cells,
            },
        );
        Applied::Held
    }

    fn remove(&mut self, key: &[u8]) {
        if let Some(index) = self.by_key.remove(key) {
            self.rows[index] = None;
        }
    }
}

/// Marks `kept` deleted by `change`, whose row is `cell`: it takes the change's sequence, and
/// the deletion time in column `at` unless it was deleted already, so it keeps when that was.
fn mark_deleted(
    kept: &mut Merged,
    change: &Change,
    cell: Cell,
    seq: usize,
    at: usize,
    sources: &Sources<'_>,
) {
    kept.seq.clone_from(&change.seq);
    kept.cells[seq] = cell;
    if sources.is_null(kept.cells[at], at) {
        kept.cells[at] = cell;
    }
}

/// The batches a merged row's cells come from.
struct Sources<'a> {
    published: &'a RecordBatch,
    aligned: &'a [RecordBatch],
}

impl Sources<'_> {
    /// Whether `cell` of `column` is null.
    fn is_null(&self, cell: Cell, column: usize) -> bool {
        match cell {
            Cell::Published(row) => self.published.column(column).is_null(row),
            Cell::Incoming(batch, row) => self.aligned[batch].column(column).is_null(row),
            Cell::Null => true,
        }
    }
}

/// Where a change stream's table keeps its sequence and deletion time, and how many columns it
/// has.
struct Columns {
    seq: usize,
    at: Option<usize>,
    count: usize,
}

/// What applying a change did to its key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Applied {
    /// It removed the key's row outright.
    Removed,
    /// A row it wrote holds the key.
    Held,
    /// Neither.
    Nothing,
}

/// One incoming row, and what it does.
struct Change {
    batch: usize,
    row: usize,
    op: ChangeOp,
    seq: Vec<u8>,
}

/// The schema a change stream's table stores: `schema` without the columns `changes` names that
/// only written batches carry.
pub(crate) fn stored(schema: &SchemaRef, changes: &ChangeColumns) -> SchemaRef {
    let directive = |name: &str| {
        name == &*changes.op
            || changes
                .unchanged
                .as_deref()
                .is_some_and(|flags| flags == name)
    };
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .filter(|field| !directive(field.name()))
        .cloned()
        .collect();
    Arc::new(Schema::new(fields))
}

/// The published rows of a change stream's table, and its tombstones, once `incoming` applies,
/// row by row in sequence order, to `published` and `buried`, its tombstones.
pub(crate) fn merge_changes(
    schema: &SchemaRef,
    published: &[RecordBatch],
    buried: &[RecordBatch],
    incoming: &[RecordBatch],
    key: &MergeKey,
    changes: &ChangeColumns,
) -> Result<(Vec<RecordBatch>, RecordBatch), ArrowError> {
    let schema = stored(schema, changes);
    let converter = converter(&schema, key)?;
    let published = concat(published, &schema)?;
    let mut table = Table::load(&published, &converter, key)?;
    let nullable = nullable(&schema);
    let aligned = incoming
        .iter()
        .map(|batch| align(batch, &nullable))
        .collect::<Result<Vec<_>, _>>()?;
    let keys = aligned
        .iter()
        .map(|batch| converter.convert_columns(&key_columns(batch, key)?))
        .collect::<Result<Vec<Rows>, _>>()?;
    let mut rows = changed_rows(incoming, &aligned, key, changes)?;
    // A stable sort keeps rows of one sequence in the order they were written.
    rows.sort_by(|left, right| left.seq.cmp(&right.seq));
    let at = match &changes.deletion {
        Deletion::Hard => None,
        Deletion::Soft { at } => Some(schema.index_of(at)?),
    };
    let columns = Columns {
        seq: schema.index_of(&key.seq)?,
        at,
        count: schema.fields().len(),
    };
    let sources = Sources {
        published: &published,
        aligned: &aligned,
    };
    let tombstone_schema = tombstones::schema(&schema, key)?;
    let mut tombstones = Tombstones::load(buried, &tombstone_schema, &converter, key)?;
    for change in rows {
        if change.op == ChangeOp::Truncate {
            if tombstones.admits(None, &change.seq) {
                table.truncate(&change, &columns, &sources);
                if columns.at.is_none() {
                    tombstones.raise(change.seq);
                }
            }
            continue;
        }
        let row_key = keys[change.batch].row(change.row).as_ref().to_vec();
        if !tombstones.admits(Some(&row_key), &change.seq) {
            continue;
        }
        let flags = unchanged(&incoming[change.batch], &schema, changes, change.row)?;
        let (batch, row, seq) = (change.batch, change.row, change.seq.clone());
        match table.apply(row_key.clone(), change, &flags, &columns, &sources) {
            Applied::Removed => tombstones.bury(row_key, seq, batch, row),
            Applied::Held => tombstones.lift(&row_key),
            Applied::Nothing => {}
        }
    }
    let merged = assemble(&schema, &published, &aligned, &table)?;
    let buried = tombstones.assemble(&tombstone_schema, &aligned, key)?;
    let merged = [merged]
        .into_iter()
        .filter(|batch| batch.num_rows() != 0)
        .collect();
    Ok((merged, buried))
}

/// `schema` with every column nullable, as incoming rows align to it: a truncate names no key.
fn nullable(schema: &SchemaRef) -> SchemaRef {
    Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone().with_nullable(true))
            .collect::<Vec<_>>(),
    ))
}

/// Every row of `incoming`, with its op and sequence.
fn changed_rows(
    incoming: &[RecordBatch],
    aligned: &[RecordBatch],
    key: &MergeKey,
    changes: &ChangeColumns,
) -> Result<Vec<Change>, ArrowError> {
    let mut rows = Vec::new();
    for (index, (raw, batch)) in incoming.iter().zip(aligned).enumerate() {
        let ops = raw
            .column_by_name(&changes.op)
            .ok_or_else(|| ArrowError::SchemaError(format!("no op column {}", changes.op)))?;
        let ops = arrow_cast::cast(ops, &DataType::Int8)?;
        let ops = ops.as_primitive::<Int8Type>();
        let seqs = binary(batch, &key.seq)?;
        let seqs = seqs.as_binary::<i32>();
        for row in 0..raw.num_rows() {
            let op = ChangeOp::from_code(ops.value(row)).ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!("{} is no op", ops.value(row)))
            })?;
            rows.push(Change {
                batch: index,
                row,
                op,
                seq: seqs.value(row).to_vec(),
            });
        }
    }
    Ok(rows)
}

/// The columns of `schema` that row `row` of `raw` flags unchanged, by their index in `schema`.
fn unchanged(
    raw: &RecordBatch,
    schema: &SchemaRef,
    changes: &ChangeColumns,
    row: usize,
) -> Result<Vec<usize>, ArrowError> {
    let Some(column) = &changes.unchanged else {
        return Ok(Vec::new());
    };
    let Some(flags) = raw.column_by_name(column) else {
        return Ok(Vec::new());
    };
    let flags = arrow_cast::cast(flags, &DataType::Binary)?;
    let flags = flags.as_binary::<i32>();
    if flags.is_null(row) {
        return Ok(Vec::new());
    }
    let bitmap = flags.value(row);
    let raw_schema = raw.schema();
    let mut indices = Vec::new();
    for (ordinal, field) in raw_schema.fields().iter().enumerate() {
        let set = bitmap
            .get(ordinal / 8)
            .is_some_and(|byte| byte & (1 << (ordinal % 8)) != 0);
        if set && let Ok(index) = schema.index_of(field.name()) {
            indices.push(index);
        }
    }
    Ok(indices)
}

/// The rows `table` holds, as one batch of `schema`, each cell taken from where it comes from.
fn assemble(
    schema: &SchemaRef,
    published: &RecordBatch,
    aligned: &[RecordBatch],
    table: &Table,
) -> Result<RecordBatch, ArrowError> {
    let rows: Vec<&Merged> = table.rows.iter().flatten().collect();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for (column, field) in schema.fields().iter().enumerate() {
        let null = new_null_array(field.data_type(), 1);
        let mut sources: Vec<&dyn Array> = vec![published.column(column).as_ref()];
        sources.extend(aligned.iter().map(|batch| batch.column(column).as_ref()));
        sources.push(null.as_ref());
        let nulls = sources.len() - 1;
        let indices: Vec<(usize, usize)> = rows
            .iter()
            .map(|merged| match merged.cells[column] {
                Cell::Published(row) => (0, row),
                Cell::Incoming(batch, row) => (batch + 1, row),
                Cell::Null => (nulls, 0),
            })
            .collect();
        columns.push(arrow_select::interleave::interleave(&sources, &indices)?);
    }
    RecordBatch::try_new_with_options(
        Arc::clone(schema),
        columns,
        &arrow_array::RecordBatchOptions::new().with_row_count(Some(rows.len())),
    )
}

/// `batch`'s column `name` as `Binary`.
fn binary(batch: &RecordBatch, name: &str) -> Result<ArrayRef, ArrowError> {
    let column = batch
        .column_by_name(name)
        .ok_or_else(|| ArrowError::SchemaError(format!("no column {name}")))?;
    arrow_cast::cast(column, &DataType::Binary)
}
