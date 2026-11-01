//! The columns a case's batches are made of: a column added or put in another's place, and the
//! values of instants, UUIDs, JSON text and a change stream's flags.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, Int8Array, RecordBatch, StringArray,
};
use arrow_schema::{Field as ArrowField, Schema};
use rdlt_connector::{
    ChangeOp, Field, LogicalType, OP_COLUMN, SEQ_COLUMN, TimeUnit, UNCHANGED_COLUMN,
};

/// A timestamp of `unit` in UTC.
pub(super) fn instant(unit: TimeUnit) -> LogicalType {
    LogicalType::Timestamp(unit, Some("UTC".into()))
}

/// `batch` and a column `name` of `logical` type holding `values`.
pub(super) fn with_column(
    batch: &RecordBatch,
    name: &str,
    logical: LogicalType,
    values: ArrayRef,
) -> RecordBatch {
    let schema = batch.schema();
    let mut fields: Vec<ArrowField> = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields.push(Field::new(name, logical, true).to_arrow());
    let mut columns = batch.columns().to_vec();
    columns.push(values);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .expect("a column of the batch's rows")
}

/// `batch` with its column `name` of `logical` type holding `values` in its place.
pub(super) fn replaced(
    batch: &RecordBatch,
    name: &str,
    logical: LogicalType,
    values: ArrayRef,
) -> RecordBatch {
    let schema = batch.schema();
    let index = schema.index_of(name).expect("the batch has the column");
    let mut fields: Vec<ArrowField> = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields[index] = Field::new(name, logical, true).to_arrow();
    let mut columns = batch.columns().to_vec();
    columns[index] = values;
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .expect("a column of the batch's rows")
}

/// `rows` distinct UUIDs.
pub(super) fn uuids(rows: u32) -> ArrayRef {
    let values = (0..u128::from(rows)).map(|row| (row * 0x9E37_79B9_7F4A_7C15).to_be_bytes());
    Arc::new(FixedSizeBinaryArray::try_from_iter(values).expect("sixteen bytes a value"))
}

/// `rows` values of JSON text: integers, but every tenth a string and every hundredth an object.
pub(super) fn json(rows: u32) -> ArrayRef {
    let value = |row: u32| match row {
        row if row % 100 == 0 => format!(r#"{{"cents":{row}}}"#),
        row if row % 10 == 0 => format!(r#""{row}""#),
        row => row.to_string(),
    };
    Arc::new(StringArray::from_iter_values((0..rows).map(value)))
}

/// `data` as a change batch of updates, each flagging one column unchanged: row `row` the column
/// `flagged(row)` places among `data`'s.
pub(super) fn changed(data: &RecordBatch, flagged: impl Fn(usize) -> usize) -> RecordBatch {
    let rows = data.num_rows();
    let columns = data.num_columns();
    let ops = Int8Array::from(vec![ChangeOp::Update.code(); rows]);
    let seqs = (0..rows).map(|row| (row as u128).to_be_bytes());
    let seqs = BinaryArray::from_iter_values(seqs);
    let bytes = (columns + 3).div_ceil(8);
    let flags = (0..rows).map(|row| {
        let column = flagged(row);
        let mut bitmap = vec![0_u8; bytes];
        bitmap[column / 8] |= 1 << (column % 8);
        bitmap
    });
    let flags = BinaryArray::from_iter_values(flags);
    let schema = data.schema();
    let mut fields: Vec<ArrowField> = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields.extend([
        ArrowField::new(OP_COLUMN, ops.data_type().clone(), false),
        ArrowField::new(SEQ_COLUMN, seqs.data_type().clone(), false),
        ArrowField::new(UNCHANGED_COLUMN, flags.data_type().clone(), true),
    ]);
    let mut arrays = data.columns().to_vec();
    arrays.extend([Arc::new(ops) as ArrayRef, Arc::new(seqs), Arc::new(flags)]);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("a change batch")
}
