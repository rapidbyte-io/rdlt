//! The shape limits of a batch pushed in process: the ones a frame meets on the wire.

use arrow_array::{Array, RecordBatch};
use arrow_data::ArrayData;
use arrow_schema::{DataType, Schema};

use super::held::Allocations;
use super::widths::count;
use crate::error::LimitExceeded;
use crate::limits::{
    MAX_BATCH_BYTES, MAX_BATCH_ROWS, MAX_BATCH_VALUES, MAX_COLUMNS, MAX_NESTING_DEPTH,
    MAX_VIEW_BYTES,
};

/// Checks `batch` against the limits a frame meets on the wire: its rows, its columns and how
/// deep they nest, the values its columns hold together, the bytes its views name, and the bytes
/// it keeps alive.
///
/// # Errors
///
/// The first [`LimitExceeded`] found; measuring stops there.
pub fn admit(batch: &RecordBatch) -> Result<(), LimitExceeded> {
    check("batch rows", count(batch.num_rows()), MAX_BATCH_ROWS)?;
    columns(&batch.schema())?;
    let mut measured = Measured::default();
    for column in batch.columns() {
        measured.data(&column.to_data())?;
    }
    check(
        "batch bytes",
        Allocations::of(batch).bytes(),
        MAX_BATCH_BYTES,
    )
}

fn check(name: &'static str, actual: u64, limit: u64) -> Result<(), LimitExceeded> {
    if actual > limit {
        return Err(LimitExceeded {
            name,
            limit,
            actual,
        });
    }
    Ok(())
}

/// Counts the columns of `schema`, nested ones too, and how deep they nest, a top-level column
/// being the first level; a type nested beyond the limit is not looked into.
fn columns(schema: &Schema) -> Result<(), LimitExceeded> {
    let mut columns = 0_u64;
    let mut pending: Vec<(&DataType, u64)> = schema
        .fields()
        .iter()
        .map(|field| (field.data_type(), 1))
        .collect();
    while let Some((data_type, depth)) = pending.pop() {
        check("nesting depth", depth, MAX_NESTING_DEPTH)?;
        columns += 1;
        check("batch columns", columns, MAX_COLUMNS)?;
        let below = depth + 1;
        match data_type {
            DataType::List(item)
            | DataType::LargeList(item)
            | DataType::ListView(item)
            | DataType::LargeListView(item)
            | DataType::FixedSizeList(item, _)
            | DataType::Map(item, _) => pending.push((item.data_type(), below)),
            DataType::Struct(fields) => {
                pending.extend(fields.iter().map(|field| (field.data_type(), below)));
            }
            DataType::Union(fields, _) => {
                pending.extend(fields.iter().map(|(_, field)| (field.data_type(), below)));
            }
            DataType::RunEndEncoded(ends, values) => {
                pending.push((ends.data_type(), below));
                pending.push((values.data_type(), below));
            }
            // A dictionary's values are its column, encoded.
            DataType::Dictionary(_, values) => {
                columns -= 1;
                pending.push((values, depth));
            }
            _ => {}
        }
    }
    Ok(())
}

/// What a batch's columns hold, counted so far.
#[derive(Default)]
struct Measured {
    values: u64,
    view_bytes: u64,
}

impl Measured {
    /// Counts every node of `data`: its length, the sizes of its list views, and the bytes its
    /// views name beyond the twelve a view holds itself.
    fn data(&mut self, data: &ArrayData) -> Result<(), LimitExceeded> {
        let mut pending = vec![data];
        while let Some(data) = pending.pop() {
            self.count(count(data.len()))?;
            match data.data_type() {
                DataType::ListView(_) => self.sizes::<i32>(data)?,
                DataType::LargeListView(_) => self.sizes::<i64>(data)?,
                DataType::Utf8View | DataType::BinaryView => self.views(data)?,
                _ => {}
            }
            pending.extend(data.child_data());
        }
        Ok(())
    }

    fn count(&mut self, values: u64) -> Result<(), LimitExceeded> {
        self.values = self.values.saturating_add(values);
        check("batch values", self.values, MAX_BATCH_VALUES)
    }

    fn sizes<O: arrow_buffer::ArrowNativeType>(
        &mut self,
        data: &ArrayData,
    ) -> Result<(), LimitExceeded> {
        let sizes = data.buffer::<O>(1);
        for size in sizes.iter().take(data.len()) {
            self.count(count(size.as_usize()))?;
        }
        Ok(())
    }

    fn views(&mut self, data: &ArrayData) -> Result<(), LimitExceeded> {
        let views = data.buffer::<u128>(0);
        for view in views.iter().take(data.len()) {
            let length = u64::try_from(view & u128::from(u32::MAX)).unwrap_or(u64::MAX);
            if length > 12 {
                self.view_bytes = self.view_bytes.saturating_add(length);
                check("view bytes", self.view_bytes, MAX_VIEW_BYTES)?;
            }
        }
        Ok(())
    }
}
