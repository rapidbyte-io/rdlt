//! Cases of a wide table of 64-bit integers, each column charged what concatenating it copies:
//! what the heap tests hold a piece of several parts to.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use rdlt_connector::ReadMode;

use super::{Case, Made, Meta, Pushed, Storage, changed, stream};
use crate::plan::WriteMode;
use crate::table::ChangeLayout;

impl Case {
    /// A change batch of `rows` updates of a table of `columns` integer columns, each update
    /// flagging one of the last eight unchanged, into a table holding its changes as `layout`
    /// says, for a destination storing as `storage`.
    pub(crate) fn wide_changes(
        rows: u32,
        columns: usize,
        layout: ChangeLayout,
        storage: Storage,
    ) -> Self {
        let created = integers(rows, columns, |row| row);
        let last = columns - 1;
        let stream = match layout {
            ChangeLayout::Log => stream(),
            ChangeLayout::Merge { .. } => stream().write(WriteMode::Merge).key(["id"]),
        };
        let pushed = changed(&created, |row| last - row % 8);
        let made = Made {
            stream: stream.read(ReadMode::Cdc),
            meta: Meta::Changes(layout),
            created,
            pushed: Pushed::Changes(pushed),
        };
        Self::planned(storage.capabilities(), made)
    }
}

/// `rows` rows of `columns` columns of 64-bit integers, `id` first, its row `row` holding
/// `key(row)`, then `c1` on.
fn integers(rows: u32, columns: usize, key: impl Fn(i64) -> i64) -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values((0..i64::from(rows)).map(key)));
    let named = (1..columns).map(|column| {
        let factor = i64::try_from(column).expect("a column's place fits in 64 bits");
        let values = (0..i64::from(rows)).map(|row| row * factor);
        let values: ArrayRef = Arc::new(Int64Array::from_iter_values(values));
        (format!("c{column}"), values)
    });
    RecordBatch::try_from_iter(std::iter::once(("id".to_owned(), ids)).chain(named))
        .expect("columns of one length")
}
