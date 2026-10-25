//! Cases of a wide table of 64-bit integers, each column charged what concatenating it copies:
//! what the heap tests hold a piece of several parts to.

use std::sync::Arc;

use arrow_array::{
    ArrayRef, Int64Array, RecordBatch, TimestampNanosecondArray, TimestampSecondArray,
};
use rdlt_connector::{ReadMode, TimeUnit};

use super::{Case, Made, Meta, Pushed, Storage, changed, instant, stream, with_column};
use crate::plan::WriteMode;
use crate::policy::{SchemaPolicy, SchemaSettings};
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

    /// A merge by `id` of `rows` rows of a table of `columns` integer columns, each key held by
    /// two rows, of which the merge keeps the later.
    pub(crate) fn wide_merge(rows: u32, columns: usize) -> Self {
        let half = i64::from(rows / 2).max(1);
        let batch = integers(rows, columns, |row| row % half);
        let made = Made::keyed(WriteMode::Merge, Meta::Merged, batch);
        let mut built = Self::planned(Storage::Native.capabilities(), made);
        built.kept.rows = usize::try_from(half).expect("a row count");
        built
    }

    /// An append of `rows` rows of a table of `columns` integer columns and an instant in
    /// nanoseconds, pushed in seconds, the first row's and every sixty-fourth's beyond what a
    /// nanosecond holds and those rows dropped by the stream.
    pub(crate) fn wide_dropped_rows(rows: u32, columns: usize) -> Self {
        let batch = integers(rows, columns, |row| row);
        let nanos = TimestampNanosecondArray::from_iter_values(0..i64::from(rows));
        let nanos = Arc::new(nanos.with_timezone("UTC"));
        let created = with_column(&batch, "at", instant(TimeUnit::Nanosecond), nanos);
        let far = (0..i64::from(rows)).map(|row| if row % 64 == 0 { i64::MAX } else { row });
        let seconds = Arc::new(TimestampSecondArray::from_iter_values(far).with_timezone("UTC"));
        let pushed = with_column(&batch, "at", instant(TimeUnit::Second), seconds);
        let mut made = Made::into_table(created, pushed);
        made.stream = made
            .stream
            .schema(SchemaSettings::new().policy(SchemaPolicy::DiscardRow));
        let mut built = Self::planned(Storage::Native.capabilities(), made);
        built.kept.rows -= usize::try_from(rows.div_ceil(64)).expect("a row count");
        built
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
