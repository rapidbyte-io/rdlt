//! Cases of a wide table of 64-bit integers, each column charged what concatenating it copies:
//! what the heap tests hold a piece to, of a row or two, or of many rows in several parts; and
//! the wide batches a source pushes, which the heap tests hold to what a push is charged.

use std::sync::Arc;

use arrow_array::types::Int32Type;
use arrow_array::{
    ArrayRef, BooleanArray, DictionaryArray, Int32Array, Int64Array, RecordBatch, StringArray,
    StringViewArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer};
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

    /// An append of `rows` rows of a table of `columns` columns of text, pushed as views each
    /// holding `buffers` data buffers.
    pub(crate) fn wide_views(rows: u32, columns: usize, buffers: usize) -> Self {
        let made = Made::append(views(rows, columns, buffers));
        Self::planned(Storage::Native.capabilities(), made)
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

    /// A history table keyed by `id` of `rows` rows of a table of `columns` integer columns and
    /// the instant `at` its versions begin at.
    pub(crate) fn wide_history(rows: u32, columns: usize) -> Self {
        let batch = integers(rows, columns, |row| row);
        let at = TimestampSecondArray::from_iter_values(0..i64::from(rows));
        let at = Arc::new(at.with_timezone("UTC"));
        let batch = with_column(&batch, "at", instant(TimeUnit::Second), at);
        let made = Made::keyed(WriteMode::History, Meta::History, batch);
        Self::planned(Storage::Native.capabilities(), made)
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
pub(crate) fn integers(rows: u32, columns: usize, key: impl Fn(i64) -> i64) -> RecordBatch {
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

/// `rows` rows of `columns` columns of text behind 32-bit keys, `c0` on, each column's keys naming
/// a dictionary of a word of its own.
pub(crate) fn dictionaries(rows: u32, columns: usize) -> RecordBatch {
    let rows = usize::try_from(rows).expect("a row count");
    let named = (0..columns).map(|column| {
        let words = Arc::new(StringArray::from(vec![format!("word {column}")]));
        let keys = Int32Array::from(vec![0; rows]);
        let keyed = DictionaryArray::<Int32Type>::try_new(keys, words);
        let keyed: ArrayRef = Arc::new(keyed.expect("keys within the dictionary"));
        (format!("c{column}"), keyed)
    });
    RecordBatch::try_from_iter(named).expect("columns of one length")
}

/// `rows` rows of `columns` columns of booleans, `c0` on, each with a validity that holds no null.
pub(crate) fn flags(rows: u32, columns: usize) -> RecordBatch {
    let rows = usize::try_from(rows).expect("a row count");
    let named = (0..columns).map(|column| {
        let valid = Some(NullBuffer::new_valid(rows));
        let flags: ArrayRef = Arc::new(BooleanArray::new(BooleanBuffer::new_set(rows), valid));
        (format!("c{column}"), flags)
    });
    RecordBatch::try_from_iter(named).expect("columns of one length")
}

/// `rows` rows of `columns` columns of text as views, `c0` on, each holding `buffers` data buffers
/// of sixteen bytes that no view names: every value is short enough to be held in its view.
pub(crate) fn views(rows: u32, columns: usize, buffers: usize) -> RecordBatch {
    let rows = usize::try_from(rows).expect("a row count");
    let named = (0..columns).map(|column| {
        let (views, _, nulls) = StringViewArray::from(vec!["view"; rows]).into_parts();
        let data: Vec<Buffer> = (0..buffers).map(|_| Buffer::from(vec![0_u8; 16])).collect();
        let views = StringViewArray::try_new(views, data, nulls).expect("views name no buffer");
        (format!("c{column}"), Arc::new(views) as ArrayRef)
    });
    RecordBatch::try_from_iter(named).expect("columns of one length")
}
