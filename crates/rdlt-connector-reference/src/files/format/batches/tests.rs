use std::sync::Arc;

use arrow_array::builder::StringViewBuilder;
use arrow_array::types::{Int16Type, Int32Type, Int64Type};
use arrow_array::{
    Array, ArrayRef, DictionaryArray, FixedSizeListArray, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeListArray, LargeListViewArray, ListArray, ListViewArray, MapArray, NullArray,
    RecordBatch, RunArray, StringArray, StructArray, UnionArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_ipc::writer::FileWriter;
use arrow_schema::{DataType, Field, Fields, UnionFields, UnionMode};
use rdlt_connector::{ConnectorErrorKind, Result};
use rdlt_wire::Limits;
use rdlt_wire::limits::{BATCH_ROWS, BATCH_VALUES, FRAME_BYTES};

use super::{UNION_UNWRITABLE, chunks, fitted, unfit};
use crate::files::FileFormat;
use crate::files::format::ipc::IpcFile;
use crate::rooted::Dir;

/// The rows one batch holds, as a count.
fn most() -> usize {
    usize::try_from(BATCH_ROWS).unwrap()
}

/// The values one frame holds, as a count.
fn values() -> usize {
    usize::try_from(BATCH_VALUES).unwrap()
}

fn single(column: ArrayRef) -> RecordBatch {
    RecordBatch::try_from_iter_with_nullable([("c", column, true)]).unwrap()
}

/// Writes `batch` as an Arrow file and reads it back.
///
/// What is written reads back, row for row, in batches a reader accepts; what is refused is
/// refused for a limit, and leaves no file.
fn written(batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
    let root = crate::scratch::tempdir().unwrap();
    let dir = Dir::ambient(root.path()).unwrap();
    let outcome = FileFormat::Arrow.write(&dir, "rows.arrow", std::slice::from_ref(batch));
    match outcome {
        Ok(written) => {
            assert_eq!(written.rows, u64::try_from(batch.num_rows()).unwrap());
            let read = FileFormat::Arrow
                .read(&dir, "rows.arrow", batch.schema_ref())
                .expect("what was written reads back");
            let rows: usize = read.iter().map(RecordBatch::num_rows).sum();
            assert_eq!(rows, batch.num_rows());
            assert!(read.iter().all(|read| read.num_rows() <= most()));
            let joined = arrow_select::concat::concat_batches(batch.schema_ref(), &read).unwrap();
            assert_eq!(joined.column(0).to_data(), batch.column(0).to_data());
            Ok(read)
        }
        Err(error) => {
            assert_eq!(error.kind(), ConnectorErrorKind::Data, "{error}");
            assert_eq!(error.code(), Some("limit_exceeded"), "{error}");
            let left = std::fs::read_dir(root.path()).unwrap().count();
            assert_eq!(left, 0, "a refused file was left");
            Err(error)
        }
    }
}

/// Writes `batch` as an Arrow file of the batches [`fitted`] makes under `limits`, and reads it
/// back under them: what is written reads back, row for row, and what is refused is refused
/// before a reader sees it.
fn fitted_under(batch: &RecordBatch, limits: &Limits) -> Result<Vec<RecordBatch>> {
    let root = crate::scratch::tempdir().unwrap();
    let path = root.path().join("rows.arrow");
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = FileWriter::try_new(file, batch.schema_ref()).unwrap();
    fitted(batch.clone(), limits, &mut |part| {
        writer.write(part).unwrap();
        Ok(())
    })?;
    writer.finish().unwrap();
    let file = std::fs::File::open(&path).unwrap();
    let mut read = IpcFile::open(file, *limits).expect("what was written opens");
    let mut parts = Vec::new();
    while let Some(part) = read.next().expect("what was written reads back") {
        parts.push(part);
    }
    let joined = arrow_select::concat::concat_batches(batch.schema_ref(), &parts).unwrap();
    assert_eq!(joined, *batch);
    Ok(parts)
}

/// The name of the limit a write of `batch` is refused for.
fn refused(batch: &RecordBatch) -> &'static str {
    let error = written(batch).expect_err("no reader accepts the batch");
    error.limit().expect("a limit").name
}

#[test]
fn rows_however_narrow_are_written_as_batches_of_at_most_the_rows_a_reader_accepts() {
    for (rows, batches) in [(most(), 1), (most() + 1, 2), (3 * most(), 3)] {
        let batch = single(Arc::new(Int8Array::from(vec![1_i8; rows])));
        let read = written(&batch).expect("narrow rows are written");
        assert_eq!(read.len(), batches, "{rows}");
    }
    // Rows of no bytes at all, too.
    let read = written(&single(Arc::new(NullArray::new(most() + 1)))).unwrap();
    assert_eq!(read.len(), 2);
}

#[test]
fn a_batch_is_cut_by_rows_and_by_bytes_whichever_is_less() {
    let limits = Limits::default();
    let narrow = single(Arc::new(Int8Array::from(vec![1_i8; 2 * most() + 5])));
    let rows: Vec<usize> = chunks(&narrow, &limits)
        .map(|chunk| chunk.num_rows())
        .collect();
    assert_eq!(rows, [most(), most(), 5]);
    let limits = Limits {
        batch_rows: 3,
        ..limits
    };
    let rows: Vec<usize> = chunks(&narrow.slice(0, 7), &limits)
        .map(|chunk| chunk.num_rows())
        .collect();
    assert_eq!(rows, [3, 3, 1]);
    assert_eq!(chunks(&narrow.slice(0, 0), &limits).count(), 0);
}

/// A dictionary of `values` distinct values, each used once.
fn dictionary(values: usize) -> ArrayRef {
    let count = i32::try_from(values).unwrap();
    let keys = Int32Array::from_iter_values(0..count);
    let values = Int32Array::from_iter_values(0..count);
    Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap())
}

#[test]
fn a_dictionary_s_values_are_bounded_as_a_frame_of_their_own() {
    // More values than a batch holds rows: each batch the rows are cut into shares them.
    let read = written(&single(dictionary(most() + 1))).expect("a dictionary within a frame");
    assert_eq!(read.len(), 2);
    // A list of nulls: the list is a value and so is each null, at the limit and one beyond.
    let keyed = |nulls| {
        let list = listed(Arc::new(NullArray::new(nulls)));
        let keys = Int32Array::from(vec![0]);
        Arc::new(DictionaryArray::<Int32Type>::try_new(keys, list).unwrap()) as ArrayRef
    };
    written(&single(keyed(values() - 1))).expect("a dictionary at the limit");
    assert_eq!(refused(&single(keyed(values()))), "batch values");
    // Nested in a struct, the same.
    let field = Field::new("d", keyed(0).data_type().clone(), true);
    let nested = |nulls| {
        let fields = Fields::from(vec![field.clone()]);
        Arc::new(StructArray::new(fields, vec![keyed(nulls)], None)) as ArrayRef
    };
    written(&single(nested(values() - 1))).expect("a dictionary at the limit");
    assert_eq!(refused(&single(nested(values()))), "batch values");
}

/// One row holding the list `items`.
fn listed(items: ArrayRef) -> ArrayRef {
    let field = Arc::new(Field::new("item", items.data_type().clone(), true));
    let offsets = OffsetBuffer::from_lengths([items.len()]);
    Arc::new(ListArray::new(field, offsets, items, None))
}

#[test]
fn a_row_s_values_are_bounded_by_a_frame_s_whatever_bytes_they_take() {
    // The list is a value and so is each item: at the limit, and one beyond.
    written(&single(listed(Arc::new(NullArray::new(values() - 1))))).expect("at the limit");
    let nulls = single(listed(Arc::new(NullArray::new(values()))));
    assert_eq!(refused(&nulls), "batch values");
    let empty = Arc::new(StructArray::new_empty_fields(values(), None));
    assert_eq!(refused(&single(listed(empty))), "batch values");
    // More values than a batch holds rows, of no bytes and of a byte each.
    let nulls = single(listed(Arc::new(NullArray::new(2 * most()))));
    written(&nulls).expect("nulls within a frame's values");
    let bytes = single(listed(Arc::new(Int8Array::from(vec![7_i8; most() + 1]))));
    written(&bytes).expect("values of a byte each");
}

#[test]
fn rows_whose_values_together_are_beyond_a_frame_s_are_written_in_batches_a_reader_accepts() {
    // As many rows as a batch holds, in one column more than a frame's values hold for so many.
    let columns = BATCH_VALUES / BATCH_ROWS + 1;
    let batch = RecordBatch::try_from_iter_with_nullable((0..columns).map(|column| {
        let nulls = Arc::new(NullArray::new(most())) as ArrayRef;
        (format!("c{column}"), nulls, true)
    }))
    .unwrap();
    let read = written(&batch).expect("rows of nulls");
    assert!(read.len() > 1, "{}", read.len());
}

/// Two rows of three items each, as every kind of list and as a map; and the values one row
/// counts in its frame.
fn paired() -> [(&'static str, ArrayRef, u64); 6] {
    let items = || Arc::new(Int8Array::from(vec![1_i8, 2, 3, 4, 5, 6])) as ArrayRef;
    let item = Arc::new(Field::new("item", DataType::Int8, true));
    let lengths = || OffsetBuffer::from_lengths([3, 3]);
    let list = ListArray::new(Arc::clone(&item), lengths(), items(), None);
    let large_lengths = OffsetBuffer::<i64>::from_lengths([3, 3]);
    let large = LargeListArray::new(Arc::clone(&item), large_lengths, items(), None);
    let fixed = FixedSizeListArray::new(Arc::clone(&item), 3, items(), None);
    let (offsets, sizes) = (
        ScalarBuffer::from(vec![0, 3]),
        ScalarBuffer::from(vec![3, 3]),
    );
    let view = ListViewArray::new(Arc::clone(&item), offsets, sizes, items(), None);
    let (offsets, sizes) = (
        ScalarBuffer::from(vec![0, 3]),
        ScalarBuffer::from(vec![3, 3]),
    );
    let large_view = LargeListViewArray::new(item, offsets, sizes, items(), None);
    let fields = Fields::from(vec![
        Field::new("keys", DataType::Int8, false),
        Field::new("values", DataType::Int8, true),
    ]);
    let entries = StructArray::new(fields, vec![items(), items()], None);
    let field = Arc::new(Field::new("entries", entries.data_type().clone(), false));
    let map = MapArray::new(field, lengths(), entries, None, false);
    // A row is a value, and so is each item; a map's item is an entry, a key and a value. A
    // list view's items are counted as it names them and as the whole child it is written with.
    [
        ("list", Arc::new(list), 4),
        ("large list", Arc::new(large), 4),
        ("fixed-size list", Arc::new(fixed), 4),
        ("list view", Arc::new(view), 10),
        ("large list view", Arc::new(large_view), 10),
        ("map", Arc::new(map), 10),
    ]
}

#[test]
fn a_row_counts_the_values_it_holds_and_not_those_of_the_rows_beside_it() {
    let parts = |batch: &RecordBatch, batch_values| -> Result<Vec<usize>> {
        let limits = Limits {
            batch_values,
            ..Limits::default()
        };
        let mut parts = Vec::new();
        fitted(batch.clone(), &limits, &mut |part| {
            parts.push(part.num_rows());
            Ok(())
        })?;
        Ok(parts)
    };
    for (name, column, row) in paired() {
        let batch = single(column);
        // The two rows together are beyond a frame of one row's values; each alone fits it.
        assert_eq!(parts(&batch, row).unwrap(), [1, 1], "{name}");
        let error = parts(&batch, row - 1).expect_err(name);
        let limit = error.limit().expect("a limit");
        assert_eq!((limit.name, limit.actual), ("batch values", row), "{name}");
    }
}

#[test]
fn a_row_larger_than_a_frame_is_refused_unwritten() {
    let frame = usize::try_from(FRAME_BYTES).unwrap();
    let text = "x".repeat(frame + 1);
    let batch = single(Arc::new(StringArray::from(vec![text.as_str()])));
    assert_eq!(refused(&batch), "frame bytes");
}

/// How a skewed column holds its rows.
#[derive(Clone, Copy, Debug)]
enum Held {
    Plain,
    Keyed,
    Nested,
}

/// 4000 rows of 64 bytes each but the row at `at`, which holds `large` bytes.
fn skewed(held: Held, at: usize, large: usize) -> RecordBatch {
    let (small, large) = ("s".repeat(64), "L".repeat(large));
    let text = |row: usize| {
        if row == at {
            large.as_str()
        } else {
            small.as_str()
        }
    };
    let rows = 4000;
    let column: ArrayRef = match held {
        Held::Plain => Arc::new(StringArray::from_iter_values((0..rows).map(text))),
        Held::Keyed => {
            let values = StringArray::from(vec![small.as_str(), large.as_str()]);
            let keys = Int32Array::from_iter_values((0..rows).map(|row| i32::from(row == at)));
            Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap())
        }
        Held::Nested => {
            let items = StringArray::from_iter_values((0..rows).map(text));
            let field = Arc::new(Field::new("item", items.data_type().clone(), true));
            let offsets = OffsetBuffer::from_lengths(std::iter::repeat_n(1, rows));
            Arc::new(ListArray::new(field, offsets, Arc::new(items), None))
        }
    };
    single(column)
}

#[test]
fn a_row_far_larger_than_the_rows_beside_it_is_written_in_a_batch_it_fits() {
    // One row a few kibibytes short of a frame among thousands of small ones: by the rows'
    // average size it is cut into a batch with hundreds of them, which no frame holds.
    let large = usize::try_from(FRAME_BYTES).unwrap() - 4096;
    for held in [Held::Plain, Held::Keyed, Held::Nested] {
        for at in [0, 2000, 3999] {
            let read = written(&skewed(held, at, large)).unwrap_or_else(|error| {
                panic!("{held:?} with the large row at {at}: {error}");
            });
            assert!(read.len() > 1, "{held:?} {at}");
        }
    }
}

#[test]
fn a_row_larger_than_a_frame_is_refused_among_small_rows_and_leaves_no_file() {
    let large = usize::try_from(FRAME_BYTES).unwrap() + 1;
    for held in [Held::Plain, Held::Keyed, Held::Nested] {
        for at in [0, 3999] {
            assert_eq!(
                refused(&skewed(held, at, large)),
                "frame bytes",
                "{held:?} {at}"
            );
        }
    }
}

#[test]
fn a_batch_is_halved_until_its_parts_fit_and_refused_only_as_one_row() {
    let limits = Limits {
        frame_bytes: 4096,
        ..Limits::default()
    };
    let rows = |sizes: &[usize]| {
        let texts: Vec<String> = sizes.iter().map(|size| "x".repeat(*size)).collect();
        single(Arc::new(StringArray::from(texts)))
    };
    let parts = |sizes: &[usize]| -> Result<Vec<usize>> {
        let mut parts = Vec::new();
        fitted(rows(sizes), &limits, &mut |part| {
            parts.push(part.num_rows());
            Ok(())
        })?;
        Ok(parts)
    };
    // What fits is written whole, in order, and what does not in the halves that do.
    assert_eq!(parts(&[100; 8]).unwrap(), [8]);
    assert_eq!(parts(&[1000; 8]).unwrap(), [2, 2, 2, 2]);
    assert_eq!(
        parts(&[3500, 10, 10, 10, 10, 10, 10, 3500]).unwrap(),
        [4, 4]
    );
    assert_eq!(
        parts(&[3500, 3500, 10, 10, 10, 10, 10, 10]).unwrap(),
        [1, 1, 2, 4]
    );
    assert_eq!(
        parts(&[10, 10, 10, 3500, 3500, 10, 10, 10]).unwrap(),
        [4, 4]
    );
    // One row that alone fits no frame is refused, wherever it stands.
    for at in 0..4 {
        let mut sizes = [10; 4];
        sizes[at] = 5000;
        let error = parts(&sizes).expect_err("no frame holds the row");
        assert_eq!(error.limit().map(|limit| limit.name), Some("frame bytes"));
    }
    // Halving takes as many steps as the rows halve, each part measured once.
    let mut measured = 0;
    let many = rows(&vec![3; 1024]);
    let tight = Limits {
        frame_bytes: 600,
        ..Limits::default()
    };
    fitted(many, &tight, &mut |_| {
        measured += 1;
        Ok(())
    })
    .unwrap();
    assert!(measured > 1 && measured <= 1024, "{measured}");
}

#[test]
fn a_batch_whose_frame_is_the_limit_fits_and_one_byte_less_refuses_it() {
    let batch = single(Arc::new(StringArray::from(vec!["x".repeat(4096)])));
    let limits = |frame_bytes| Limits {
        frame_bytes,
        ..Limits::default()
    };
    let size = unfit(&batch, &limits(0))
        .unwrap()
        .expect("no frame fits no bytes")
        .limit()
        .map(|limit| limit.actual)
        .expect("a limit");
    assert!(size > 4096, "{size}");
    assert!(unfit(&batch, &limits(size)).unwrap().is_none());
    // The size counts what the file holds of the frame besides its message and body.
    fitted_under(&batch, &limits(size)).expect("a frame at the limit reads back");
    let refusal = unfit(&batch, &limits(size - 1))
        .unwrap()
        .expect("one byte over");
    let limit = refusal.limit().expect("a limit");
    assert_eq!(
        (limit.name, limit.limit, limit.actual),
        ("frame bytes", size - 1, size)
    );
}

#[test]
fn a_dictionary_whose_frame_is_the_limit_fits_and_one_byte_less_refuses_it() {
    let values = StringArray::from(vec!["x".repeat(4096)]);
    let keys = Int32Array::from(vec![0]);
    let keyed = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();
    let batch = single(Arc::new(keyed));
    let limits = |frame_bytes| Limits {
        frame_bytes,
        ..Limits::default()
    };
    // A dictionary beyond a frame is refused whatever the rows, not halved.
    let size = unfit(&batch, &limits(0))
        .expect_err("no frame fits no bytes")
        .limit()
        .map(|limit| limit.actual)
        .expect("a limit");
    assert!(size > 4096, "{size}");
    assert!(unfit(&batch, &limits(size)).unwrap().is_none());
    let refusal = unfit(&batch, &limits(size - 1)).expect_err("one byte over");
    let limit = refusal.limit().expect("a limit");
    assert_eq!(
        (limit.name, limit.limit, limit.actual),
        ("frame bytes", size - 1, size)
    );
}

/// `rows` rows of 13 bytes each, every row's bytes in a data buffer of its own.
fn apart(rows: usize) -> ArrayRef {
    let mut builder = StringViewBuilder::new().with_fixed_block_size(13);
    for _ in 0..rows {
        builder.append_value("x".repeat(13));
    }
    let views = builder.finish();
    assert_eq!(views.data_buffers().len(), rows);
    Arc::new(views)
}

#[test]
fn a_batch_far_within_a_frame_in_memory_is_measured_as_it_is_written() {
    // Held, the rows take less than half a frame; written, each data buffer is padded and
    // described, and every part of the column carries all of them.
    let limits = Limits {
        frame_bytes: 64 * 1024,
        ..Limits::default()
    };
    let batch = single(apart(1000));
    let held = u64::try_from(batch.get_array_memory_size()).unwrap();
    assert!(held < limits.frame_bytes / 2, "{held}");
    let error = fitted_under(&batch, &limits).expect_err("no frame holds a row of the column");
    assert_eq!(error.limit().map(|limit| limit.name), Some("frame bytes"));
}

/// `rows` rows of the same 200 bytes, which one data buffer holds once.
fn repeated(rows: usize) -> ArrayRef {
    let mut builder = StringViewBuilder::new().with_deduplicate_strings();
    for _ in 0..rows {
        builder.append_value("x".repeat(200));
    }
    let views = builder.finish();
    assert_eq!(views.data_buffers().len(), 1);
    Arc::new(views)
}

#[test]
fn the_bytes_a_batch_s_views_name_are_bounded_by_a_frame_however_few_it_holds() {
    // The views name 200 bytes each, eight times what the frame holds, from one buffer of 200.
    let limits = Limits {
        frame_bytes: 24 * 1024,
        ..Limits::default()
    };
    let read = fitted_under(&single(repeated(1000)), &limits).expect("views within a frame");
    assert!(read.len() >= 8, "{}", read.len());
    // A dictionary's values are a frame of their own, which no halving of the rows changes.
    let keys = Int32Array::from(vec![0]);
    let keyed = DictionaryArray::<Int32Type>::try_new(keys, repeated(1000)).unwrap();
    let error = fitted_under(&single(Arc::new(keyed)), &limits).expect_err("views beyond a frame");
    assert_eq!(error.limit().map(|limit| limit.name), Some("view bytes"));
}

#[test]
fn a_run_end_column_counts_only_the_runs_its_rows_span() {
    // A hundred runs of one row each: ten rows are ten runs ends and ten values besides.
    let ends = Int32Array::from_iter_values(1..=100);
    let values = Int8Array::from_iter_values(0..100);
    let runs = RunArray::<Int32Type>::try_new(&ends, &values).unwrap();
    let batch = single(Arc::new(runs)).slice(10, 10);
    let limits = Limits {
        batch_values: 30,
        ..Limits::default()
    };
    let read = fitted_under(&batch, &limits).expect("the rows' runs within a frame's values");
    assert_eq!(read.len(), 1);
}

/// Six rows of a union of integers and strings.
fn union(mode: UnionMode) -> ArrayRef {
    let fields = [
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Utf8, true),
    ];
    let fields = UnionFields::try_new([0, 1], fields).unwrap();
    let ids = ScalarBuffer::from(vec![0_i8, 1, 0, 1, 1, 0]);
    let union = match mode {
        UnionMode::Sparse => {
            let numbers = Arc::new(Int32Array::from_iter_values(1..=6));
            let texts = Arc::new(StringArray::from_iter_values([
                "a", "b", "c", "d", "e", "f",
            ]));
            UnionArray::try_new(fields, ids, None, vec![numbers, texts])
        }
        UnionMode::Dense => {
            let offsets = ScalarBuffer::from(vec![0, 0, 1, 1, 2, 2]);
            let numbers = Arc::new(Int32Array::from(vec![1, 3, 6]));
            let texts = Arc::new(StringArray::from_iter_values(["b", "d", "e"]));
            UnionArray::try_new(fields, ids, Some(offsets), vec![numbers, texts])
        }
    };
    Arc::new(union.unwrap())
}

fn field(items: &ArrayRef) -> Arc<Field> {
    Arc::new(Field::new("item", items.data_type().clone(), true))
}

/// A list of `items` in rows of `lengths` items each.
fn list(items: ArrayRef, lengths: [usize; 3]) -> ArrayRef {
    let offsets = OffsetBuffer::from_lengths(lengths);
    Arc::new(ListArray::new(field(&items), offsets, items, None))
}

/// Every way a list or a map holds `items`, six of them, in three rows.
fn holding(items: &ArrayRef) -> Vec<(&'static str, ArrayRef)> {
    let large = LargeListArray::new(
        field(items),
        OffsetBuffer::from_lengths([2, 2, 2]),
        Arc::clone(items),
        None,
    );
    let fields = Fields::from(vec![
        Field::new("keys", DataType::Int8, false),
        Field::new("values", items.data_type().clone(), true),
    ]);
    let keys = Arc::new(Int8Array::from_iter_values(0..6));
    let entries = StructArray::new(fields.clone(), vec![keys, Arc::clone(items)], None);
    let entry = Arc::new(Field::new("entries", entries.data_type().clone(), false));
    let lengths = OffsetBuffer::from_lengths([2, 2, 2]);
    let map = MapArray::new(entry, lengths, entries.clone(), None, false);
    let pairs = FixedSizeListArray::new(field(items), 2, Arc::clone(items), None);
    let lists = list(Arc::clone(items), [2, 2, 2]);
    let short = RunArray::<Int16Type>::try_new(&Int16Array::from(vec![1, 2, 3]), &lists);
    let runs = RunArray::<Int32Type>::try_new(&Int32Array::from(vec![1, 2, 3]), &lists);
    let long = RunArray::<Int64Type>::try_new(&Int64Array::from(vec![1, 2, 3]), &lists);
    vec![
        ("list", list(Arc::clone(items), [2, 2, 2])),
        ("large list", Arc::new(large)),
        ("map", Arc::new(map)),
        ("list of structs", list(Arc::new(entries), [2, 2, 2])),
        ("list of pairs", list(Arc::new(pairs), [1, 1, 1])),
        ("runs of lists, by 16-bit ends", Arc::new(short.unwrap())),
        ("runs of lists", Arc::new(runs.unwrap())),
        ("runs of lists, by 64-bit ends", Arc::new(long.unwrap())),
    ]
}

/// Checks that a write of `batch` is refused for a union Arrow's writer writes from other rows
/// than those it holds, and leaves no file.
fn unwritable(batch: &RecordBatch) {
    let root = crate::scratch::tempdir().unwrap();
    let dir = Dir::ambient(root.path()).unwrap();
    let error = FileFormat::Arrow
        .write(&dir, "rows.arrow", std::slice::from_ref(batch))
        .expect_err("a union written from other rows");
    assert_eq!(error.kind(), ConnectorErrorKind::Data, "{error}");
    assert_eq!(error.code(), Some(UNION_UNWRITABLE), "{error}");
    let left = std::fs::read_dir(root.path()).unwrap().count();
    assert_eq!(left, 0, "a refused file was left");
}

#[test]
fn a_union_a_list_holds_only_some_rows_of_is_refused_unwritten() {
    for mode in [UnionMode::Sparse, UnionMode::Dense] {
        let union = union(mode);
        // A union of a batch's own is written as its rows, however the batch is cut.
        written(&single(Arc::clone(&union)).slice(1, 2)).expect("a union of the batch's own");
        for (name, column) in holding(&union) {
            let batch = single(column);
            let what = format!("{mode:?} {name}");
            // A list that holds every item of its child is written as it is.
            written(&batch).unwrap_or_else(|error| panic!("{what}: {error}"));
            // Arrow writes the union's type ids from its first item, not the list's.
            unwritable(&batch.slice(1, 1));
            unwritable(&batch.slice(1, 2));
            // From its first item, a dense union reads back; a sparse one is written whole,
            // where a reader takes its children to be as long as itself.
            match mode {
                UnionMode::Dense => {
                    written(&batch.slice(0, 1)).unwrap_or_else(|error| panic!("{what}: {error}"));
                }
                UnionMode::Sparse => unwritable(&batch.slice(0, 1)),
            }
        }
        // A row of no lists cuts their child to no items, from its first.
        let none = single(list(list(union, [2, 2, 2]), [1, 0, 2])).slice(1, 1);
        match mode {
            UnionMode::Dense => drop(written(&none).expect("no items of a dense union")),
            UnionMode::Sparse => unwritable(&none),
        }
    }
}

#[test]
fn the_dictionaries_of_a_file_are_bounded_together_as_a_reader_holds_them() {
    let limits = Limits {
        frame_bytes: 64 * 1024,
        ..Limits::default()
    };
    let keyed = |bytes: usize| -> ArrayRef {
        let values = StringArray::from(vec!["x".repeat(bytes)]);
        let keys = Int32Array::from(vec![0]);
        Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap())
    };
    let pair = |bytes| RecordBatch::try_from_iter([("a", keyed(bytes)), ("b", keyed(bytes))]);
    // Each within a frame, and together within what a reader holds of them, then beyond it.
    fitted_under(&pair(20_000).unwrap(), &limits).expect("dictionaries a reader holds");
    let error = fitted_under(&pair(40_000).unwrap(), &limits).expect_err("too much to hold");
    assert_eq!(
        error.limit().map(|limit| limit.name),
        Some("dictionary bytes")
    );
}
