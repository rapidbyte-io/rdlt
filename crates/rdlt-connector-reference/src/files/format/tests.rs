use std::io::{BufReader, Cursor, Write as _};
use std::sync::Arc;

use arrow_array::builder::{
    Float64Builder, GenericListViewBuilder, Int64Builder, ListBuilder, StringBuilder,
    StringViewBuilder,
};
use arrow_array::types::{
    ArrowDictionaryKeyType, Int8Type, Int16Type, Int32Type, Int64Type, RunEndIndexType, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, BinaryViewArray, BooleanArray, DictionaryArray, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeListArray, LargeStringArray, NullArray, PrimitiveArray, RecordBatch, RecordBatchOptions,
    RunArray, StringArray, StringViewArray, StructArray,
};
use arrow_ipc::writer::{FileWriter, IpcWriteOptions};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use proptest::prelude::*;
use rdlt_connector::ConnectorErrorKind;
use rdlt_testkit::drawn::{Drawn, Scalar, array, field, values};
use rdlt_wire::limits::{FRAME_BYTES, NESTING_DEPTH, SCHEMA_COLUMNS};

use super::lines::{Bounded, LINE_LIMIT, Lines, content, holds_a_record};
use super::{FileFormat, Reader, Writer, Written};
use crate::limits::{CHUNK_BYTES, LINE_BYTES};
use crate::rooted::{Dir, Refusal, refusal};

fn scratch() -> (tempfile::TempDir, Dir) {
    let root = tempfile::tempdir().unwrap();
    let dir = Dir::ambient(root.path()).unwrap();
    (root, dir)
}

/// The batch of one column `c` holding `column`.
fn single(column: ArrayRef) -> RecordBatch {
    RecordBatch::try_from_iter_with_nullable([("c", column, true)]).unwrap()
}

/// The rows of `batches` as one batch.
fn joined(batches: &[RecordBatch]) -> RecordBatch {
    arrow_select::concat::concat_batches(batches[0].schema_ref(), batches).unwrap()
}

/// `batch` written as `format` and read back, as one batch.
fn round_trip(format: FileFormat, batch: &RecordBatch) -> RecordBatch {
    let (_root, dir) = scratch();
    let name = format!("rows.{}", format.extension());
    let written = format
        .write(&dir, &name, std::slice::from_ref(batch))
        .unwrap();
    assert_eq!(written.rows, u64::try_from(batch.num_rows()).unwrap());
    let read = format.read(&dir, &name, batch.schema_ref()).unwrap();
    if read.is_empty() {
        return RecordBatch::new_empty(batch.schema());
    }
    joined(&read)
}

fn dictionary<K: ArrowDictionaryKeyType>(keys: Vec<Option<K::Native>>) -> ArrayRef {
    let keys: PrimitiveArray<K> = keys.into_iter().collect();
    let values = Arc::new(StringArray::from(vec!["a", "bb", "ccc"]));
    Arc::new(DictionaryArray::<K>::try_new(keys, values).unwrap())
}

fn run_ends<R: RunEndIndexType>(ends: Vec<R::Native>) -> ArrayRef {
    let ends = PrimitiveArray::<R>::from_iter_values(ends);
    let values = StringArray::from(vec![Some("a"), None, Some("ccc")]);
    Arc::new(RunArray::<R>::try_new(&ends, &values).unwrap())
}

fn list_view<O: arrow_array::OffsetSizeTrait>() -> ArrayRef {
    let mut builder = GenericListViewBuilder::<O, _>::new(Int64Builder::new());
    builder.append_value([Some(1), None, Some(3)]);
    builder.append_null();
    builder.append_value([]);
    builder.append_value([Some(4)]);
    Arc::new(builder.finish())
}

/// A string longer than the twelve bytes a view holds inline.
const LONG: &str = "a string longer than a view's twelve inline bytes";

type Named = (&'static str, ArrayRef);

fn column<A: Array + 'static>(name: &'static str, array: A) -> Named {
    (name, Arc::new(array))
}

/// Four rows of every plain fixed-width type, the second null.
fn plain() -> Vec<Named> {
    vec![
        column("null", NullArray::new(4)),
        column(
            "bool",
            BooleanArray::from(vec![Some(true), None, Some(false), Some(true)]),
        ),
        column(
            "int8",
            Int8Array::from(vec![Some(i8::MIN), None, Some(0), Some(i8::MAX)]),
        ),
        column(
            "int16",
            Int16Array::from(vec![Some(i16::MIN), None, Some(0), Some(i16::MAX)]),
        ),
        column(
            "int32",
            Int32Array::from(vec![Some(i32::MIN), None, Some(0), Some(i32::MAX)]),
        ),
        column(
            "int64",
            Int64Array::from(vec![Some(i64::MIN), None, Some(0), Some(i64::MAX)]),
        ),
        column(
            "float32",
            Float32Array::from(vec![Some(f32::MIN), None, Some(-0.0), Some(1.5)]),
        ),
        column(
            "float64",
            Float64Array::from(vec![Some(f64::MAX), None, Some(-0.0), Some(1.5)]),
        ),
    ]
}

/// Four rows of text and bytes in their plain, large, view and fixed-size encodings.
fn texts() -> Vec<Named> {
    let mut views = StringViewBuilder::new();
    for value in [Some("a"), None, Some(LONG), Some("ccc")] {
        views.append_option(value);
    }
    let fixed = [Some([1_u8, 2]), None, Some([0, 0]), Some([9, 9])];
    vec![
        column(
            "utf8",
            StringArray::from(vec![Some("a"), None, Some(""), Some("ccc")]),
        ),
        column(
            "large utf8",
            LargeStringArray::from(vec![Some("a"), None, Some(""), Some(LONG)]),
        ),
        column("utf8 view", views.finish()),
        column(
            "large binary",
            LargeBinaryArray::from(vec![Some(&b"a"[..]), None, Some(b""), Some(b"ccc")]),
        ),
        column(
            "binary view",
            BinaryViewArray::from(vec![
                Some(&b"a"[..]),
                None,
                Some(LONG.as_bytes()),
                Some(b""),
            ]),
        ),
        column(
            "fixed binary",
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(fixed.into_iter(), 2).unwrap(),
        ),
    ]
}

/// Four rows as a dictionary of every key type, and as run ends of every run-end type.
fn keyed() -> Vec<Named> {
    vec![
        (
            "dictionary int8",
            dictionary::<Int8Type>(vec![Some(0), None, Some(2), Some(1)]),
        ),
        (
            "dictionary int16",
            dictionary::<Int16Type>(vec![Some(0), None, Some(2), Some(1)]),
        ),
        (
            "dictionary int32",
            dictionary::<Int32Type>(vec![Some(0), None, Some(2), Some(1)]),
        ),
        (
            "dictionary int64",
            dictionary::<Int64Type>(vec![Some(0), None, Some(2), Some(1)]),
        ),
        (
            "dictionary uint8",
            dictionary::<UInt8Type>(vec![Some(0), None, Some(2), Some(1)]),
        ),
        (
            "dictionary uint16",
            dictionary::<UInt16Type>(vec![Some(0), None, Some(2), Some(1)]),
        ),
        (
            "dictionary uint32",
            dictionary::<UInt32Type>(vec![Some(0), None, Some(2), Some(1)]),
        ),
        (
            "dictionary uint64",
            dictionary::<UInt64Type>(vec![Some(0), None, Some(2), Some(1)]),
        ),
        ("run ends int16", run_ends::<Int16Type>(vec![1, 3, 4])),
        ("run ends int32", run_ends::<Int32Type>(vec![2, 3, 4])),
        ("run ends int64", run_ends::<Int64Type>(vec![1, 2, 4])),
    ]
}

/// Four rows of lists and list views, and of the encodings above nested in one another.
fn nested_encodings() -> Vec<Named> {
    let mut lists = ListBuilder::new(StringBuilder::new());
    lists.append_value([Some("a"), None]);
    lists.append_null();
    lists.append_value::<[Option<&str>; 0], _>([]);
    lists.append_value([Some("ccc")]);
    let lists: ArrayRef = Arc::new(lists.finish());
    let items = dictionary::<Int8Type>(vec![Some(0), Some(2), None, Some(1), Some(0)]);
    let item = Arc::new(Field::new("item", items.data_type().clone(), true));
    let offsets = arrow_buffer::OffsetBuffer::new(vec![0_i64, 2, 2, 3, 5].into());
    let dictionaries = LargeListArray::new(item, offsets, items, None);
    let ends = run_ends::<Int16Type>(vec![1, 2, 4]);
    let fields = Fields::from(vec![
        Field::new("run", ends.data_type().clone(), true),
        Field::new("view", DataType::Utf8View, true),
        Field::new("list", lists.data_type().clone(), true),
    ]);
    let views: ArrayRef = Arc::new(StringViewArray::from(vec![
        Some("a"),
        None,
        None,
        Some(LONG),
    ]));
    let nulls = arrow_buffer::NullBuffer::from(vec![true, false, true, true]);
    let children = vec![ends, views, Arc::clone(&lists)];
    vec![
        ("list", lists),
        ("list view", list_view::<i32>()),
        ("large list view", list_view::<i64>()),
        column("large list of dictionary", dictionaries),
        column(
            "struct of run ends, view and list",
            StructArray::new(fields, children, Some(nulls)),
        ),
    ]
}

/// A four-row column in every encoding an Arrow file may hold one in: plain, large, views,
/// dictionaries of every key type, run ends of every run-end type, list views, nulls, and those
/// nested in one another.
fn encodings() -> Vec<Named> {
    [plain(), texts(), keyed(), nested_encodings()].concat()
}

#[test]
fn every_encoding_reads_back_from_an_arrow_file_as_it_was_written() {
    for (name, column) in encodings() {
        let batch = single(column);
        assert_eq!(round_trip(FileFormat::Arrow, &batch), batch, "{name}");
        // Sliced, as a merged table's batches are, and with no row at all.
        let sliced = batch.slice(1, 2);
        assert_eq!(
            round_trip(FileFormat::Arrow, &sliced),
            sliced,
            "{name} sliced"
        );
        let empty = batch.slice(0, 0);
        assert_eq!(
            round_trip(FileFormat::Arrow, &empty).num_rows(),
            0,
            "{name} empty"
        );
    }
    // Every encoding beside every other, in one file of several batches.
    let columns = encodings();
    let wide = RecordBatch::try_from_iter_with_nullable(
        columns
            .iter()
            .map(|(name, column)| (*name, Arc::clone(column), true)),
    )
    .unwrap();
    let (_root, dir) = scratch();
    let batches = [wide.clone(), wide.clone(), wide.clone()];
    let written = FileFormat::Arrow
        .write(&dir, "wide.arrow", &batches)
        .unwrap();
    assert_eq!(written.rows, 12);
    let read = FileFormat::Arrow
        .read(&dir, "wide.arrow", wide.schema_ref())
        .unwrap();
    assert_eq!(read, batches);
}

/// The batch `drawn` describes.
fn batch((columns, rows): &Drawn) -> RecordBatch {
    let arrays: Vec<ArrayRef> = columns
        .iter()
        .enumerate()
        .map(|(column, (_, shape))| {
            let values: Vec<&Scalar> = rows.iter().map(|row| &row[column]).collect();
            array(shape, &values)
        })
        .collect();
    let fields: Vec<_> = columns
        .iter()
        .zip(&arrays)
        .map(|((name, shape), array)| field(name, shape, array, true))
        .collect();
    let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
    RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), arrays, &options).unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(256)))]

    #[test]
    fn every_drawn_batch_reads_back_from_an_arrow_file_as_it_was_written(drawn in values::drawn()) {
        let batch = batch(&drawn);
        prop_assert_eq!(round_trip(FileFormat::Arrow, &batch), batch);
    }

    #[test]
    fn a_corrupted_arrow_file_is_read_or_refused_but_never_unwinds_or_aborts(
        drawn in values::drawn(),
        flips in proptest::collection::vec((any::<usize>(), any::<u8>()), 1..8),
    ) {
        let batch = batch(&drawn);
        let (root, dir) = scratch();
        FileFormat::Arrow.write(&dir, "rows.arrow", std::slice::from_ref(&batch)).unwrap();
        let mut bytes = std::fs::read(root.path().join("rows.arrow")).unwrap();
        for (at, value) in &flips {
            let len = bytes.len();
            bytes[at % len] ^= value;
        }
        std::fs::write(root.path().join("rows.arrow"), bytes).unwrap();
        let read = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            FileFormat::Arrow.read(&dir, "rows.arrow", batch.schema_ref())
        }));
        prop_assert!(read.is_ok(), "reading a corrupted file unwound");
        if let Ok(Err(error)) = read {
            prop_assert_eq!(error.kind(), ConnectorErrorKind::Data);
        }
    }
}

/// A one-row batch whose column `c` nests `depth` levels of lists or of structs.
fn nested(depth: u64, lists: bool) -> RecordBatch {
    let mut data_type = DataType::Int64;
    for _ in 1..depth {
        data_type = if lists {
            DataType::List(Arc::new(Field::new("item", data_type, true)))
        } else {
            DataType::Struct(Fields::from(vec![Field::new("f", data_type, true)]))
        };
    }
    single(arrow_array::new_null_array(&data_type, 1))
}

#[test]
fn schemas_nested_to_the_limit_read_back_and_deeper_ones_are_refused_unwritten() {
    for lists in [true, false] {
        for depth in [1, 2, 55, 60, 61, 62, 63, NESTING_DEPTH] {
            let batch = nested(depth, lists);
            assert_eq!(
                round_trip(FileFormat::Arrow, &batch),
                batch,
                "{lists} {depth}"
            );
        }
        let (root, dir) = scratch();
        let deeper = nested(NESTING_DEPTH + 1, lists);
        let error = FileFormat::Arrow
            .write(&dir, "rows.arrow", std::slice::from_ref(&deeper))
            .unwrap_err();
        let limit = error.limit().expect("a limit");
        assert_eq!(
            (limit.name, limit.limit, limit.actual),
            ("nesting depth", 64, 65)
        );
        assert_eq!(
            std::fs::read_dir(root.path()).unwrap().count(),
            0,
            "a file was left"
        );
        // A file another writer made that deep is refused by the same limit, by name.
        let file = std::fs::File::create(root.path().join("deep.arrow")).unwrap();
        let mut writer = FileWriter::try_new(file, deeper.schema_ref()).unwrap();
        writer.write(&deeper).unwrap();
        writer.finish().unwrap();
        let error = FileFormat::Arrow
            .read(&dir, "deep.arrow", deeper.schema_ref())
            .unwrap_err();
        assert_eq!(error.limit().map(|limit| limit.name), Some("nesting depth"));
    }
}

#[test]
fn a_schema_of_more_columns_than_a_reader_accepts_is_refused_unwritten() {
    let wide = |columns: u64| {
        let fields: Vec<Field> = (0..columns)
            .map(|column| Field::new(format!("c{column}"), DataType::Null, true))
            .collect();
        let options = RecordBatchOptions::new().with_row_count(Some(1));
        let arrays = fields
            .iter()
            .map(|_| Arc::new(NullArray::new(1)) as ArrayRef)
            .collect();
        RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), arrays, &options).unwrap()
    };
    let at = wide(SCHEMA_COLUMNS);
    assert_eq!(round_trip(FileFormat::Arrow, &at), at);
    let (root, dir) = scratch();
    let error = FileFormat::Arrow
        .write(&dir, "rows.arrow", &[wide(SCHEMA_COLUMNS + 1)])
        .unwrap_err();
    assert_eq!(
        error.limit().map(|limit| limit.name),
        Some("schema columns")
    );
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    // JSON lines hold no schema: nothing bounds one there.
    FileFormat::Jsonl
        .write(&dir, "rows.jsonl", &[wide(SCHEMA_COLUMNS + 1)])
        .unwrap();
}

/// A batch of `rows` rows of one string of `bytes` bytes each.
fn strings(rows: usize, bytes: usize) -> RecordBatch {
    let value = "x".repeat(bytes);
    single(Arc::new(StringArray::from(vec![value.as_str(); rows])))
}

#[test]
fn a_large_batch_is_written_as_batches_a_reader_accepts() {
    let (_root, dir) = scratch();
    // 40 MiB in 40 rows: five batches of the size a batch aims for.
    let batch = strings(40, 1024 * 1024);
    FileFormat::Arrow
        .write(&dir, "rows.arrow", std::slice::from_ref(&batch))
        .unwrap();
    let read = FileFormat::Arrow
        .read(&dir, "rows.arrow", batch.schema_ref())
        .unwrap();
    let chunk = u64::try_from(batch.get_array_memory_size()).unwrap() / 40;
    let rows = usize::try_from(CHUNK_BYTES / chunk).unwrap();
    assert!(rows > 1 && rows < 40, "{rows}");
    assert_eq!(read.len(), 40_usize.div_ceil(rows));
    assert!(read.iter().all(|read| read.num_rows() <= rows));
    assert_eq!(joined(&read), batch);
    // A small batch stays one.
    let small = strings(40, 16);
    FileFormat::Arrow
        .write(&dir, "small.arrow", std::slice::from_ref(&small))
        .unwrap();
    let read = FileFormat::Arrow
        .read(&dir, "small.arrow", small.schema_ref())
        .unwrap();
    assert_eq!(read, [small]);
}

#[test]
fn a_row_beyond_the_frame_limit_is_refused_and_its_file_removed() {
    let (root, dir) = scratch();
    let frame = usize::try_from(FRAME_BYTES).unwrap();
    let batch = strings(1, frame + 1);
    let error = FileFormat::Arrow
        .write(&dir, "rows.arrow", std::slice::from_ref(&batch))
        .unwrap_err();
    let limit = error.limit().expect("a limit");
    assert_eq!((limit.name, limit.limit), ("frame bytes", FRAME_BYTES));
    assert_eq!(
        std::fs::read_dir(root.path()).unwrap().count(),
        0,
        "the file was left"
    );
    // A file another writer made with such a batch is refused before the batch is held.
    let file = std::fs::File::create(root.path().join("big.arrow")).unwrap();
    let mut writer = FileWriter::try_new(file, batch.schema_ref()).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    let error = FileFormat::Arrow
        .read(&dir, "big.arrow", batch.schema_ref())
        .unwrap_err();
    assert_eq!(error.limit().map(|limit| limit.name), Some("frame bytes"));
    assert_eq!(error.kind(), ConnectorErrorKind::Data);
}

#[test]
fn a_line_beyond_the_limit_is_refused_written_or_read() {
    let (root, dir) = scratch();
    let line = usize::try_from(LINE_BYTES).unwrap();
    // `{"c":"…"}` is eight bytes around the string.
    let fits = strings(2, line - 8);
    FileFormat::Jsonl
        .write(&dir, "fits.jsonl", std::slice::from_ref(&fits))
        .unwrap();
    let read = FileFormat::Jsonl
        .read(&dir, "fits.jsonl", fits.schema_ref())
        .unwrap();
    assert_eq!(joined(&read), fits);
    let over = strings(1, line - 7);
    let error = FileFormat::Jsonl
        .write(&dir, "over.jsonl", std::slice::from_ref(&over))
        .unwrap_err();
    let limit = error.limit().expect("a limit");
    assert_eq!((limit.name, limit.limit), (LINE_LIMIT, LINE_BYTES));
    assert!(
        !root.path().join("over.jsonl").exists(),
        "the file was left"
    );
    let mut longer = std::fs::File::create(root.path().join("longer.jsonl")).unwrap();
    longer.write_all(b"{\"c\":\"").unwrap();
    longer.write_all(&vec![b'x'; line]).unwrap();
    longer.write_all(b"\"}\n").unwrap();
    let error = FileFormat::Jsonl
        .read(&dir, "longer.jsonl", over.schema_ref())
        .unwrap_err();
    let limit = error.limit().expect("a limit");
    assert_eq!(
        (limit.name, limit.limit, limit.actual),
        (LINE_LIMIT, LINE_BYTES, LINE_BYTES + 1)
    );
}

#[test]
fn json_lines_are_read_back_in_batches_of_bounded_rows_and_bytes() {
    let (_root, dir) = scratch();
    let many = single(Arc::new(Int64Array::from_iter_values(0..2500)));
    FileFormat::Jsonl
        .write(&dir, "many.jsonl", std::slice::from_ref(&many))
        .unwrap();
    let read = FileFormat::Jsonl
        .read(&dir, "many.jsonl", many.schema_ref())
        .unwrap();
    let rows: Vec<usize> = read.iter().map(RecordBatch::num_rows).collect();
    assert_eq!(rows, [1024, 1024, 452]);
    assert_eq!(joined(&read), many);
    // Rows of 3 MiB: a batch ends once it holds the bytes a batch aims for.
    let wide = strings(10, 3 * 1024 * 1024);
    FileFormat::Jsonl
        .write(&dir, "wide.jsonl", std::slice::from_ref(&wide))
        .unwrap();
    let read = FileFormat::Jsonl
        .read(&dir, "wide.jsonl", wide.schema_ref())
        .unwrap();
    let rows: Vec<usize> = read.iter().map(RecordBatch::num_rows).collect();
    assert_eq!(rows, [3, 3, 3, 1]);
    assert_eq!(joined(&read), wide);
    // Lines of exactly half that size, their endings counted: two fill a batch.
    let half = usize::try_from(CHUNK_BYTES / 2).unwrap();
    let exact = strings(5, half - 9);
    FileFormat::Jsonl
        .write(&dir, "exact.jsonl", std::slice::from_ref(&exact))
        .unwrap();
    let read = FileFormat::Jsonl
        .read(&dir, "exact.jsonl", exact.schema_ref())
        .unwrap();
    let rows: Vec<usize> = read.iter().map(RecordBatch::num_rows).collect();
    assert_eq!(rows, [2, 2, 1]);
}

/// The bits of each value of a float column, a NaN as one value whatever its payload.
fn bits(column: &ArrayRef) -> Vec<Option<u64>> {
    use arrow_array::cast::AsArray as _;
    use arrow_array::types::{Float32Type, Float64Type};
    let canonical = |value: f64| {
        if value.is_nan() {
            f64::NAN.to_bits()
        } else {
            value.to_bits()
        }
    };
    match column.data_type() {
        DataType::Float32 => column
            .as_primitive::<Float32Type>()
            .iter()
            .map(|value| value.map(|value| canonical(f64::from(value))))
            .collect(),
        _ => column
            .as_primitive::<Float64Type>()
            .iter()
            .map(|value| value.map(canonical))
            .collect(),
    }
}

/// Every kind of double: those that are no number, both zeros, the extremes and values whose
/// shortest text is long.
const DOUBLES: [f64; 14] = [
    f64::NAN,
    f64::INFINITY,
    f64::NEG_INFINITY,
    -0.0,
    0.0,
    1.5,
    f64::MAX,
    f64::MIN,
    f64::MIN_POSITIVE,
    5e-324,
    0.1,
    1e21,
    9_007_199_254_740_993.0,
    f64::EPSILON,
];

const SINGLES: [f32; 14] = [
    f32::NAN,
    f32::INFINITY,
    f32::NEG_INFINITY,
    -0.0,
    0.0,
    1.5,
    f32::MAX,
    f32::MIN,
    f32::MIN_POSITIVE,
    1e-45,
    0.1,
    1e21,
    16_777_217.0,
    f32::EPSILON,
];

#[test]
fn every_float_reads_back_from_json_lines_as_the_value_written() {
    let mut doubles: Vec<Option<f64>> = DOUBLES.iter().copied().map(Some).collect();
    doubles.push(None);
    let mut singles: Vec<Option<f32>> = SINGLES.iter().copied().map(Some).collect();
    singles.push(None);
    let x: ArrayRef = Arc::new(Float64Array::from(doubles));
    let y: ArrayRef = Arc::new(Float32Array::from(singles));
    let batch = RecordBatch::try_from_iter_with_nullable([
        ("x", Arc::clone(&x), true),
        ("y", Arc::clone(&y), true),
    ])
    .unwrap();
    let read = round_trip(FileFormat::Jsonl, &batch);
    assert_eq!(bits(read.column(0)), bits(&x));
    assert_eq!(bits(read.column(1)), bits(&y));
}

#[test]
fn a_float_that_is_no_number_is_named_in_json_lines_wherever_it_nests() {
    // Named in the file, so no reader takes one for a missing value.
    let (root, dir) = scratch();
    let named = single(Arc::new(Float64Array::from(vec![
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ])));
    FileFormat::Jsonl
        .write(&dir, "named.jsonl", &[named])
        .unwrap();
    let text = std::fs::read_to_string(root.path().join("named.jsonl")).unwrap();
    assert_eq!(
        text,
        "{\"c\":\"NaN\"}\n{\"c\":\"Infinity\"}\n{\"c\":\"-Infinity\"}\n"
    );
    // Inside lists and structs alike, required or not.
    let mut items = ListBuilder::new(Float64Builder::new());
    items.append_value([Some(f64::NAN), None, Some(f64::NEG_INFINITY)]);
    let inner = Arc::new(Field::new("x", DataType::Float32, false));
    let point = StructArray::from(vec![(
        inner,
        Arc::new(Float32Array::from(vec![f32::INFINITY])) as ArrayRef,
    )]);
    let nested = RecordBatch::try_from_iter_with_nullable([
        ("items", Arc::new(items.finish()) as ArrayRef, false),
        ("point", Arc::new(point) as ArrayRef, false),
    ])
    .unwrap();
    FileFormat::Jsonl
        .write(&dir, "nested.jsonl", std::slice::from_ref(&nested))
        .unwrap();
    let text = std::fs::read_to_string(root.path().join("nested.jsonl")).unwrap();
    assert_eq!(
        text,
        "{\"items\":[\"NaN\",null,\"-Infinity\"],\"point\":{\"x\":\"Infinity\"}}\n"
    );
    let read = FileFormat::Jsonl
        .read(&dir, "nested.jsonl", nested.schema_ref())
        .unwrap();
    assert_eq!(
        format!("{:?}", read[0].column(0)),
        format!("{:?}", nested.column(0))
    );
    assert_eq!(read[0].column(1), nested.column(1));
}

/// Three rows of every scalar type JSON lines keep, the second null.
fn kept_scalars() -> Vec<Named> {
    vec![
        column(
            "bool",
            BooleanArray::from(vec![Some(true), None, Some(false)]),
        ),
        column(
            "int8",
            Int8Array::from(vec![Some(i8::MIN), None, Some(i8::MAX)]),
        ),
        column(
            "int16",
            Int16Array::from(vec![Some(i16::MIN), None, Some(i16::MAX)]),
        ),
        column(
            "int32",
            Int32Array::from(vec![Some(i32::MIN), None, Some(i32::MAX)]),
        ),
        column(
            "int64",
            Int64Array::from(vec![Some(i64::MIN), None, Some(i64::MAX)]),
        ),
        column(
            "float32",
            Float32Array::from(vec![Some(0.1), None, Some(-2.5)]),
        ),
        column(
            "float64",
            Float64Array::from(vec![Some(0.1), None, Some(1e300)]),
        ),
        column(
            "utf8",
            StringArray::from(vec![Some("a\u{2028}\u{0}é"), None, Some("")]),
        ),
    ]
}

#[test]
fn every_type_json_lines_keep_reads_back_as_it_was_written() {
    let mut lists = ListBuilder::new(Int64Builder::new());
    lists.append_value([Some(1), None]);
    lists.append_null();
    lists.append_value([]);
    let inner = Arc::new(Field::new("tag", DataType::Utf8, true));
    let tags: ArrayRef = Arc::new(StringArray::from(vec![Some("a\n\"b\""), None, Some("")]));
    let point = StructArray::new(
        Fields::from(vec![inner]),
        vec![tags],
        Some(arrow_buffer::NullBuffer::from(vec![true, true, false])),
    );
    let mut columns = kept_scalars();
    columns.push(column("list", lists.finish()));
    columns.push(column("struct", point));
    let batch = RecordBatch::try_from_iter_with_nullable(
        columns
            .into_iter()
            .map(|(name, column)| (name, column, true)),
    )
    .unwrap();
    assert_eq!(round_trip(FileFormat::Jsonl, &batch), batch);
    assert_eq!(
        round_trip(FileFormat::Jsonl, &batch.slice(0, 0)).num_rows(),
        0
    );
}

#[test]
fn a_file_of_no_batch_or_of_a_taken_name_is_not_written() {
    let (root, dir) = scratch();
    for format in [FileFormat::Jsonl, FileFormat::Arrow] {
        assert_eq!(
            format.write(&dir, "none", &[]).unwrap_err().kind(),
            ConnectorErrorKind::Internal
        );
        let batch = strings(1, 1);
        let name = format!("rows.{}", format.extension());
        let written = format
            .write(&dir, &name, std::slice::from_ref(&batch))
            .unwrap();
        let size = std::fs::metadata(root.path().join(&name)).unwrap().len();
        assert_eq!(
            written,
            Written {
                rows: 1,
                bytes: size
            }
        );
        assert!(size > 0);
        // The name is taken: the file stays as it is.
        assert!(format.write(&dir, &name, &[strings(3, 3)]).is_err());
        assert_eq!(
            std::fs::metadata(root.path().join(&name)).unwrap().len(),
            size
        );
        // A link is no file to write through either.
        let linked = format!("link.{}", format.extension());
        std::os::unix::fs::symlink(root.path().join("target"), root.path().join(&linked)).unwrap();
        assert!(
            format
                .write(&dir, &linked, std::slice::from_ref(&batch))
                .is_err()
        );
        assert!(!root.path().join("target").exists());
        let missing = format.read(&dir, "gone", batch.schema_ref()).unwrap_err();
        assert_eq!(missing.code(), Some("file_missing"));
        let read = format.read(&dir, &linked, batch.schema_ref()).unwrap_err();
        assert_eq!(read.code(), Some("not_a_regular_file"));
    }
    assert_eq!(FileFormat::named("a/b.jsonl"), Some(FileFormat::Jsonl));
    assert_eq!(FileFormat::named("b.ndjson"), Some(FileFormat::Jsonl));
    assert_eq!(FileFormat::named("b.arrow"), Some(FileFormat::Arrow));
    assert_eq!(FileFormat::named("b.txt"), None);
    assert_eq!(FileFormat::named("arrow"), None);
}

#[test]
fn a_writer_dropped_unfinished_leaves_no_file_and_a_finished_one_is_synced() {
    use crate::rooted::trace;
    let (root, dir) = scratch();
    let batch = strings(2, 2);
    for format in [FileFormat::Jsonl, FileFormat::Arrow] {
        let mut writer = Writer::create(format, &dir, "rows", batch.schema_ref()).unwrap();
        writer.write(&batch).unwrap();
        assert!(root.path().join("rows").exists());
        drop(writer);
        assert!(!root.path().join("rows").exists());
        trace::clear();
        let mut writer = Writer::create(format, &dir, "kept", batch.schema_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.write(&batch.slice(0, 0)).unwrap();
        assert_eq!(writer.finish().unwrap().rows, 2);
        assert_eq!(trace::synced(), [root.path().to_owned()]);
        std::fs::remove_file(root.path().join("kept")).unwrap();
    }
}

#[test]
fn files_are_appended_to_a_file_being_written_row_for_row() {
    let (root, dir) = scratch();
    let (first, second) = (strings(2, 3), strings(3, 1));
    for format in [FileFormat::Jsonl, FileFormat::Arrow] {
        format
            .write(&dir, "first", std::slice::from_ref(&first))
            .unwrap();
        format
            .write(&dir, "second", std::slice::from_ref(&second))
            .unwrap();
        let mut writer = Writer::create(format, &dir, "both", first.schema_ref()).unwrap();
        for name in ["first", "second"] {
            writer
                .append(dir.file(name).unwrap(), dir.at(name))
                .unwrap();
        }
        assert_eq!(writer.finish().unwrap().rows, 5);
        let read = format.read(&dir, "both", first.schema_ref()).unwrap();
        assert_eq!(joined(&read), joined(&[first.clone(), second.clone()]));
        for name in ["first", "second", "both"] {
            std::fs::remove_file(root.path().join(name)).unwrap();
        }
    }
    // JSON lines are appended as they are: a last line without its ending gets one, and lines
    // holding no record are left out.
    std::fs::write(
        root.path().join("ragged"),
        "{\"c\":\"a\"}\r\n\n  \n{\"c\":\"b\"}",
    )
    .unwrap();
    let mut writer = Writer::create(FileFormat::Jsonl, &dir, "copied", first.schema_ref()).unwrap();
    writer
        .append(dir.file("ragged").unwrap(), dir.at("ragged"))
        .unwrap();
    writer
        .append(dir.file("ragged").unwrap(), dir.at("ragged"))
        .unwrap();
    assert_eq!(writer.finish().unwrap().rows, 4);
    let copied = std::fs::read_to_string(root.path().join("copied")).unwrap();
    assert_eq!(copied, "{\"c\":\"a\"}\r\n{\"c\":\"b\"}\n".repeat(2));
}

fn lines_of(text: &[u8], limit: u64, buffer: usize) -> std::io::Result<Vec<Vec<u8>>> {
    let mut lines = Lines::new(BufReader::with_capacity(buffer, Cursor::new(text)), limit);
    let (mut read, mut line) = (Vec::new(), Vec::new());
    while lines.next(&mut line)? {
        read.push(line.clone());
    }
    Ok(read)
}

#[test]
fn lines_are_read_whole_with_their_endings_whatever_the_buffer() {
    let text = b"one\n\ntwo\r\n   \nlast";
    let expected: Vec<&[u8]> = vec![b"one\n", b"\n", b"two\r\n", b"   \n", b"last"];
    for buffer in [1, 2, 3, 4, 5, 64] {
        assert_eq!(lines_of(text, 4, buffer).unwrap(), expected, "{buffer}");
    }
    assert!(lines_of(b"", 4, 8).unwrap().is_empty());
    assert_eq!(lines_of(b"\n", 0, 8).unwrap(), [b"\n"]);
    assert_eq!(lines_of(b"\r\n", 0, 8).unwrap(), [b"\r\n"]);
    assert_eq!(content(b"two\r\n"), b"two");
    assert_eq!(content(b"two\n"), b"two");
    assert_eq!(content(b"two\r"), b"two");
    assert_eq!(content(b"two"), b"two");
    assert!(holds_a_record(b" {} \n") && !holds_a_record(b" \t\r\n") && !holds_a_record(b""));
}

#[test]
fn a_line_is_refused_one_byte_beyond_its_limit_and_no_more_of_it_is_read() {
    let too_large = |error: &std::io::Error| match refusal(error) {
        Some(Refusal::TooLarge {
            name,
            limit,
            actual,
        }) => (name, limit, actual),
        other => panic!("{other:?}"),
    };
    for buffer in [1, 3, 4, 5, 6, 7, 64] {
        // The limit bounds the line's content: its ending is apart, with or without a
        // carriage return, and so is the end of the file.
        for fits in [
            &b"1234\n"[..],
            b"1234\r\n",
            b"1234",
            b"1234\r",
            b"12\n1234\n",
        ] {
            assert!(lines_of(fits, 4, buffer).is_ok(), "{fits:?} {buffer}");
        }
        for over in [
            &b"12345\n"[..],
            b"12345\r\n",
            b"12345",
            b"1234\r\r\n",
            b"12\n12345\n",
            b"123456789",
        ] {
            let error = lines_of(over, 4, buffer).unwrap_err();
            assert_eq!(too_large(&error), (LINE_LIMIT, 4, 5), "{over:?} {buffer}");
        }
    }
    // A line without end is given up after the limit: the reader took no more than that.
    let endless = std::io::repeat(b'x');
    let mut counted = Counting {
        inner: endless,
        read: 0,
    };
    let mut lines = Lines::new(BufReader::with_capacity(16, &mut counted), 100);
    assert!(lines.next(&mut Vec::new()).is_err());
    drop(lines);
    assert!(counted.read <= 100 + 2 + 16, "{}", counted.read);
}

/// A reader that counts the bytes read through it.
struct Counting<R> {
    inner: R,
    read: usize,
}

impl<R: std::io::Read> std::io::Read for Counting<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buffer)?;
        self.read += read;
        Ok(read)
    }
}

#[test]
fn a_bounded_writer_refuses_a_line_beyond_its_limit_however_it_is_written() {
    let written = |parts: &[&[u8]]| {
        let mut writer = Bounded::new(Vec::new(), 4);
        for part in parts {
            writer.write_all(part)?;
        }
        writer.flush()?;
        let count = writer.written;
        Ok::<_, std::io::Error>((writer.into_inner(), count))
    };
    let whole: &[&[u8]] = &[b"1234\n12\n", b"12", b"34\n", b"\n", b"1234"];
    let (bytes, count) = written(whole).unwrap();
    assert_eq!(bytes, b"1234\n12\n1234\n\n1234");
    assert_eq!(count, 18);
    for over in [
        &[&b"12345\n"[..]][..],
        &[b"123", b"45"],
        &[b"12\n", b"1234", b"5\n"],
        &[b"1\n23456"],
    ] {
        let error = written(over).unwrap_err();
        assert!(
            matches!(
                refusal(&error),
                Some(Refusal::TooLarge {
                    name: LINE_LIMIT,
                    limit: 4,
                    actual: 5
                })
            ),
            "{over:?}"
        );
    }
}

/// An Arrow file of `batches` batches of a dictionary column and an integer column, written
/// with `options`.
fn arrow_bytes(batches: usize, options: IpcWriteOptions) -> (Vec<u8>, SchemaRef) {
    let batch = RecordBatch::try_from_iter_with_nullable([
        (
            "tag",
            dictionary::<Int8Type>(vec![Some(0), None, Some(2)]),
            true,
        ),
        (
            "id",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            false,
        ),
    ])
    .unwrap();
    let mut writer =
        FileWriter::try_new_with_options(Vec::new(), batch.schema_ref(), options).unwrap();
    for _ in 0..batches {
        writer.write(&batch).unwrap();
    }
    writer.finish().unwrap();
    (writer.into_inner().unwrap(), batch.schema())
}

/// `bytes`, an Arrow file, with its footer's lists of dictionary and record batch blocks
/// changed by `change`; the footer's own schema, which no reader here follows, is left empty.
fn refooted(
    bytes: &[u8],
    change: impl Fn(&mut Vec<arrow_ipc::Block>, &mut Vec<arrow_ipc::Block>),
) -> Vec<u8> {
    let end = bytes.len() - 10;
    let length =
        usize::try_from(i32::from_le_bytes(bytes[end..end + 4].try_into().unwrap())).unwrap();
    let footer = arrow_ipc::root_as_footer(&bytes[end - length..end]).unwrap();
    let mut dictionaries: Vec<arrow_ipc::Block> =
        footer.dictionaries().unwrap().iter().copied().collect();
    let mut batches: Vec<arrow_ipc::Block> =
        footer.recordBatches().unwrap().iter().copied().collect();
    change(&mut dictionaries, &mut batches);
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let dictionaries = builder.create_vector(&dictionaries);
    let batches = builder.create_vector(&batches);
    let schema = arrow_ipc::convert::IpcSchemaEncoder::new()
        .schema_to_fb_offset(&mut builder, &Schema::empty());
    let mut rebuilt = arrow_ipc::FooterBuilder::new(&mut builder);
    rebuilt.add_version(arrow_ipc::MetadataVersion::V5);
    rebuilt.add_schema(schema);
    rebuilt.add_dictionaries(dictionaries);
    rebuilt.add_recordBatches(batches);
    let root = rebuilt.finish();
    builder.finish(root, None);
    let footer = builder.finished_data();
    let mut file = bytes[..end - length].to_vec();
    file.extend_from_slice(footer);
    file.extend_from_slice(&i32::try_from(footer.len()).unwrap().to_le_bytes());
    file.extend_from_slice(b"ARROW1");
    file
}

fn read_bytes(bytes: &[u8], schema: &SchemaRef) -> rdlt_connector::Result<Vec<RecordBatch>> {
    let (root, dir) = scratch();
    std::fs::write(root.path().join("rows.arrow"), bytes).unwrap();
    FileFormat::Arrow.read(&dir, "rows.arrow", schema)
}

#[test]
fn an_arrow_file_reads_whatever_its_writer_aligned_its_messages_to() {
    for alignment in [8, 16, 32, 64] {
        let options =
            IpcWriteOptions::try_new(alignment, false, arrow_ipc::MetadataVersion::V5).unwrap();
        let (bytes, schema) = arrow_bytes(2, options);
        let read = read_bytes(&bytes, &schema).unwrap();
        assert_eq!(
            read.iter().map(RecordBatch::num_rows).sum::<usize>(),
            6,
            "{alignment}"
        );
        // The footer rebuilt as it was reads the same.
        let same = refooted(&bytes, |_, _| {});
        assert_eq!(read_bytes(&same, &schema).unwrap(), read, "{alignment}");
    }
    let (empty, schema) = arrow_bytes(0, IpcWriteOptions::default());
    assert!(read_bytes(&empty, &schema).unwrap().is_empty());
    // A last block that ends where the footer starts, with no end-of-stream marker between.
    let (bytes, schema) = arrow_bytes(2, IpcWriteOptions::default());
    let end = bytes.len() - 10;
    let length = i32::from_le_bytes(bytes[end..end + 4].try_into().unwrap());
    let footer = end - usize::try_from(length).unwrap();
    let mut flush = bytes[..footer - 8].to_vec();
    flush.extend_from_slice(&bytes[footer..]);
    let flush = refooted(&flush, |_, _| {});
    assert_eq!(read_bytes(&flush, &schema).unwrap().len(), 2);
}

/// A change to a footer's dictionary blocks and record batch blocks.
type Change = Box<dyn Fn(&mut Vec<arrow_ipc::Block>, &mut Vec<arrow_ipc::Block>)>;

/// Every way a footer can mislead about the blocks of a file `end` bytes long.
fn misleading(end: i64) -> Vec<(&'static str, Change)> {
    let moved = |offset: i64, metadata: i32, body: i64| -> Change {
        Box::new(move |_, batches| batches[0] = arrow_ipc::Block::new(offset, metadata, body))
    };
    let dictionary: Change = Box::new(|dictionaries, batches| batches[0] = dictionaries[0]);
    let batch: Change = Box::new(|dictionaries, batches| dictionaries[0] = batches[0]);
    let shorter: Change = Box::new(|_, batches| {
        let (at, metadata) = (batches[0].offset(), batches[0].metaDataLength());
        batches[0] = arrow_ipc::Block::new(at, metadata, batches[0].bodyLength() - 8);
    });
    let frame = i64::try_from(FRAME_BYTES).unwrap();
    let twice: Change = Box::new(|_, batches| batches[1] = batches[0]);
    let swapped: Change = Box::new(|_, batches| batches.swap(0, 1));
    let within: Change = Box::new(|_, batches| {
        let (at, metadata) = (batches[1].offset(), batches[1].metaDataLength());
        batches[1] = arrow_ipc::Block::new(at - 8, metadata, batches[1].bodyLength());
    });
    vec![
        ("a block listed twice", twice),
        ("blocks out of their order in the file", swapped),
        ("a block starting within the block before it", within),
        ("a dictionary where a batch belongs", dictionary),
        ("a batch where a dictionary belongs", batch),
        ("a block before the first message", moved(0, 64, 0)),
        ("a block of no message", moved(8, 8, 0)),
        ("a block of less than a message's prefix", moved(64, 7, 0)),
        ("a block into the footer", moved(end - 200, 100, 150)),
        ("a block past the end", moved(end + 8, 8, 8)),
        ("a body that overflows", moved(64, 64, i64::MAX)),
        ("an offset that overflows", moved(i64::MAX, 64, 64)),
        ("a body beyond the frame limit", moved(64, 64, frame)),
        ("a negative offset", moved(-64, 64, 64)),
        ("a negative metadata length", moved(64, -64, 64)),
        ("a negative body", moved(64, 64, -1)),
        ("a body of other bytes", shorter),
    ]
}

#[test]
fn an_arrow_file_whose_footer_misleads_is_refused() {
    let (bytes, schema) = arrow_bytes(2, IpcWriteOptions::default());
    for (name, change) in misleading(i64::try_from(bytes.len()).unwrap()) {
        let damaged = refooted(&bytes, change);
        let error = read_bytes(&damaged, &schema).expect_err(name);
        assert_eq!(error.kind(), ConnectorErrorKind::Data, "{name}");
    }
}

#[test]
fn an_arrow_file_cut_short_or_whose_footer_or_magic_is_damaged_is_refused() {
    let (bytes, schema) = arrow_bytes(2, IpcWriteOptions::default());
    // Every byte the file may be cut at, and every length its footer may claim.
    for cut in 0..bytes.len() {
        let error = read_bytes(&bytes[..cut], &schema).expect_err("a cut file");
        assert_eq!(error.kind(), ConnectorErrorKind::Data, "cut at {cut}");
    }
    let tail = bytes.len() - 10;
    let whole = i32::try_from(bytes.len()).unwrap();
    for length in [i32::MIN, -1, 0, 1, whole, i32::MAX] {
        let mut damaged = bytes.clone();
        damaged[tail..tail + 4].copy_from_slice(&length.to_le_bytes());
        let error = read_bytes(&damaged, &schema).expect_err("a footer outside the file");
        assert_eq!(error.kind(), ConnectorErrorKind::Data, "footer of {length}");
    }
    for (at, name) in [
        (0, "the opening magic"),
        (bytes.len() - 1, "the closing magic"),
    ] {
        let mut damaged = bytes.clone();
        damaged[at] ^= 1;
        assert_eq!(
            read_bytes(&damaged, &schema).expect_err(name).kind(),
            ConnectorErrorKind::Data
        );
    }
}

#[test]
fn an_arrow_file_of_messages_no_reader_frames_is_refused() {
    // Messages without the marker their length follows, as writers before Arrow 0.15 wrote.
    let legacy = IpcWriteOptions::try_new(8, true, arrow_ipc::MetadataVersion::V4).unwrap();
    let (bytes, schema) = arrow_bytes(1, legacy);
    assert_eq!(
        read_bytes(&bytes, &schema).unwrap_err().kind(),
        ConnectorErrorKind::Data
    );
    // A schema message that claims more than the file holds, or nothing.
    let (bytes, schema) = arrow_bytes(1, IpcWriteOptions::default());
    let at = (8..72)
        .step_by(8)
        .find(|at| bytes[*at..*at + 4] == [0xff; 4])
        .unwrap()
        + 4;
    for length in [
        i32::MIN,
        -1,
        0,
        i32::try_from(bytes.len()).unwrap(),
        i32::MAX,
    ] {
        let mut damaged = bytes.clone();
        damaged[at..at + 4].copy_from_slice(&length.to_le_bytes());
        let error = read_bytes(&damaged, &schema).expect_err("a schema outside the file");
        assert_eq!(error.kind(), ConnectorErrorKind::Data, "schema of {length}");
    }
    // A batch message whose length reaches beyond its block.
    let end = bytes.len() - 10;
    let footer_length =
        usize::try_from(i32::from_le_bytes(bytes[end..end + 4].try_into().unwrap())).unwrap();
    let footer = arrow_ipc::root_as_footer(&bytes[end - footer_length..end]).unwrap();
    let block = footer.recordBatches().unwrap().get(0);
    let at = usize::try_from(block.offset()).unwrap() + 4;
    for length in [-1, 0, block.metaDataLength() - 7, i32::MAX] {
        let mut damaged = bytes.clone();
        damaged[at..at + 4].copy_from_slice(&length.to_le_bytes());
        let error = read_bytes(&damaged, &schema).expect_err("a message beyond its block");
        assert_eq!(
            error.kind(),
            ConnectorErrorKind::Data,
            "message of {length}"
        );
    }
}

#[test]
fn an_arrow_reader_skips_batches_and_tells_its_schema() {
    let (root, dir) = scratch();
    let (bytes, schema) = arrow_bytes(3, IpcWriteOptions::default());
    std::fs::write(root.path().join("rows.arrow"), bytes).unwrap();
    let empty = Arc::new(Schema::empty());
    // It tells how many it skipped: fewer than asked where the file holds fewer.
    for (asked, skipped, left) in [(0, 0, 3), (1, 1, 2), (3, 3, 0), (4, 3, 0), (u64::MAX, 3, 0)] {
        let mut reader = Reader::open(FileFormat::Arrow, &dir, "rows.arrow", &empty).unwrap();
        assert_eq!(reader.schema(), Some(&schema));
        assert_eq!(reader.skip(asked), skipped, "{asked}");
        let mut read = 0;
        while reader.next().unwrap().is_some() {
            read += 1;
        }
        assert_eq!(read, left, "{asked}");
    }
    std::fs::write(root.path().join("rows.jsonl"), "{}\n").unwrap();
    let mut lines = Reader::open(FileFormat::Jsonl, &dir, "rows.jsonl", &empty).unwrap();
    assert_eq!(lines.schema(), None);
    assert_eq!(lines.skip(1), 0);
    assert_eq!(lines.next().unwrap().map(|batch| batch.num_rows()), Some(1));
}
