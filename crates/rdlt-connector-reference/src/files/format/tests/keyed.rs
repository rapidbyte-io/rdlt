//! Dictionaries whose nulls are null keys, keys of a null value, or both, read back from both
//! formats as the values and nulls they stand for.

use std::sync::Arc;

use arrow_array::types::{
    ArrowDictionaryKeyType, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, DictionaryArray, Int64Array, ListArray, PrimitiveArray, RecordBatch,
    StringArray, StructArray,
};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{Field, Fields};

use super::{scratch, single};
use crate::files::FileFormat;
use crate::files::format::plain::{plain, unkeyed};

/// Where a dictionary's nulls are.
#[derive(Clone, Copy, Debug)]
enum Nulls {
    Keys,
    Values,
    Both,
}

/// Four rows keyed by `K` into `values`, which hold a null in their second place, and the rows
/// as the plain values they stand for.
fn keyed<K: ArrowDictionaryKeyType>(values: &ArrayRef, nulls: Nulls) -> (ArrayRef, ArrayRef)
where
    K::Native: TryFrom<u8>,
{
    let keys: [Option<u8>; 4] = match nulls {
        Nulls::Keys => [Some(0), None, Some(2), Some(0)],
        Nulls::Values => [Some(0), Some(1), Some(2), Some(1)],
        Nulls::Both => [Some(1), None, Some(2), Some(0)],
    };
    let taken: arrow_array::UInt32Array = keys.iter().map(|key| key.map(u32::from)).collect();
    let stood_for = arrow_select::take::take(values, &taken, None);
    let keys: PrimitiveArray<K> = keys
        .into_iter()
        .map(|key| key.and_then(|key| K::Native::try_from(key).ok()))
        .collect();
    let dictionary = DictionaryArray::<K>::try_new(keys, Arc::clone(values)).unwrap();
    (Arc::new(dictionary), stood_for.unwrap())
}

/// Dictionaries of every key type over text and over integers, with their nulls in each place.
fn dictionaries() -> Vec<(String, ArrayRef, ArrayRef)> {
    let texts: ArrayRef = Arc::new(StringArray::from(vec![Some("x"), None, Some("")]));
    let numbers: ArrayRef = Arc::new(Int64Array::from(vec![Some(7), None, Some(0)]));
    let mut found = Vec::new();
    for values in [&texts, &numbers] {
        for nulls in [Nulls::Keys, Nulls::Values, Nulls::Both] {
            let all = [
                ("int8", keyed::<Int8Type>(values, nulls)),
                ("int16", keyed::<Int16Type>(values, nulls)),
                ("int32", keyed::<Int32Type>(values, nulls)),
                ("int64", keyed::<Int64Type>(values, nulls)),
                ("uint8", keyed::<UInt8Type>(values, nulls)),
                ("uint16", keyed::<UInt16Type>(values, nulls)),
                ("uint32", keyed::<UInt32Type>(values, nulls)),
                ("uint64", keyed::<UInt64Type>(values, nulls)),
            ];
            for (key, (dictionary, stood_for)) in all {
                let name = format!("{key} keys of {}, {nulls:?} null", values.data_type());
                found.push((name, dictionary, stood_for));
            }
        }
    }
    found
}

/// `column` in a struct, the struct null in its last row, and in a list of two rows.
fn nested(column: &ArrayRef) -> [ArrayRef; 2] {
    let fields = Fields::from(vec![Field::new("d", column.data_type().clone(), true)]);
    let nulls = NullBuffer::from(vec![true, true, true, false]);
    let item = Arc::new(Field::new("item", column.data_type().clone(), true));
    let offsets = OffsetBuffer::from_lengths([3, 1]);
    [
        Arc::new(StructArray::new(
            fields,
            vec![Arc::clone(column)],
            Some(nulls),
        )),
        Arc::new(ListArray::new(item, offsets, Arc::clone(column), None)),
    ]
}

/// `batch` written as `format` and read back as the values it stands for, as one batch.
fn stood_for(format: FileFormat, batch: &RecordBatch) -> RecordBatch {
    let (_root, dir) = scratch();
    let name = format!("rows.{}", format.extension());
    format
        .write(&dir, &name, std::slice::from_ref(batch))
        .unwrap();
    // A table's schema holds no dictionary: JSON lines are read back as the values.
    let schema = plain(batch.schema_ref());
    let read = format.read(&dir, &name, &schema).unwrap();
    let read: Vec<RecordBatch> = read
        .iter()
        .map(|read| unkeyed(read, &schema).unwrap())
        .collect();
    arrow_select::concat::concat_batches(&schema, &read).unwrap()
}

#[test]
fn a_dictionary_s_nulls_read_back_null_wherever_they_are_held_and_wherever_it_nests() {
    for (name, dictionary, values) in dictionaries() {
        let cases = [(dictionary.clone(), values.clone())]
            .into_iter()
            .chain(nested(&dictionary).into_iter().zip(nested(&values)));
        for (depth, (written, expected)) in cases.enumerate() {
            let (written, expected) = (single(written), single(expected));
            for format in [FileFormat::Jsonl, FileFormat::Arrow] {
                let read = stood_for(format, &written);
                assert_eq!(read, expected, "{name}, nested {depth}, {format:?}");
                // Sliced, as part of a batch is.
                let rows = written.num_rows();
                let read = stood_for(format, &written.slice(1, rows - 1));
                let sliced = expected.slice(1, rows - 1);
                assert_eq!(read, sliced, "{name}, nested {depth}, sliced, {format:?}");
            }
        }
    }
}

#[test]
fn a_dictionary_of_no_values_reads_back_as_nulls() {
    let values: ArrayRef = Arc::new(StringArray::from(Vec::<Option<&str>>::new()));
    let keys: PrimitiveArray<Int8Type> = [None, None].into_iter().collect();
    let dictionary: ArrayRef = Arc::new(DictionaryArray::try_new(keys, values).unwrap());
    let nulls: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>, None]));
    for format in [FileFormat::Jsonl, FileFormat::Arrow] {
        assert_eq!(
            stood_for(format, &single(dictionary.clone())),
            single(nulls.clone())
        );
    }
}

#[test]
fn run_ends_and_views_with_nulls_read_back_from_json_lines_as_their_values() {
    use arrow_array::types::Int16Type;
    use arrow_schema::DataType;
    let texts: ArrayRef = Arc::new(StringArray::from(vec![Some("x"), None, Some("")]));
    let (dictionary, _) = keyed::<Int8Type>(&texts, Nulls::Both);
    let ends = PrimitiveArray::<Int16Type>::from_iter_values([1, 3, 4]);
    let runs: ArrayRef = Arc::new(arrow_array::RunArray::try_new(&ends, &texts).unwrap());
    let ends = PrimitiveArray::<Int16Type>::from_iter_values([1, 2, 3, 4]);
    let keyed_runs: ArrayRef =
        Arc::new(arrow_array::RunArray::try_new(&ends, &dictionary).unwrap());
    let views: ArrayRef = Arc::new(arrow_array::StringViewArray::from(vec![
        Some("x"),
        None,
        Some("a string longer than a view holds inline"),
        Some(""),
    ]));
    let cases = [
        ("run ends", runs),
        ("run ends of a dictionary", keyed_runs),
        ("views", views),
    ];
    for (name, column) in cases {
        let expected = single(arrow_cast::cast(&column, &DataType::Utf8).unwrap());
        let (_root, dir) = scratch();
        let written = single(column);
        FileFormat::Jsonl
            .write(&dir, "rows.jsonl", std::slice::from_ref(&written))
            .unwrap();
        let read = FileFormat::Jsonl
            .read(&dir, "rows.jsonl", expected.schema_ref())
            .unwrap();
        assert_eq!(read, [expected], "{name}");
    }
}
