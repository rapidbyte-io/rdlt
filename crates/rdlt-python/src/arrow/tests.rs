use pyo3::exceptions::PyValueError;
use pyo3::types::{PyAnyMethods as _, PyDict};
use pyo3::{Bound, PyAny, Python};

use super::{Handed, NativeBatch, Produced, array_of, batches, produced};
use crate::embedded;

/// Defines, beside a sound batch `sound`, Arrow whose buffers break its types, built from them as
/// they are given, which nothing checks: `unicode`, a batch whose text is no UTF-8, `backwards`,
/// one whose offsets run backwards, and `keyed`, a dictionary array whose key lies past its
/// dictionary.
const BROKEN: &std::ffi::CStr = c"import pyarrow as pa
def text(offsets, values):
    made = pa.array(offsets, pa.int32()).buffers()[1]
    column = pa.Array.from_buffers(pa.utf8(), len(offsets) - 1, [None, made, pa.py_buffer(values)])
    return pa.record_batch([column], names=['s'])
sound = text([0, 2], b'ab')
unicode = text([0, 2], b'\\xff\\xfe')
backwards = text([0, 2, 1], b'ab')
keyed = pa.DictionaryArray.from_arrays(pa.array([5], pa.int32()), pa.array(['a']), safe=False)
";

fn broken(py: Python<'_>) -> Bound<'_, PyDict> {
    let globals = PyDict::new(py);
    py.run(BROKEN, Some(&globals), None).unwrap();
    globals
}

fn evaluated<'py>(py: Python<'py>, code: &str) -> Bound<'py, PyAny> {
    py.eval(&std::ffi::CString::new(code).unwrap(), None, None)
        .unwrap()
}

fn values_address(column: &Bound<'_, PyAny>) -> usize {
    column
        .call_method0("buffers")
        .unwrap()
        .get_item(1)
        .unwrap()
        .getattr("address")
        .unwrap()
        .extract()
        .unwrap()
}

#[test]
fn a_pyarrow_batch_crosses_into_rust_without_a_copy() {
    embedded::with(|py| {
        let batch = evaluated(
            py,
            "__import__('pyarrow').record_batch({'n': __import__('pyarrow').array(range(1000), __import__('pyarrow').int64())})",
        );
        let address = values_address(&batch.call_method1("column", (0,)).unwrap());
        let Some(Produced::Batch(rust)) = produced(&batch).unwrap() else {
            panic!("a batch")
        };
        assert_eq!(rust.num_rows(), 1000);
        assert_eq!(
            rust.column(0).to_data().buffers()[0].as_ptr().addr(),
            address
        );
    });
}

#[test]
fn a_rust_batch_crosses_into_pyarrow_without_a_copy() {
    embedded::with(|py| {
        let rust = arrow_array::RecordBatch::try_from_iter([(
            "n",
            std::sync::Arc::new(arrow_array::Int64Array::from_iter_values(0..1000))
                as arrow_array::ArrayRef,
        )])
        .unwrap();
        let address = rust.column(0).to_data().buffers()[0].as_ptr().addr();
        let lent = Bound::new(py, NativeBatch::new(rust)).unwrap();
        let batch = evaluated(py, "__import__('pyarrow').record_batch")
            .call1((lent,))
            .unwrap();
        assert_eq!(
            values_address(&batch.call_method1("column", (0,)).unwrap()),
            address
        );
    });
}

#[test]
fn a_rust_batch_keeps_its_schema_s_metadata_crossing_into_pyarrow() {
    embedded::with(|py| {
        let schema = arrow_schema::Schema::new_with_metadata(
            vec![arrow_schema::Field::new(
                "n",
                arrow_schema::DataType::Int64,
                false,
            )],
            std::collections::HashMap::from([("origin".to_owned(), "orders".to_owned())]),
        );
        let rust = arrow_array::RecordBatch::try_new(
            std::sync::Arc::new(schema),
            vec![
                std::sync::Arc::new(arrow_array::Int64Array::from_iter_values(0..3))
                    as arrow_array::ArrayRef,
            ],
        )
        .unwrap();
        let lent = Bound::new(py, NativeBatch::new(rust)).unwrap();
        let batch = evaluated(py, "__import__('pyarrow').record_batch")
            .call1((lent,))
            .unwrap();
        let metadata = batch
            .getattr("schema")
            .unwrap()
            .getattr("metadata")
            .unwrap();
        let metadata: std::collections::HashMap<Vec<u8>, Vec<u8>> = metadata.extract().unwrap();
        assert_eq!(
            metadata,
            std::collections::HashMap::from([(b"origin".to_vec(), b"orders".to_vec())])
        );
    });
}

#[test]
fn a_table_and_other_libraries_cross_as_streams() {
    embedded::with(|py| {
        for code in [
            "__import__('pyarrow').table({'n': [1, 2, 3]})",
            "__import__('polars').DataFrame({'n': [1, 2, 3]})",
            "__import__('duckdb').sql('SELECT * FROM range(3) t(n)')",
        ] {
            let Some(Produced::Stream(reader)) = produced(&evaluated(py, code)).unwrap() else {
                panic!("{code}: a stream")
            };
            let rows: usize = reader.map(|batch| batch.unwrap().num_rows()).sum();
            assert_eq!(rows, 3, "{code}");
        }
    });
}

#[test]
fn a_struct_array_with_nulls_is_refused_as_a_batch() {
    embedded::with(|py| {
        let array = evaluated(py, "__import__('pyarrow').array([{'n': 1}, None])");
        let Err(error) = produced(&array) else {
            panic!("a struct array whose entries are null is no batch")
        };
        assert!(error.is_instance_of::<PyValueError>(py), "{error}");
        let whole = evaluated(py, "__import__('pyarrow').array([{'n': 1}, {'n': 2}])");
        let Some(Produced::Batch(batch)) = produced(&whole).unwrap() else {
            panic!("a struct array without nulls is a batch")
        };
        assert_eq!(batch.num_rows(), 2);
    });
}

#[test]
fn a_chunked_array_of_structs_with_a_null_entry_is_refused_and_one_without_is_a_stream() {
    embedded::with(|py| {
        let column = |rows: &str| {
            evaluated(
                py,
                &format!("__import__('pyarrow').table({{'payload': {rows}}})['payload']"),
            )
        };
        let Err(error) = produced(&column("[{'n': 1}, None, {'n': 3}]")) else {
            panic!("a struct column holding a null entry is no stream of rows")
        };
        assert!(error.is_instance_of::<PyValueError>(py), "{error}");
        let Some(Produced::Stream(reader)) = produced(&column("[{'n': 1}, {'n': 3}]")).unwrap()
        else {
            panic!("a struct column without nulls is a stream")
        };
        let rows: usize = reader.map(|batch| batch.unwrap().num_rows()).sum();
        assert_eq!(rows, 2);
    });
}

#[test]
fn what_is_no_arrow_producer_is_none() {
    embedded::with(|py| {
        assert!(produced(&evaluated(py, "{'n': 1}")).unwrap().is_none());
    });
}

#[test]
fn a_batch_that_breaks_its_types_is_refused_alone_and_in_a_stream() {
    embedded::with(|py| {
        let globals = broken(py);
        for name in ["unicode", "backwards"] {
            let batch = globals.get_item(name).unwrap();
            let Err(error) = produced(&batch) else {
                panic!("{name}: refused as a batch")
            };
            assert!(error.is_instance_of::<PyValueError>(py), "{name}: {error}");
            let table = batch
                .py()
                .eval(c"pa.Table.from_batches", Some(&globals), None)
                .unwrap()
                .call1(([&batch],))
                .unwrap();
            let Some(Produced::Stream(mut read)) = produced(&table).unwrap() else {
                panic!("{name}: a stream")
            };
            assert!(
                matches!(read.next(), Some(Err(_))),
                "{name}: refused in a stream"
            );
        }
        let sound = globals.get_item("sound").unwrap();
        let Some(Produced::Batch(batch)) = produced(&sound).unwrap() else {
            panic!("a sound batch")
        };
        assert_eq!(batch.num_rows(), 1);
    });
}

#[test]
fn an_array_that_breaks_its_type_is_refused() {
    embedded::with(|py| {
        let globals = broken(py);
        let Err(error) = array_of(&globals.get_item("keyed").unwrap()) else {
            panic!("a key past its dictionary is refused")
        };
        assert!(error.is_instance_of::<PyValueError>(py), "{error}");
        let sound = py
            .eval(c"__import__('pyarrow').array([True, False])", None, None)
            .unwrap();
        assert_eq!(array_of(&sound).unwrap().len(), 2);
    });
}

#[test]
fn batches_handed_over_that_break_their_types_are_refused_as_batches_and_as_streams() {
    embedded::with(|py| {
        let globals = broken(py);
        let unicode = globals.get_item("unicode").unwrap();
        let capsules = unicode.call_method0("__arrow_c_array__").unwrap();
        let alone = Handed::Batch(capsules.get_item(0).unwrap(), capsules.get_item(1).unwrap());
        assert!(batches(py, vec![alone]).is_err(), "a batch");
        let table = py
            .eval(c"pa.Table.from_batches([unicode])", Some(&globals), None)
            .unwrap();
        let stream = Handed::Stream(table.call_method0("__arrow_c_stream__").unwrap());
        assert!(batches(py, vec![stream]).is_err(), "a stream");
        let sound = globals.get_item("sound").unwrap();
        let capsules = sound.call_method0("__arrow_c_array__").unwrap();
        let alone = Handed::Batch(capsules.get_item(0).unwrap(), capsules.get_item(1).unwrap());
        assert_eq!(batches(py, vec![alone]).unwrap().len(), 1);
    });
}
