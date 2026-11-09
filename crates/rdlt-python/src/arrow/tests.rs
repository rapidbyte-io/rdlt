use pyo3::types::PyAnyMethods as _;
use pyo3::{Bound, PyAny, Python};

use super::{NativeBatch, Produced, produced};
use crate::embedded;

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
fn what_is_no_arrow_producer_is_none() {
    embedded::with(|py| {
        assert!(produced(&evaluated(py, "{'n': 1}")).unwrap().is_none());
    });
}
