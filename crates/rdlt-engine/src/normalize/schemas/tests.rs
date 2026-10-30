use std::sync::Arc;

use arrow_array::types::Int32Type;
use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch, RunArray, StructArray};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field as ArrowField, Fields, Schema};
use rdlt_connector::TableSchema;
use serde_json::json;

use super::PartSchemas;
use crate::normalize::tests::{shape, shredded};
use crate::normalize::{Part, normalize};

/// A batch of rows each holding an object and an array of objects, shredded on its own, so two
/// such batches have equal schemas but not one `Arc`.
fn records(first: i64) -> RecordBatch {
    let rows: Vec<_> = (first..first + 3)
        .map(|id| json!({"id": id, "o": {"x": id}, "items": [{"sku": "a", "n": id}]}))
        .collect();
    shredded(&rows, 1 << 20).remove(0)
}

/// Whether `kept` holds what `alone` holds, a part for a part: the same rows under equal schemas.
fn same(kept: &[Part], alone: &[Part]) -> bool {
    kept.len() == alone.len()
        && kept.iter().zip(alone).all(|(kept, alone)| {
            kept.path == alone.path && kept.columns == alone.columns && kept.batch == alone.batch
        })
}

#[test]
fn the_parts_of_batches_of_one_schema_share_their_schemas() {
    let (first, second) = (records(0), records(10));
    assert!(!Arc::ptr_eq(first.schema_ref(), second.schema_ref()));
    let (shape, mut schemas) = (shape(8), PartSchemas::default());
    let one = normalize(&first, &shape, &mut schemas).unwrap();
    let two = normalize(&second, &shape, &mut schemas).unwrap();
    assert_eq!(one.len(), 2);
    assert_eq!(schemas.built, one.len(), "each part's schema is built once");
    for (one, two) in one.iter().zip(&two) {
        assert!(Arc::ptr_eq(one.batch.schema_ref(), two.batch.schema_ref()));
        assert!(Arc::ptr_eq(&one.columns, &two.columns));
    }
    assert!(same(
        &two,
        &normalize(&second, &shape, &mut PartSchemas::default()).unwrap()
    ));
}

/// A batch of `rows` rows of an object `o` whose field `r` is in runs, the object null at each row
/// `nulls` names.
fn runs(rows: i32, nulls: &[usize]) -> RecordBatch {
    let ends = Int32Array::from_iter_values(1..=rows);
    let values = Int64Array::from_iter_values(0..i64::from(rows));
    let runs: ArrayRef = Arc::new(RunArray::<Int32Type>::try_new(&ends, &values).unwrap());
    let fields = Fields::from(vec![ArrowField::new("r", runs.data_type().clone(), true)]);
    let rows = usize::try_from(rows).unwrap();
    let valid: Vec<bool> = (0..rows).map(|row| !nulls.contains(&row)).collect();
    let valid = (!nulls.is_empty()).then(|| NullBuffer::from(valid));
    let object: ArrayRef = Arc::new(StructArray::new(fields, vec![runs], valid));
    let field = ArrowField::new("o", object.data_type().clone(), true);
    RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![object]).unwrap()
}

#[test]
fn a_part_whose_values_decode_otherwise_takes_a_schema_of_its_own() {
    let (shape, mut schemas) = (shape(8), PartSchemas::default());
    let plain = runs(4, &[]);
    let nulled = runs(4, &[1]);
    assert_eq!(plain.schema(), nulled.schema());
    let kept = normalize(&plain, &shape, &mut schemas).unwrap();
    // Under an object with nulls the runs are decoded, so the column is of another type.
    let decoded = normalize(&nulled, &shape, &mut schemas).unwrap();
    let types = |parts: &[Part]| parts[0].batch.schema().field(0).data_type().clone();
    assert!(matches!(types(&kept), DataType::RunEndEncoded(..)));
    assert_eq!(types(&decoded), DataType::Int64);
    assert_eq!(schemas.built, 2);
    assert!(same(
        &decoded,
        &normalize(&nulled, &shape, &mut PartSchemas::default()).unwrap()
    ));
}

#[test]
fn a_batch_of_another_schema_lets_the_kept_schemas_go_and_a_part_of_other_columns_takes_its_own() {
    let (deep, whole, mut schemas) = (shape(8), shape(0), PartSchemas::default());
    let first = normalize(&records(0), &deep, &mut schemas).unwrap();
    normalize(&runs(2, &[]), &deep, &mut schemas).unwrap();
    let again = normalize(&records(0), &deep, &mut schemas).unwrap();
    assert!(!Arc::ptr_eq(
        first[0].batch.schema_ref(),
        again[0].batch.schema_ref()
    ));
    // Kept whole to depth 0, the objects and the array are columns of the stream's table.
    let kept = normalize(&records(0), &whole, &mut schemas).unwrap();
    assert_eq!(kept.len(), 1);
    assert_eq!(schemas.built, 2 + 1 + 2 + 1);
    assert!(same(
        &kept,
        &normalize(&records(0), &whole, &mut PartSchemas::default()).unwrap()
    ));
}

#[test]
fn a_parts_table_schema_is_converted_once_for_the_schema_kept_for_it() {
    let (shape, mut schemas) = (shape(8), PartSchemas::default());
    let parts = normalize(&records(0), &shape, &mut schemas).unwrap();
    for part in &parts {
        let arrow = part.batch.schema_ref();
        let table = schemas.table(&part.path, arrow).unwrap();
        assert!(Arc::ptr_eq(
            &table,
            &schemas.table(&part.path, arrow).unwrap()
        ));
        assert_eq!(*table, TableSchema::from_arrow(arrow).unwrap());
        // A batch of a schema of its own, as dropping columns makes one, is converted each time.
        let projected = part.batch.project(&[0]).unwrap();
        let alone = schemas.table(&part.path, projected.schema_ref()).unwrap();
        assert!(!Arc::ptr_eq(
            &alone,
            &schemas.table(&part.path, projected.schema_ref()).unwrap()
        ));
        assert_eq!(
            *alone,
            TableSchema::from_arrow(projected.schema_ref()).unwrap()
        );
    }
}
