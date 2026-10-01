//! The files destination reads every float back as the value it was given, in both formats.

use std::sync::Arc;

use arrow_array::builder::{Float64Builder, ListBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type};
use arrow_array::{ArrayRef, Float32Array, Float64Array, Int64Array, RecordBatch, StructArray};
use arrow_schema::{DataType, Field as ArrowField};
use rdlt_connector::{CommitSeq, TableSchema};
use serde_json::json;

use crate::fixtures::{connect_with, merge_table, meta, open, stage, table};

/// A float column of both widths, each holding every non-finite value, a negative zero and a
/// finite value; nested in a struct and a list too.
fn floats(nullable: bool) -> (TableSchema, RecordBatch) {
    let doubles = [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.0, 1.5];
    let singles = [f32::NAN, f32::INFINITY, f32::NEG_INFINITY, -0.0, 2.5];
    let x: ArrayRef = Arc::new(Float64Array::from(doubles.to_vec()));
    let y: ArrayRef = Arc::new(Float32Array::from(singles.to_vec()));
    let inner = Arc::new(ArrowField::new("x", DataType::Float64, nullable));
    let point: ArrayRef = Arc::new(StructArray::from(vec![(inner, Arc::clone(&x))]));
    let mut items = ListBuilder::new(Float64Builder::new());
    for value in doubles {
        items.append_value([Some(value), Some(1.0)]);
    }
    let items: ArrayRef = Arc::new(items.finish());
    let id: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
    let batch = RecordBatch::try_from_iter_with_nullable([
        ("id", id, false),
        ("x", x, nullable),
        ("y", y, nullable),
        ("point", point, nullable),
        ("items", items, true),
    ])
    .expect("the fixture is made");
    let schema = TableSchema::from_arrow(&batch.schema()).expect("the fixture is made");
    (schema, batch)
}

/// The bits of every float of column `name`, a NaN as one value whatever its payload.
fn bits(batches: &[RecordBatch], name: &str) -> Vec<u64> {
    let canonical = |value: f64| {
        if value.is_nan() {
            f64::NAN.to_bits()
        } else {
            value.to_bits()
        }
    };
    batches
        .iter()
        .flat_map(|batch| {
            let column = batch.column_by_name(name).expect("the column");
            match column.data_type() {
                DataType::Float32 => column
                    .as_primitive::<Float32Type>()
                    .iter()
                    .map(|value| canonical(f64::from(value.expect("no null"))))
                    .collect::<Vec<_>>(),
                DataType::Float64 => column
                    .as_primitive::<Float64Type>()
                    .iter()
                    .map(|value| canonical(value.expect("no null")))
                    .collect(),
                DataType::Struct(_) => bits(&[RecordBatch::from(column.as_struct().clone())], "x"),
                _ => column
                    .as_list::<i32>()
                    .iter()
                    .flat_map(|items| {
                        let items = items.expect("no null");
                        items
                            .as_primitive::<Float64Type>()
                            .iter()
                            .map(|value| canonical(value.expect("no null")))
                            .collect::<Vec<_>>()
                    })
                    .collect(),
            }
        })
        .collect()
}

#[tokio::test]
async fn non_finite_floats_read_back_as_they_were_written() {
    for format in ["jsonl", "arrow"] {
        for nullable in [true, false] {
            for merge in [false, true] {
                let case = format!("{format} nullable={nullable} merge={merge}");
                let root = tempfile::tempdir().unwrap();
                let (destination, reader) =
                    connect_with(root.path(), json!({ "format": format })).await;
                let mut opened = open(destination.as_ref(), 1).await;
                let (schema, batch) = floats(nullable);
                let rows = if merge {
                    let mut rows = merge_table("rows");
                    if let Some(key) = &mut rows.merge {
                        key.seq = "id".into();
                    }
                    rows
                } else {
                    table("rows")
                };
                stage(&mut opened, &rows, &schema, batch.clone(), 1).await;
                opened
                    .session
                    .commit(&meta(&opened, 1, CommitSeq::FIRST, &[1]))
                    .await
                    .expect(&case);
                let published = reader.published(&rows).await.expect(&case);
                for column in ["x", "y", "point", "items"] {
                    assert_eq!(
                        bits(&published, column),
                        bits(std::slice::from_ref(&batch), column),
                        "{case} {column}"
                    );
                }
            }
        }
    }
}
