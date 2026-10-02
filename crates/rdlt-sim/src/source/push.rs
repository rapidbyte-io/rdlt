//! What a simulated source pushes: its rows as Arrow batches, or as JSON.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, RecordBatchOptions, StringArray};
use arrow_buffer::{Buffer, MutableBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field as ArrowField, Schema};
use bytes::Bytes;
use rdlt_testkit::drawn::json::text;
use rdlt_testkit::drawn::{Encoding, Scalar, Shape, array, field};
use serde_json::{Value, json};

use crate::workload::{Row, SimStream};

/// `rows` of `stream` as one batch: the base columns, the key of a merge stream, and every drift
/// column present where the rows were delivered; for a sliced stream, a slice of a batch holding
/// the rows three times.
///
/// Its ids lie in an allocation of `ballast` bytes where that is more than they take, which the
/// batch keeps alive.
pub(super) fn batch(stream: &SimStream, rows: &[Row], ballast: usize) -> RecordBatch {
    if !stream.sliced {
        return whole(stream, rows, ballast);
    }
    let tripled: Vec<Row> = rows.iter().chain(rows).chain(rows).cloned().collect();
    whole(stream, &tripled, ballast).slice(rows.len(), rows.len())
}

/// `rows`' ids, in an allocation of `ballast` bytes at least, never written beyond the ids.
fn ids(rows: &[Row], ballast: usize) -> Int64Array {
    let mut buffer = MutableBuffer::with_capacity(ballast.max(rows.len() * 8));
    for row in rows {
        buffer.push(row.id);
    }
    Int64Array::new(ScalarBuffer::from(Buffer::from(buffer)), None)
}

fn whole(stream: &SimStream, rows: &[Row], ballast: usize) -> RecordBatch {
    let column =
        |value: fn(&Row) -> i64| Arc::new(Int64Array::from_iter_values(rows.iter().map(value)));
    let base = |name: &str| ArrowField::new(name, DataType::Int64, false);
    let mut fields = vec![base("id"), base("partition"), base("offset"), base("value")];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(ids(rows, ballast)),
        column(|row| row.partition),
        column(|row| row.offset),
        column(|row| row.value),
    ];
    let (partition, delivered) = rows.first().map_or((0, 0), |row| {
        (usize::try_from(row.partition).unwrap_or(0), row.delivered)
    });
    if stream.keys > 0 {
        let logical = stream.key_type(partition, delivered);
        let shape = Shape {
            logical: logical.clone(),
            encoding: Encoding::Plain,
            children: Vec::new(),
        };
        let keys: Vec<Scalar> = rows.iter().map(|row| row.key_value(logical)).collect();
        let array = array(&shape, &keys.iter().collect::<Vec<_>>());
        fields.push(field("key", &shape, &array, false));
        columns.push(array);
    }
    if stream.keys > 0 && stream.composite {
        fields.push(ArrowField::new("tag", DataType::Utf8, false));
        let tags = rows.iter().map(|row| row.tag.clone().unwrap_or_default());
        columns.push(Arc::new(StringArray::from_iter_values(tags)));
    }
    for (index, drift) in stream.drift.iter().enumerate() {
        if let Some(shape) = &drift.shapes[partition][delivered] {
            let values: Vec<&Scalar> = rows
                .iter()
                .map(|row| row.extras[index].as_ref().unwrap_or(&Scalar::Null))
                .collect();
            let array = array(shape, &values);
            fields.push(field(&drift.name, shape, &array, true));
            columns.push(array);
        }
    }
    let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
    RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), columns, &options)
        .expect("equal-length columns make a batch")
}

/// `rows` of `stream` as a JSON push with the columns a batch of them has: a JSON array when
/// `array`, else JSON lines.
pub(super) fn json_push(stream: &SimStream, rows: &[Row], array: bool) -> Bytes {
    // Each object is written as text, so an integer beyond 64 bits pushes as the integer it is.
    let objects = rows.iter().map(|row| {
        let mut fields = vec![
            ("id", json!(row.id).to_string()),
            ("partition", json!(row.partition).to_string()),
            ("offset", json!(row.offset).to_string()),
            ("value", json!(row.value).to_string()),
        ];
        if stream.keys > 0 {
            fields.push(("key", json!(row.key.unwrap_or_default()).to_string()));
        }
        if let Some(tag) = &row.tag {
            fields.push(("tag", json!(tag).to_string()));
        }
        let partition = usize::try_from(row.partition).unwrap_or(0);
        for (drift, extra) in stream.drift.iter().zip(&row.extras) {
            let shape = drift.shapes[partition][row.delivered].as_ref();
            if let (Some(extra), Some(shape)) = (extra, shape) {
                fields.push((drift.name.as_str(), text(extra, &shape.logical)));
            }
        }
        let fields: Vec<String> = fields
            .iter()
            .map(|(name, value)| format!("{}:{value}", Value::from(*name)))
            .collect();
        format!("{{{}}}", fields.join(","))
    });
    let objects: Vec<String> = objects.collect();
    Bytes::from(if array {
        format!("[{}]", objects.join(","))
    } else {
        objects.join("\n")
    })
}
