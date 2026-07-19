//! Test-side conversion helpers.

use arrow::json::writer::{JsonArray, WriterBuilder};
use arrow::record_batch::RecordBatch;
use serde_json::{Map, Value};

/// Render a batch as JSON rows (explicit nulls) for easy assertions.
pub fn batch_to_rows(batch: &RecordBatch) -> Vec<Map<String, Value>> {
    let buf = Vec::new();
    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, JsonArray>(buf);
    writer
        .write(batch)
        .expect("in-memory JSON write cannot fail");
    writer.finish().expect("finish JSON array");
    serde_json::from_slice(&writer.into_inner()).expect("arrow JSON writer emits valid JSON")
}
