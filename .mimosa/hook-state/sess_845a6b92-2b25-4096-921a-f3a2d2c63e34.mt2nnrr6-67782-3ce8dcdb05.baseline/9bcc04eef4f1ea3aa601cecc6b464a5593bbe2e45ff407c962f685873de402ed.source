//! Smoke coverage over the hidden fuzz/bench entry points: they wrap
//! PRODUCTION paths (arena parse, tape shred, arrow mapping,
//! passthrough), so a signature drift or panic in any wrapper breaks the
//! fuzz targets and the perf gate at their next run — this catches it in
//! the ordinary suite instead.

use std::sync::Arc;

use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use rdlt_engine::fuzzing;

#[test]
fn parse_and_shred_slabs_accept_valid_and_garbage_input() {
    for input in [
        &br#"{"id":1,"tags":[{"k":"a"}]}"#[..],
        &br#"[{"id":1},{"id":2}]"#[..],
        &b"42"[..],
        &b"not json at all"[..],
        &b"\xff\xfe\x00"[..],
        &b""[..],
    ] {
        fuzzing::parse_slab(input);
        fuzzing::shred_slab(input);
    }
}

#[test]
fn arrow_type_mapping_never_panics_across_the_catalogue() {
    for dt in [
        DataType::Int64,
        DataType::Utf8,
        DataType::Boolean,
        DataType::Float64,
        DataType::Binary,
        DataType::Date32,
        DataType::Duration(arrow::datatypes::TimeUnit::Nanosecond),
        DataType::FixedSizeBinary(16),
        DataType::Struct(arrow::datatypes::Fields::empty()),
    ] {
        fuzzing::map_arrow_type(&dt);
    }
}

#[test]
fn bench_entry_points_count_rows_exactly() {
    let rows = fuzzing::bench_shred_bytes(
        br#"{"id":1}
{"id":2}
{"id":3}
"#,
    );
    assert_eq!(rows, 3, "tape shred emits one row per NDJSON line");

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
    )
    .expect("batch");
    assert_eq!(
        fuzzing::bench_passthrough(&batch),
        4,
        "passthrough emits the batch's rows"
    );
}
