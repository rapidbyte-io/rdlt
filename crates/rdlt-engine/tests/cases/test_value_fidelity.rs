//! Values reach the destination, are refused, or are counted — never silently
//! replaced with NULL.
//!
//! The shredder's rule is "counted, never silent". These pins cover the four
//! places it was not true: an unsigned integer too large for the type inference
//! chooses, a declared type hint dropped by a non-scalar value, a decimal beyond
//! its declared precision, and a hint the engine cannot honour reaching a code
//! path that panics.

use rdlt_connector::StreamSpec;
use rdlt_core::LogicalType;
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryBatch, MemoryDestination, MemorySource, MemoryStream};
use serde_json::json;

fn discarded_values(report: &rdlt_core::RunReport) -> u64 {
    report.tables.values().map(|t| t.discarded_values).sum()
}

fn one_batch(spec: StreamSpec, rows: Vec<serde_json::Value>) -> MemorySource {
    MemorySource::new(vec![MemoryStream::new(
        spec,
        vec![MemoryBatch::new(rows).with_checkpoint(1)],
    )])
}

/// A `u64` above `i64::MAX` has no exact `Int64` or `Float64` representation, so
/// the column resolves to text and the digits survive. Before this was pinned the
/// column stayed `Int64` and every such value was written as NULL — no error, no
/// discard count, a successful run, and the data gone.
#[tokio::test]
async fn unsigned_integers_beyond_i64_survive_as_text() {
    let dest = MemoryDestination::new();
    let source = one_batch(
        StreamSpec::new("t"),
        vec![
            json!({ "id": 1, "big": 18_446_744_073_709_551_615u64 }),
            json!({ "id": 2, "big": 9_223_372_036_854_775_807i64 }),
        ],
    );
    let report = Engine::new(EngineConfig::new("uint"), source, dest.clone())
        .run()
        .await
        .expect("run succeeds");
    assert_eq!(report.total_rows(), 2);

    let rows = dest.committed_rows("t");
    assert_eq!(
        rows[0]["big"],
        json!("18446744073709551615"),
        "a u64 above i64::MAX must arrive intact, not as NULL"
    );
    assert_eq!(
        rows[1]["big"],
        json!("9223372036854775807"),
        "the in-range value shares the column's resolved type"
    );
}

/// Arrival order must not decide the resolved type: the same two values in the
/// other order resolve the same way. A range-conditional rule would break this.
#[tokio::test]
async fn unsigned_widening_is_order_independent() {
    let dest = MemoryDestination::new();
    let source = one_batch(
        StreamSpec::new("t"),
        vec![
            json!({ "id": 1, "big": 1i64 }),
            json!({ "id": 2, "big": 18_446_744_073_709_551_615u64 }),
        ],
    );
    Engine::new(EngineConfig::new("uint-order"), source, dest.clone())
        .run()
        .await
        .expect("run succeeds");

    let rows = dest.committed_rows("t");
    assert_eq!(rows[0]["big"], json!("1"));
    assert_eq!(rows[1]["big"], json!("18446744073709551615"));
}

/// A declared hint pins the column's type. An object arriving in that column is a
/// value that does not fit the pin — it is nulled and counted — but it must NOT
/// silently un-pin the column. Before this was pinned the column widened to Json
/// and stored the subtree verbatim, quietly overriding the user's declaration.
#[tokio::test]
async fn a_type_hint_is_not_dropped_by_an_object_value() {
    let dest = MemoryDestination::new();
    let source = one_batch(
        StreamSpec::new("t").with_type_hint("payload", LogicalType::Utf8),
        vec![
            json!({ "id": 1, "payload": "plain" }),
            json!({ "id": 2, "payload": { "nested": 1 } }),
        ],
    );
    let report = Engine::new(EngineConfig::new("hint"), source, dest.clone())
        .run()
        .await
        .expect("run succeeds");

    let schema = dest.schema("t").expect("schema");
    let column = schema.column("payload").expect("hinted column exists");
    assert_eq!(
        column.column_type,
        rdlt_core::ColumnType::scalar(LogicalType::Utf8),
        "the hint governs the column type even after an object is observed"
    );

    let rows = dest.committed_rows("t");
    assert_eq!(rows[0]["payload"], json!("plain"));
    assert_eq!(
        rows[1]["payload"],
        json!(null),
        "a value that cannot be represented under the hint is nulled, not stored verbatim"
    );
    assert!(
        discarded_values(&report) > 0,
        "and it is counted — the crate's rule is `counted, never silent`"
    );
}

/// A decimal wider than its declared precision cannot be stored at that
/// precision, so it is refused by the same rule that already refuses values
/// needing more scale. Before this was pinned the raw value was written into the
/// array unvalidated and surfaced much later as a destination-side overflow.
///
/// Reachable only from an embedding application: no shipped connector can place
/// `LogicalType::Decimal` in `type_hints`. The evidence is therefore SYNTHETIC.
#[tokio::test]
async fn a_decimal_beyond_its_declared_precision_is_refused() {
    let dest = MemoryDestination::new();
    let source = one_batch(
        StreamSpec::new("t").with_type_hint(
            "amount",
            LogicalType::Decimal {
                precision: 5,
                scale: 2,
            },
        ),
        vec![
            json!({ "id": 1, "amount": "12.34" }),
            json!({ "id": 2, "amount": "1234567.89" }),
        ],
    );
    let report = Engine::new(EngineConfig::new("decimal"), source, dest.clone())
        .run()
        .await
        .expect("run succeeds");

    let rows = dest.committed_rows("t");
    assert_eq!(rows[0]["amount"], json!(12.34), "the fitting value lands");
    assert_eq!(
        rows[1]["amount"],
        json!(null),
        "a value needing more than the declared precision is refused, never stored out of range"
    );
    assert!(discarded_values(&report) > 0, "and the refusal is counted");
}

/// A hint the engine cannot honour is a configuration error, named at the column,
/// before any row is read. Before this was pinned it reached the batch builder and
/// panicked there — `Decimal { precision: 200 }` exceeds what the value type can
/// represent, and the lattice that justifies the builder's `expect` is bypassed by
/// hints entirely.
///
/// Embedder-reachable only; the evidence is SYNTHETIC for the same reason as above.
#[tokio::test]
async fn an_unrepresentable_type_hint_is_a_typed_config_error() {
    let dest = MemoryDestination::new();
    let source = one_batch(
        StreamSpec::new("t").with_type_hint(
            "amount",
            LogicalType::Decimal {
                precision: 200,
                scale: 0,
            },
        ),
        vec![json!({ "id": 1, "amount": "1" })],
    );
    let error = Engine::new(EngineConfig::new("bad-hint"), source, dest)
        .run()
        .await
        .expect_err("an unrepresentable hint must not be accepted");

    assert!(
        matches!(error, rdlt_core::RdltError::Config { .. }),
        "an unhonourable hint is a configuration error, not a panic or an internal error: {error:?}"
    );
}
