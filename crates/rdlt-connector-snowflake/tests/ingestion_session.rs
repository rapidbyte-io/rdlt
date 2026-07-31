//! The recorded ingestion session: what the single path costs, against what
//! the two it replaced cost.
//!
//! The claim this feature rests on is that uploading to the service's own
//! storage supersedes both removed paths. The paths that were deleted carry
//! recorded numbers, so replacing them on assertion would be a regression in
//! method for a project whose previous feature overturned its own expectation
//! by measuring.
//!
//! The row shape is 022's, byte for byte, and must not be "improved": the
//! recorded figures refer to it, and changing it destroys the comparison
//! rather than refining it.
//!
//! `#[ignore]` by default: an INSTRUMENT, not a gate. Numbers from a hosted
//! service carry network variance and cannot gate a build under the benchmark
//! governance — they are recorded, never barred.
//!
//! ```text
//! cargo nextest run -p rdlt-connector-snowflake --test ingestion_session \
//!   --run-ignored all --no-capture
//! ```

use rdlt_connector::StreamSpec;
use rdlt_connector_snowflake::dest::{Snowflake, SnowflakeConfig};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::memory::{MemoryBatch, MemorySource, MemoryStream};
mod common;

use common::{credentials, scratch_schema};
use serde_json::json;

/// What 022 recorded, on this exact row shape, for the two removed paths.
const RECORDED_INSERT_250K: f64 = 582.0;
const RECORDED_COPY_250K: f64 = 2_191.0;
const RECORDED_COPY_1M: f64 = 1_941.0;

/// Rows the engine hands over at a time — unchanged from 022, and load-bearing
/// rather than incidental: the connector writes ONE part per delivered batch,
/// so this is what decides how many parts a load produces.
const ROWS_PER_BATCH: usize = 10_000;

fn config_in(schema: &str) -> Option<SnowflakeConfig> {
    let creds = credentials()?;
    Some(
        SnowflakeConfig::from_value(json!({
            "account": creds.account,
            "user": creds.user,
            "database": creds.database,
            "schema": schema,
            "warehouse": creds.warehouse,
            "role": creds.role,
            "auth": {"key_pair": {
                "private_key": creds.private_key_path,
                "passphrase": creds.passphrase,
            }},
        }))
        .expect("valid config"),
    )
}

/// 022's bench row: a dozen columns of mixed width, so a part grows the way
/// real data makes it grow rather than the way one integer column does.
fn rows(from: i64, count: i64) -> Vec<serde_json::Value> {
    (from..from + count)
        .map(|id| {
            json!({
                "id": id,
                "name": format!("customer-{id}"),
                "email": format!("user{id}@example.invalid"),
                "region": if id % 3 == 0 { "emea" } else { "amer" },
                "amount": (id % 997) as f64 / 7.0,
                "quantity": id % 13,
                "active": id % 2 == 0,
                "note": "a description of moderate length, as free text tends to be",
                "sku": format!("SKU-{:08}", id % 50_000),
                "channel": "web",
                "source_system": "oracle-erp",
                "batch_ref": format!("batch-{}", id / 1000),
            })
        })
        .collect()
}

/// One load of `total` rows; returns seconds elapsed.
async fn timed_load(label: &str, total: i64, rows_per_batch: usize) -> Option<f64> {
    let admin = config_in("PUBLIC")?;
    let schema = scratch_schema(label);
    let config = config_in(&schema)?;
    let dest = Snowflake::new(config.clone()).expect("valid config");

    let batches: Vec<MemoryBatch> = (0..total)
        .step_by(rows_per_batch)
        .map(|from| {
            let count = (rows_per_batch as i64).min(total - from);
            MemoryBatch::new(rows(from, count)).with_checkpoint(from)
        })
        .collect();
    let source = MemorySource::new(vec![MemoryStream::new(StreamSpec::new("events"), batches)]);

    let started = std::time::Instant::now();
    let report = Engine::new(EngineConfig::new("sf-session"), source, dest)
        .run()
        .await
        .unwrap_or_else(|e| panic!("{label} must settle: {e:?}"));
    let elapsed = started.elapsed().as_secs_f64();
    assert_eq!(report.total_rows(), total as u64, "every row must land");

    let _ = rdlt_connector_snowflake::dest::testhook::apply(
        &admin,
        &format!(
            "DROP SCHEMA IF EXISTS \"{}\".\"{}\"",
            admin.database.to_uppercase(),
            schema
        ),
    )
    .await;
    Some(elapsed)
}

#[tokio::test]
#[ignore = "measurement instrument: costs warehouse time, gates nothing"]
async fn record_the_single_path_against_what_it_replaced() {
    if credentials().is_none() {
        return;
    }

    println!("\n=== the single path, against 022's recorded figures ===");
    println!("  row shape: 022's 12-column bench row, unchanged");
    println!(
        "  {:>9}  {:>8}  {:>10}  {:>26}",
        "rows", "wall s", "rows/s", "022 recorded"
    );

    for (total, recorded_insert, recorded_copy) in [
        (
            250_000_i64,
            Some(RECORDED_INSERT_250K),
            Some(RECORDED_COPY_250K),
        ),
        (1_000_000, None, Some(RECORDED_COPY_1M)),
    ] {
        let Some(elapsed) = timed_load("session", total, ROWS_PER_BATCH).await else {
            return;
        };
        let rate = total as f64 / elapsed;
        let against = match (recorded_insert, recorded_copy) {
            (Some(i), Some(c)) => format!("insert {i:.0} / bucket {c:.0}"),
            (None, Some(c)) => format!("bucket {c:.0}"),
            _ => String::new(),
        };
        println!("  {total:>9}  {elapsed:>8.2}  {rate:>10.0}  {against:>26}");
    }

    // The open question 022 could not answer: its bucket path ran 11% slower
    // per row at 1M than at 250k, on ONE run of each, which cannot separate a
    // multi-part effect from ordinary variance. Repeating the smaller size
    // gives a spread to judge that 11% against — if the spread is wider than
    // the gap, the gap was never evidence of anything.
    println!("\n=== repeat runs at 250k, to size the variance ===");
    let mut rates = Vec::new();
    for run in 1..=3 {
        let Some(elapsed) = timed_load("variance", 250_000, ROWS_PER_BATCH).await else {
            return;
        };
        let rate = 250_000.0 / elapsed;
        rates.push(rate);
        println!("  run {run}: {elapsed:>8.2}s  {rate:>9.0} rows/s");
    }
    let (lo, hi) = (
        rates.iter().cloned().fold(f64::MAX, f64::min),
        rates.iter().cloned().fold(0.0, f64::max),
    );
    println!(
        "  spread across identical runs: {:.1}%  (022's unexplained gap was 11%)\n",
        (hi - lo) / lo * 100.0
    );
}
