//! The recorded ingestion session: where the two paths cross, and whether
//! either degrades with scale.
//!
//! Two questions, and only one of them is about speed.
//!
//! **Where does COPY start beating INSERT?** The shipped rule — a configured
//! bucket means the bulk path — is a decision, and a decision about which of
//! two mechanisms to use deserves the row count at which the answer flips.
//! That crossover sits at the SMALL end, where one PUT plus one COPY costs
//! more than one INSERT statement.
//!
//! **Does anything degrade non-linearly?** A sweep over small batches cannot
//! see a per-statement accumulation or a multi-part COPY misbehaving with
//! dozens of parts rather than two. That is a robustness question, and it is
//! why a larger run earns its warehouse time.
//!
//! `#[ignore]` by default: an instrument, not a gate. Numbers from a SaaS
//! destination carry network variance and cannot gate a build under the
//! benchmark governance — they are recorded, never barred.
//!
//! ```text
//! cargo nextest run -p rdlt-connector-snowflake --test ingestion_session \
//!   --run-ignored all --no-capture
//! ```

use rdlt_connector::StreamSpec;
use rdlt_connector_snowflake::dest::{S3Stage, Snowflake, SnowflakeConfig, Stage};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::memory::{MemoryBatch, MemorySource, MemoryStream};
use rdlt_testkit::snowflake::{credentials, scratch_schema, stage_credentials};
use serde_json::json;

/// Rows the engine hands over at a time. Large enough that the connector's own
/// byte-budget packing is what decides statement size, rather than the source
/// dribbling rows in.
const ROWS_PER_BATCH: usize = 10_000;

fn config_in(schema: &str, staged: bool) -> Option<SnowflakeConfig> {
    let creds = credentials()?;
    let mut doc = json!({
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
    });
    if staged {
        let stage = stage_credentials()?;
        let mut s3 = S3Stage::new(stage.bucket, stage.access_key, stage.secret_key);
        s3.region = stage.region;
        s3.endpoint = stage.endpoint;
        s3.prefix = stage.prefix;
        doc["stage"] = serde_json::to_value(Stage::s3(s3)).expect("the stage serializes");
    }
    Some(SnowflakeConfig::from_value(doc).expect("valid config"))
}

/// A bench-shaped row: a dozen columns of mixed width, so a statement grows the
/// way real data makes it grow rather than the way one integer column does.
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

/// One load of `total` rows through `staged` or not; returns seconds elapsed.
async fn timed_load(label: &str, total: i64, staged: bool) -> Option<f64> {
    let admin = config_in("PUBLIC", false)?;
    let schema = scratch_schema(label);
    let config = config_in(&schema, staged)?;
    let dest = Snowflake::new(config.clone()).expect("valid config");

    let batches: Vec<MemoryBatch> = (0..total)
        .step_by(ROWS_PER_BATCH)
        .map(|from| {
            let count = (ROWS_PER_BATCH as i64).min(total - from);
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
async fn record_the_crossover_and_the_scaling_behaviour() {
    if credentials().is_none() {
        return;
    }
    let has_bucket = stage_credentials().is_some();

    println!("\n=== INSERT vs COPY: where the answer flips ===");
    println!(
        "  {:>9}  {:>10}  {:>10}  {:>8}",
        "rows", "insert s", "copy s", "winner"
    );
    let mut crossover: Option<i64> = None;
    for total in [100_i64, 250, 500, 1_000, 2_500, 5_000, 10_000] {
        let insert = timed_load("xinsert", total, false).await;
        let copy = if has_bucket {
            timed_load("xcopy", total, true).await
        } else {
            None
        };
        match (insert, copy) {
            (Some(i), Some(c)) => {
                let winner = if c < i { "COPY" } else { "INSERT" };
                if c < i && crossover.is_none() {
                    crossover = Some(total);
                }
                println!("  {total:>9}  {i:>10.2}  {c:>10.2}  {winner:>8}");
            }
            (Some(i), None) => println!("  {total:>9}  {i:>10.2}  {:>10}  {:>8}", "-", "INSERT"),
            _ => {}
        }
    }
    match crossover {
        Some(rows) => println!("  → COPY first wins at {rows} rows"),
        None => println!("  → INSERT won at every size measured"),
    }

    // The scaling question. A per-statement accumulation, or a multi-part COPY
    // that misbehaves with dozens of parts rather than two, is invisible below
    // this size — and is a correctness signal rather than a speed one.
    println!("\n=== scaling ===");
    for (total, staged) in [(250_000_i64, false), (250_000, true), (1_000_000, true)] {
        if staged && !has_bucket {
            continue;
        }
        let path = if staged { "copy" } else { "insert" };
        let Some(elapsed) = timed_load("scale", total, staged).await else {
            continue;
        };
        println!(
            "  {total:>9} rows via {path:<6}: {elapsed:>8.2}s  {:>9.0} rows/s",
            total as f64 / elapsed
        );
    }
    println!();
}
