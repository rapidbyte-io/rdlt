//! Postgres crash-point sweep (feature 003 gate G2.1): the ENGINE-OWNED
//! protocol boundaries against a real Postgres — stage COPY, the publish
//! transaction edges, the D3 redelivery window. The DB's internal transaction
//! atomicity is its own guarantee (research R20 scope guard).
//!
//! Needs a container runtime; SKIPS cleanly without one (per-PR CI), and the
//! scheduled deep job always provides one. Run with `--features failpoints`.

#![cfg(feature = "failpoints")]

use rdlt_connector::StreamSpec;
use rdlt_connector::core::WriteMode;
use rdlt_connector::core::failpoint::fail;
use rdlt_engine::{Engine, EngineConfig};
use rdlt_postgres::dest::Postgres;
use rdlt_testkit::{MemoryBatch, MemorySource, MemoryStream};
use serde_json::json;
use testcontainers_modules::postgres::Postgres as PostgresImage;
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const TOTAL_ROWS: u64 = 100;

/// Same multi-commit shape as the engine sweep: 4 checkpointed batches × 25
/// rows under the default EveryCheckpoints(1) policy.
fn source() -> MemorySource {
    let batches = (0..4)
        .map(|b| {
            MemoryBatch::new(
                (0..25)
                    .map(|i| json!({"id": b * 25 + i, "name": format!("row-{b}-{i}")}))
                    .collect(),
            )
            .with_checkpoint(json!({"batch": b}))
        })
        .collect();
    MemorySource::new(vec![MemoryStream::new(StreamSpec::new("s"), batches)])
}

async fn count_rows(conn: &str, dataset: &str) -> u64 {
    let (client, connection) = tokio_postgres::connect(conn, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    match client
        .query_one(&format!("SELECT count(*) FROM \"{dataset}\".\"s\""), &[])
        .await
    {
        Ok(row) => row.get::<_, i64>(0) as u64,
        Err(_) => 0,
    }
}

async fn attempt(
    workdir: &std::path::Path,
    dest: &Postgres,
    mode: &WriteMode,
) -> Result<(), String> {
    let mut config = EngineConfig::new("pg-sweep");
    config.workdir = Some(workdir.to_path_buf());
    config.write_mode = mode.clone();
    let engine = Engine::new(config, source(), dest.clone());
    match tokio::spawn(engine.run()).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(join) => Err(format!("panicked: {join}")),
    }
}

/// Registry discipline (G2.2, destination half): the crate's exported list is
/// pinned here, and the sweep below iterates exactly it.
#[test]
fn registry_is_pinned() {
    let mut registry: Vec<&str> = rdlt_postgres::dest::FAIL_POINTS.to_vec();
    registry.sort_unstable();
    let mut expected = vec!["pg.stage.copy", "pg.publish.begin", "pg.tx.commit"];
    expected.sort_unstable();
    assert_eq!(registry, expected, "update BOTH the const and this list");
}

#[tokio::test(flavor = "multi_thread")]
async fn sweep_postgres_destination() {
    let Ok(container) = PostgresImage::default().with_tag("16-alpine").start().await else {
        eprintln!("skipping postgres sweep: no container runtime available");
        return;
    };
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("mapped port");
    let conn =
        format!("host=127.0.0.1 port={port} user=postgres password=postgres dbname=postgres");

    let mut fired: std::collections::BTreeSet<(&str, &str)> = std::collections::BTreeSet::new();
    for &point in rdlt_postgres::dest::FAIL_POINTS {
        for action in ["return", "panic", "1*off->return"] {
            for mode in [
                WriteMode::Append,
                WriteMode::Replace,
                // Shredded identity merge (feature 006 sweep extension): the
                // dedup DELETE+INSERT arm under the same protocol edges.
                WriteMode::Merge {
                    key: vec!["id".into()],
                },
            ] {
                // Fresh dataset per cell isolates state on the one container.
                let mode_label = match &mode {
                    WriteMode::Append => "append",
                    WriteMode::Replace => "replace",
                    _ => "merge",
                };
                let dataset = format!(
                    "sweep_{}_{}_{}",
                    point.replace('.', "_"),
                    action.replace(['*', '-', '>'], "_"),
                    mode_label
                );
                let dir = tempfile::tempdir().expect("tempdir");
                let workdir = dir.path().join("wal");
                let dest = Postgres::connect(&conn).dataset(&dataset);

                fail::cfg(point, action).expect("configure fail point");
                let armed1 = attempt(&workdir, &dest, &mode).await;
                // Second run still armed: a crash during recovery itself.
                let armed2 = attempt(&workdir, &dest, &mode).await;
                fail::remove(point);
                if armed1.is_err() || armed2.is_err() {
                    fired.insert((point, mode_label));
                }

                let recovered = attempt(&workdir, &dest, &mode).await;
                assert!(
                    recovered.is_ok(),
                    "[{point} / {action} / {mode:?}] recovery failed: {recovered:?}"
                );
                assert_eq!(
                    count_rows(&conn, &dataset).await,
                    TOTAL_ROWS,
                    "[{point} / {action} / {mode:?}] exactly-once violated"
                );
            }
        }
    }
    // Anti-vacuousness pin (005 review): every registered point must have
    // failed at least one armed attempt in ALL THREE modes — a dead crash_point!
    // site fails here instead of passing silently.
    let expected: std::collections::BTreeSet<(&str, &str)> = rdlt_postgres::dest::FAIL_POINTS
        .iter()
        .flat_map(|&p| [(p, "append"), (p, "replace"), (p, "merge")])
        .collect();
    assert_eq!(
        fired, expected,
        "armed-fire pin diverged — a missing entry means a crash_point! site went dead"
    );
}

// ---- Review F11: the KEYED structured-merge arm under the destination's own
// fail points (contract merge-structured.md conformance) — the shredded
// MemorySource cells above exercise only the identity-merge branch. ----

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use rdlt_connector::{ConnectorSpec, Cursor, ReadRequest, Source, SourceError};
use std::sync::Arc;

/// Structured stream with a declared key: 4 checkpointed batches × 25 rows,
/// resumable by batch index — the keyed DELETE+INSERT path end to end.
struct KeyedArrowSource;

#[async_trait]
impl Source for KeyedArrowSource {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("keyed-arrow-sweep", "0.0.0")
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        Ok(vec![
            StreamSpec::new("s").structured().with_primary_key(["id"]),
        ])
    }

    async fn read(&self, mut req: ReadRequest) -> Result<(), SourceError> {
        let start = match &req.since {
            None => 0usize,
            Some(c) => c.as_value().as_u64().unwrap_or(0) as usize,
        };
        for b in start..4 {
            let ids: Vec<i64> = (0..25).map(|i| (b * 25 + i) as i64).collect();
            let names: Vec<String> = ids.iter().map(|i| format!("row-{i}")).collect();
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("name", DataType::Utf8, true),
                ])),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(StringArray::from(
                        names.iter().map(String::as_str).collect::<Vec<_>>(),
                    )),
                ],
            )
            .expect("batch");
            if req.out.arrow(batch).await.is_err() {
                return Ok(());
            }
            if req
                .out
                .checkpoint(Cursor::new((b + 1) as u64))
                .await
                .is_err()
            {
                return Ok(());
            }
        }
        Ok(())
    }
}

fn with_strategy(dest: Postgres, strategy: rdlt_postgres::dest::MergeStrategy) -> Postgres {
    dest.options(rdlt_postgres::dest::PgDestOptions {
        merge_strategy: strategy,
        ..Default::default()
    })
    .expect("valid options")
}

async fn attempt_keyed(workdir: &std::path::Path, dest: &Postgres) -> Result<(), String> {
    let mut config = EngineConfig::new("pg-sweep-keyed");
    config.workdir = Some(workdir.to_path_buf());
    config.write_mode = WriteMode::Merge {
        key: vec!["id".into()],
    };
    let engine = Engine::new(config, KeyedArrowSource, dest.clone());
    match tokio::spawn(engine.run()).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(join) => Err(format!("panicked: {join}")),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sweep_postgres_destination_keyed_structured_merge() {
    let Ok(container) = PostgresImage::default().with_tag("16-alpine").start().await else {
        eprintln!("skipping keyed merge sweep: no container runtime available");
        return;
    };
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("mapped port");
    let conn =
        format!("host=127.0.0.1 port={port} user=postgres password=postgres dbname=postgres");

    // Feature 008 T008: BOTH strategies cross every boundary (M2) — the
    // upsert arm's conflict-update runs inside the same publish transaction.
    let strategies = [
        ("di", rdlt_postgres::dest::MergeStrategy::DeleteInsert),
        ("up", rdlt_postgres::dest::MergeStrategy::Upsert),
    ];
    let mut fired: std::collections::BTreeSet<(&str, &str)> = std::collections::BTreeSet::new();
    for &point in rdlt_postgres::dest::FAIL_POINTS {
        for action in ["return", "panic", "1*off->return"] {
            for (label, strategy) in strategies {
                let dataset = format!(
                    "sweepk_{}_{}_{}",
                    point.replace('.', "_"),
                    action.replace(['*', '-', '>'], "_"),
                    label,
                );
                let dir = tempfile::tempdir().expect("tempdir");
                let workdir = dir.path().join("wal");
                let dest = with_strategy(Postgres::connect(&conn).dataset(&dataset), strategy);

                fail::cfg(point, action).expect("configure fail point");
                let armed1 = attempt_keyed(&workdir, &dest).await;
                let armed2 = attempt_keyed(&workdir, &dest).await;
                fail::remove(point);
                if armed1.is_err() || armed2.is_err() {
                    fired.insert((point, label));
                }

                let recovered = attempt_keyed(&workdir, &dest).await;
                assert!(
                    recovered.is_ok(),
                    "[{point} / {action} / keyed-{label}] recovery failed: {recovered:?}"
                );
                assert_eq!(
                    count_rows(&conn, &dataset).await,
                    TOTAL_ROWS,
                    "[{point} / {action} / keyed-{label}] exactly-once violated"
                );
            }
        }
    }
    // Anti-vacuousness: every registered point fires under BOTH strategy arms.
    let expected: std::collections::BTreeSet<(&str, &str)> = rdlt_postgres::dest::FAIL_POINTS
        .iter()
        .flat_map(|&p| [(p, "di"), (p, "up")])
        .collect();
    assert_eq!(
        fired, expected,
        "keyed-merge armed-fire pin diverged — a missing (point, strategy) means \
         that arm never crossed that boundary"
    );
}
