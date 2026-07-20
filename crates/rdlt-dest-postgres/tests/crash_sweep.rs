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
use rdlt_dest_postgres::Postgres;
use rdlt_engine::{Engine, EngineConfig};
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
    let mut registry: Vec<&str> = rdlt_dest_postgres::FAIL_POINTS.to_vec();
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
    for &point in rdlt_dest_postgres::FAIL_POINTS {
        for action in ["return", "panic", "1*off->return"] {
            for mode in [WriteMode::Append, WriteMode::Replace] {
                // Fresh dataset per cell isolates state on the one container.
                let dataset = format!(
                    "sweep_{}_{}_{}",
                    point.replace('.', "_"),
                    action,
                    match mode {
                        WriteMode::Append => "append",
                        _ => "replace",
                    }
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
                    fired.insert((
                        point,
                        match mode {
                            WriteMode::Append => "append",
                            _ => "replace",
                        },
                    ));
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
    // failed at least one armed attempt in BOTH modes — a dead crash_point!
    // site fails here instead of passing silently.
    let expected: std::collections::BTreeSet<(&str, &str)> = rdlt_dest_postgres::FAIL_POINTS
        .iter()
        .flat_map(|&p| [(p, "append"), (p, "replace")])
        .collect();
    assert_eq!(
        fired, expected,
        "armed-fire pin diverged — a missing entry means a crash_point! site went dead"
    );
}
