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
use rdlt_connector_postgres::dest::Postgres;
use rdlt_engine::{Engine, EngineConfig};
use rdlt_connector_postgres::fixtures::PgFixture;
use rdlt_testkit::{MemoryBatch, MemorySource, MemoryStream};
use serde_json::json;

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
    config = config.with_workdir(workdir.to_path_buf());
    config = config.with_write_mode(mode.clone());
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
    let mut registry: Vec<&str> = rdlt_connector_postgres::dest::FAIL_POINTS.to_vec();
    registry.sort_unstable();
    let mut expected = vec![
        "pg.unit.begin",
        "pg.target.clear",
        "pg.unit.write",
        "pg.publish.begin",
        "pg.tx.commit",
        "pg.tx.acked",
    ];
    expected.sort_unstable();
    assert_eq!(registry, expected, "update BOTH the const and this list");
}

/// Which registered points a write mode can actually reach.
///
/// `pg.target.clear` brackets the TRUNCATE that precedes a Replace target's
/// first direct write. Append never clears, and Merge does not write the
/// target at all — it stages and publishes through merge arms — so neither can
/// reach it. Every other point sits on the unit path all three modes share.
///
/// Encoding this keeps the anti-vacuousness pins honest in both directions: a
/// dead `crash_point!` site still fails the pin, and a point is not demanded
/// from a mode whose code path cannot contain it.
fn reachable(mode: &str) -> Vec<&'static str> {
    rdlt_connector_postgres::dest::FAIL_POINTS
        .iter()
        .copied()
        .filter(|point| *point != "pg.target.clear" || mode == "replace")
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn sweep_postgres_destination() {
    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();

    let mut fired: std::collections::BTreeSet<(&str, &str)> = std::collections::BTreeSet::new();
    for &point in rdlt_connector_postgres::dest::FAIL_POINTS {
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
    let expected: std::collections::BTreeSet<(&str, &str)> = ["append", "replace", "merge"]
        .into_iter()
        .flat_map(|mode| reachable(mode).into_iter().map(move |p| (p, mode)))
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
            StreamSpec::new("s")
                .with_structured()
                .with_primary_key(["id"]),
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

fn with_strategy(
    dest: Postgres,
    strategy: rdlt_connector_postgres::dest::MergeStrategy,
) -> Postgres {
    dest.options(rdlt_connector_postgres::dest::DestOptions {
        merge_strategy: Some(strategy),
        ..Default::default()
    })
    .expect("valid options")
}

async fn attempt_keyed(workdir: &std::path::Path, dest: &Postgres) -> Result<(), String> {
    let mut config = EngineConfig::new("pg-sweep-keyed");
    config = config.with_workdir(workdir.to_path_buf());
    config = config.with_write_mode(WriteMode::Merge {
        key: vec!["id".into()],
    });
    let engine = Engine::new(config, KeyedArrowSource, dest.clone());
    match tokio::spawn(engine.run()).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(join) => Err(format!("panicked: {join}")),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sweep_postgres_destination_keyed_structured_merge() {
    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();

    // Feature 008 T008: BOTH strategies cross every boundary (M2) — the
    // upsert arm's conflict-update runs inside the same publish transaction.
    let strategies = [
        (
            "di",
            rdlt_connector_postgres::dest::MergeStrategy::DeleteInsert,
        ),
        ("up", rdlt_connector_postgres::dest::MergeStrategy::Upsert),
    ];
    let mut fired: std::collections::BTreeSet<(&str, &str)> = std::collections::BTreeSet::new();
    for &point in rdlt_connector_postgres::dest::FAIL_POINTS {
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
    let expected: std::collections::BTreeSet<(&str, &str)> = ["di", "up"]
        .into_iter()
        .flat_map(|arm| reachable("merge").into_iter().map(move |p| (p, arm)))
        .collect();
    assert_eq!(
        fired, expected,
        "keyed-merge armed-fire pin diverged — a missing (point, strategy) means \
         that arm never crossed that boundary"
    );
}

// ---- Feature 010 T007: the refined-merge arm (dedup_sort + merge_scope)
// crosses every registered boundary — receipts must survive crash/replay
// without double-deleting a scope (contract merge-refinements.md MR5). ----

/// Keyed structured stream with scope + ordering columns: ONE checkpointed
/// unit (the MR5 single-unit rule), 100 ids each delivered TWICE (stale
/// seq=1 first, surviving seq=2 second — wrong-arrival order is exercised
/// under every crash), `day = id % 3` as the scope.
struct RefinedArrowSource;

#[async_trait]
impl Source for RefinedArrowSource {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("refined-arrow-sweep", "0.0.0")
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        Ok(vec![
            StreamSpec::new("s")
                .with_structured()
                .with_primary_key(["id"]),
        ])
    }

    async fn read(&self, mut req: ReadRequest) -> Result<(), SourceError> {
        if req.since.is_some() {
            return Ok(()); // the single unit committed — nothing to resume
        }
        {
            let ids: Vec<i64> = (0..100).flat_map(|id| [id as i64, id as i64]).collect();
            let days: Vec<i64> = ids.iter().map(|id| id % 3).collect();
            let seqs: Vec<i64> = (0..ids.len() as i64).map(|n| 1 + (n % 2)).collect();
            let names: Vec<String> = ids
                .iter()
                .zip(&seqs)
                .map(|(id, seq)| {
                    if *seq == 2 {
                        format!("row-{id}")
                    } else {
                        "stale".to_string()
                    }
                })
                .collect();
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("day", DataType::Int64, true),
                    Field::new("seq", DataType::Int64, true),
                    Field::new("name", DataType::Utf8, true),
                ])),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(Int64Array::from(days)),
                    Arc::new(Int64Array::from(seqs)),
                    Arc::new(StringArray::from(
                        names.iter().map(String::as_str).collect::<Vec<_>>(),
                    )),
                ],
            )
            .expect("batch");
            if req.out.arrow(batch).await.is_err() {
                return Ok(());
            }
            if req.out.checkpoint(Cursor::new(1u64)).await.is_err() {
                return Ok(());
            }
        }
        Ok(())
    }
}

async fn attempt_refined(workdir: &std::path::Path, dest: &Postgres) -> Result<(), String> {
    let mut config = EngineConfig::new("pg-sweep-refined");
    config = config.with_workdir(workdir.to_path_buf());
    config = config.with_write_mode(WriteMode::Merge {
        key: vec!["id".into()],
    });
    let engine = Engine::new(config, RefinedArrowSource, dest.clone());
    match tokio::spawn(engine.run()).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(join) => Err(format!("panicked: {join}")),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn sweep_postgres_destination_refined_merge() {
    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    let (client, connection) = tokio_postgres::connect(&conn, tokio_postgres::NoTls)
        .await
        .expect("probe connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let mut fired: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
    for &point in rdlt_connector_postgres::dest::FAIL_POINTS {
        for action in ["return", "panic", "1*off->return"] {
            let dataset = format!(
                "sweepr_{}_{}",
                point.replace('.', "_"),
                action.replace(['*', '-', '>'], "_"),
            );
            let dir = tempfile::tempdir().expect("tempdir");
            let workdir = dir.path().join("wal");
            let dest = Postgres::connect(&conn)
                .dataset(&dataset)
                .options(rdlt_connector_postgres::dest::DestOptions {
                    tables: [(
                        "s".to_string(),
                        rdlt_connector_postgres::dest::TableOptions {
                            dedup_sort: Some(rdlt_connector_postgres::dest::DedupSort {
                                column: "seq".into(),
                                order: rdlt_connector_postgres::dest::SortOrder::Desc,
                            }),
                            merge_scope: Some(vec!["day".into()]),
                            ..Default::default()
                        },
                    )]
                    .into_iter()
                    .collect(),
                    ..Default::default()
                })
                .expect("valid options");

            fail::cfg(point, action).expect("configure fail point");
            let armed1 = attempt_refined(&workdir, &dest).await;
            let armed2 = attempt_refined(&workdir, &dest).await;
            fail::remove(point);
            if armed1.is_err() || armed2.is_err() {
                fired.insert(point);
            }

            let recovered = attempt_refined(&workdir, &dest).await;
            assert!(
                recovered.is_ok(),
                "[{point} / {action} / refined] recovery failed: {recovered:?}"
            );
            // Exactly-once under scope replacement + ordered dedup: every id
            // once, no stale survivor — a replayed or resumed load never
            // half-replaces a scope (MR5 single-unit rule).
            let counts = client
                .query_one(
                    &format!(
                        "SELECT count(*), count(DISTINCT id),
                                count(*) FILTER (WHERE name = 'stale')
                         FROM \"{dataset}\".s"
                    ),
                    &[],
                )
                .await
                .expect("probe");
            assert_eq!(
                (
                    counts.get::<_, i64>(0),
                    counts.get::<_, i64>(1),
                    counts.get::<_, i64>(2)
                ),
                (100, 100, 0),
                "[{point} / {action} / refined] exactly-once or survivor violated"
            );
        }
    }
    let expected: std::collections::BTreeSet<&str> = reachable("merge").into_iter().collect();
    assert_eq!(
        fired, expected,
        "refined-merge armed-fire pin diverged — a missing point means the \
         refined arm never crossed that boundary"
    );
}

/// The registry names exactly the crash points armed in this crate's sources.
///
/// The sweep's own `fired == registry` check cannot establish this: it compares
/// the registry against itself, so deleting a point from BOTH the code and the
/// list leaves it true while the matrix quietly shrinks. This reads the sources
/// instead, which is the only way a dropped point becomes visible.
///
/// THREE registries over one source tree, checked against their union.
///
/// This crate is why the check has two directions rather than set equality:
/// three of its points are armed INDIRECTLY, with the name supplied by a
/// constructor rather than written beside the macro. A set-equality scanner
/// reports those three as missing, and the plausible reading is "the registry is
/// too big" — shrinking it, and removing points from the sweep while every
/// assertion passes.
#[test]
fn the_registry_matches_the_sources() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    rdlt_testkit::assert_registry_is_armed(
        &src,
        &[
            rdlt_connector_postgres::dest::FAIL_POINTS,
            rdlt_connector_postgres::source::FAIL_POINTS,
            rdlt_connector_postgres::source::CDC_FAIL_POINTS,
        ],
    );
}
