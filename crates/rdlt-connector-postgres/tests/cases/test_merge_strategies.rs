//! Merge strategies against a live server: delete_insert, upsert, scd2 and
//! hard_delete (contract merge-strategies.md).

use std::sync::Arc;

use arrow_array::{BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use rdlt_connector::{ConnectorSpec, Cursor, ReadRequest, Source, SourceError, StreamSpec};
use rdlt_connector_postgres::dest::{DestinationOptions, MergeStrategy, Postgres, TableOptions};
use rdlt_engine::{Engine, EngineConfig};

use rdlt_connector_postgres::fixtures::PgFixture;

/// Keyed structured stream with a bool `deleted` flag column.
struct FlaggedSource {
    batch: RecordBatch,
}

#[async_trait]
impl Source for FlaggedSource {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("flagged-test", "0.0.0")
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        Ok(vec![
            StreamSpec::new("events")
                .with_structured()
                .with_primary_key(["id"]),
        ])
    }

    async fn read(&self, mut req: ReadRequest) -> Result<(), SourceError> {
        let _ = req.out.arrow(self.batch.clone()).await;
        let _ = req.out.checkpoint(Cursor::new(1u64)).await;
        Ok(())
    }
}

fn batch(rows: &[(i64, &str, Option<bool>)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("deleted", DataType::Boolean, true),
        ])),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("batch")
}

fn upsert_dest(conn: &str, dataset: &str) -> Postgres {
    Postgres::connect(conn)
        .dataset(dataset)
        .options(DestinationOptions {
            merge_strategy: Some(MergeStrategy::Upsert),
            tables: [(
                "events".to_string(),
                TableOptions {
                    hard_delete: Some("deleted".into()),
                    ..TableOptions::default()
                },
            )]
            .into_iter()
            .collect(),
        })
        .expect("valid options")
}

async fn run_merge(dest: Postgres, rows: &[(i64, &str, Option<bool>)]) {
    let mut config = EngineConfig::new("strat");
    config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
        key: vec!["id".into()],
    });
    Engine::new(config, FlaggedSource { batch: batch(rows) }, dest)
        .run()
        .await
        .expect("merge run");
}

#[tokio::test(flavor = "multi_thread")]
async fn upsert_converges_and_hard_delete_removes_keys() {
    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();

    // Run 1: three live rows.
    run_merge(
        upsert_dest(&conn, "strat"),
        &[(1, "a", Some(false)), (2, "b", None), (3, "c", Some(false))],
    )
    .await;

    // Run 2: key 2 updates in place, key 4 is new, key 1 is
    // FLAGGED deleted, and key 9 is a never-loaded flagged key (no-op).
    let round2: &[(i64, &str, Option<bool>)] = &[
        (1, "gone", Some(true)),
        (2, "b2", None),
        (4, "d", Some(false)),
        (9, "ghost", Some(true)),
    ];
    run_merge(upsert_dest(&conn, "strat"), round2).await;

    let (client, connection) = tokio_postgres::connect(&conn, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let rows: Vec<(i64, String)> = client
        .query("SELECT id, name FROM strat.events ORDER BY id", &[])
        .await
        .expect("rows")
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();
    assert_eq!(
        rows,
        vec![
            (2, "b2".to_string()),
            (3, "c".to_string()),
            (4, "d".to_string())
        ],
        "updated in place, inserted new, deleted flagged, ghost no-op"
    );

    // Three further re-runs: totals never move (idempotent conflict-update).
    for _ in 0..3 {
        run_merge(upsert_dest(&conn, "strat"), round2).await;
        let n: i64 = client
            .query_one("SELECT count(*) FROM strat.events", &[])
            .await
            .expect("count")
            .get(0);
        assert_eq!(n, 3, "totals exactly stable");
    }

    // M3/M5: the unique index the strategy required exists.
    let indexes: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_indexes
             WHERE schemaname = 'strat' AND tablename = 'events'
               AND indexname LIKE 'rdlt_ux%'",
            &[],
        )
        .await
        .expect("indexes")
        .get(0);
    assert_eq!(indexes, 1, "unique index auto-ensured");
}

#[tokio::test(flavor = "multi_thread")]
async fn duplicate_keys_under_upsert_fail_typed_naming_the_key() {
    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    // A table that already violates key uniqueness.
    {
        let (client, connection) = tokio_postgres::connect(&conn, tokio_postgres::NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(
                "CREATE SCHEMA IF NOT EXISTS dup;
                 CREATE TABLE dup.events (
                     id BIGINT NOT NULL, name TEXT, deleted BOOLEAN);
                 INSERT INTO dup.events VALUES (1, 'x', NULL), (1, 'x-again', NULL);",
            )
            .await
            .expect("dup table");
    }
    let mut config = EngineConfig::new("dup");
    config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
        key: vec!["id".into()],
    });
    let err = Engine::new(
        config,
        FlaggedSource {
            batch: batch(&[(1, "a", None)]),
        },
        upsert_dest(&conn, "dup"),
    )
    .run()
    .await
    .expect_err("duplicate keys must block upsert");
    let msg = err.to_string();
    assert!(
        msg.contains("unique index") && msg.contains("id") && msg.contains("23505"),
        "M3 typed, names the key: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn shredded_upsert_is_rejected_typed_at_ensure() {
    // Review F4 / contract M7 (amended): a keyless shredded stream's
    // _rdlt_id is a content hash — conflict-update can never match an
    // updated row, so upsert on shredded streams is rejected outright.
    use rdlt_testkit::MemorySource;
    use serde_json::json;

    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    let dest = Postgres::connect(&conn)
        .dataset("shup")
        .options(DestinationOptions {
            merge_strategy: Some(MergeStrategy::Upsert),
            ..DestinationOptions::default()
        })
        .expect("options");
    let mut config = EngineConfig::new("shup");
    config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
        key: vec!["id".into()],
    });
    let source = MemorySource::single_stream(
        rdlt_connector::StreamSpec::new("users").with_primary_key(["id"]),
        vec![json!({"id": 1, "name": "ada"})],
    );
    let err = Engine::new(config, source, dest)
        .run()
        .await
        .expect_err("shredded upsert must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("KEYED structured") && msg.contains("delete_insert"),
        "{msg}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn flagged_then_recreated_root_keeps_its_subtree() {
    // Review F3: the hard-delete flag decision must come from the
    // DEDUPED last-wins row — a root flagged then re-created in the
    // SAME load keeps its row AND its children.
    use rdlt_testkit::MemorySource;
    use serde_json::json;

    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    let dest = Postgres::connect(&conn)
        .dataset("recreate")
        .options(DestinationOptions {
            tables: [(
                "users".to_string(),
                TableOptions {
                    hard_delete: Some("deleted".into()),
                    ..TableOptions::default()
                },
            )]
            .into_iter()
            .collect(),
            ..DestinationOptions::default()
        })
        .expect("options");
    let mut config = EngineConfig::new("recreate");
    config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
        key: vec!["id".into()],
    });
    // One load, same key twice: flagged first, re-created after (arrival
    // order matters — last wins).
    let source = MemorySource::single_stream(
        rdlt_connector::StreamSpec::new("users").with_primary_key(["id"]),
        vec![
            json!({"id": 1, "name": "ada", "deleted": true, "tags": []}),
            json!({"id": 1, "name": "ada-again", "deleted": null,
                   "tags": [{"label": "back"}]}),
        ],
    );
    Engine::new(config, source, dest).run().await.expect("run");

    let (client, connection) = tokio_postgres::connect(&conn, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let name: String = client
        .query_one("SELECT name FROM recreate.users WHERE id = 1", &[])
        .await
        .expect("root survives")
        .get(0);
    assert_eq!(name, "ada-again", "last-wins row survives the flag");
    let tags: i64 = client
        .query_one("SELECT count(*) FROM recreate.users__tags", &[])
        .await
        .expect("children")
        .get(0);
    assert_eq!(tags, 1, "the re-created root keeps its subtree");
}

#[tokio::test(flavor = "multi_thread")]
async fn child_hard_delete_is_rejected_typed() {
    // Review F6: hard_delete on a child table was silently inert.
    use rdlt_testkit::MemorySource;
    use serde_json::json;

    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    let dest = Postgres::connect(&conn)
        .dataset("childhd")
        .options(DestinationOptions {
            tables: [(
                "users__tags".to_string(),
                TableOptions {
                    hard_delete: Some("deleted".into()),
                    ..TableOptions::default()
                },
            )]
            .into_iter()
            .collect(),
            ..DestinationOptions::default()
        })
        .expect("options");
    let mut config = EngineConfig::new("childhd");
    config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
        key: vec!["id".into()],
    });
    let source = MemorySource::single_stream(
        rdlt_connector::StreamSpec::new("users").with_primary_key(["id"]),
        vec![json!({"id": 1, "tags": [{"label": "x"}]})],
    );
    let err = Engine::new(config, source, dest)
        .run()
        .await
        .expect_err("child hard_delete must be rejected");
    let msg = err.to_string();
    assert!(msg.contains("ROOT table"), "{msg}");
}
