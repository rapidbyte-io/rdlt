//! Destination option edge cases the strategy and refinement suites do not
//! reach: the dataset default, cross-mode strategy rejection, and the
//! non-bool hard-delete flag.

use rdlt_connector_postgres::dest::{DestinationOptions, MergeStrategy, Postgres, TableOptions};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::MemorySource;
use serde_json::json;

use rdlt_connector_postgres::fixtures::PgFixture;

/// `dataset` default — omitted, tables land in `public` (observed,
/// not inferred; PM3).
#[tokio::test(flavor = "multi_thread")]
async fn default_dataset_is_public() {
    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    let dest = Postgres::connect(&conn); // no .dataset(...)
    let source = MemorySource::single_stream(
        rdlt_connector::StreamSpec::new("things").with_primary_key(["id"]),
        vec![json!({"id": 1, "v": "a"})],
    );
    Engine::new(EngineConfig::new("dflt-ds"), source, dest)
        .run()
        .await
        .expect("run");
    let client = crate::cases::common::connect(&conn).await;
    let n: i64 = client
        .query_one("SELECT count(*) FROM public.things", &[])
        .await
        .expect("public table")
        .get(0);
    assert_eq!(n, 1, "omitted dataset lands in the `public` schema");
}

/// An EXPLICITLY configured merge_strategy under
/// append/replace is a typed error; the unconfigured default never
/// rejects.
#[tokio::test(flavor = "multi_thread")]
async fn explicit_strategy_under_non_merge_mode_is_typed() {
    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    let source = || {
        MemorySource::single_stream(
            rdlt_connector::StreamSpec::new("things").with_primary_key(["id"]),
            vec![json!({"id": 1, "v": "a"})],
        )
    };
    let run = |mode: rdlt_connector::WriteMode, options: DestinationOptions| {
        let dest = Postgres::connect(&conn)
            .dataset("r5")
            .options(options)
            .expect("options");
        let mut config = EngineConfig::new("r5");
        config = config.with_write_mode(mode);
        Engine::new(config, source(), dest).run()
    };

    // Destination-wide explicit strategy under APPEND: typed.
    let err = run(
        rdlt_connector::WriteMode::Append,
        DestinationOptions {
            merge_strategy: Some(MergeStrategy::Upsert),
            ..DestinationOptions::default()
        },
    )
    .await
    .expect_err("explicit strategy under append")
    .to_string();
    assert!(err.contains("merge_strategy"), "{err}");
    assert!(err.contains("requires the merge write mode"), "{err}");

    // Per-table explicit strategy under REPLACE: typed too.
    let err = run(
        rdlt_connector::WriteMode::Replace,
        DestinationOptions {
            tables: [(
                "things".to_string(),
                TableOptions {
                    merge_strategy: Some(MergeStrategy::DeleteInsert),
                    ..TableOptions::default()
                },
            )]
            .into_iter()
            .collect(),
            ..DestinationOptions::default()
        },
    )
    .await
    .expect_err("per-table explicit strategy under replace")
    .to_string();
    assert!(err.contains("`things`"), "{err}");

    // UNCONFIGURED default: append works exactly as before.
    run(
        rdlt_connector::WriteMode::Append,
        DestinationOptions::default(),
    )
    .await
    .expect("default options never reject append");
}

/// `hard_delete` on a NON-boolean column — M4's other arm: the flag
/// fires on `IS NOT NULL` (any value), keeps on NULL.
#[tokio::test(flavor = "multi_thread")]
async fn non_bool_hard_delete_flag_uses_is_not_null() {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use async_trait::async_trait;
    use rdlt_connector::{ConnectorSpec, ReadRequest, Source, SourceError, StreamSpec};

    struct TsFlagged {
        batch: RecordBatch,
    }

    #[async_trait]
    impl Source for TsFlagged {
        fn spec(&self) -> ConnectorSpec {
            ConnectorSpec::new("ts-flagged", "0.0.0")
        }
        async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
            Ok(vec![
                StreamSpec::new("ev")
                    .with_structured()
                    .with_primary_key(["id"]),
            ])
        }
        async fn read(&self, mut req: ReadRequest) -> Result<(), SourceError> {
            let _ = req.out.arrow(self.batch.clone()).await;
            Ok(())
        }
    }

    fn batch(rows: &[(i64, &str, Option<i64>)]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
                Field::new(
                    "deleted_at",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ),
            ])),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(TimestampMicrosecondArray::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("batch")
    }

    let Some(pg) = PgFixture::start().await else {
        return;
    };
    let conn = pg.conn.clone();
    let dest = Postgres::connect(&conn)
        .dataset("nbhd")
        .options(DestinationOptions {
            merge_strategy: Some(MergeStrategy::Upsert),
            tables: [(
                "ev".to_string(),
                TableOptions {
                    hard_delete: Some("deleted_at".into()),
                    ..TableOptions::default()
                },
            )]
            .into_iter()
            .collect(),
        })
        .expect("options");
    let run = |rows: &[(i64, &str, Option<i64>)]| {
        let mut config = EngineConfig::new("nbhd");
        config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
            key: vec!["id".into()],
        });
        Engine::new(config, TsFlagged { batch: batch(rows) }, dest.clone()).run()
    };
    run(&[(1, "a", None), (2, "b", None)]).await.expect("seed");
    // A deletion TIMESTAMP (non-bool) fires the flag; NULL keeps.
    run(&[(1, "a2", Some(1_700_000_000_000_000)), (2, "b2", None)])
        .await
        .expect("flagged");
    let client = crate::cases::common::connect(&conn).await;
    let names: Vec<String> = client
        .query("SELECT name FROM nbhd.ev ORDER BY id", &[])
        .await
        .expect("rows")
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(
        names,
        vec!["b2"],
        "non-bool flag: IS NOT NULL deletes, NULL merges normally (M4)"
    );
}
