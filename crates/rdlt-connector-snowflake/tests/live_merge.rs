//! Merge, against the real account.
//!
//! Snowflake enforces no unique constraints — primary keys are informational —
//! so nothing in the database stops a merge from leaving duplicates. The only
//! thing that keeps a key unique here is the merge SQL being right, which is
//! why these read the result back rather than trusting a statement to have
//! done what it says.

use rdlt_connector::StreamSpec;
use rdlt_connector::core::WriteMode;
use rdlt_connector_snowflake::dest::{Snowflake, SnowflakeConfig};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::snowflake::{credentials, scratch_schema};
use serde_json::json;

fn config_in(schema: &str, options: serde_json::Value) -> Option<SnowflakeConfig> {
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
    for (key, value) in options.as_object().expect("an options map") {
        doc[key] = value.clone();
    }
    Some(SnowflakeConfig::from_value(doc).expect("valid config"))
}

async fn scalar(config: &SnowflakeConfig, sql: &str) -> String {
    rdlt_connector_snowflake::dest::testhook::connect_and_run(config, sql)
        .await
        .unwrap_or_else(|e| panic!("`{sql}` must run: {e:?}"))
}

/// Run `body` in a scratch schema, dropping it whatever happened.
async fn in_scratch_schema<F, Fut>(label: &str, options: serde_json::Value, body: F)
where
    F: FnOnce(SnowflakeConfig) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let Some(admin) = config_in("PUBLIC", json!({})) else {
        return;
    };
    let schema = scratch_schema(label);
    let config = config_in(&schema, options).expect("credentials");
    let dest = Snowflake::new(config.clone()).expect("valid config");
    rdlt_connector::Destination::open(
        &dest,
        rdlt_connector::OpenCtx::new(
            rdlt_connector::core::PipelineId::from("setup"),
            rdlt_connector::core::LoadId::from("setup"),
        ),
    )
    .await
    .expect("the scratch schema is created by open");

    let outcome =
        futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(body(config))).await;

    let _ = rdlt_connector_snowflake::dest::testhook::apply(
        &admin,
        &format!(
            "DROP SCHEMA IF EXISTS \"{}\".\"{}\"",
            admin.database.to_uppercase(),
            schema
        ),
    )
    .await;

    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

/// A source pushing already-structured Arrow — what a KEYED merge requires.
///
/// A shredded stream has no declared key: its identity is a content hash, and
/// the strategies that converge ON a key are refused for it. So the merge legs
/// cannot use the JSON memory source, which shreds.
struct KeyedSource {
    batch: arrow_array::RecordBatch,
}

#[async_trait::async_trait]
impl rdlt_connector::Source for KeyedSource {
    fn spec(&self) -> rdlt_connector::ConnectorSpec {
        rdlt_connector::ConnectorSpec::new("snowflake-merge-test", "0.0.0")
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, rdlt_connector::SourceError> {
        Ok(vec![
            StreamSpec::new("events")
                .with_structured()
                .with_primary_key(["id"]),
        ])
    }

    async fn read(
        &self,
        mut req: rdlt_connector::ReadRequest,
    ) -> Result<(), rdlt_connector::SourceError> {
        let _ = req.out.arrow(self.batch.clone()).await;
        Ok(())
    }
}

/// The `(id, note)` batch these legs merge on.
fn batch(rows: &[(i64, &str)]) -> arrow_array::RecordBatch {
    use std::sync::Arc;
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new("note", arrow_schema::DataType::Utf8, true),
    ]));
    arrow_array::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow_array::Int64Array::from(
                rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(arrow_array::StringArray::from(
                rows.iter().map(|(_, note)| *note).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("batch")
}

/// The same batch plus a deletion flag, for the hard-delete leg.
fn flagged_batch(rows: &[(i64, &str, bool)]) -> arrow_array::RecordBatch {
    use std::sync::Arc;
    let schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new("note", arrow_schema::DataType::Utf8, true),
        arrow_schema::Field::new("deleted", arrow_schema::DataType::Boolean, true),
    ]));
    arrow_array::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow_array::Int64Array::from(
                rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(arrow_array::StringArray::from(
                rows.iter().map(|(_, note, _)| *note).collect::<Vec<_>>(),
            )),
            Arc::new(arrow_array::BooleanArray::from(
                rows.iter().map(|(_, _, gone)| *gone).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("batch")
}

/// One merge load under `pipeline`.
async fn merge_load(config: &SnowflakeConfig, pipeline: &str, batch: arrow_array::RecordBatch) {
    let dest = Snowflake::new(config.clone()).expect("valid config");
    let mut engine_config = EngineConfig::new(pipeline);
    engine_config = engine_config.with_write_mode(WriteMode::Merge {
        key: vec!["id".into()],
    });
    Engine::new(engine_config, KeyedSource { batch }, dest)
        .run()
        .await
        .unwrap_or_else(|e| panic!("{pipeline} must settle: {e:?}"));
}

#[tokio::test]
async fn an_upsert_converges_on_the_key_and_leaves_no_duplicates() {
    in_scratch_schema("upsert", json!({"merge_strategy": "upsert"}), |config| async move {
        merge_load(
            &config,
            "sf-upsert-a",
            batch(&[(1, "first"), (2, "second")]),
        )
        .await;
        merge_load(
            &config,
            "sf-upsert-b",
            batch(&[(2, "SECOND-UPDATED"), (3, "third")]),
        )
        .await;

        assert_eq!(scalar(&config, "SELECT count(*) FROM \"EVENTS\"").await, "3");
        assert_eq!(
            scalar(
                &config,
                "SELECT count(*) FROM \"EVENTS\" WHERE \"ID\" = 2 AND \"NOTE\" = 'SECOND-UPDATED'"
            )
            .await,
            "1",
            "the matched row was updated in place"
        );
        // Nothing in the database enforces this — no unique constraint exists
        // — so it is worth asserting rather than assuming.
        assert_eq!(
            scalar(
                &config,
                "SELECT count(*) FROM (SELECT \"ID\" FROM \"EVENTS\" GROUP BY 1 HAVING count(*) > 1)"
            )
            .await,
            "0",
            "the merge key must be unique after a merge"
        );
    })
    .await;
}

#[tokio::test]
async fn last_wins_within_one_load_without_a_unique_constraint_to_lean_on() {
    in_scratch_schema(
        "lastwins",
        json!({"merge_strategy": "upsert"}),
        |config| async move {
            // The same key three times in ONE batch. Postgres would raise a
            // cardinality error from an unqualified MERGE; the survivor subquery
            // is what prevents that, and QUALIFY is how it is spelled here.
            merge_load(
                &config,
                "sf-lastwins",
                batch(&[(7, "oldest"), (7, "middle"), (7, "newest")]),
            )
            .await;
            assert_eq!(
                scalar(&config, "SELECT count(*) FROM \"EVENTS\"").await,
                "1"
            );
            assert_eq!(
                scalar(
                    &config,
                    "SELECT count(*) FROM \"EVENTS\" WHERE \"NOTE\" = 'newest'"
                )
                .await,
                "1",
                "arrival order decides the survivor"
            );
        },
    )
    .await;
}

#[tokio::test]
async fn delete_insert_replaces_the_delivered_keys() {
    in_scratch_schema(
        "delins",
        json!({"merge_strategy": "delete_insert"}),
        |config| async move {
            merge_load(
                &config,
                "sf-delins-a",
                batch(&[(1, "keep"), (2, "replace-me")]),
            )
            .await;
            merge_load(&config, "sf-delins-b", batch(&[(2, "replaced")])).await;

            assert_eq!(
                scalar(&config, "SELECT count(*) FROM \"EVENTS\"").await,
                "2"
            );
            assert_eq!(
                scalar(
                    &config,
                    "SELECT count(*) FROM \"EVENTS\" WHERE \"ID\" = 2 AND \"NOTE\" = 'replaced'"
                )
                .await,
                "1"
            );
            assert_eq!(
                scalar(
                    &config,
                    "SELECT count(*) FROM \"EVENTS\" WHERE \"ID\" = 1 AND \"NOTE\" = 'keep'"
                )
                .await,
                "1",
                "an undelivered key is untouched"
            );
        },
    )
    .await;
}

#[tokio::test]
async fn a_hard_delete_flag_removes_the_row_rather_than_keeping_it_flagged() {
    in_scratch_schema(
        "harddel",
        json!({
            "merge_strategy": "upsert",
            "tables": {"events": {"hard_delete": "deleted"}},
        }),
        |config| async move {
            merge_load(
                &config,
                "sf-hd-a",
                flagged_batch(&[(1, "alive", false), (2, "doomed", false)]),
            )
            .await;
            merge_load(&config, "sf-hd-b", flagged_batch(&[(2, "doomed", true)])).await;

            assert_eq!(
                scalar(&config, "SELECT count(*) FROM \"EVENTS\"").await,
                "1",
                "the flagged row is REMOVED, not kept with a flag set"
            );
            assert_eq!(
                scalar(&config, "SELECT count(*) FROM \"EVENTS\" WHERE \"ID\" = 1").await,
                "1"
            );
        },
    )
    .await;
}

#[tokio::test]
async fn scd2_versions_meet_exactly_because_the_unit_shares_one_instant() {
    in_scratch_schema(
        "scd2",
        json!({"merge_strategy": "scd2"}),
        |config| async move {
            merge_load(&config, "sf-scd2-a", batch(&[(1, "v1")])).await;
            merge_load(&config, "sf-scd2-b", batch(&[(1, "v2")])).await;

            // Two versions, one current.
            assert_eq!(
                scalar(&config, "SELECT count(*) FROM \"EVENTS\"").await,
                "2"
            );
            assert_eq!(
                scalar(
                    &config,
                    "SELECT count(*) FROM \"EVENTS\" WHERE \"_RDLT_VALID_TO\" IS NULL"
                )
                .await,
                "1",
                "exactly one version is current"
            );
            // The retired version's end must equal the new version's start. The
            // clock moves between statements here, so this only holds because the
            // unit captured one instant and both statements read it — a gap would
            // mean the entity had no current version for that interval.
            assert_eq!(
                scalar(
                    &config,
                    "SELECT count(*) FROM \"EVENTS\" old JOIN \"EVENTS\" new \
                   ON old.\"ID\" = new.\"ID\" \
                 WHERE old.\"_RDLT_VALID_TO\" IS NOT NULL \
                   AND new.\"_RDLT_VALID_TO\" IS NULL \
                   AND old.\"_RDLT_VALID_TO\" <> new.\"_RDLT_VALID_FROM\""
                )
                .await,
                "0",
                "the retired version's end must be the new version's start exactly"
            );
        },
    )
    .await;
}
