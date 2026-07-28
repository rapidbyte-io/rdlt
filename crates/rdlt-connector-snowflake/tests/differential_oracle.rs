//! The same inputs, two destinations, the same rows out.
//!
//! Every other test in this crate checks Snowflake against its own
//! expectations, which cannot catch a misunderstanding shared by the test and
//! the code. This one checks it against a DIFFERENT implementation of the same
//! contract: identical seeded inputs go through the merge strategies on both
//! Postgres and Snowflake, and the resulting tables must agree row for row.
//!
//! A difference is a defect in whichever is wrong, and nothing else would find
//! it.
//!
//! Gated on BOTH a container runtime and Snowflake credentials — either
//! missing skips, visibly.

use rdlt_connector::core::WriteMode;
use rdlt_connector::{ConnectorSpec, ReadRequest, Source, SourceError, StreamSpec};
use rdlt_connector_postgres::dest::Postgres;
use rdlt_connector_snowflake::dest::{Snowflake, SnowflakeConfig};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::PgFixture;
use rdlt_testkit::snowflake::{credentials, scratch_schema};
use serde_json::json;

/// A structured keyed source — what the converging strategies require.
struct KeyedSource {
    batch: arrow_array::RecordBatch,
}

#[async_trait::async_trait]
impl Source for KeyedSource {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("oracle-source", "0.0.0")
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
        Ok(())
    }
}

fn batch(rows: &[(i64, &str, bool)]) -> arrow_array::RecordBatch {
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

/// The two loads every strategy is fed: an initial set, then updates,
/// insertions and one deletion flag.
fn seeded_loads() -> [arrow_array::RecordBatch; 2] {
    [
        batch(&[
            (1, "one", false),
            (2, "two", false),
            (3, "three", false),
            (4, "four", false),
        ]),
        batch(&[
            (2, "TWO-UPDATED", false),
            (3, "three", true),
            (5, "five", false),
        ]),
    ]
}

fn snowflake_config(schema: &str, strategy: &str, hard_delete: bool) -> Option<SnowflakeConfig> {
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
        "merge_strategy": strategy,
    });
    if hard_delete {
        doc["tables"] = json!({"events": {"hard_delete": "deleted"}});
    }
    Some(SnowflakeConfig::from_value(doc).expect("valid config"))
}

fn postgres_dest(conn: &str, dataset: &str, strategy: &str, hard_delete: bool) -> Postgres {
    use rdlt_connector_sqlcore::options::{DestOptions, MergeStrategy, TableOptions};
    let strategy = match strategy {
        "upsert" => MergeStrategy::Upsert,
        "delete_insert" => MergeStrategy::DeleteInsert,
        other => panic!("unmapped strategy {other}"),
    };
    let mut options = DestOptions {
        merge_strategy: Some(strategy),
        ..DestOptions::default()
    };
    if hard_delete {
        options.tables = [(
            "events".to_string(),
            TableOptions {
                hard_delete: Some("deleted".into()),
                ..TableOptions::default()
            },
        )]
        .into_iter()
        .collect();
    }
    Postgres::connect(conn)
        .dataset(dataset)
        .options(options)
        .expect("options")
}

/// Every surviving row, rendered as one comparable string per row.
///
/// The WHOLE row, not just its key: an upsert that inserted correctly but
/// never updated `note` would leave the key set identical and the data wrong,
/// which is exactly the class of defect an oracle exists to catch. A set,
/// because both sides are compared as sets and order is not part of the
/// contract.
async fn snowflake_rows(config: &SnowflakeConfig) -> std::collections::BTreeSet<String> {
    rdlt_connector_snowflake::dest::testhook::read_column(
        config,
        "SELECT \"ID\" || '|' || COALESCE(\"NOTE\", '<null>') FROM \"EVENTS\"",
    )
    .await
    .expect("rows")
}

async fn postgres_rows(conn: &str, dataset: &str) -> std::collections::BTreeSet<String> {
    let (client, connection) = tokio_postgres::connect(conn, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .query(
            &format!(
                "SELECT id::text || '|' || COALESCE(note, '<null>') FROM \"{dataset}\".\"events\""
            ),
            &[],
        )
        .await
        .expect("rows")
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect()
}

async fn run_snowflake(config: &SnowflakeConfig, pipeline: &str, batches: [arrow_array::RecordBatch; 2]) {
    for (index, batch) in batches.into_iter().enumerate() {
        let dest = Snowflake::new(config.clone()).expect("valid config");
        let mut engine_config = EngineConfig::new(format!("{pipeline}-{index}"));
        engine_config.write_mode = WriteMode::Merge {
            key: vec!["id".into()],
        };
        Engine::new(engine_config, KeyedSource { batch }, dest)
            .run()
            .await
            .unwrap_or_else(|e| panic!("snowflake load {index} must settle: {e:?}"));
    }
}

async fn run_postgres(dest: &Postgres, pipeline: &str, batches: [arrow_array::RecordBatch; 2]) {
    for (index, batch) in batches.into_iter().enumerate() {
        let mut engine_config = EngineConfig::new(format!("{pipeline}-{index}"));
        engine_config.write_mode = WriteMode::Merge {
            key: vec!["id".into()],
        };
        Engine::new(engine_config, KeyedSource { batch }, dest.clone())
            .run()
            .await
            .unwrap_or_else(|e| panic!("postgres load {index} must settle: {e:?}"));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn both_destinations_land_the_same_rows_for_every_strategy() {
    let (Some(pg), Some(_)) = (PgFixture::start().await, credentials()) else {
        return;
    };
    let admin = snowflake_config("PUBLIC", "upsert", false).expect("credentials");
    let mut schemas = Vec::new();

    for (strategy, hard_delete) in [
        ("upsert", false),
        ("upsert", true),
        ("delete_insert", false),
        ("delete_insert", true),
    ] {
        let label = format!("{strategy}{}", if hard_delete { "hd" } else { "" });
        let schema = scratch_schema(&label);
        schemas.push(schema.clone());
        let config = snowflake_config(&schema, strategy, hard_delete).expect("credentials");
        let dataset = format!("oracle_{label}");
        let dest = postgres_dest(&pg.conn, &dataset, strategy, hard_delete);

        run_snowflake(&config, &format!("sf-{label}"), seeded_loads()).await;
        run_postgres(&dest, &format!("pg-{label}"), seeded_loads()).await;

        let sf = snowflake_rows(&config).await;
        let pg_rows = postgres_rows(&pg.conn, &dataset).await;
        assert!(
            !pg_rows.is_empty(),
            "[{label}] the reference loaded nothing — two empty tables also agree"
        );
        assert_eq!(
            sf, pg_rows,
            "[{label}] the two destinations disagree about which rows survive"
        );
    }

    for schema in schemas {
        let _ = rdlt_connector_snowflake::dest::testhook::apply(
            &admin,
            &format!(
                "DROP SCHEMA IF EXISTS \"{}\".\"{}\"",
                admin.database.to_uppercase(),
                schema
            ),
        )
        .await;
    }
}
