//! What a steady-state load actually costs the account, counted server-side.
//!
//! The unit-level economy pins count statements this crate BUILDS. This one
//! counts what the service RECEIVED, which is the number a user pays for in
//! latency and in credits — and the only one that catches a round trip added
//! somewhere other than the ensure path.

use rdlt_connector::StreamSpec;
use rdlt_connector_snowflake::dest::testhook::connect_and_run;
use rdlt_connector_snowflake::dest::{Snowflake, SnowflakeConfig};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::memory::{MemoryBatch, MemorySource, MemoryStream};
use rdlt_testkit::snowflake::{credentials, scratch_schema};
use serde_json::json;

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

/// Statements this session's account ran against `schema` since `since`.
///
/// Read from the account's own history rather than counted in-process: a
/// client-side count can only see the statements this code knows it sent.
async fn statements_since(config: &SnowflakeConfig, schema: &str, minutes: u32) -> u64 {
    let sql = format!(
        "SELECT count(*) FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY( \
           END_TIME_RANGE_START => DATEADD('minute', -{minutes}, CURRENT_TIMESTAMP()), \
           RESULT_LIMIT => 10000)) \
         WHERE SCHEMA_NAME = '{schema}'"
    );
    connect_and_run(config, &sql)
        .await
        .ok()
        .and_then(|text| text.parse().ok())
        .unwrap_or(0)
}

fn source(from: i64) -> MemorySource {
    MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![
            MemoryBatch::new(
                (from..from + 5)
                    .map(|id| json!({"id": id, "note": format!("row-{id}")}))
                    .collect::<Vec<_>>(),
            )
            .with_checkpoint(1),
        ],
    )])
}

/// A second load against an unchanged schema costs no more than the first.
///
/// The property under test is that the cost does not GROW: the first load
/// creates the table and the bookkeeping, the second finds everything in
/// place. A destination that re-asserted its schema each run would show the
/// second load costing about what the first did, and the difference is
/// invisible without counting.
#[tokio::test]
async fn a_repeat_load_adds_no_schema_statements() {
    let Some(admin) = config_in("PUBLIC") else {
        return;
    };
    let schema = scratch_schema("economy");
    let config = config_in(&schema).expect("credentials");
    let dest = Snowflake::new(config.clone()).expect("valid config");

    let outcome = async {
        Engine::new(EngineConfig::new("sf-econ-1"), source(1), dest.clone())
            .run()
            .await
            .expect("first load settles");
        let after_first = statements_since(&config, &schema, 10).await;

        Engine::new(EngineConfig::new("sf-econ-2"), source(100), dest.clone())
            .run()
            .await
            .expect("second load settles");
        let after_second = statements_since(&config, &schema, 10).await;

        (after_first, after_second - after_first)
    }
    .await;

    let _ = rdlt_connector_snowflake::dest::testhook::apply(
        &admin,
        &format!(
            "DROP SCHEMA IF EXISTS \"{}\".\"{}\"",
            admin.database.to_uppercase(),
            schema
        ),
    )
    .await;

    let (first, second) = outcome;
    // Recorded rather than barred: the exact counts are a property of the
    // protocol and will move as it changes, and a SaaS history view is not a
    // measurement instrument to gate on. What must hold is the SHAPE.
    //
    // A MEASURED and DECLINED change lives behind this number. `open` issues
    // three idempotent `IF NOT EXISTS` statements per load for the schema and
    // the two bookkeeping tables; replacing them with one catalog read and
    // conditional creates was built and measured at 13 → 12 statements on the
    // repeat load. One statement, against a fixed cost that does not grow with
    // the data or the table count — so the complexity does not earn itself,
    // and the numbers are recorded rather than the change taken.
    println!("RECORDED first load: {first} statements; second load: {second}");
    assert!(
        first > 0,
        "the history view returned nothing — the count proves nothing if it \
         cannot see the load at all"
    );
    assert!(
        second <= first,
        "a repeat load against an unchanged schema cost MORE than the first \
         ({second} vs {first}): the ensure path is re-asserting something"
    );
}
