//! Drop scratch schemas that a killed test run never got to clean up.
//!
//! Every live test here works in a schema of its own and drops it when it
//! finishes. A run that is INTERRUPTED — a killed sweep, a panicking cell, a
//! machine that went away — leaves those schemas behind, where they cost
//! storage and clutter the account for anyone reading it.
//!
//! `#[ignore]` by default: a maintenance instrument, not a gate. It DELETES,
//! so it must never run as a side effect of an ordinary test invocation.
//!
//! ```text
//! cargo nextest run -p rdlt-connector-snowflake --test scratch_reclaim \
//!   --run-ignored all --no-capture
//! ```

use rdlt_connector_snowflake::dest::SnowflakeConfig;
use rdlt_testkit::snowflake::credentials;
use serde_json::json;

/// Scratch schemas are named by the testkit and by nothing else.
///
/// The prefix is the whole safety argument: it is what the harness generates
/// and what no human would name a schema, so matching on it cannot reach a
/// real one. Anything outside it is left alone even if it looks abandoned.
const SCRATCH_PREFIX: &str = "RDLT_T_";

fn admin_config() -> Option<SnowflakeConfig> {
    let creds = credentials()?;
    Some(
        SnowflakeConfig::from_value(json!({
            "account": creds.account,
            "user": creds.user,
            "database": creds.database,
            "schema": "PUBLIC",
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

#[tokio::test]
#[ignore = "maintenance instrument: DELETES schemas, gates nothing"]
async fn drop_scratch_schemas_left_by_interrupted_runs() {
    let Some(admin) = admin_config() else {
        println!("SKIP: no credentials — nothing to reclaim against");
        return;
    };
    let database = admin.database.to_uppercase();

    let listed = rdlt_connector_snowflake::dest::testhook::script_rows(
        &admin,
        &[&format!(
            "SHOW SCHEMAS LIKE '{SCRATCH_PREFIX}%' IN DATABASE \"{database}\""
        )],
        "SELECT \"name\" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))",
        &["name"],
    )
    .await
    .expect("the listing runs");

    // Re-checked here rather than trusted to the pattern: the match runs on the
    // service and a wildcard is easy to get wrong, while what follows is a
    // DROP. A name that does not start with the harness's prefix is skipped
    // even though the service just told us it matched.
    let names: Vec<&String> = listed
        .iter()
        .map(|row| &row[0])
        .filter(|name| name.starts_with(SCRATCH_PREFIX))
        .collect();

    if names.is_empty() {
        println!("nothing to reclaim: no scratch schemas remain");
        return;
    }

    println!("reclaiming {} scratch schema(s):", names.len());
    for name in &names {
        println!("  {name}");
        rdlt_connector_snowflake::dest::testhook::apply(
            &admin,
            &format!("DROP SCHEMA IF EXISTS \"{database}\".\"{name}\""),
        )
        .await
        .unwrap_or_else(|e| panic!("dropping {name}: {e:?}"));
    }

    let remaining = rdlt_connector_snowflake::dest::testhook::script_rows(
        &admin,
        &[&format!(
            "SHOW SCHEMAS LIKE '{SCRATCH_PREFIX}%' IN DATABASE \"{database}\""
        )],
        "SELECT \"name\" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))",
        &["name"],
    )
    .await
    .expect("the re-listing runs");
    assert!(
        remaining.is_empty(),
        "scratch schemas survived the reclaim: {remaining:?}"
    );
    println!("reclaimed {}", names.len());
}
