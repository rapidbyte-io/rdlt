//! The shared destination conformance suite, against the real account.
//!
//! The certification gate every destination passes: a change that breaks the
//! SPI contract fails here rather than in a consumer's pipeline. Running it
//! live rather than against a stand-in is the point — the clauses are about
//! what a destination DOES, and only the service can say what this one does.

use async_trait::async_trait;
use rdlt_connector_snowflake::dest::{Snowflake, SnowflakeConfig};
use rdlt_testkit::TableProbe;
use rdlt_testkit::conformance::dest::verify_destination;
mod common;

use common::{credentials, scratch_schema};
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

/// Counts rows the way the suite's clauses expect.
struct SnowflakeProbe(SnowflakeConfig);

#[async_trait]
impl TableProbe for SnowflakeProbe {
    async fn count(&self, table: &rdlt_connector::TableName) -> u64 {
        // A table the load never created is zero rows, not a failure: the
        // suite probes tables that may legitimately not exist yet.
        rdlt_connector_snowflake::dest::testhook::connect_and_run(
            &self.0,
            &format!(
                "SELECT count(*) FROM \"{}\"",
                table.as_str().to_uppercase().replace('"', "\"\"")
            ),
        )
        .await
        .ok()
        .and_then(|text| text.parse().ok())
        .unwrap_or(0)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_snowflake_destination_is_conformant() {
    let Some(admin) = config_in("PUBLIC") else {
        return;
    };
    let schema = scratch_schema("conf");
    let config = config_in(&schema).expect("credentials");
    let dest = Snowflake::new(config.clone()).expect("valid config");
    let probe = SnowflakeProbe(config.clone());

    // The suite opens the destination itself; opening once here is what
    // creates the scratch schema for it to work in.
    rdlt_connector::Destination::open(
        &dest,
        rdlt_connector::OpenContext::new(
            rdlt_connector::core::PipelineId::from("setup"),
            rdlt_connector::core::LoadId::from("setup"),
        ),
    )
    .await
    .expect("the scratch schema is created by open");

    let failures = verify_destination(&dest, &probe).await;

    let _ = rdlt_connector_snowflake::dest::testhook::apply(
        &admin,
        &format!(
            "DROP SCHEMA IF EXISTS \"{}\".\"{}\"",
            admin.database.to_uppercase(),
            schema
        ),
    )
    .await;

    rdlt_testkit::assert_conformant(failures);
}
