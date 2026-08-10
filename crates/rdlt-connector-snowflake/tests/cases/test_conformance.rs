//! Certification against the real service: the sdk shell over this
//! connector passes the SAME testkit kit every destination answers to.

use async_trait::async_trait;
use rdlt_connector_sdk::spi::core::TableName;
use rdlt_connector_snowflake::destination::{Shell, testhook};
use rdlt_testkit::{ProbeError, TableProbe, assert_conformant, verify_destination};

use super::common::{config_for, credentials, scratch_schema};

/// Reader-visible rows, straight off the warehouse.
struct LiveProbe {
    config: rdlt_connector_snowflake::destination::Config,
    schema: String,
}

#[async_trait]
impl TableProbe for LiveProbe {
    async fn count(&self, table: &TableName) -> Result<u64, ProbeError> {
        let sql = format!(
            "SELECT COUNT(*) AS N FROM \"{}\".\"{}\".\"{}\"",
            self.config.database.to_uppercase(),
            self.schema.to_uppercase(),
            table.as_str().to_uppercase()
        );
        Ok(testhook::rows(&self.config, &sql, &["n"])
            .await
            .ok()
            .and_then(|rows| rows.first().and_then(|row| row[0].parse().ok()))
            .unwrap_or(0))
    }
}

#[tokio::test]
async fn the_destination_is_conformant_against_the_live_service() {
    let Some(creds) = credentials() else { return };
    let schema = scratch_schema("conf");
    let doc = config_for(&creds, &schema);

    let shell = Shell::from_value(doc.clone()).expect("valid credentials document");
    let config = {
        use rdlt_connector_sdk::config::Document;
        rdlt_connector_snowflake::destination::Config::from_value(doc).expect("valid")
    };
    let probe = LiveProbe {
        config: config.clone(),
        schema: schema.clone(),
    };

    assert_conformant(verify_destination(&shell, &probe).await.expecting_no_skips());

    // Scratch teardown, best effort.
    let _ = testhook::connect_and_run(
        &config,
        &format!(
            "DROP SCHEMA IF EXISTS \"{}\".\"{}\" CASCADE",
            config.database.to_uppercase(),
            schema.to_uppercase()
        ),
    )
    .await;
}
