//! The Snowflake destination.

pub(crate) mod client;
mod config;

pub use config::{Auth, ConfigError, KeyPair, Password, SnowflakeConfig, TableType, config_schema};

/// Client seam, exposed ONLY for the live cells: they must provoke real
/// service errors to check how this crate classifies them, and a mock cannot
/// say what Snowflake actually returns. Not a public API.
#[doc(hidden)]
pub mod testhook {
    use rdlt_connector::DestinationError;

    use super::client::{self, DmlOnly, Executor};
    use super::config::SnowflakeConfig;

    /// Connect and run one statement, returning its first scalar as text.
    pub async fn connect_and_run(
        config: &SnowflakeConfig,
        sql: &str,
    ) -> Result<String, DestinationError> {
        let executor = client::connect(config).await?;
        if sql.trim_start().to_ascii_uppercase().starts_with("SELECT") {
            Ok(executor.scalar_u64(sql, &[]).await?.to_string())
        } else {
            executor.execute(sql).await.map(|()| String::new())
        }
    }

    /// Run one statement through the UNIT executor — the one that refuses DDL.
    pub async fn run_in_unit(config: &SnowflakeConfig, sql: &str) -> Result<(), DestinationError> {
        let executor = client::connect(config).await?;
        DmlOnly(&executor).execute(sql).await
    }

    /// The structured Snowflake code carried by a classified error, if any.
    pub fn classify_live_error(err: &DestinationError) -> Option<String> {
        client::code_in(err)
    }

    /// The code a duplicate merge key surfaces as, so the cell asserting it
    /// names the same constant the merge path will key on rather than
    /// repeating a magic number.
    pub const DUPLICATE_ROW_IN_DML: &str = client::DUPLICATE_ROW_IN_DML;
}
