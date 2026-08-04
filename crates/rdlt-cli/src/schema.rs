//! `rdlt schema <connector>` — the config JSON Schemas the connectors
//! already export, printed for editors, linters and CI. Machine
//! output, so it goes to STDOUT like the report.

use crate::CliError;
use crate::args::SchemaFor;

pub(crate) fn print(connector: SchemaFor) -> Result<(), CliError> {
    let schema = match connector {
        SchemaFor::RestSource => rdlt::connector::rest::source::config_schema(),
        SchemaFor::PostgresSource => rdlt::connector::postgres::source::config_schema(),
        SchemaFor::OracleSource => rdlt::connector::oracle::source::config_schema(),
        SchemaFor::FileSource => rdlt::connector::file::source::config_schema(),
        SchemaFor::FileDest => rdlt::connector::file::destination::config_schema(),
        SchemaFor::PostgresDest => rdlt::connector::postgres::destination::config_schema(),
        SchemaFor::DuckdbDest => rdlt::connector::duckdb::destination::config_schema(),
        SchemaFor::SnowflakeDest => rdlt::connector::snowflake::destination::config_schema(),
        SchemaFor::IcebergDest => rdlt::connector::iceberg::destination::config_schema(),
    };
    let json = serde_json::to_string_pretty(&schema)
        .map_err(|e| CliError::Usage(format!("encoding schema: {e}")))?;
    println!("{json}");
    Ok(())
}
