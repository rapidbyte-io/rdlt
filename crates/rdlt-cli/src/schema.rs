//! `rdlt schema <connector>` — the config JSON Schemas the connectors
//! already export, printed for editors, linters and CI. Machine
//! output, so it goes to STDOUT like the report.
//!
//! Two resolution tiers, in order: the NINE compiled-in spellings match
//! first and their output is byte-identical to what the `ValueEnum` era
//! printed (the compatibility contract's schema half, pinned by
//! tests/cli_contract.rs); anything else is an OUT-OF-PROCESS connector
//! — a reverse-DNS id discovered on PATH, or an explicit binary path —
//! spawned and asked over the config-free `Spec` RPC.

use crate::CliError;

pub(crate) async fn print(connector: &str) -> Result<(), CliError> {
    // The frozen kebab spellings, exactly the set the ValueEnum
    // accepted — matched BEFORE any id/path interpretation, so the
    // compiled tier can never be shadowed by a stray binary on PATH.
    let compiled = match connector {
        "rest-source" => Some(rdlt::connector::rest::source::config_schema()),
        "postgres-source" => Some(rdlt::connector::postgres::source::config_schema()),
        "oracle-source" => Some(rdlt::connector::oracle::source::config_schema()),
        "file-source" => Some(rdlt::connector::file::source::config_schema()),
        "file-dest" => Some(rdlt::connector::file::destination::config_schema()),
        "postgres-dest" => Some(rdlt::connector::postgres::destination::config_schema()),
        "duckdb-dest" => Some(rdlt::connector::duckdb::destination::config_schema()),
        "snowflake-dest" => Some(rdlt::connector::snowflake::destination::config_schema()),
        "iceberg-dest" => Some(rdlt::connector::iceberg::destination::config_schema()),
        _ => None,
    };
    let schema = match compiled {
        Some(schema) => schema,
        None => spawned_schema(connector).await?,
    };
    let json = serde_json::to_string_pretty(&schema)
        .map_err(|e| CliError::Usage(format!("encoding schema: {e}")))?;
    // Not `println!`: a closed stdout must be exit 74, not a panic.
    use std::io::Write as _;
    let mut stdout = std::io::stdout().lock();
    stdout
        .write_all(json.as_bytes())
        .and_then(|()| stdout.write_all(b"\n"))
        .map_err(|e| CliError::Io(format!("writing schema to stdout: {e}")))?;
    Ok(())
}

/// The out-of-process tier: treat the value as a connector id — or,
/// when it names an existing file, as an explicit binary path — spawn
/// it through the provider, and ask the config-free `Spec` RPC. No
/// handshake, no config: the schema is the connector's static identity.
async fn spawned_schema(value: &str) -> Result<serde_json::Value, CliError> {
    let provider = rdlt::runtime::LocalBinaryConnectorProvider::new();
    let path = std::path::Path::new(value);
    let requirement = if path.is_file() {
        rdlt::runtime::ConnectorRequirement::new(value).with_path(path)
    } else {
        rdlt::runtime::ConnectorRequirement::new(value)
    };
    let spec = provider
        .spec(&requirement)
        .await
        // The provider's typed errors — the frozen NotFound spelling
        // included — render verbatim as config errors (exit 2).
        .map_err(|e| CliError::Usage(e.to_string()))?;
    spec.config_schema.ok_or_else(|| {
        // Frozen spelling: a connector may legitimately describe
        // nothing, and that is the connector's answer, not an IO error.
        CliError::Usage(format!("connector `{value}` publishes no config schema"))
    })
}
