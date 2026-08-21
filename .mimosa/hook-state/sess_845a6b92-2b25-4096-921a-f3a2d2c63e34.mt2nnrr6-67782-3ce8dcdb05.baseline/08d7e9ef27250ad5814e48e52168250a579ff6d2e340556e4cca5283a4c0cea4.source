//! `rdlt schema <connector>` — the config JSON Schema a connector
//! publishes, printed for editors, linters and CI. Machine output, so
//! it goes to STDOUT like the report.
//!
//! ONE tier: every spelling names an out-of-process connector, spawned
//! and asked over the config-free `Spec` RPC — source-first, or under
//! exactly the half `--role` names (the dual-role door, 040 T9). The
//! seven short names (`postgres`, `file`, …) map through the SAME
//! desugar table the pipeline document's rich spellings resolve
//! through; anything else is a reverse-DNS id discovered on PATH, or
//! an explicit binary path.

use crate::CliError;
use crate::args::SchemaRole;

pub(crate) async fn print(connector: &str, role: Option<SchemaRole>) -> Result<(), CliError> {
    let schema = spawned_schema(connector, role).await?;
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

/// Resolve the spelling and ask the spawned connector for its schema.
/// A short name maps through the desugar table to its reverse-DNS id; a
/// value outside that table naming an existing file is an explicit
/// binary path; anything else is used as an id verbatim. No handshake,
/// no config: the schema is the connector's static identity. Without
/// `--role` the provider probes source-first (039's behavior); with it,
/// exactly the named half is asked and a single-role binary refusing
/// that half is a refusal, never a silent retry as the other.
async fn spawned_schema(
    value: &str,
    role: Option<SchemaRole>,
) -> Result<serde_json::Value, CliError> {
    let provider = rdlt::runtime::LocalBinaryConnectorProvider::new();
    let path = std::path::Path::new(value);
    let requirement = if let Some(id) = rdlt::pipeline_spec::connector_id(value) {
        rdlt::runtime::ConnectorRequirement::new(id)
    } else if path.is_file() {
        rdlt::runtime::ConnectorRequirement::new(value).with_path(path)
    } else {
        rdlt::runtime::ConnectorRequirement::new(value)
    };
    let spec = match role {
        None => provider.spec(&requirement).await,
        Some(SchemaRole::Source) => {
            provider
                .spec_for_role(&requirement, rdlt::runtime::Role::Source)
                .await
        }
        Some(SchemaRole::Destination) => {
            provider
                .spec_for_role(&requirement, rdlt::runtime::Role::Destination)
                .await
        }
    }
    // The provider's typed errors — the frozen NotFound spelling
    // included — render verbatim as config errors (exit 2).
    .map_err(|e| CliError::Usage(e.to_string()))?;
    spec.config_schema.ok_or_else(|| {
        // Frozen spelling: a connector may legitimately describe
        // nothing, and that is the connector's answer, not an IO error.
        CliError::Usage(format!("connector `{value}` publishes no config schema"))
    })
}
