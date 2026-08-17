//! `rdlt schema <connector>` — the config JSON Schema a connector
//! publishes, printed for editors, linters and CI. Machine output, so
//! it goes to STDOUT like the report.
//!
//! ONE tier: every spelling names an out-of-process connector, spawned
//! and asked over the config-free `Spec` RPC — source-first, or under
//! exactly the half `--role` names (the dual-role door). The
//! seven short names (`postgres`, `file`, …) map through the SAME
//! desugar table the pipeline document's rich spellings resolve
//! through; anything else is a reverse-DNS id discovered on PATH, or
//! an explicit binary path.

use rdlt::runtime::local::Local;
use rdlt_connector_client::handshake::{Requirement, Role};

use crate::args::SchemaRole;
use crate::exit;

pub(crate) async fn print(connector: &str, role: Option<SchemaRole>) -> Result<(), exit::Error> {
    let schema = spawned_schema(connector, role).await?;
    let json = serde_json::to_string_pretty(&schema)
        .map_err(|e| exit::Error::Usage(format!("encoding schema: {e}")))?;
    // Not `println!`: a closed stdout must be exit 74, not a panic.
    use std::io::Write as _;
    let mut stdout = std::io::stdout().lock();
    stdout
        .write_all(json.as_bytes())
        .and_then(|()| stdout.write_all(b"\n"))
        .map_err(|e| exit::Error::Io(format!("writing schema to stdout: {e}")))?;
    Ok(())
}

/// Resolve the spelling and ask the spawned connector for its schema.
/// A short name maps through the desugar table to its reverse-DNS id; a
/// value outside that table naming an existing file is an explicit
/// binary path; anything else is used as an id verbatim. No handshake,
/// no config: the schema is the connector's static identity. Without
/// `--role` the provider probes source-first; with it, exactly the
/// named half is asked and a single-role binary refusing that half is
/// a refusal, never a silent retry as the other.
async fn spawned_schema(
    value: &str,
    role: Option<SchemaRole>,
) -> Result<serde_json::Value, exit::Error> {
    let provider = Local::new();
    let path = std::path::Path::new(value);
    let requirement = if let Some(id) = rdlt::pipeline_spec::connector_id(value) {
        Requirement::new(id)
    } else if path.is_file() {
        Requirement::new(value).with_path(path)
    } else {
        Requirement::new(value)
    };
    let role = match role {
        None => None,
        Some(SchemaRole::Source) => Some(Role::Source),
        Some(SchemaRole::Destination) => Some(Role::Destination),
    };
    let spec = provider
        .spec(&requirement, role)
        .await
        // The provider's typed errors — the frozen NotFound spelling
        // included — render verbatim as config errors (exit 2).
        .map_err(|e| exit::Error::Usage(e.to_string()))?;
    spec.config_schema.ok_or_else(|| {
        // Frozen spelling: a connector may legitimately describe
        // nothing, and that is the connector's answer, not an IO error.
        exit::Error::Usage(format!("connector `{value}` publishes no config schema"))
    })
}
