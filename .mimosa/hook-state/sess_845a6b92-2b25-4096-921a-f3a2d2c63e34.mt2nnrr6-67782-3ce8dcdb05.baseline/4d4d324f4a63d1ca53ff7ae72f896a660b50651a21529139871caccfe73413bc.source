//! `rdlt schema <connector>` — the config JSON Schema a connector
//! publishes, printed for editors, linters and CI. Machine output, so
//! it goes to STDOUT like the report.
//!
//! ONE tier: every value names an out-of-process connector, spawned
//! and asked over the config-free `Spec` RPC — source-first, or under
//! exactly the half `--role` names (the dual-role door). The value is
//! the FULL reverse-DNS connector id, discovered on PATH by its last
//! segment (`io.rapidbyte.reference` → `rdlt-connector-reference`; a
//! shorthand like `reference` discovers the same binary and is then
//! refused as an identity mismatch, the same rule a document's `id`
//! follows), or an explicit binary path.

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

/// Resolve the value and ask the spawned connector for its schema. A
/// value naming an existing file is an explicit binary path; anything
/// else is used as a connector id verbatim. No handshake,
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
    let requirement = if path.is_file() {
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
