//! `rdlt check`, `discover`, `schema` and `connectors`.

use std::path::Path;

use crate::Exit;
use crate::args::{HostArgs, Side};
use crate::output::{self, Failure, Mode};

/// Builds and checks the pipeline `document` describes, moving no data.
pub(crate) async fn check(document: &Path, host: &HostArgs, mode: Mode) -> Result<Exit, Failure> {
    let builder = crate::host::document(document).await?;
    let pipeline = builder.build(&crate::host::host(host).await?).await?;
    let checked = pipeline.check().await?;
    let json = serde_json::to_value(&checked).expect("a check's report serializes");
    let plain = || {
        vec![format!(
            "ok: pipeline {} loads {} from {} into {}",
            checked.pipeline,
            checked.streams.join(", "),
            checked.source.id,
            checked.destination.id
        )]
    };
    output::result(mode, plain, &json).await?;
    Ok(Exit::Success)
}

/// Prints the catalog of the source `document` names, its streams not chosen yet.
pub(crate) async fn discover(
    document: &Path,
    host: &HostArgs,
    mode: Mode,
) -> Result<Exit, Failure> {
    let builder = crate::host::document(document).await?;
    let catalog = builder.discover(&crate::host::host(host).await?).await?;
    let json = serde_json::to_value(&catalog).expect("a catalog serializes");
    let plain = || {
        catalog
            .iter()
            .map(|stream| stream.name().to_string())
            .collect()
    };
    output::result(mode, plain, &json).await?;
    Ok(Exit::Success)
}

/// Prints the document's JSON Schema, or the configuration schema of the connector `id`, placed
/// as a pipeline would place it, on standard output in either mode.
pub(crate) async fn schema(
    document: bool,
    id: Option<&str>,
    role: Side,
    host: &HostArgs,
) -> Result<Exit, Failure> {
    let schema = match (document, id) {
        (true, _) => rdlt::Document::json_schema(),
        (false, Some(id)) => {
            let role = match role {
                Side::Source => rdlt::Role::Source,
                Side::Destination => rdlt::Role::Destination,
            };
            let host = crate::host::host(host).await?;
            host.spec(&rdlt::Connector::new(id), role)
                .await?
                .config_schema
        }
        (false, None) => return Err(Failure::usage("name a connector, or --document")),
    };
    let written = serde_json::to_string_pretty(&schema).expect("a schema serializes");
    output::stdout(&format!("{written}\n")).await?;
    Ok(Exit::Success)
}

/// What the host can place: each connector binary its directories hold, and whether it reaches
/// remote connectors; compiled-in connectors are an embedder's, and the command line has none.
pub(crate) async fn connectors(host: &HostArgs, mode: Mode) -> Result<Exit, Failure> {
    let policy = crate::host::policy(host).await?;
    let mut local = Vec::new();
    for dir in &policy.connectors {
        let Ok(mut entries) = tokio::fs::read_dir(dir).await else {
            continue;
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with("rdlt-connector-") {
                let path = entry.path().display().to_string();
                local.push(serde_json::json!({ "name": name, "path": path }));
            }
        }
    }
    local.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    let json = serde_json::json!({
        "local": local,
        "sandboxed": !policy.trust_binaries,
        "remote": policy.remote.is_some(),
    });
    let plain = || {
        let mut lines: Vec<String> = local
            .iter()
            .map(|entry| format!("local   {}", entry["path"].as_str().unwrap_or_default()))
            .collect();
        let local = if policy.trust_binaries {
            "trusted, in no sandbox"
        } else {
            "in the sandbox"
        };
        lines.push(format!("local connectors run {local}"));
        let remote = if policy.remote.is_some() {
            "are reached over mutual TLS"
        } else {
            "are not reached"
        };
        lines.push(format!("remote connectors {remote}"));
        lines
    };
    output::result(mode, plain, &json).await?;
    Ok(Exit::Success)
}
