//! `rdlt connector new`: a connector project whose certify test passes on its first run.

use std::path::Path;

use tokio::io::AsyncWriteExt as _;

use crate::Exit;
use crate::output::{self, Failure, Mode};
use crate::ui::{self, Ui};

const MANIFEST: &str = include_str!("../templates/rust/Cargo.toml.template");
const LIBRARY: &str = include_str!("../templates/rust/src/lib.rs");
const MAIN: &str = include_str!("../templates/rust/src/main.rs.template");

/// Names the template already gives a type, or takes from the prelude: a connector of one of
/// these would not compile.
const RESERVED: [&str; 13] = [
    "Self",
    "Config",
    "People",
    "Result",
    "Streams",
    "Emitter",
    "Partition",
    "StreamSpec",
    "StreamName",
    "ReadStream",
    "ConnectContext",
    "ConnectorError",
    "SourceConnector",
];

/// `name` in upper camel case, as the connector's type is named.
fn camel_case(name: &str) -> String {
    name.split('_')
        .map(|part| {
            let mut chars = part.chars();
            chars
                .next()
                .map(|first| first.to_ascii_uppercase().to_string() + chars.as_str())
                .unwrap_or_default()
        })
        .collect()
}

/// Writes the template's files, named for `name`, into `project`.
async fn written(project: &Path, name: &str, camel: &str) -> Result<(), Failure> {
    let named = |text: &str| text.replace("greetings", name).replace("Greetings", camel);
    tokio::fs::create_dir(project.join("src"))
        .await
        .map_err(|error| Failure::io("creating src", &error))?;
    for (file, text) in [
        ("Cargo.toml", MANIFEST),
        ("src/lib.rs", LIBRARY),
        ("src/main.rs", MAIN),
    ] {
        let writing = async {
            let mut opened = tokio::fs::File::create(project.join(file)).await?;
            opened.write_all(named(text).as_bytes()).await?;
            opened.flush().await
        };
        writing
            .await
            .map_err(|error| Failure::io(&format!("writing {file}"), &error))?;
    }
    Ok(())
}

/// Writes project `rdlt-connector-<name>` beneath `into`.
pub(crate) async fn new(name: &str, into: &Path, mode: Mode, ui: &Ui) -> Result<Exit, Failure> {
    let lower = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_';
    if !name.starts_with(|c: char| c.is_ascii_lowercase()) || !name.chars().all(lower) {
        return Err(Failure::usage(
            "a connector's name is lower-case letters, digits and `_`, starting with a letter",
        ));
    }
    let camel = camel_case(name);
    if RESERVED.contains(&camel.as_str()) {
        return Err(Failure::usage(format!(
            "a connector named {name} would collide with the scaffold's own {camel}"
        )));
    }
    let project = into.join(format!("rdlt-connector-{name}"));
    tokio::fs::create_dir(&project)
        .await
        .map_err(|error| Failure::io(&format!("creating {}", project.display()), &error))?;
    if let Err(failure) = written(&project, name, &camel).await {
        // A project left half written would refuse the next attempt.
        tokio::fs::remove_dir_all(&project).await.ok();
        return Err(failure);
    }
    let plain = || {
        vec![format!(
            "wrote {}; `cargo test` certifies it",
            project.display()
        )]
    };
    let wrote = project.display().to_string();
    let rich = || {
        let next = [format!("cd {} && cargo test", project.display())];
        ui::created(&project, &next, ui)
    };
    let json = serde_json::json!({ "wrote": wrote });
    output::result(ui, mode, None, plain, rich, &json).await?;
    Ok(Exit::Success)
}
