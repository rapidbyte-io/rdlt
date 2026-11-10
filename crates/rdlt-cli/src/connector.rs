//! `rdlt connector new` and `install`: a connector project whose certify test passes on its
//! first run, and a Python connector's wheel installed as a directory connector.

mod install;

use std::path::Path;

use tokio::io::AsyncWriteExt as _;

use crate::Exit;
use crate::args::{HostArgs, InstallArgs};
use crate::output::{self, Failure, Mode};
use crate::ui::{self, Ui};
use install::{InstallError, Installation};

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

/// Installs the wheel `args` names in the first connector directory the host's policy names.
pub(crate) async fn install(
    args: InstallArgs,
    host: &HostArgs,
    mode: Mode,
    ui: &Ui,
) -> Result<Exit, Failure> {
    let policy = crate::host::policy(host).await?;
    let Some(connectors) = policy.connectors.first() else {
        return Err(Failure::usage(
            "there is no connector directory to install into: name one with --connectors",
        ));
    };
    // The launcher names the tree's interpreter by an absolute path.
    let connectors = std::path::absolute(connectors)
        .map_err(|error| Failure::io(&format!("resolving {}", connectors.display()), &error))?;
    let installation = Installation {
        wheel: args.wheel,
        python: args.python,
        requirements: args.requirements,
        connectors,
    };
    let installing = tokio::task::spawn_blocking(move || install::install(&installation));
    let tree = installing
        .await
        .unwrap_or_else(|joined| std::panic::resume_unwind(joined.into_panic()))?;
    let plain = || vec![format!("installed {}", tree.display())];
    let rich = || ui::created(&tree, &[], ui);
    let json = serde_json::json!({ "installed": tree.display().to_string() });
    output::result(ui, mode, None, plain, rich, &json).await?;
    Ok(Exit::Success)
}

impl From<InstallError> for Failure {
    fn from(error: InstallError) -> Self {
        let exit = match error {
            InstallError::Io { .. } => Exit::Io,
            _ => Exit::Config,
        };
        let mut message = error.to_string();
        let mut cause = std::error::Error::source(&error);
        while let Some(source) = cause {
            message.push_str(": ");
            message.push_str(&source.to_string());
            cause = source.source();
        }
        Self {
            exit,
            message,
            report: None,
            file_not_found: false,
        }
    }
}
