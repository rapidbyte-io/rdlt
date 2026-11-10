//! `rdlt connector new` and `install`: a connector project whose certify test passes on its
//! first run, and a Python connector's wheel installed as a directory connector.

mod install;

use std::path::Path;

use tokio::io::AsyncWriteExt as _;

use crate::Exit;
use crate::args::{HostArgs, InstallArgs, Lang};
use crate::output::{self, Failure, Mode};
use crate::ui::{self, Ui};
use install::{InstallError, Installation};

/// A language's project: its files, each path and text naming the connector `greetings`, and
/// the names it gives a type or takes from what it imports, which a connector of one of them
/// would collide with.
struct Template {
    files: &'static [(&'static str, &'static str)],
    reserved: &'static [&'static str],
    /// The command that runs its certify test.
    certified_by: &'static str,
}

const RUST: Template = Template {
    files: &[
        (
            "Cargo.toml",
            include_str!("../templates/rust/Cargo.toml.template"),
        ),
        ("src/lib.rs", include_str!("../templates/rust/src/lib.rs")),
        (
            "src/main.rs",
            include_str!("../templates/rust/src/main.rs.template"),
        ),
    ],
    reserved: &[
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
    ],
    certified_by: "cargo test",
};

const PYTHON: Template = Template {
    files: &[
        (
            "pyproject.toml",
            include_str!("../templates/python/pyproject.toml"),
        ),
        ("README.md", include_str!("../templates/python/README.md")),
        (
            "src/rdlt_connector_greetings/__init__.py",
            include_str!("../templates/python/src/rdlt_connector_greetings/__init__.py"),
        ),
        (
            "tests/test_certify.py",
            include_str!("../templates/python/tests/test_certify.py"),
        ),
    ],
    // Its own names, what it imports, and the constants a class may not be named.
    reserved: &[
        "Config", "Cursor", "Iterator", "Position", "None", "True", "False",
    ],
    certified_by: "pytest",
};

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

/// Writes `template`'s files, named for `name`, into `project`.
async fn written(
    project: &Path,
    template: &Template,
    name: &str,
    camel: &str,
) -> Result<(), Failure> {
    let named = |text: &str| text.replace("greetings", name).replace("Greetings", camel);
    for (file, text) in template.files {
        let file = named(file);
        let path = project.join(&file);
        let writing = async {
            if let Some(directory) = path.parent() {
                tokio::fs::create_dir_all(directory).await?;
            }
            let mut opened = tokio::fs::File::create(&path).await?;
            opened.write_all(named(text).as_bytes()).await?;
            opened.flush().await
        };
        writing
            .await
            .map_err(|error| Failure::io(&format!("writing {file}"), &error))?;
    }
    Ok(())
}

/// Writes project `rdlt-connector-<name>`, in `lang`, beneath `into`.
pub(crate) async fn new(
    name: &str,
    lang: Lang,
    into: &Path,
    mode: Mode,
    ui: &Ui,
) -> Result<Exit, Failure> {
    let lower = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_';
    if !name.starts_with(|c: char| c.is_ascii_lowercase()) || !name.chars().all(lower) {
        return Err(Failure::usage(
            "a connector's name is lower-case letters, digits and `_`, starting with a letter",
        ));
    }
    let template = match lang {
        Lang::Rust => &RUST,
        Lang::Python => &PYTHON,
    };
    let camel = camel_case(name);
    if template.reserved.contains(&camel.as_str()) {
        return Err(Failure::usage(format!(
            "a connector named {name} would collide with the scaffold's own {camel}"
        )));
    }
    let project = into.join(format!("rdlt-connector-{name}"));
    tokio::fs::create_dir(&project)
        .await
        .map_err(|error| Failure::io(&format!("creating {}", project.display()), &error))?;
    if let Err(failure) = written(&project, template, name, &camel).await {
        // A project left half written would refuse the next attempt.
        tokio::fs::remove_dir_all(&project).await.ok();
        return Err(failure);
    }
    let plain = || {
        vec![format!(
            "wrote {}; `{}` certifies it",
            project.display(),
            template.certified_by
        )]
    };
    let wrote = project.display().to_string();
    let rich = || {
        let next = [format!(
            "cd {} && {}",
            project.display(),
            template.certified_by
        )];
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
    let installation = Installation {
        wheel: args.wheel,
        python: args.python,
        requirements: args.requirements,
        connectors: connectors.clone(),
    };
    let installed = install::install(&installation).await?;
    if let Some(left) = &installed.left {
        output::note(&format!("rdlt: {left}"));
    }
    let tree = installed.tree;
    let plain = || vec![format!("installed {}", tree.display())];
    let rich = || ui::created(&tree, &[], ui);
    let json = serde_json::json!({ "installed": tree.display().to_string() });
    output::result(ui, mode, None, plain, rich, &json).await?;
    Ok(Exit::Success)
}

impl From<InstallError> for Failure {
    fn from(error: InstallError) -> Self {
        // A file that could not be read or written fails as I/O, and every refusal as config.
        let exit = if error.code() == "io" {
            Exit::Io
        } else {
            Exit::Config
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
