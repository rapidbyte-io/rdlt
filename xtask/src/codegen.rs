//! `cargo xtask codegen`: generates the wire protocol's Rust code from its `.proto` files.
//!
//! The generated code is committed, so building rdlt-wire needs neither `protoc` nor a build
//! script; `--check` fails when the committed code is stale.

#[cfg(test)]
mod tests;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use anyhow::Context as _;

/// The directory holding the protocol's `.proto` files, relative to the repository root.
const PROTO_DIR: &str = "crates/rdlt-wire/proto";

/// The generated file, relative to the repository root.
const GENERATED: &str = "crates/rdlt-wire/src/generated/rdlt.connector.v1.rs";

/// Generates the code, writing it, or with `check` comparing it to what is committed.
#[expect(clippy::print_stdout, reason = "the verdict is the command's output")]
pub(crate) fn run(root: &Path, check: bool) -> anyhow::Result<ExitCode> {
    let generated = generate(root)?;
    let path = root.join(GENERATED);
    if check {
        let committed = fs::read_to_string(&path).unwrap_or_default();
        if committed != generated {
            println!("{GENERATED} is stale: run `cargo xtask codegen`");
            return Ok(ExitCode::FAILURE);
        }
        return Ok(ExitCode::SUCCESS);
    }
    fs::create_dir_all(
        path.parent()
            .context("the generated file has a directory")?,
    )?;
    fs::write(&path, generated).with_context(|| format!("writing {}", path.display()))?;
    Ok(ExitCode::SUCCESS)
}

/// The protocol's Rust code, formatted as `cargo fmt` formats the repository.
pub(crate) fn generate(root: &Path) -> Result<String, anyhow::Error> {
    let proto_dir = root.join(PROTO_DIR);
    let files = protos(&proto_dir)?;
    let descriptors = protox::compile(&files, [&proto_dir]).context("compiling the protocol")?;
    let out = tempfile::tempdir().context("creating a scratch directory")?;
    prost_build::Config::new()
        .bytes(["."])
        .out_dir(out.path())
        .compile_fds(descriptors)
        .context("generating the protocol's code")?;
    let file = out.path().join("rdlt.connector.v1.rs");
    let status = Command::new("rustfmt")
        .arg("--edition=2024")
        .arg("--config-path")
        .arg(root)
        .arg(&file)
        .status()
        .context("running rustfmt")?;
    if !status.success() {
        return Err(RustfmtFailed(status).into());
    }
    fs::read_to_string(&file).context("reading the generated code")
}

/// rustfmt could not format the generated code.
#[derive(Debug)]
struct RustfmtFailed(std::process::ExitStatus);

impl std::fmt::Display for RustfmtFailed {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "rustfmt failed on the generated code: {}",
            self.0
        )
    }
}

impl std::error::Error for RustfmtFailed {}

/// Every `.proto` file under `dir`, relative to it, in a stable order.
fn protos(dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in walkdir::WalkDir::new(dir).sort_by_file_name() {
        let entry = entry.with_context(|| format!("walking {}", dir.display()))?;
        if entry.path().extension().is_some_and(|ext| ext == "proto") {
            files.push(entry.path().strip_prefix(dir)?.to_path_buf());
        }
    }
    Ok(files)
}
