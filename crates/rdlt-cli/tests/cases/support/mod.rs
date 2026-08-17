//! Shared support for the CLI cases: the built binary and the pipeline
//! documents the cells write.

use std::path::{Path, PathBuf};
use std::process::Command;

/// The built `rdlt` binary, ready for arguments.
pub(crate) fn rdlt() -> Command {
    Command::new(env!("CARGO_BIN_EXE_rdlt"))
}

/// Write a pipeline document under `dir` and hand back its path.
pub(crate) fn spec_file(dir: &Path, name: &str, yaml: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, yaml).expect("the spec writes");
    path
}
