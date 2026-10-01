//! `cargo xtask lint`: runs the rules over every Rust file in the repository, and checks where
//! `unsafe` code may be.

#[cfg(test)]
mod tests;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::Context as _;
use walkdir::WalkDir;

use crate::codegen::GENERATED;
use crate::lexer::scan;
use crate::rules::{self, FileRole, Finding, Severity};
use crate::unsafe_code;

/// Build output, never scanned, relative to the repository root.
const BUILD_OUTPUT: &str = "fuzz/target";

/// Directories scanned for Rust sources, relative to the repository root.
const SOURCE_ROOTS: &[&str] = &["crates", "fuzz", "xtask"];

/// Every finding in the tree under `root`, with paths relative to `root`.
///
/// The code generated from the protocol's definitions is held to no comment or style rule, and
/// to every rule about `unsafe` code.
pub(crate) fn lint_tree(root: &Path) -> anyhow::Result<Vec<(PathBuf, Finding)>> {
    let mut all = Vec::new();
    for dir in SOURCE_ROOTS
        .iter()
        .map(|dir| root.join(dir))
        .filter(|dir| dir.exists())
    {
        let walk = WalkDir::new(&dir).sort_by_file_name().into_iter();
        for entry in walk.filter_entry(|entry| entry.path() != root.join(BUILD_OUTPUT)) {
            let entry = entry.with_context(|| format!("walking {}", dir.display()))?;
            let path = entry.path();
            if path.extension().is_none_or(|ext| ext != "rs") {
                continue;
            }
            let source =
                fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
            let relative = path.strip_prefix(root).unwrap_or(path).to_path_buf();
            let mut findings = Vec::new();
            if relative != Path::new(GENERATED) {
                findings = rules::check(FileRole::of(&relative), &scan(&source));
            }
            findings.extend(unsafe_code::check_file(&relative, &source));
            all.extend(
                findings
                    .into_iter()
                    .map(|finding| (relative.clone(), finding)),
            );
        }
    }
    Ok(all)
}

/// Prints every finding under `root` and fails when any has error severity.
#[expect(clippy::print_stdout, reason = "findings are the command's output")]
pub(crate) fn run(root: &Path) -> anyhow::Result<ExitCode> {
    let mut findings = lint_tree(root)?;
    findings.extend(unsafe_code::check(root)?);
    let mut errors = 0;
    for (path, finding) in &findings {
        let level = match finding.rule.severity() {
            Severity::Error => {
                errors += 1;
                "error"
            }
            Severity::Warning => "warning",
        };
        let (line, rule, message) = (finding.line, finding.rule, &finding.message);
        println!("{}:{line}: {level}[{rule:?}]: {message}", path.display());
    }
    Ok(if errors == 0 {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}
