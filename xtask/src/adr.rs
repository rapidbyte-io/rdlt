//! `cargo xtask lint`'s check that each ADR states its decision in force: a record whose
//! decision changed is rewritten, never annotated with what changed it.

#[cfg(test)]
mod tests;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use anyhow::Context as _;
use regex::Regex;

use crate::rules::{Finding, Rule};

/// The directory of the decision records, relative to the repository root.
const RECORDS: &str = "docs/adr";

/// Words that annotate a record with a change rather than state its decision: an amendment or
/// supersession, a line-leading note of what a newer record does, and "since" a record.
static NARRATED: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(concat!(
        r"(?im)\bamend(?:s|ed|ing|ments?)?\b",
        r"|\bsupersed(?:es|ed)\s+(?:by\s+)?ADR\b|\(superseded\b",
        r"|^[ \t]*\(ADR \d{4} (?:replaces|adds|has|bounds|charges|keeps)\b",
        r"|\bsince ADR \d{4}\b|\bADR \d{4} \w+ since\b",
    ))
    .expect("the pattern is a valid regular expression")
});

/// Every finding in the records under `root`, with paths relative to `root`.
pub(crate) fn check(root: &Path) -> anyhow::Result<Vec<(PathBuf, Finding)>> {
    let dir = root.join(RECORDS);
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let listing = fs::read_dir(&dir).with_context(|| format!("listing {}", dir.display()))?;
    let mut paths = Vec::new();
    for entry in listing {
        let path = entry
            .with_context(|| format!("listing {}", dir.display()))?
            .path();
        if path.extension().is_some_and(|ext| ext == "md") {
            paths.push(path);
        }
    }
    paths.sort();
    let mut findings = Vec::new();
    for path in paths {
        let text =
            fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
        let relative = path.strip_prefix(root).unwrap_or(&path).to_path_buf();
        for found in NARRATED.find_iter(&text) {
            let line = text[..found.start()].matches('\n').count() + 1;
            let words = found
                .as_str()
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ");
            let message =
                format!("`{words}` narrates a change: state the decision in force instead");
            findings.push((
                relative.clone(),
                Finding {
                    line,
                    rule: Rule::RecordHistory,
                    message,
                },
            ));
        }
    }
    Ok(findings)
}
