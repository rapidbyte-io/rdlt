//! `cargo xtask deps`: enforces which workspace crates may depend on which.

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;
use std::path::Path;
use std::process::ExitCode;

use anyhow::Context as _;
use cargo_metadata::{DependencyKind, MetadataCommand};

/// The workspace crates each crate may use as a normal or build dependency.
const RULES: &[(&str, &[&str])] = &[
    ("rdlt-connector", &["rdlt-connector-macros", "rdlt-wire"]),
    ("rdlt-connector-macros", &[]),
    ("rdlt-wire", &[]),
    ("rdlt-host", &["rdlt-connector", "rdlt-wire"]),
    ("rdlt-engine", &["rdlt-connector"]),
    ("rdlt-sql", &["rdlt-engine", "rdlt-connector"]),
    (
        "rdlt",
        &["rdlt-engine", "rdlt-connector", "rdlt-host", "rdlt-sql"],
    ),
    ("rdlt-cli", &["rdlt"]),
    (
        "rdlt-certify",
        &["rdlt-connector", "rdlt-host", "rdlt-wire"],
    ),
    ("rdlt-python", &["rdlt", "rdlt-connector"]),
    ("rdlt-connector-reference", &["rdlt-connector"]),
    ("rdlt-sim", &["rdlt-engine", "rdlt-connector"]),
    ("xtask", &[]),
];

/// Crates nothing may depend on, not even as a dev-dependency.
const LEAVES: &[&str] = &["rdlt-cli", "rdlt-sim", "xtask"];

/// A dependency of one workspace crate on another.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Edge {
    pub(crate) from: String,
    pub(crate) to: String,
    pub(crate) dev: bool,
}

/// A breach of the dependency rule.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Violation {
    /// A workspace crate that has no entry in [`RULES`].
    Unlisted(String),
    /// A dependency [`RULES`] or [`LEAVES`] does not allow.
    Forbidden(Edge),
}

/// Every violation among `crates` and the `edges` between them.
pub(crate) fn check(crates: &[String], edges: &[Edge]) -> Vec<Violation> {
    let mut violations: Vec<Violation> = crates
        .iter()
        .filter(|krate| allowed(krate).is_none())
        .map(|krate| Violation::Unlisted(krate.clone()))
        .collect();
    for edge in edges {
        let permitted = if edge.dev {
            !LEAVES.contains(&edge.to.as_str())
        } else {
            allowed(&edge.from).is_some_and(|deps| deps.contains(&edge.to.as_str()))
        };
        if !permitted {
            violations.push(Violation::Forbidden(edge.clone()));
        }
    }
    violations
}

fn allowed(krate: &str) -> Option<&'static [&'static str]> {
    RULES
        .iter()
        .find(|(name, _)| *name == krate)
        .map(|(_, deps)| *deps)
}

/// Checks the workspace rooted at `root` and prints every violation.
#[expect(clippy::print_stdout, reason = "violations are the command's output")]
pub(crate) fn run(root: &Path) -> anyhow::Result<ExitCode> {
    let metadata = MetadataCommand::new()
        .manifest_path(root.join("Cargo.toml"))
        .no_deps()
        .exec()
        .context("running cargo metadata")?;
    let packages = metadata.workspace_packages();
    let names: BTreeSet<String> = packages.iter().map(|p| p.name.to_string()).collect();
    let mut edges = Vec::new();
    for package in &packages {
        for dependency in &package.dependencies {
            if names.contains(dependency.name.as_str()) {
                edges.push(Edge {
                    from: package.name.to_string(),
                    to: dependency.name.clone(),
                    dev: matches!(dependency.kind, DependencyKind::Development),
                });
            }
        }
    }
    let violations = check(&names.into_iter().collect::<Vec<_>>(), &edges);
    for violation in &violations {
        match violation {
            Violation::Unlisted(krate) => {
                println!("error: `{krate}` has no entry in xtask's RULES");
            }
            Violation::Forbidden(edge) => {
                println!("error: `{}` may not depend on `{}`", edge.from, edge.to);
            }
        }
    }
    Ok(if violations.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}
