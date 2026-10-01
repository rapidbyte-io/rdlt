//! `cargo xtask shipped`: checks that a connector binary built as a release builds it, its
//! package alone, carries none of certification's probes and no test connector.
//!
//! A build of the whole workspace turns `rdlt-connector`'s `certify` feature on for every
//! package, through the certifier's own dependency on it. A release therefore builds a
//! connector's package by itself, and this check holds what that build resolves to.

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;
use std::path::Path;
use std::process::{Command, ExitCode};

use anyhow::Context as _;
use cargo_metadata::MetadataCommand;

/// The packages whose binaries a deployment runs, each with the binaries it ships.
const SHIPPED: &[(&str, &[&str])] = &[(
    "rdlt-connector-reference",
    &["rdlt-connector-files", "rdlt-connector-sqlite"],
)];

/// The features that add a test or certification surface, by the package that has them.
const SURFACES: &[(&str, &[&str])] = &[
    ("rdlt-connector", &["certify", "testing"]),
    ("rdlt-connector-reference", &["certify", "test-connectors"]),
    ("rdlt-engine", &["bench", "failpoints"]),
];

/// A way a shipped build carries what it must not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Violation {
    /// A build of `package` alone turns `feature` of `of` on.
    Surface {
        package: String,
        of: String,
        feature: String,
    },
    /// `package` builds `binary` without being asked, and it is not a binary it ships.
    Binary { package: String, binary: String },
    /// `package` no longer builds `binary`, which it ships, without being asked.
    Missing { package: String, binary: String },
}

/// The surfaces a build of `package` alone turns on, from `tree`: `cargo tree`'s lines of
/// `name version ...|features` for the build's packages.
pub(crate) fn surfaces(package: &str, tree: &str) -> Vec<Violation> {
    let mut violations = BTreeSet::new();
    for line in tree.lines() {
        let Some((named, features)) = line.split_once('|') else {
            continue;
        };
        let name = named.split(' ').next().unwrap_or_default();
        let Some((_, surfaces)) = SURFACES.iter().find(|(of, _)| *of == name) else {
            continue;
        };
        for feature in features.split(',').map(str::trim) {
            if surfaces.contains(&feature) {
                violations.insert((name.to_owned(), feature.to_owned()));
            }
        }
    }
    violations
        .into_iter()
        .map(|(of, feature)| Violation::Surface {
            package: package.to_owned(),
            of,
            feature,
        })
        .collect()
}

/// How the binaries `package` builds without being asked, `built`, differ from those it ships.
pub(crate) fn binaries(package: &str, ships: &[&str], built: &[String]) -> Vec<Violation> {
    let extra = built
        .iter()
        .filter(|binary| !ships.contains(&binary.as_str()))
        .map(|binary| Violation::Binary {
            package: package.to_owned(),
            binary: binary.clone(),
        });
    let missing = ships
        .iter()
        .filter(|binary| !built.iter().any(|built| built == *binary))
        .map(|binary| Violation::Missing {
            package: package.to_owned(),
            binary: (*binary).to_owned(),
        });
    extra.chain(missing).collect()
}

/// Checks every shipped package, and reports what its build alone carries that it must not.
pub(crate) fn run(root: &Path) -> anyhow::Result<ExitCode> {
    let metadata = MetadataCommand::new()
        .current_dir(root)
        .no_deps()
        .exec()
        .context("running cargo metadata")?;
    let mut violations = Vec::new();
    for (package, ships) in SHIPPED {
        let tree = Command::new("cargo")
            .current_dir(root)
            .args(["tree", "--locked", "--edges", "normal", "--prefix", "none"])
            .args(["--format", "{p}|{f}", "--package", package])
            .output()
            .context("running cargo tree")?;
        anyhow::ensure!(tree.status.success(), "cargo tree failed for {package}");
        violations.extend(surfaces(package, &String::from_utf8_lossy(&tree.stdout)));
        let found = metadata
            .packages
            .iter()
            .find(|found| found.name.as_str() == *package)
            .with_context(|| format!("no package {package}"))?;
        let built: Vec<String> = found
            .targets
            .iter()
            .filter(|target| target.is_bin() && target.required_features.is_empty())
            .map(|target| target.name.clone())
            .collect();
        violations.extend(binaries(package, ships, &built));
    }
    for violation in &violations {
        #[expect(clippy::print_stderr, reason = "the check reports to its user")]
        {
            eprintln!("{}", describe(violation));
        }
    }
    Ok(if violations.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}

fn describe(violation: &Violation) -> String {
    match violation {
        Violation::Surface {
            package,
            of,
            feature,
        } => format!("a build of {package} alone turns on `{feature}` of {of}"),
        Violation::Binary { package, binary } => format!(
            "{package} builds `{binary}` without a feature, and does not ship it: give it \
             `required-features`, or list it as shipped"
        ),
        Violation::Missing { package, binary } => {
            format!("{package} ships `{binary}`, and no longer builds it without a feature")
        }
    }
}
