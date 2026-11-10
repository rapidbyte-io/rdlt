//! `cargo xtask tools`: checks that every tool `mise.toml` names is locked to the bytes it installs,
//! and that a workflow holding one apart from mise holds it at the version `mise.toml` names.

#[cfg(test)]
mod tests;

use std::fs;
use std::path::Path;
use std::process::ExitCode;
use std::sync::LazyLock;

use anyhow::Context as _;
use regex::Regex;
use toml::{Table, Value};

/// The platforms tools are installed on: CI's runners and the machines the project is built on.
pub(crate) const PLATFORMS: &[&str] = &["linux-x64", "macos-arm64"];

/// Tools their authors build for no more than some platforms, and the platforms they lack.
pub(crate) const UNBUILT: &[(&str, &str)] = &[
    ("github:rust-fuzz/cargo-fuzz", "macos-arm64"),
    ("github:sourcefrog/cargo-mutants", "macos-arm64"),
    ("aqua:rui314/mold", "macos-arm64"),
];

/// A tool the lockfile does not hold to one download.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Unlocked {
    /// The lockfile has no entry for the tool.
    Missing { tool: String },
    /// The lockfile's entry is for another version than `mise.toml` names.
    Version { tool: String },
    /// The entry has no download for a platform.
    Platform { tool: String, platform: String },
    /// The entry's download for a platform has no checksum, or one that is no digest.
    Checksum { tool: String, platform: String },
    /// The entry locks an npm package through a package lockfile that is missing, of other bytes
    /// than its digest, or names a package without its integrity.
    Packages { tool: String },
}

/// A tool a workflow holds apart from mise, by `SETUP_<TOOL>_VERSION`, at another version than
/// `mise.toml` names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Drifted {
    /// The workflow, by its path from the repository root.
    pub(crate) workflow: String,
    /// The tool, as `mise.toml` names it.
    pub(crate) tool: String,
    /// The version the workflow holds.
    pub(crate) held: String,
    /// The version `mise.toml` names, none where it names no such tool.
    pub(crate) named: Option<String>,
}

/// A workflow's `SETUP_<TOOL>_VERSION: "<version>"`, the tool's name in upper case.
static SETUP: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"(?m)^\s*SETUP_([A-Z0-9_]+)_VERSION:\s*"([^"]*)""#)
        .expect("the pattern is a valid regular expression")
});

/// Digest algorithms a checksum may use, each with the length of its digest in hex digits.
const DIGESTS: &[(&str, usize)] = &[("sha256:", 64), ("blake3:", 64)];

/// Every tool of the configuration `config` that the lockfile `lock` does not hold to one
/// download, reading a package lockfile it names, by its path from the repository root, with
/// `read`.
pub(crate) fn check(
    config: &str,
    lock: &str,
    read: &dyn Fn(&str) -> Option<Vec<u8>>,
) -> anyhow::Result<Vec<Unlocked>> {
    let config: Table = config.parse().context("reading mise.toml")?;
    let lock: Table = lock.parse().context("reading mise.lock")?;
    let empty = Table::new();
    let tools = tools_of(&config)?.unwrap_or(&empty);
    let locked = tools_of(&lock)?.unwrap_or(&empty);
    let mut all = Vec::new();
    for (tool, wanted) in tools {
        let version =
            version_of(wanted).with_context(|| format!("mise.toml names no version of {tool}"))?;
        let Some(entries) = locked.get(tool).and_then(Value::as_array) else {
            all.push(Unlocked::Missing { tool: tool.clone() });
            continue;
        };
        let at_version =
            |entry: &&Value| entry.get("version").and_then(Value::as_str) == Some(version);
        let Some(entry) = entries.iter().find(at_version) else {
            all.push(Unlocked::Version { tool: tool.clone() });
            continue;
        };
        if entry.get("aube").is_some() {
            all.extend(unlocked_packages(tool, entry, read));
            continue;
        }
        let built = |platform: &&&str| !UNBUILT.contains(&(tool.as_str(), **platform));
        for platform in PLATFORMS.iter().filter(built) {
            all.extend(unlocked_on(tool, entry, platform));
        }
    }
    // Whatever else the lockfile names, a locked install takes: it is held like the rest.
    for (tool, entries) in locked {
        let entries = entries.as_array().map_or(&[][..], Vec::as_slice);
        let tables = entries.iter().filter_map(Value::as_table);
        for (entry, key) in tables.flat_map(|entry| entry.keys().map(move |key| (entry, key))) {
            let unlocked = key
                .strip_prefix("platforms.")
                .and_then(|platform| unlocked_on(tool, &Value::Table(entry.clone()), platform));
            all.extend(unlocked.filter(|unlocked| !all.contains(unlocked)));
        }
    }
    Ok(all)
}

/// Every tool the workflows `workflows`, each its path and text, hold apart from mise at another
/// version than the configuration `config` names; mise itself, which `mise.toml` names no version
/// of, is each workflow's own.
pub(crate) fn drifted(
    config: &str,
    workflows: &[(String, String)],
) -> anyhow::Result<Vec<Drifted>> {
    let config: Table = config.parse().context("reading mise.toml")?;
    let empty = Table::new();
    let tools = tools_of(&config)?.unwrap_or(&empty);
    let mut all = Vec::new();
    for (workflow, text) in workflows {
        for held in SETUP.captures_iter(text) {
            if &held[1] == "MISE" {
                continue;
            }
            let tool = held[1].to_lowercase().replace('_', "-");
            let named = tools.get(&tool).and_then(version_of);
            if named != Some(&held[2]) {
                all.push(Drifted {
                    workflow: workflow.clone(),
                    tool,
                    held: held[2].to_owned(),
                    named: named.map(str::to_owned),
                });
            }
        }
    }
    Ok(all)
}

/// The version a `mise.toml` entry names: the entry itself, or its `version`.
fn version_of(wanted: &Value) -> Option<&str> {
    match wanted {
        Value::String(version) => Some(version.as_str()),
        Value::Table(options) => options.get("version").and_then(Value::as_str),
        _ => None,
    }
}

/// What the lockfile entry `entry` of `tool` lacks for `platform`: a download, or its digest.
fn unlocked_on(tool: &str, entry: &Value, platform: &str) -> Option<Unlocked> {
    let (tool, platform) = (tool.to_owned(), platform.to_owned());
    let download = entry.get(format!("platforms.{platform}"));
    let text = |key: &str| download.and_then(|download| download.get(key)?.as_str());
    if !text("url").is_some_and(|url| url.starts_with("https://")) {
        Some(Unlocked::Platform { tool, platform })
    } else if !text("checksum").is_some_and(is_digest) {
        Some(Unlocked::Checksum { tool, platform })
    } else {
        None
    }
}

/// What the entry `entry` of `tool`, an npm package mise locks through npm's own lockfile, lacks:
/// that lockfile at the path it names, bytes that hash to its digest, or an integrity for each
/// package the lockfile resolves.
fn unlocked_packages(
    tool: &str,
    entry: &Value,
    read: &dyn Fn(&str) -> Option<Vec<u8>>,
) -> Option<Unlocked> {
    use sha2::Digest as _;
    let aube = entry.get("aube")?;
    let path = aube.get("path").and_then(Value::as_str);
    let digest = aube
        .get("digest")
        .and_then(Value::as_str)
        .filter(|digest| digest.starts_with("sha256:") && is_digest(digest));
    let held = path.zip(digest).and_then(|(path, digest)| {
        let lockfile = read(&format!("{path}/aube-lock.yaml"))?;
        let found = format!("sha256:{:x}", sha2::Sha256::digest(&lockfile));
        (found == digest).then_some(lockfile)
    });
    let unheld = || Unlocked::Packages {
        tool: tool.to_owned(),
    };
    let Some(lockfile) = held else {
        return Some(unheld());
    };
    let text = String::from_utf8_lossy(&lockfile);
    let unhashed = text
        .lines()
        .filter(|line| line.trim_start().starts_with("resolution:"))
        .any(|line| !line.contains("integrity: sha512-"));
    unhashed.then(unheld)
}

/// The `tools` table of a mise configuration or lockfile, when it has one.
fn tools_of(file: &Table) -> anyhow::Result<Option<&Table>> {
    file.get("tools")
        .map(|tools| tools.as_table().context("`tools` is a table"))
        .transpose()
}

/// Whether `checksum` names a digest algorithm and carries a digest of its length.
fn is_digest(checksum: &str) -> bool {
    DIGESTS.iter().any(|(algorithm, length)| {
        checksum.strip_prefix(algorithm).is_some_and(|digest| {
            digest.len() == *length
                && digest
                    .bytes()
                    .all(|digit| digit.is_ascii_digit() || (b'a'..=b'f').contains(&digit))
        })
    })
}

/// Checks the repository at `root` and prints every tool its lockfile does not hold.
#[expect(clippy::print_stdout, reason = "findings are the command's output")]
pub(crate) fn run(root: &Path) -> anyhow::Result<ExitCode> {
    let read = |name: &str| {
        let path = root.join(name);
        fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))
    };
    let read_tracked = |path: &str| fs::read(root.join(path)).ok();
    let unlocked = check(&read("mise.toml")?, &read("mise.lock")?, &read_tracked)?;
    for tool in &unlocked {
        match tool {
            Unlocked::Missing { tool } => println!("error: mise.lock has no entry for {tool}"),
            Unlocked::Version { tool } => {
                println!("error: mise.lock holds another version of {tool} than mise.toml names");
            }
            Unlocked::Platform { tool, platform } => {
                println!("error: mise.lock has no download of {tool} for {platform}");
            }
            Unlocked::Checksum { tool, platform } => {
                println!("error: mise.lock has no checksum of {tool} for {platform}");
            }
            Unlocked::Packages { tool } => println!(
                "error: mise.lock's package lockfile of {tool} is missing, of other bytes than \
                 its digest, or names a package without its integrity"
            ),
        }
    }
    let drifted = drifted(&read("mise.toml")?, &workflows(root)?)?;
    for tool in &drifted {
        let Drifted {
            workflow,
            tool,
            held,
            named,
        } = tool;
        match named {
            Some(named) => {
                println!("error: {workflow} holds {tool} at {held}, mise.toml names {named}");
            }
            None => {
                println!("error: {workflow} holds {tool} at {held}, which mise.toml does not name");
            }
        }
    }
    Ok(if unlocked.is_empty() && drifted.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}

/// Every workflow of the repository at `root`, its path from the root and its text, in order.
fn workflows(root: &Path) -> anyhow::Result<Vec<(String, String)>> {
    let dir = root.join(".github/workflows");
    let listing = fs::read_dir(&dir).with_context(|| format!("listing {}", dir.display()))?;
    let mut all = Vec::new();
    for entry in listing {
        let path = entry
            .with_context(|| format!("listing {}", dir.display()))?
            .path();
        if path
            .extension()
            .is_some_and(|ext| ext == "yml" || ext == "yaml")
        {
            let text =
                fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
            let relative = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .display()
                .to_string();
            all.push((relative, text));
        }
    }
    all.sort();
    Ok(all)
}
