//! `cargo xtask tools`: checks that every tool `mise.toml` names is locked to the bytes it installs.

#[cfg(test)]
mod tests;

use std::fs;
use std::path::Path;
use std::process::ExitCode;

use anyhow::Context as _;
use toml::{Table, Value};

/// The platforms tools are installed on: CI's runners and the machines the project is built on.
pub(crate) const PLATFORMS: &[&str] = &["linux-x64", "macos-arm64"];

/// Tools their authors build for no more than some platforms, and the platforms they lack.
pub(crate) const UNBUILT: &[(&str, &str)] = &[
    ("github:rust-fuzz/cargo-fuzz", "macos-arm64"),
    ("github:sourcefrog/cargo-mutants", "macos-arm64"),
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
}

/// Digest algorithms a checksum may use, each with the length of its digest in hex digits.
const DIGESTS: &[(&str, usize)] = &[("sha256:", 64), ("blake3:", 64)];

/// Every tool of the configuration `config` that the lockfile `lock` does not hold to one download.
pub(crate) fn check(config: &str, lock: &str) -> anyhow::Result<Vec<Unlocked>> {
    let config: Table = config.parse().context("reading mise.toml")?;
    let lock: Table = lock.parse().context("reading mise.lock")?;
    let empty = Table::new();
    let tools = tools_of(&config)?.unwrap_or(&empty);
    let locked = tools_of(&lock)?.unwrap_or(&empty);
    let mut all = Vec::new();
    for (tool, wanted) in tools {
        let version = match wanted {
            Value::String(version) => Some(version.as_str()),
            Value::Table(options) => options.get("version").and_then(Value::as_str),
            _ => None,
        }
        .with_context(|| format!("mise.toml names no version of {tool}"))?;
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
    let unlocked = check(&read("mise.toml")?, &read("mise.lock")?)?;
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
        }
    }
    Ok(if unlocked.is_empty() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    })
}
