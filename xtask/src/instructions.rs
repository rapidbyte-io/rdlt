//! `cargo xtask instructions`: counts the instructions and allocations each case of the engine's
//! `allocations` benchmark takes per iteration, on this tree and on a base revision built beside
//! it with the same toolchain, and fails where a case takes more than a limit above the base.
//!
//! A case is counted under callgrind at two iteration counts, and its count is their difference
//! over the iterations between them: what the process does once, making the case's inputs among
//! it, cancels out, and every thread is counted, so work moved to another thread still counts.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use cargo_metadata::Message;

/// The iteration counts a case is counted at; neither is its first iteration alone.
const ITERATIONS: (u64, u64) = (1, 3);

/// The oldest valgrind the counts are taken under: Ubuntu 24.04's, which CI installs.
pub(crate) const VALGRIND: (u32, u32) = (3, 22);

/// The commit trailer naming a case whose growth the commit's change accepts.
pub(crate) const ACCEPTED: &str = "Instructions-Accepted";

/// What one iteration of a case takes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Counts {
    pub(crate) instructions: u64,
    pub(crate) allocations: u64,
}

/// One case's counts on the base revision and on this tree, where it exists.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Counted {
    pub(crate) case: String,
    pub(crate) base: Option<Counts>,
    pub(crate) head: Option<Counts>,
}

/// What a case's counts come to against the limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// Neither count more than the limit above the base's.
    Within,
    /// A count above the limit, and no commit accepts it.
    Grew,
    /// A count above the limit, and a commit's trailer accepts it.
    Accepted,
    /// Only this tree has the case.
    New,
    /// Only the base has the case.
    Gone,
}

/// The allocations a run of the `allocations` benchmark made, from the line it prints.
pub(crate) fn allocations(output: &str) -> anyhow::Result<u64> {
    let count = output
        .lines()
        .find_map(|line| line.strip_prefix("allocations "))
        .context("the run printed no allocation count")?;
    count
        .trim()
        .parse()
        .with_context(|| format!("the allocation count {count:?} is not a count"))
}

/// The instructions a callgrind output file counts in all, from its `totals:` line.
pub(crate) fn totals(callgrind: &str) -> anyhow::Result<u64> {
    let line = callgrind
        .lines()
        .find_map(|line| line.strip_prefix("totals:"))
        .context("the callgrind output has no totals line")?;
    let first = line
        .split_whitespace()
        .next()
        .context("the callgrind totals line is empty")?;
    first
        .parse()
        .with_context(|| format!("the callgrind total {first:?} is not a count"))
}

/// A count per iteration, from the totals counted at each of [`ITERATIONS`].
pub(crate) fn per_iteration(fewer: u64, more: u64) -> anyhow::Result<u64> {
    let grown = more
        .checked_sub(fewer)
        .with_context(|| format!("{more} counted at more iterations is below {fewer}"))?;
    Ok(grown / (ITERATIONS.1 - ITERATIONS.0))
}

/// The change from `base` to `head`, in percent of `base`; any growth from none is infinite.
pub(crate) fn change(base: u64, head: u64) -> f64 {
    if base == 0 {
        return if head == 0 { 0.0 } else { f64::INFINITY };
    }
    #[expect(clippy::cast_precision_loss, reason = "counts are far below 2^52")]
    let (base, head) = (base as f64, head as f64);
    (head - base) * 100.0 / base
}

/// What `counted` comes to against `limit`, a percentage above the base's count, given the cases
/// commits accept.
pub(crate) fn verdict(counted: &Counted, limit: f64, accepted: &BTreeSet<String>) -> Verdict {
    match (counted.base, counted.head) {
        (None, _) => Verdict::New,
        (_, None) => Verdict::Gone,
        (Some(base), Some(head))
            if change(base.instructions, head.instructions) <= limit
                && change(base.allocations, head.allocations) <= limit =>
        {
            Verdict::Within
        }
        _ if accepted.contains(&counted.case) => Verdict::Accepted,
        _ => Verdict::Grew,
    }
}

/// The cases named by trailers, one value a line, as `git log --format=%(trailers)` prints them.
pub(crate) fn accepted(trailers: &str) -> BTreeSet<String> {
    trailers
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

/// The first case a trailer names that neither tree has.
pub(crate) fn unknown<'a>(accepted: &'a BTreeSet<String>, counted: &[Counted]) -> Option<&'a str> {
    accepted
        .iter()
        .find(|case| !counted.iter().any(|counted| counted.case == **case))
        .map(String::as_str)
}

/// A Markdown table of every case's counts, their changes and its verdict.
pub(crate) fn table(counted: &[Counted], limit: f64, accepted: &BTreeSet<String>) -> String {
    let mut table = String::from(
        "| Case | Instructions | Change | Allocations | Change | Verdict |\n\
         |---|---:|---:|---:|---:|---|\n",
    );
    for case in counted {
        let metric = |of: fn(Counts) -> u64| match (case.base, case.head) {
            (Some(base), Some(head)) => (
                format!("{} → {}", of(base), of(head)),
                format!("{:+.2}%", change(of(base), of(head))),
            ),
            (Some(only), None) | (None, Some(only)) => (of(only).to_string(), "-".to_owned()),
            (None, None) => ("-".to_owned(), "-".to_owned()),
        };
        let (instructions, instructions_changed) = metric(|counts| counts.instructions);
        let (allocations, allocations_changed) = metric(|counts| counts.allocations);
        let verdict = match verdict(case, limit, accepted) {
            Verdict::Within => "within".to_owned(),
            Verdict::Grew => format!("more than {limit}% above the base"),
            Verdict::Accepted => format!("accepted by an {ACCEPTED} trailer"),
            Verdict::New => "new".to_owned(),
            Verdict::Gone => "gone".to_owned(),
        };
        writeln!(
            table,
            "| `{}` | {instructions} | {instructions_changed} | {allocations} | \
             {allocations_changed} | {verdict} |",
            case.case
        )
        .expect("writing to a string");
    }
    table
}

/// The major and minor version in what `valgrind --version` prints.
pub(crate) fn valgrind_version(printed: &str) -> anyhow::Result<(u32, u32)> {
    let version = printed
        .trim()
        .strip_prefix("valgrind-")
        .with_context(|| format!("{printed:?} names no valgrind version"))?;
    let mut parts = version.split('.');
    let mut part = |name: &str| -> anyhow::Result<u32> {
        let text = parts
            .next()
            .with_context(|| format!("valgrind's version {version:?} has no {name} number"))?;
        text.parse()
            .with_context(|| format!("valgrind's {name} version {text:?} is not a number"))
    };
    Ok((part("major")?, part("minor")?))
}

/// Counts every case on `base` and on the tree at `root`, writes the report to
/// `target/instructions/report.md`, prints it, and fails where a case grew past `limit` percent.
#[expect(clippy::print_stdout, reason = "the report is the command's output")]
pub(crate) fn run(root: &Path, base: &str, limit: f64) -> anyhow::Result<ExitCode> {
    anyhow::ensure!(
        cfg!(target_os = "linux"),
        "instruction counts need valgrind, which runs on Linux only"
    );
    let valgrind = printed(root, "valgrind", &["--version"])?;
    anyhow::ensure!(
        valgrind_version(&valgrind)? >= VALGRIND,
        "{valgrind} is older than valgrind-{}.{}, the oldest the counts are taken under",
        VALGRIND.0,
        VALGRIND.1
    );
    let out = root.join("target/instructions");
    fs::create_dir_all(&out).with_context(|| format!("creating {}", out.display()))?;
    let tree = Worktree::add(root, base)?;
    let started = Instant::now();
    let binaries = builds(root, tree.path(), &out, &toolchain(root)?)?;
    let built = started.elapsed();
    let counted = count_cases(&binaries, &out)?;
    let took = (built, started.elapsed().saturating_sub(built));
    let trailers = printed(
        root,
        "git",
        &[
            "log",
            &format!("--format=%(trailers:key={ACCEPTED},valueonly)"),
            &format!("{base}..HEAD"),
        ],
    )?;
    let accepted = accepted(&trailers);
    if let Some(unknown) = unknown(&accepted, &counted) {
        anyhow::bail!("an {ACCEPTED} trailer names {unknown:?}, which is no case");
    }
    let report = format!(
        "{}\n{}",
        heading(root, tree.path(), &valgrind, took)?,
        table(&counted, limit, &accepted)
    );
    fs::write(out.join("report.md"), &report).context("writing the report")?;
    println!("{report}");
    let grew = counted
        .iter()
        .any(|counted| verdict(counted, limit, &accepted) == Verdict::Grew);
    Ok(if grew {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    })
}

/// A detached checkout of a revision in a temporary directory outside this tree, so none of this
/// tree's cargo configuration reaches its build; removed when dropped.
struct Worktree {
    root: PathBuf,
    directory: tempfile::TempDir,
}

impl Worktree {
    fn add(root: &Path, revision: &str) -> anyhow::Result<Self> {
        // A run that was killed leaves its checkout registered after its directory is gone.
        printed(root, "git", &["worktree", "prune"])?;
        let directory = tempfile::Builder::new()
            .prefix("rdlt-instructions-base-")
            .tempdir()
            .context("making a directory for the base")?;
        let path = directory
            .path()
            .to_str()
            .context("the base's directory is not UTF-8")?;
        printed(
            root,
            "git",
            &["worktree", "add", "--detach", path, revision],
        )?;
        Ok(Self {
            root: root.to_path_buf(),
            directory,
        })
    }

    fn path(&self) -> &Path {
        self.directory.path()
    }
}

impl Drop for Worktree {
    fn drop(&mut self) {
        let removed = Command::new("git")
            .current_dir(&self.root)
            .args(["worktree", "remove", "--force"])
            .arg(self.directory.path())
            .status();
        drop(removed);
    }
}

/// The channel `rust-toolchain.toml` names, which both builds use.
fn toolchain(root: &Path) -> anyhow::Result<String> {
    let text = fs::read_to_string(root.join("rust-toolchain.toml"))
        .context("reading rust-toolchain.toml")?;
    let table: toml::Table = text.parse().context("parsing rust-toolchain.toml")?;
    table
        .get("toolchain")
        .and_then(|toolchain| toolchain.get("channel"))
        .and_then(toml::Value::as_str)
        .map(ToOwned::to_owned)
        .context("rust-toolchain.toml names no channel")
}

/// Builds the `allocations` benchmark of the base at `base` and of this tree side by side, each
/// into its own target directory under `out`, and returns their executables, the base's first.
fn builds(root: &Path, base: &Path, out: &Path, toolchain: &str) -> anyhow::Result<[PathBuf; 2]> {
    // Cargo fingerprints a workspace's units by paths within it, so two checkouts building into
    // one target directory would take each other's outputs as their own. Each build ends in a
    // long single-threaded link, so the two overlap.
    std::thread::scope(|scope| {
        let based = scope.spawn(|| build(base, &out.join("base"), toolchain));
        let head = build(root, &out.join("head"), toolchain);
        let based = based
            .join()
            .map_err(|_| anyhow::anyhow!("building the base panicked"))?;
        Ok([based?, head?])
    })
}

/// Builds the `allocations` benchmark of the workspace at `tree` into `target`, as `cargo bench`
/// builds it, and returns its executable.
fn build(tree: &Path, target: &Path, toolchain: &str) -> anyhow::Result<PathBuf> {
    let mut child = Command::new("cargo")
        .current_dir(tree)
        .env("RUSTUP_TOOLCHAIN", toolchain)
        .args(["bench", "--locked", "--package", "rdlt-engine"])
        .args(["--features", "bench", "--bench", "allocations", "--no-run"])
        .args([
            "--message-format",
            "json-render-diagnostics",
            "--target-dir",
        ])
        .arg(target)
        .stdout(Stdio::piped())
        .spawn()
        .context("running cargo bench")?;
    let stdout = child.stdout.take().context("cargo's output")?;
    let mut executable = None;
    for message in Message::parse_stream(BufReader::new(stdout)) {
        if let Message::CompilerArtifact(artifact) = message.context("reading cargo's output")?
            && artifact.target.name == "allocations"
        {
            executable = artifact.executable.or(executable);
        }
    }
    let status = child.wait().context("waiting for cargo bench")?;
    anyhow::ensure!(
        status.success(),
        "building the benchmark in {} failed",
        tree.display()
    );
    executable
        .map(PathBuf::from)
        .with_context(|| format!("{} has no allocations benchmark", tree.display()))
}

/// Every case's counts on both sides, from `binaries`, the base's first.
fn count_cases(binaries: &[PathBuf; 2], out: &Path) -> anyhow::Result<Vec<Counted>> {
    let mut counts: BTreeMap<String, [Option<Counts>; 2]> = BTreeMap::new();
    for (side, binary) in binaries.iter().enumerate() {
        let kept = out.join(["base", "head"][side]).join("callgrind");
        for case in cases(binary)? {
            let count = count(binary, &case, &kept)?;
            counts.entry(case).or_default()[side] = Some(count);
        }
    }
    Ok(counts
        .into_iter()
        .map(|(case, [base, head])| Counted { case, base, head })
        .collect())
}

/// The cases `binary` runs, from the test list every revision's root prints; a base from before
/// the cases lists its benches alone, so it has none.
fn cases(binary: &Path) -> anyhow::Result<Vec<String>> {
    let output = Command::new(binary)
        .arg("--list")
        .output()
        .with_context(|| format!("running {}", binary.display()))?;
    anyhow::ensure!(
        output.status.success(),
        "{} --list failed",
        binary.display()
    );
    Ok(listed_cases(&String::from_utf8_lossy(&output.stdout)))
}

/// The cases in `list`, a test harness's list of the root's tests: those whose names hold a `/`,
/// which no bench's does.
pub(crate) fn listed_cases(list: &str) -> Vec<String> {
    list.lines()
        .filter_map(|line| line.strip_suffix(": test"))
        .filter(|name| name.contains('/'))
        .map(ToOwned::to_owned)
        .collect()
}

/// What one iteration of `case` takes, counted under callgrind with its output kept in `out`.
fn count(binary: &Path, case: &str, out: &Path) -> anyhow::Result<Counts> {
    fs::create_dir_all(out).with_context(|| format!("creating {}", out.display()))?;
    let mut at = Vec::new();
    for iterations in [ITERATIONS.0, ITERATIONS.1] {
        let file = out.join(format!("{}.{iterations}.out", case.replace('/', "-")));
        let run = Command::new("valgrind")
            .arg("--tool=callgrind")
            // Threads take valgrind's lock in turn rather than as the kernel wakes them.
            .arg("--fair-sched=yes")
            .arg(format!("--callgrind-out-file={}", file.display()))
            .arg(binary)
            .args([case, &iterations.to_string()])
            .output()
            .context("running valgrind, which must be installed")?;
        anyhow::ensure!(
            run.status.success(),
            "{case} failed under callgrind:\n{}",
            String::from_utf8_lossy(&run.stderr)
        );
        let text =
            fs::read_to_string(&file).with_context(|| format!("reading {}", file.display()))?;
        at.push(Counts {
            instructions: totals(&text)?,
            allocations: allocations(&String::from_utf8_lossy(&run.stdout))?,
        });
    }
    Ok(Counts {
        instructions: per_iteration(at[0].instructions, at[1].instructions)?,
        allocations: per_iteration(at[0].allocations, at[1].allocations)?,
    })
}

/// What the counts were taken with: both revisions, the compiler, valgrind, the build profile and
/// the load average, and how long building and counting took.
fn heading(
    root: &Path,
    base: &Path,
    valgrind: &str,
    (built, counted): (Duration, Duration),
) -> anyhow::Result<String> {
    let load = fs::read_to_string("/proc/loadavg").context("reading the load average")?;
    Ok(format!(
        "Base `{}`, this tree `{}`; {}; {valgrind}; built as `cargo bench` builds, in the \
         release profile; load average {}; built in {} s, counted in {} s\n",
        printed(base, "git", &["rev-parse", "--short=12", "HEAD"])?,
        printed(root, "git", &["rev-parse", "--short=12", "HEAD"])?,
        printed(root, "rustc", &["--version"])?,
        load.split_whitespace()
            .take(3)
            .collect::<Vec<_>>()
            .join(" "),
        built.as_secs(),
        counted.as_secs(),
    ))
}

/// What `program` printed, trimmed, run in `directory` with `arguments`.
fn printed(directory: &Path, program: &str, arguments: &[&str]) -> anyhow::Result<String> {
    let output = Command::new(program)
        .current_dir(directory)
        .args(arguments)
        .output()
        .with_context(|| format!("running {program}"))?;
    anyhow::ensure!(
        output.status.success(),
        "{program} {} failed: {}",
        arguments.join(" "),
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}
