#![forbid(unsafe_code)]
//! The observing example tells its run as its README shows, stops it cleanly asked from code or
//! at Ctrl-C, and a run after a stop reads on, landing every message once.

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// How long a test waits for the program to say something, or to end.
const PATIENCE: Duration = Duration::from_secs(60);

/// The line the program prints as it asks for the stop itself.
const ASKED: &str = "stopping after the next commit";

/// The line that tells a stopped run's end.
const ENDED: &str = "run ended: Stopped";

/// The starts of the lines whose numbers vary from run to run, besides the commits': the
/// report's, and the counters of what landed.
const VARYING: &[&str] = &[
    "Stopped: committed ",
    "rdlt_bytes_committed_total{",
    "rdlt_commits_total{",
    "rdlt_rows_committed_total{",
];

/// What the program prints, loading into `database` with `arguments`, where it succeeds.
fn run(database: &Path, arguments: &[&str]) -> String {
    let output: Output = Command::new(env!("CARGO_BIN_EXE_observing"))
        .arg(database)
        .args(arguments)
        .output()
        .expect("the program runs");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    String::from_utf8(output.stdout).expect("the program prints text")
}

/// The README's console block: the program's arguments after `cargo run -q -p observing --`,
/// and what it printed.
fn shown() -> (Vec<String>, String) {
    let example = Path::new(env!("CARGO_MANIFEST_DIR"));
    let readme = std::fs::read_to_string(example.join("README.md")).expect("a README");
    let block = readme
        .split("```console\n")
        .nth(1)
        .expect("a console block");
    let running = concat!("cargo run -q -p ", env!("CARGO_PKG_NAME"), " -- ");
    let mut runs: Vec<(Vec<String>, String)> = Vec::new();
    for line in block.split("```").next().unwrap_or_default().lines() {
        if let Some(command) = line.strip_prefix("$ ") {
            if let Some(arguments) = command.strip_prefix(running) {
                let arguments = arguments.split_whitespace().map(ToOwned::to_owned);
                runs.push((arguments.collect(), String::new()));
            } else {
                assert!(
                    !command.starts_with("cargo"),
                    "it runs no example: {command}"
                );
            }
        } else if let Some((_, printed)) = runs.last_mut() {
            printed.push_str(line);
            printed.push('\n');
        }
    }
    assert_eq!(runs.len(), 1, "one run is shown");
    runs.remove(0)
}

/// `line` with each run of digits in it as `#`: its form, whatever the run's counts and times.
fn form(line: &str) -> String {
    let mut formed = String::new();
    for character in line.chars() {
        match character {
            '0'..='9' if formed.ends_with('#') => {}
            '0'..='9' => formed.push('#'),
            other => formed.push(other),
        }
    }
    formed
}

/// The number a commit line tells and its rows, where `line` is one: `commit N: R rows in T ms`.
fn commit(line: &str) -> Option<(usize, u64)> {
    let (number, rest) = line.strip_prefix("commit ")?.split_once(": ")?;
    let (rows, took) = rest.split_once(" rows in ")?;
    took.strip_suffix(" ms")?.parse::<u64>().ok()?;
    Some((number.parse().ok()?, rows.parse().ok()?))
}

/// The rows of each commit `printed` tells, in order.
fn commits(printed: &str) -> Vec<u64> {
    printed
        .lines()
        .filter_map(commit)
        .map(|(_, rows)| rows)
        .collect()
}

/// The number at the end of the line of `lines` that starts with `prefix`.
fn told(lines: &[&str], prefix: &str) -> u64 {
    let line = lines.iter().find(|line| line.starts_with(prefix));
    let line = line.unwrap_or_else(|| panic!("{prefix} is told: {lines:?}"));
    let (_, number) = line.rsplit_once(' ').expect("a number at the end");
    number.parse().expect("a number")
}

/// A run stopped after `stop_after` commits as `printed` tells it, checked, and the rows it
/// committed; with the lines but its commits', those whose numbers vary as their form.
///
/// Each commit line has its form and comes in turn: `stop_after` of them before the stop is
/// asked, and any others between that and the run's end, as many as the run read meanwhile.
/// The commits' rows add up to the report's and the counter's, and their count to the commits
/// the report and the counter tell.
fn checked(printed: &str, stop_after: usize) -> (Vec<String>, u64) {
    let lines: Vec<&str> = printed.lines().collect();
    let place = |wanted: &str| lines.iter().position(|line| *line == wanted);
    let asked = place(ASKED).unwrap_or_else(|| panic!("the stop is asked: {printed}"));
    let ended = place(ENDED).unwrap_or_else(|| panic!("the run ends stopped: {printed}"));
    let (mut rows, mut others) = (Vec::new(), Vec::new());
    for (at, line) in lines.iter().enumerate() {
        if let Some((number, committed)) = commit(line) {
            assert_eq!(number, rows.len() + 1, "commit {number} in turn: {printed}");
            let placed = at < asked || (asked < at && at < ended);
            assert!(placed, "commit {number} in its place: {printed}");
            rows.push(committed);
        } else {
            assert!(!line.starts_with("commit "), "a commit's form: {line}");
            let varies = VARYING.iter().any(|start| line.starts_with(start));
            others.push(if varies {
                form(line)
            } else {
                (*line).to_owned()
            });
        }
    }
    let before = lines
        .get(asked.wrapping_sub(1))
        .and_then(|line| commit(line));
    assert_eq!(
        before.map(|(number, _)| number),
        Some(stop_after),
        "{printed}"
    );
    let total: u64 = rows.iter().sum();
    let report = format!("Stopped: committed {total} rows in {} commits", rows.len());
    assert!(lines.contains(&report.as_str()), "{report}: {printed}");
    assert_eq!(
        told(&lines, "rdlt_rows_committed_total{"),
        total,
        "{printed}"
    );
    let counted = u64::try_from(rows.len()).expect("a count of commits");
    assert_eq!(told(&lines, "rdlt_commits_total{"), counted, "{printed}");
    (others, total)
}

/// The count `query` answers in the SQLite database at `path`.
fn count(path: &Path, query: &str) -> i64 {
    let database = rusqlite::Connection::open(path).expect("the database opens");
    let counted = database.query_row(query, [], |row| row.get(0));
    counted.unwrap_or_else(|error| panic!("{query}: {error}"))
}

/// The messages the table holds, and those of them that are distinct.
fn held(database: &Path) -> (i64, i64) {
    let all = count(database, r#"SELECT count(*) FROM "events""#);
    let distinct = count(
        database,
        r#"SELECT count(*) FROM (SELECT DISTINCT "partition", "offset" FROM "events")"#,
    );
    (all, distinct)
}

#[test]
fn a_run_stopped_from_code_prints_what_the_readme_shows() {
    let (arguments, shown) = shown();
    assert_eq!(arguments.len(), 2, "{arguments:?}");
    let stop_after: usize = arguments[1].parse().unwrap();
    // The README's block holds as the program's output must, its own numbers adding up.
    let (expected, _) = checked(&shown, stop_after);
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("observing.db");
    let printed = run(&database, &[arguments[1].as_str()]);
    let (lines, total) = checked(&printed, stop_after);
    // Each line but the commits', the report's and the landed counters' is the README's, digits
    // and all; how many commits a run makes, and what it lands, depend on how fast it reads.
    assert_eq!(lines, expected, "{printed}");
    let total = i64::try_from(total).unwrap();
    assert_eq!(held(&database), (total, total));
}

#[test]
fn a_run_after_a_stop_reads_on_and_lands_every_message_once() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("observing.db");
    let stopped = run(&database, &["3"]);
    assert!(stopped.contains("\nrun ended: Stopped\n"), "{stopped}");
    let finished = run(&database, &[]);
    assert!(finished.contains("\nrun ended: Succeeded\n"), "{finished}");
    assert_eq!(held(&database), (1_000_000, 1_000_000));
    // Read to their heads, the logs hold nothing more.
    let again = run(&database, &[]);
    assert!(
        again.contains("\nSucceeded: committed 0 rows in "),
        "{again}"
    );
}

/// The program as a child of the test's, killed where the test ends before it does.
struct Running(Child);

impl Drop for Running {
    fn drop(&mut self) {
        // One that has ended already is not killed again.
        if matches!(self.0.try_wait(), Ok(None)) {
            self.0.kill().ok();
            self.0.wait().ok();
        }
    }
}

impl Running {
    /// Waits for the program to end, within the test's patience.
    fn ended(&mut self) -> ExitStatus {
        let deadline = Instant::now() + PATIENCE;
        loop {
            if let Some(status) = self.0.try_wait().expect("the program is waited for") {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "the program did not end within {PATIENCE:?}"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

#[test]
fn ctrl_c_stops_the_run_after_its_next_commit_and_exits_130() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("observing.db");
    let child = Command::new(env!("CARGO_BIN_EXE_observing"))
        .arg(&database)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("the program starts");
    let mut running = Running(child);
    let stdout = running.0.stdout.take().expect("its standard output");
    let (lines, told) = mpsc::channel();
    let reading = std::thread::Builder::new()
        .name("observing-stdout".to_owned())
        .spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if lines.send(line).is_err() {
                    break;
                }
            }
        })
        .expect("the reader starts");
    // Interrupted once a commit has landed, as a terminal's Ctrl-C interrupts it.
    let mut printed = Vec::new();
    while !printed
        .last()
        .is_some_and(|line: &String| line.starts_with("commit "))
    {
        let line = told.recv_timeout(PATIENCE);
        printed.push(line.unwrap_or_else(|_| panic!("the program tells a commit: {printed:?}")));
    }
    let pid = running.0.id().to_string();
    let interrupted = Command::new("kill").args(["-INT", &pid]).status();
    assert!(interrupted.expect("kill runs").success(), "kill -INT {pid}");
    let status = running.ended();
    reading.join().expect("the reader ends");
    printed.extend(told.try_iter());
    let stderr = std::io::read_to_string(running.0.stderr.take().expect("its standard error"));
    let stderr = stderr.expect("its standard error is text");
    let printed = printed.join("\n");
    // 130, a Ctrl-C's status, as the command line exits with it, though the run stopped cleanly.
    assert_eq!(status.code(), Some(130), "{printed}\n{stderr}");
    let note = "stopping after the next commit; interrupt again to stop now";
    assert!(stderr.contains(note), "{stderr}");
    assert!(printed.contains("\nrun ended: Stopped\n"), "{printed}");
    // What the run committed is in the table, each message once.
    let total = i64::try_from(commits(&printed).iter().sum::<u64>()).unwrap();
    assert_eq!(held(&database), (total, total), "{printed}");
}
