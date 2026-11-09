//! Secrets from the environment and from files, made to fail at each boundary through the
//! binary: no value appears, in any form a scrub or an encoding could leave it in, in what
//! `run`, `check` and `discover` print, as JSON and as plain text, on standard output and
//! standard error, nor in a run's events and report.
//!
//! Each place is shown able to fail too: a value the host was not told is a secret reaches it by
//! the same paths, and is found there.

use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;

use rdlt_testkit::secrets::leaks;

use crate::support::Scratch;
use crate::verbs::reference_connectors;

const ENV_SECRET: &str = "CANARY-env-4d2e";

/// What the file secret holds: two lines, each long enough to be found alone.
const FILE_SECRET: &str = "CANARY-file-9aa1\nCANARY-file-line-two-6f";

/// The reference a document makes to the environment's secret.
const ENV: &str = "${env:RDLT_CANARY}";

/// The words the failing connector says as it fails, with what it was sent scrubbed.
const CHECKED: &str = "the check refused ***";
const READ: &str = "the read failed holding ***";
const PANICKED: &str = "the read panicked holding ***";

/// Written out in a document: the host cannot know it is a secret, so nothing scrubs it.
const UNTOLD: &str = "CANARY-untold-3c8a";

/// Written out where nothing may quote it: a configuration's field, or a row a query fails on.
const UNQUOTED: &str = "CANARY-unquoted-5d0e";

/// Every secret the host is told of, and each line of the secret of several lines.
fn secrets() -> impl Iterator<Item = &'static str> {
    [ENV_SECRET, FILE_SECRET]
        .into_iter()
        .chain(FILE_SECRET.lines())
}

/// What a verb left in each place the scan reads.
struct Said {
    code: Option<i32>,
    stdout: String,
    stderr: String,
    report: String,
}

impl Said {
    fn places(&self) -> [(&'static str, &str); 3] {
        [
            ("stdout", &self.stdout),
            ("stderr", &self.stderr),
            ("report", &self.report),
        ]
    }
}

/// What `rdlt <verb>` printed of `case`'s document as `output` says, allowed its environment
/// secret and the secrets beneath `files`: a run also writes its events to standard output and
/// its report to a file.
async fn ran(scratch: &Scratch, verb: &str, output: &str, case: &Case, files: &Path) -> Said {
    let name = case.name;
    let document = scratch.document(&format!("{name}.yaml"), &case.document);
    let report = scratch.path().join(format!("{name}.{verb}.{output}.json"));
    let path = |path: &Path| path.to_str().expect("a UTF-8 path").to_owned();
    let (document, files, written) = (path(&document), path(files), path(&report));
    let mut args = vec![
        verb,
        &document,
        "--allow-env",
        "RDLT_CANARY",
        "--allow-files",
        &files,
        "--log-level",
        "trace",
        "--output",
        output,
    ];
    if verb == "run" {
        args.extend(["--events", "-", "--report", &written]);
    }
    let mut command = scratch.command(&args);
    command.env("RDLT_CANARY", ENV_SECRET);
    let output = crate::support::output_of(&mut command).await;
    Said {
        code: output.status.code(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        report: std::fs::read_to_string(&report).unwrap_or_default(),
    }
}

/// A document of `source`, YAML of a `source:` value, into SQLite, its rest `extra`.
fn document(name: &str, source: &str, extra: &str, database: &Path) -> String {
    format!(
        "version: 1\npipeline: {name}\nsource: {source}\ndestination:\n  connector: io.rapidbyte.sqlite\n  config: {{ path: {} }}\n{extra}",
        database.display()
    )
}

/// A document whose failing source fails at `fail`, saying `token`.
fn failing_document(name: &str, fail: &str, token: &str, database: &Path) -> String {
    let source = format!(
        "{{ connector: io.rapidbyte.failing, config: {{ fail: \"{fail}\", token: \"{token}\" }} }}"
    );
    let extra =
        "retry: { max_attempts: 2, initial: 1ms, max_delay: 1ms }\nstreams: [{ name: rows }]\n";
    document(name, &source, extra, database)
}

/// A document whose generator's one stream is named `stream`, loaded as every stream it offers.
fn named_document(name: &str, stream: &str, database: &Path) -> String {
    let source = format!(
        "{{ connector: io.rapidbyte.generator, config: {{ seed: 7, streams: [{{ name: \"{stream}\", rows: 3 }}] }} }}"
    );
    document(name, &source, "streams: [{ name: \"*\" }]\n", database)
}

/// A document whose memory source's `users` hold `rows`, loaded through a query that fails on a
/// row whose `name` is no integer.
fn sql_document(name: &str, rows: &str, database: &Path) -> String {
    let source =
        format!("{{ connector: io.rapidbyte.memory, config: {{ streams: {{ users: {rows} }} }} }}");
    let extra = "streams:\n  - name: users\n    transform: [{ sql: \"SELECT CAST(name AS BIGINT) AS n FROM input\" }]\n";
    document(name, &source, extra, database)
}

/// A connector directory with the failing and reference connectors, and a private directory
/// holding the file secret, with the reference a document makes to it.
fn prepared() -> (Scratch, tempfile::TempDir, String) {
    let scratch = reference_connectors().connector("serve_failing", "failing");
    let files = tempfile::tempdir().expect("a temporary directory");
    let token = files.path().join("token");
    std::fs::write(&token, FILE_SECRET).expect("the secret writes");
    let private = std::fs::Permissions::from_mode(0o600);
    std::fs::set_permissions(&token, private).expect("the secret is private");
    let reference = format!("${{file:{}}}", token.display());
    (scratch, files, reference)
}

/// A document the scan runs, the code its verbs exit with, the connector's words it must show,
/// the host's code its JSON error must have, and the verbs it runs beside `run`.
struct Case {
    name: &'static str,
    document: String,
    code: i32,
    says: Option<&'static str>,
    coded: Option<&'static str>,
    verbs: &'static [&'static str],
}

impl Case {
    fn new(name: &'static str, document: String, code: i32) -> Self {
        Self {
            name,
            document,
            code,
            says: None,
            coded: None,
            verbs: &[],
        }
    }
}

/// Each boundary a secret could leak by: a configuration the connector refuses, a check and a
/// read that say a secret, a read tried again, a panic, a SQL error on a row holding a secret, a
/// stream named by one, and a run that loads one.
fn cases(file: &str, database: &Path) -> Vec<Case> {
    let failures = [
        ("refused", ENV, "", 2, None),
        ("check-env", "check", ENV, 2, Some(CHECKED)),
        ("check-file", "check", file, 2, Some(CHECKED)),
        ("read-env", "read", ENV, 4, Some(READ)),
        ("read-file", "read", file, 4, Some(READ)),
        ("retried", "transient", ENV, 4, Some(READ)),
        ("panic", "panic", ENV, 4, Some(PANICKED)),
        ("loaded", "never", ENV, 0, None),
    ];
    let mut cases: Vec<Case> = failures
        .into_iter()
        .map(|(name, fail, token, code, says)| Case {
            says,
            ..Case::new(name, failing_document(name, fail, token, database), code)
        })
        .collect();
    let rows = format!("[{{ name: \"{ENV}\" }}]");
    cases.push(Case::new(
        "bad-sql",
        sql_document("bad-sql", &rows, database),
        6,
    ));
    cases.push(Case {
        coded: Some("named_by_secret"),
        verbs: &["check", "discover"],
        ..Case::new("named", named_document("named", ENV, database), 2)
    });
    cases
}

/// What each verb `case` runs left, as JSON and then as plain text, each verb's exit code
/// checked.
async fn each_said(scratch: &Scratch, case: &Case, files: &Path) -> Vec<Said> {
    let verbs: Vec<&str> = std::iter::once("run")
        .chain(case.verbs.iter().copied())
        .collect();
    let mut said = Vec::new();
    for verb in verbs {
        for output in ["json", "plain"] {
            let printed = ran(scratch, verb, output, case, files).await;
            let (name, stderr) = (case.name, &printed.stderr);
            let expected = Some(case.code);
            assert_eq!(
                printed.code, expected,
                "{name}, {verb} as {output}: {stderr}"
            );
            if let (Some(code), "json") = (case.coded, output) {
                // The host's code, and none of the connector's.
                let stdout = &printed.stdout;
                let coded = format!("\"code\":\"{code}\"");
                let by_host = stdout.contains(&coded) && stdout.contains("\"connector_code\":null");
                assert!(by_host, "{name}, {verb}: {stdout}");
            }
            said.push(printed);
        }
    }
    said
}

#[tokio::test]
async fn no_secret_reaches_the_command_line_s_output() {
    let (scratch, files, file) = prepared();
    let database = scratch.path().join("leak.db");
    for case in cases(&file, &database) {
        let name = case.name;
        for said in each_said(&scratch, &case, files.path()).await {
            if let Some(words) = case.says {
                let shown = said.places().iter().any(|(_, text)| text.contains(words));
                let (stdout, stderr) = (&said.stdout, &said.stderr);
                assert!(
                    shown,
                    "{name}: the connector's words reach it: {stdout} {stderr}"
                );
            }
            for (place, text) in said.places() {
                let found = leaks(text, secrets());
                assert_eq!(found, Vec::<String>::new(), "{name}, {place}: {text}");
            }
        }
    }
}

/// A value the host was not told is a secret takes the paths of each boundary unscrubbed, so
/// the scan above finds what reaches each place it reads.
#[tokio::test]
async fn a_secret_that_reaches_a_place_is_found_there() {
    let (scratch, files, _) = prepared();
    let database = scratch.path().join("found.db");
    let read = failing_document("untold-read", "read", UNTOLD, &database);
    let panic = failing_document("untold-panic", "panic", UNTOLD, &database);
    let named = Case {
        verbs: &["discover"],
        ..Case::new(
            "untold-named",
            named_document("untold-named", UNTOLD, &database),
            0,
        )
    };
    let read = each_said(&scratch, &Case::new("untold-read", read, 4), files.path()).await;
    let panic = each_said(&scratch, &Case::new("untold-panic", panic, 4), files.path()).await;
    let named = each_said(&scratch, &named, files.path()).await;
    // Each verb's run as JSON, then as plain text.
    let reached = [
        ("stdout as JSON", &read[0].stdout),
        ("stderr as plain text", &read[1].stderr),
        ("report", &read[0].report),
        ("stderr of a panic", &panic[0].stderr),
        ("events of a stream", &named[0].stdout),
        ("catalog discovered", &named[2].stdout),
    ];
    for (place, text) in reached {
        assert!(
            leaks(text, [UNTOLD]).contains(&UNTOLD.to_owned()),
            "the scan of the {place} finds what reaches it: {text}"
        );
    }
}

/// A configuration the connector refuses and a query that fails on a row are said without the
/// value they refuse, a secret or not: the scan above holds them to more than the scrub.
#[tokio::test]
async fn a_refusal_never_quotes_the_value_it_refuses() {
    let (scratch, files, _) = prepared();
    let database = scratch.path().join("unquoted.db");
    let refused = failing_document("unquoted-refused", UNQUOTED, "", &database);
    let refused = Case {
        verbs: &["check"],
        ..Case::new("unquoted-refused", refused, 2)
    };
    let rows = format!("[{{ name: \"{UNQUOTED}\" }}]");
    let failed = Case::new(
        "unquoted-sql",
        sql_document("unquoted-sql", &rows, &database),
        6,
    );
    for case in [refused, failed] {
        for said in each_said(&scratch, &case, files.path()).await {
            for (place, text) in said.places() {
                let (name, found) = (case.name, leaks(text, [UNQUOTED]));
                assert_eq!(found, Vec::<String>::new(), "{name}, {place}: {text}");
            }
        }
    }
}
