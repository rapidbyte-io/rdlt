//! The verbs that inspect or reset a pipeline: init, check, discover, schema, connectors and
//! state reset.

use crate::support::{Scratch, output_of};

/// A document loading `rows` of the memory source's stream `users` into SQLite beside it.
pub(crate) fn memory_document(scratch: &Scratch, name: &str, rows: &str) -> std::path::PathBuf {
    let database = scratch.path().join(format!("{name}.db"));
    scratch.document(
        &format!("{name}.yaml"),
        &format!(
            "version: 1\npipeline: {name}\nsource:\n  connector: io.rapidbyte.memory\n  config: {{ streams: {{ users: {rows} }} }}\ndestination:\n  connector: io.rapidbyte.sqlite\n  config: {{ path: {} }}\nstreams:\n  - name: users\n",
            database.display()
        ),
    )
}

pub(crate) fn reference_connectors() -> Scratch {
    Scratch::new()
        .connector("serve_reference", "memory")
        .connector("serve_reference", "sqlite")
        .connector("serve_source", "generator")
}

#[tokio::test]
async fn init_writes_a_starter_that_checks_and_refuses_to_overwrite() {
    let scratch = reference_connectors();
    assert_eq!(scratch.rdlt(&["init"]).await.status.code(), Some(0));
    let checked = scratch
        .rdlt(&["check", "rdlt.yaml", "--output", "json"])
        .await;
    assert_eq!(
        checked.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&checked.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&checked.stdout).unwrap();
    assert_eq!(report["streams"], serde_json::json!(["events"]));
    assert_eq!(scratch.rdlt(&["init"]).await.status.code(), Some(74));
}

#[tokio::test]
async fn discover_schema_and_connectors_print_what_they_find() {
    let scratch = reference_connectors();
    let document = memory_document(&scratch, "found", "[{ id: 1 }]");
    let document = document.to_str().unwrap();
    let catalog = scratch
        .rdlt(&["discover", document, "--output", "json"])
        .await;
    assert_eq!(catalog.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&catalog.stdout).contains("\"users\""));
    let schema = scratch.rdlt(&["schema", "--document"]).await;
    let schema: serde_json::Value = serde_json::from_slice(&schema.stdout).unwrap();
    assert!(schema["properties"]["streams"].is_object());
    let generator = scratch.rdlt(&["schema", "io.rapidbyte.generator"]).await;
    assert_eq!(generator.status.code(), Some(0));
    assert!(String::from_utf8_lossy(&generator.stdout).contains("seed"));
    let listed = scratch.rdlt(&["connectors", "--output", "json"]).await;
    let listed: serde_json::Value = serde_json::from_slice(&listed.stdout).unwrap();
    let names: Vec<&str> = listed["local"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|entry| entry["name"].as_str())
        .collect();
    assert_eq!(
        names,
        [
            "rdlt-connector-generator",
            "rdlt-connector-memory",
            "rdlt-connector-sqlite"
        ]
    );
}

/// A default host file that cannot be examined fails, where one that is not there is absent.
#[tokio::test]
async fn a_default_host_file_that_cannot_be_read_is_io_where_a_missing_one_is_no_host_file() {
    let scratch = Scratch::new();
    // `$XDG_CONFIG_HOME/rdlt/host.yaml` is beneath a file, which is not "absent".
    std::fs::write(scratch.path().join("config"), "a file").unwrap();
    let examined = output_of(&mut scratch.bare_command(&["connectors"])).await;
    assert_eq!(examined.status.code(), Some(74), "{examined:?}");
}

#[tokio::test]
async fn each_verb_prints_a_plain_result_when_asked() {
    let scratch = reference_connectors();
    let document = memory_document(&scratch, "plain", "[{ id: 1 }]");
    let document = document.to_str().unwrap();
    let says = [
        (
            vec!["check", document, "--output", "plain"],
            "ok: pipeline plain loads users",
        ),
        (vec!["discover", document, "--output", "plain"], "users"),
        (
            vec!["connectors", "--output", "plain"],
            "local connectors run trusted",
        ),
        (vec!["init", "--output", "plain"], "wrote rdlt.yaml"),
    ];
    for (args, said) in says {
        let output = scratch.rdlt(&args).await;
        let text = String::from_utf8_lossy(&output.stdout).into_owned();
        assert_eq!(output.status.code(), Some(0), "{args:?}: {text}");
        assert!(text.contains(said), "{args:?}: {text}");
    }
    assert_eq!(
        scratch.rdlt(&["run", document]).await.status.code(),
        Some(0)
    );
    let reset = scratch
        .rdlt(&["state", "reset", document, "users", "--output", "plain"])
        .await;
    assert!(String::from_utf8_lossy(&reset.stdout).contains("reset users at epoch"));
    let made = scratch
        .rdlt(&["connector", "new", "plainly", "--output", "plain"])
        .await;
    assert!(String::from_utf8_lossy(&made.stdout).contains("rdlt-connector-plainly"));
}

/// The starter, sandboxed as a host is by default: its first run leaves the database where it
/// says it does.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn the_starter_loads_for_real_in_the_sandbox() {
    let usable = rdlt_host::Bubblewrap::new().usable();
    if let Err(unusable) = &usable {
        rdlt_testkit::process::without_sandbox(unusable);
        return;
    }
    let scratch = Scratch::new()
        .connector("serve_reference", "sqlite")
        .connector("serve_source", "generator");
    let work = scratch.path().join("work");
    std::fs::create_dir(&work).unwrap();
    let mut init = scratch.bare_command(&["init"]);
    init.current_dir(&work);
    let output = output_of(&mut init).await;
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let connectors = scratch.path().join("connectors");
    let mut run = scratch.bare_command(&[
        "run",
        "rdlt.yaml",
        "--connectors",
        connectors.to_str().unwrap(),
        "--writable",
        work.to_str().unwrap(),
    ]);
    run.current_dir(&work);
    let output = output_of(&mut run).await;
    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let database = work.join("starter.db");
    assert!(database.is_file(), "the run left its database");
    let rows: i64 = rusqlite::Connection::open(&database)
        .unwrap()
        .query_row("SELECT count(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 1000);
}

/// A document loading a row of `users` into SQLite at `path`, its destination's `access` a line
/// of YAML or nothing.
#[cfg(target_os = "linux")]
fn into_sqlite(scratch: &Scratch, name: &str, path: &str, access: &str) -> std::path::PathBuf {
    let body = format!(
        "version: 1\npipeline: {name}\nsource:\n  connector: io.rapidbyte.memory\n  config: {{ streams: {{ users: [{{ id: 1 }}] }} }}\ndestination:\n  connector: io.rapidbyte.sqlite\n  config: {{ path: \"{path}\" }}\n{access}streams:\n  - name: users\n"
    );
    scratch.document(&format!("{name}.yaml"), &body)
}

/// `rdlt run document`, sandboxed by default, its connectors the scratch's.
#[cfg(target_os = "linux")]
fn run_sandboxed(scratch: &Scratch, document: &std::path::Path) -> tokio::process::Command {
    let connectors = scratch.path().join("connectors");
    let mut run = scratch.bare_command(&["run", document.to_str().expect("a UTF-8 path")]);
    run.arg("--connectors").arg(connectors);
    run
}

/// A destination beneath `/tmp` without a grant fails, as nothing of a connector outlives it
/// there, and one under a grant beneath `/tmp` holds what was committed.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_destination_beneath_tmp_fails_in_the_sandbox_unless_it_is_granted() {
    let usable = rdlt_host::Bubblewrap::new().usable();
    if let Err(unusable) = &usable {
        rdlt_testkit::process::without_sandbox(unusable);
        return;
    }
    let scratch = reference_connectors();
    let ungranted = format!("/tmp/rdlt-ungranted-{}.db", std::process::id());
    let document = into_sqlite(&scratch, "tmp-ungranted", &ungranted, "");
    let mut run = run_sandboxed(&scratch, &document);
    run.args(["--output", "json"]);
    let output = output_of(&mut run).await;
    assert_eq!(
        output.status.code(),
        Some(2),
        "a commit into nothing: {output:?}"
    );
    let said: serde_json::Value = serde_json::from_slice(&output.stdout).expect("JSON");
    assert_eq!(said["error"]["kind"], "config", "{said}");
    assert!(
        !std::path::Path::new(&ungranted).exists(),
        "nothing reached the host"
    );
    let granted = tempfile::tempdir_in("/tmp").unwrap();
    let kept = granted.path().join("kept.db");
    let access = format!(
        "  access: {{ write: [\"{}\"] }}\n",
        granted.path().display()
    );
    let document = into_sqlite(&scratch, "tmp-granted", kept.to_str().unwrap(), &access);
    let mut run = run_sandboxed(&scratch, &document);
    run.args(["--writable", granted.path().to_str().unwrap()]);
    let output = output_of(&mut run).await;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(0), "{stderr}");
    let rows: i64 = rusqlite::Connection::open(&kept)
        .unwrap()
        .query_row("SELECT count(*) FROM users", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 1, "a grant beneath /tmp holds what was committed");
}

/// A path holding bytes that are no text is reported, where a file system takes such a name, as
/// macOS's does not.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_path_that_is_not_text_is_reported_and_never_panics() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt as _;

    let scratch = reference_connectors();
    let connectors = scratch.path().join("connectors");
    let unnamed = OsStr::from_bytes(b"rdlt-connector-\xff");
    std::fs::write(connectors.join(unnamed), "").unwrap();
    let listed = scratch.rdlt(&["connectors", "--output", "json"]).await;
    assert_eq!(listed.status.code(), Some(0), "{listed:?}");
    let listed: serde_json::Value = serde_json::from_slice(&listed.stdout).unwrap();
    assert_eq!(
        listed["local"].as_array().map(Vec::len),
        Some(4),
        "{listed}"
    );
    let mut init = scratch.bare_command(&["init"]);
    init.arg(OsStr::from_bytes(b"bad\xff.yaml"))
        .args(["--output", "json"]);
    let initialized = output_of(&mut init).await;
    assert_eq!(initialized.status.code(), Some(0), "{initialized:?}");
    let wrote: serde_json::Value = serde_json::from_slice(&initialized.stdout).unwrap();
    assert!(wrote["wrote"].is_string(), "{wrote}");
    let elsewhere = scratch.path().join(OsStr::from_bytes(b"connectors-\xff"));
    std::fs::create_dir(&elsewhere).unwrap();
    for name in ["memory", "sqlite"] {
        let file = format!("rdlt-connector-{name}");
        std::fs::copy(connectors.join(&file), elsewhere.join(&file)).unwrap();
    }
    let document = memory_document(&scratch, "bytes", "[{ id: 1 }]");
    let mut check =
        scratch.bare_command(&["check", document.to_str().unwrap(), "--trust-binaries"]);
    check
        .arg("--connectors")
        .arg(&elsewhere)
        .args(["--output", "json"]);
    let checked = output_of(&mut check).await;
    assert_eq!(checked.status.code(), Some(0), "{checked:?}");
    let checked: serde_json::Value = serde_json::from_slice(&checked.stdout).unwrap();
    assert!(
        checked["source"]["placement"]["process"]["path"].is_string(),
        "{checked}"
    );
}

#[tokio::test]
async fn a_host_file_with_a_flag_over_it_says_what_the_host_allows() {
    let scratch = reference_connectors();
    let dir = scratch.path().join("connectors");
    let elsewhere = scratch.path().join("elsewhere");
    std::fs::create_dir(&elsewhere).unwrap();
    // The file finds no connector; the flag `Scratch::command` passes stands in place of it.
    let file = scratch.document(
        "host.yaml",
        &format!("connectors: [{}]\nreadable: [/data]\n", elsewhere.display()),
    );
    let listed = scratch
        .rdlt(&[
            "connectors",
            "--output",
            "json",
            "--host",
            file.to_str().unwrap(),
        ])
        .await;
    let listed: serde_json::Value = serde_json::from_slice(&listed.stdout).unwrap();
    assert_eq!(
        listed["local"].as_array().map(Vec::len),
        Some(3),
        "{listed}"
    );
    assert_eq!(
        listed["local"][0]["path"],
        serde_json::json!(dir.join("rdlt-connector-generator"))
    );
    // A host file found at its default place holds where nothing else says.
    let defaulted = Scratch::new();
    let config = defaulted.path().join("config/rdlt");
    std::fs::create_dir_all(&config).unwrap();
    std::fs::write(
        config.join("host.yaml"),
        "trust_binaries: true\nconnectors: [/nowhere]\n",
    )
    .unwrap();
    let listed = output_of(&mut defaulted.bare_command(&["connectors", "--output", "json"])).await;
    let listed: serde_json::Value = serde_json::from_slice(&listed.stdout).unwrap();
    assert_eq!(listed["sandboxed"], false, "{listed}");
    assert_eq!(
        listed["local"].as_array().map(Vec::len),
        Some(0),
        "{listed}"
    );
    let broken = scratch.document("broken.yaml", "conectors: [/opt]\n");
    let refused = scratch
        .rdlt(&["connectors", "--host", broken.to_str().unwrap()])
        .await;
    assert_eq!(
        refused.status.code(),
        Some(2),
        "a host file that is wrong is config"
    );
    let absent = scratch.rdlt(&["connectors", "--host", "absent.yaml"]).await;
    assert_eq!(
        absent.status.code(),
        Some(74),
        "a host file that cannot be read is I/O"
    );
}

#[tokio::test]
async fn with_no_host_file_or_flag_connectors_come_from_the_default_directory() {
    let scratch = Scratch::new();
    let connectors = scratch.path().join("data/rdlt/connectors");
    std::fs::create_dir_all(&connectors).unwrap();
    std::fs::write(connectors.join("rdlt-connector-sample"), "").unwrap();
    let listed = output_of(&mut scratch.bare_command(&["connectors", "--output", "json"])).await;
    assert_eq!(listed.status.code(), Some(0));
    let listed: serde_json::Value = serde_json::from_slice(&listed.stdout).unwrap();
    assert_eq!(
        listed["sandboxed"], true,
        "the default host is sandboxed: {listed}"
    );
    assert_eq!(
        listed["local"][0]["name"], "rdlt-connector-sample",
        "{listed}"
    );
}

#[tokio::test]
async fn a_document_asking_the_network_of_a_host_that_does_not_allow_it_is_refused() {
    let scratch = reference_connectors();
    let database = scratch.path().join("net.db");
    let document = scratch.document(
        "net.yaml",
        &format!(
            "version: 1\npipeline: net\nsource:\n  connector: io.rapidbyte.memory\n  config: {{ streams: {{ users: [{{ id: 1 }}] }} }}\n  access: {{ network: true }}\ndestination:\n  connector: io.rapidbyte.sqlite\n  config: {{ path: {} }}\nstreams:\n  - name: users\n",
            database.display()
        ),
    );
    let document = document.to_str().unwrap();
    // Sandboxed, so the document's asking is the operator's to allow.
    let refused =
        output_of(&mut scratch.bare_command(&["check", document, "--output", "json"])).await;
    assert_eq!(
        refused.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&refused.stderr)
    );
    let said: serde_json::Value = serde_json::from_slice(&refused.stdout).unwrap();
    assert_eq!(said["error"]["code"], "placement_refused", "{said}");
    let found = output_of(&mut scratch.command(&["discover", document, "--output", "json"])).await;
    assert_eq!(
        found.status.code(),
        Some(0),
        "trusting the binaries grants the network"
    );
}

#[tokio::test]
async fn state_reset_resets_what_a_run_committed() {
    let scratch = reference_connectors();
    let document = memory_document(&scratch, "kept", "[{ id: 1 }, { id: 2 }]");
    let document = document.to_str().unwrap();
    assert_eq!(
        scratch.rdlt(&["run", document]).await.status.code(),
        Some(0)
    );
    let reset = scratch
        .rdlt(&["state", "reset", document, "users", "--output", "json"])
        .await;
    assert_eq!(reset.status.code(), Some(0));
    let reset: serde_json::Value = serde_json::from_slice(&reset.stdout).unwrap();
    assert_eq!(reset["streams"], serde_json::json!(["users"]));
}
