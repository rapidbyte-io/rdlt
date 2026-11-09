//! The verbs that inspect or reset a pipeline: init, check, discover, schema, connectors and
//! state reset.

use crate::support::Scratch;

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
    let mut listing = defaulted.bare_command(&["connectors", "--output", "json"]);
    let listed = listing.output().await.unwrap();
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
    let mut listing = scratch.bare_command(&["connectors", "--output", "json"]);
    let listed = listing.output().await.unwrap();
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
    let mut sandboxed = scratch.bare_command(&["check", document, "--output", "json"]);
    let refused = sandboxed.output().await.unwrap();
    assert_eq!(
        refused.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&refused.stderr)
    );
    let said: serde_json::Value = serde_json::from_slice(&refused.stdout).unwrap();
    assert_eq!(said["error"]["code"], "placement_refused", "{said}");
    let mut trusted = scratch.command(&["discover", document, "--output", "json"]);
    let found = trusted.output().await.unwrap();
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
