//! The old defects the public surface must never bring back, each a test of public behaviour.

use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::path::Path;

use rdlt::{Commit, Connector, Host, Pipeline, Side, Stream};
use rdlt_connector_reference::{MemorySource, SqliteDestination};

use crate::support::{memory_host, within};

/// A subscriber that falls behind is told how many events it missed, and reads on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_subscriber_sees_lag() {
    let rows: Vec<serde_json::Value> = (0..40).map(|id| serde_json::json!({ "id": id })).collect();
    let source = Side::new(Connector::new("io.rapidbyte.memory"))
        .config(serde_json::json!({ "streams": { "users": rows }, "page_size": 5 }));
    let destination = Side::new(Connector::new("io.rapidbyte.memory"))
        .config(serde_json::json!({ "store": "ledger-lag" }));
    let pipeline = Pipeline::builder("lagging", source, destination)
        .stream(Stream::new("users"))
        .commit(Commit::new().rows(5))
        .build(&memory_host())
        .await
        .expect("built")
        .with_event_buffer(NonZeroUsize::new(2).unwrap());
    let run = pipeline.start();
    let mut slow = run.events();
    let outcome = within(run).await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    let lagged = within(slow.next()).await.expect("an item");
    let lagged = lagged.expect_err("the slow subscriber missed events");
    assert!(lagged.dropped > 0);
    let next = within(slow.next()).await.expect("an item");
    let next = next.expect("an event after the lag");
    assert_eq!(
        next.seq,
        lagged.dropped + 1,
        "it reads on from the first event kept"
    );
    let mut last = next.seq;
    while let Some(item) = within(slow.next()).await {
        let event = item.expect("no further lag");
        assert_eq!(event.seq, last + 1);
        last = event.seq;
    }
}

/// The names of the files in `directory`.
fn files(directory: &Path) -> BTreeSet<String> {
    let listed = std::fs::read_dir(directory).expect("the directory lists");
    let name = |entry: std::io::Result<std::fs::DirEntry>| {
        let entry = entry.expect("an entry reads");
        entry.file_name().to_string_lossy().into_owned()
    };
    listed.map(name).collect()
}

fn sqlite_host() -> Host {
    Host::new()
        .source::<MemorySource>()
        .destination::<SqliteDestination>()
}

/// A pipeline of the memory source's `streams` into the SQLite file at `database`.
fn into_sqlite(name: &str, streams: &serde_json::Value, database: &Path) -> rdlt::PipelineBuilder {
    let source = Side::new(Connector::new("io.rapidbyte.memory"))
        .config(serde_json::json!({ "streams": streams }));
    let destination = Side::new(Connector::new("io.rapidbyte.sqlite"))
        .config(serde_json::json!({ "path": database }));
    Pipeline::builder(name, source, destination)
}

/// The variable naming the directory a stand-in loads in, which is its working directory.
const LOADED_IN: &str = "RDLT_TEST_LOADED_IN";

/// Two pipelines whose names a sanitizer would make one share no file, and the facade keeps
/// none of its own: in a process of its own, working in a directory that holds nothing else,
/// each run adds only its destination's files there.
#[tokio::test]
async fn two_pipelines_leave_no_shared_file() {
    let directory = tempfile::tempdir().unwrap();
    let stand_in = rdlt_testkit::process::stand_in("ledger::two_pipelines_load_in_their_directory");
    let mut command = tokio::process::Command::from(stand_in);
    command
        .current_dir(directory.path())
        .env(LOADED_IN, directory.path())
        .kill_on_drop(true);
    let output = within(command.output()).await.expect("the stand-in runs");
    let (said, stderr) = (
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(output.status.success(), "{said}\n{stderr}");
    assert!(
        said.contains("1 passed"),
        "the stand-in ran: {said}\n{stderr}"
    );
}

/// What [`two_pipelines_leave_no_shared_file`] runs in a process of its own; nothing where no
/// test runs it so.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "a stand-in, run by two_pipelines_leave_no_shared_file in a directory of its own"]
async fn two_pipelines_load_in_their_directory() {
    let Some(directory) = std::env::var_os(LOADED_IN).map(std::path::PathBuf::from) else {
        return;
    };
    // The system names its working directory as it resolves it, and a temporary directory may
    // be reached through a link, as macOS's beneath /var is.
    let working = std::fs::canonicalize(std::env::current_dir().unwrap()).unwrap();
    assert_eq!(working, std::fs::canonicalize(&directory).unwrap());
    let host = sqlite_host();
    let mut seen = files(&directory);
    let mut added = Vec::new();
    for (name, database) in [("orders-a", "a.db"), ("orders_a", "b.db")] {
        let rows = serde_json::json!({ "users": [{ "id": 1 }] });
        let building = into_sqlite(name, &rows, &directory.join(database))
            .stream(Stream::new("users"))
            .build(&host);
        let pipeline = within(building).await.expect("built");
        assert!(within(pipeline.start()).await.error.is_none());
        let now = files(&directory);
        let new: BTreeSet<String> = now.difference(&seen).cloned().collect();
        assert!(
            new.iter().all(|file| file.starts_with(database)),
            "{name} added {new:?}"
        );
        added.push(new);
        seen = now;
    }
    assert!(added[0].is_disjoint(&added[1]));
}

/// A connector no directory holds is a typed not-found error, its cause the host's.
#[cfg(feature = "local")]
#[tokio::test]
async fn missing_binary_is_not_found_error() {
    let empty = tempfile::tempdir().unwrap();
    let host = Host::new().trust_binaries().connectors(empty.path());
    let side = Side::new(Connector::new("io.example.missing"));
    let building = Pipeline::builder("missing", side.clone(), side)
        .stream(Stream::new("s"))
        .build(&host);
    let error = within(building).await.expect_err("refused");
    assert_eq!(
        error.code().map(rdlt::Code::as_str),
        Some("connector_not_found")
    );
    assert_eq!(
        (error.kind(), error.origin()),
        (rdlt::ErrorKind::Config, rdlt::Origin::Source)
    );
    let cause = std::error::Error::source(&error)
        .and_then(|cause| cause.downcast_ref::<rdlt_host::ProviderError>());
    assert!(
        matches!(cause, Some(rdlt_host::ProviderError::NotFound { .. })),
        "{error:?}"
    );
}

/// One resolution of policies, column over stream over pipeline, from a document's defaults, a
/// stream's rules and a column's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn policy_inherits_column_table_stream_pipeline() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("policies.db");
    let pipeline = |rows: &serde_json::Value| {
        let text = format!(
            "version: 1\n\
             pipeline: policies\n\
             source:\n  connector: io.rapidbyte.memory\n  config: {{ streams: {rows} }}\n\
             destination:\n  connector: io.rapidbyte.sqlite\n  config: {{ path: \"{}\" }}\n\
             defaults: {{ schema: {{ policy: discard_value }} }}\n\
             streams:\n\
             \x20 - name: a\n\
             \x20   schema: {{ policy: evolve }}\n\
             \x20   columns: {{ x: {{ policy: discard_row }} }}\n\
             \x20 - name: b\n",
            database.display()
        );
        Pipeline::from_text(&text, "policies.yaml").expect("the document reads")
    };
    let first = serde_json::json!({ "a": [{ "id": 1, "x": 1 }], "b": [{ "id": 1, "y": 1 }] });
    let built = within(pipeline(&first).build(&sqlite_host()))
        .await
        .unwrap();
    assert!(within(built.start()).await.error.is_none());
    let second = serde_json::json!({
        "a": [{ "id": 2, "x": "text" }, { "id": 3, "x": 3, "z": 5 }],
        "b": [{ "id": 2, "y": "text" }],
    });
    let built = within(pipeline(&second).build(&sqlite_host()))
        .await
        .unwrap();
    let outcome = within(built.start()).await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    let (a, b) = (&outcome.report.streams["a"], &outcome.report.streams["b"]);
    assert_eq!(
        a.discarded_rows, 1,
        "the column's discard_row drops the row whose x changed"
    );
    assert_eq!(
        (b.discarded_rows, b.discarded_values > 0),
        (0, true),
        "the pipeline's discard_value nulls b's y"
    );
    let published = rdlt_connector_reference::sqlite::published(&database, "a").unwrap();
    assert!(
        published
            .iter()
            .any(|batch| batch.schema().field_with_name("z").is_ok()),
        "the stream's evolve added z"
    );
}
