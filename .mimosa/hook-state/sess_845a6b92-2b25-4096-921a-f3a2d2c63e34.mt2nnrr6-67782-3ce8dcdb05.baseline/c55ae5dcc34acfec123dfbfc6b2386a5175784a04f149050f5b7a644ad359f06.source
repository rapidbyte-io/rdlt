//! `Engine::check` — connectivity, discovery and plan checks with no
//! load session: the summary counts discovered streams, failures
//! classify exactly as a run's would, and NOTHING is created anywhere —
//! no workdir, no lock, no WAL, no destination session.

use async_trait::async_trait;
use rdlt_connector::error::SourceError;
use rdlt_connector::source::{ReadRequest, Source, StreamSpec};
use rdlt_connector::spec::ConnectorSpec;
use rdlt_core::error::Error;
use rdlt_engine::config::Config;
use rdlt_engine::engine::Engine;
use rdlt_testkit::memory;
use serde_json::json;

fn two_stream_source() -> memory::Source {
    memory::Source::new(vec![
        memory::Stream::new(
            StreamSpec::new("orders"),
            vec![memory::Batch::new(vec![json!({"id": 1})])],
        ),
        memory::Stream::new(StreamSpec::new("users"), vec![]),
    ])
}

/// The clean path: the summary counts what discovery declared, and the
/// configured workdir does not come into existence — a check takes no
/// lock, writes no WAL, opens no session, moves no rows.
#[tokio::test]
async fn a_clean_check_counts_streams_and_writes_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let workdir = dir.path().join("wd");
    let destination = memory::Destination::new();
    let engine = Engine::new(
        Config::new("check").with_workdir(&workdir),
        two_stream_source(),
        destination.clone(),
    );

    let summary = engine.check().await.expect("a clean check");
    assert_eq!(summary.streams, 2);
    assert!(
        !workdir.exists(),
        "a check must not create the workdir — no lock and no WAL"
    );
    assert!(
        dir.path()
            .read_dir()
            .expect("the tempdir is listable")
            .next()
            .is_none(),
        "a check leaves nothing on disk at all"
    );
    assert_eq!(destination.opens(), 0, "a check never opens a load session");
}

/// A source whose probe refuses surfaces as `Error::Source`, classified
/// exactly as a run would classify the same refusal (fatal stays
/// non-retryable), under the `<check>` pseudo-stream.
#[tokio::test]
async fn a_failing_source_probe_classifies_as_a_source_error() {
    struct Unreachable;

    #[async_trait]
    impl Source for Unreachable {
        fn spec(&self) -> ConnectorSpec {
            ConnectorSpec::new("unreachable", "0.0.0")
        }
        async fn check(&self) -> Result<(), SourceError> {
            Err(SourceError::fatal("credentials refused"))
        }
        async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
            Ok(vec![])
        }
        async fn read(&self, _request: ReadRequest) -> Result<(), SourceError> {
            Ok(())
        }
    }

    let engine = Engine::new(
        Config::new("check"),
        Unreachable,
        memory::Destination::new(),
    );
    let error = engine.check().await.expect_err("the probe refuses");
    match error {
        Error::Source {
            stream,
            message,
            retryable,
            ..
        } => {
            assert_eq!(stream.as_str(), "<check>");
            assert!(message.contains("credentials refused"), "{message}");
            assert!(!retryable, "a fatal probe refusal is not retryable");
        }
        other => panic!("a source probe failure is Error::Source: {other:?}"),
    }
}

/// The plan-validation leg is the run's own: a stream set a run would
/// refuse at plan time (two streams owning one table) refuses the check
/// identically, as `Error::Config`.
#[tokio::test]
async fn check_runs_the_plan_validation_a_run_would() {
    let colliding = memory::Source::new(vec![
        memory::Stream::new(StreamSpec::new("Users"), vec![]),
        memory::Stream::new(StreamSpec::new("users"), vec![]),
    ]);
    let engine = Engine::new(Config::new("check"), colliding, memory::Destination::new());
    let error = engine.check().await.expect_err("one stream owns a table");
    assert!(
        matches!(error, Error::Config { .. }),
        "a plan collision refuses as config: {error:?}"
    );
}
