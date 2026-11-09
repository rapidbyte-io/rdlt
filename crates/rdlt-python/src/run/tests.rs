use std::sync::Arc;
use std::time::Duration;

use pyo3::exceptions::{PyKeyboardInterrupt, PyValueError};
use pyo3::types::{PyAnyMethods as _, PyTypeMethods as _};
use rdlt_connector::prelude::*;
use rdlt_connector_reference::{GeneratorSource, MemoryDestination};

use super::{Command, NativeRun, interrupted};
use crate::embedded;
use crate::pipeline::NativePipeline;
use crate::pipeline::tests::{memory_document, memory_host};
use crate::runtime::{self, Interrupted};

#[derive(serde::Deserialize, schemars::JsonSchema)]
struct Nothing {}

/// A source whose one stream fails every read with a data error.
struct Failing;

#[source(id = "test.failing")]
impl SourceConnector for Failing {
    type Config = Nothing;

    async fn connect(_config: Nothing, _context: &ConnectContext) -> Result<Self> {
        Ok(Self)
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new().with(Fails)
    }
}

struct Fails;

impl ReadStream<Failing> for Fails {
    type Cursor = u64;

    fn spec(&self) -> StreamSpec {
        StreamSpec::new(StreamName::new("fails").expect("a valid name"))
    }

    async fn read(&self, _: &Failing, _: &Partition, _: u64, _: &mut Emitter<u64>) -> Result<()> {
        Err(ConnectorError::data("the test source fails every read").with_code("test_failure"))
    }
}

fn started(py: pyo3::Python<'_>, document: &str, host: Arc<rdlt::Host>) -> NativeRun {
    let builder = rdlt::Pipeline::from_text(document, "test").unwrap();
    let pipeline = NativePipeline::built(py, builder, host).unwrap();
    pipeline.start(py).expect("it starts")
}

/// A run of the generator's billion rows into the memory store `name`, committing often, which
/// goes on until it is stopped.
fn endless(py: pyo3::Python<'_>, name: &str) -> NativeRun {
    let document = serde_json::json!({
        "version": 1, "pipeline": name,
        "source": { "connector": "io.rapidbyte.generator", "config": {
            "seed": 1, "streams": [{ "name": "rows", "rows": 1_000_000_000_u64 }] } },
        "destination": { "connector": "io.rapidbyte.memory", "config": { "store": name } },
        "streams": [{ "name": "rows" }],
        "commit": { "rows": 100 },
    })
    .to_string();
    let host = rdlt::Host::new()
        .source::<GeneratorSource>()
        .destination::<MemoryDestination>();
    started(py, &document, Arc::new(host))
}

#[test]
fn a_run_answers_its_outcome_once_it_ends() {
    embedded::with(|py| {
        let run = started(py, &memory_document("answered", 25), memory_host());
        let (outcome, error) = run.wait(py).expect("it ends");
        assert_eq!(error, None);
        let outcome: serde_json::Value = serde_json::from_str(&outcome).unwrap();
        assert_eq!(outcome["report"]["rows"], 25);
    });
}

#[test]
fn a_failed_run_answers_its_error_report() {
    embedded::with(|py| {
        let document = serde_json::json!({
            "version": 1, "pipeline": "failing",
            "source": { "connector": "test.failing", "config": {} },
            "destination": { "connector": "io.rapidbyte.memory", "config": { "store": "failing" } },
            "streams": [{ "name": "fails" }],
            "retry": { "max_attempts": 1 },
        })
        .to_string();
        let host = rdlt::Host::new()
            .source::<Failing>()
            .destination::<MemoryDestination>();
        let (_, error) = started(py, &document, Arc::new(host))
            .wait(py)
            .expect("it ends");
        let report: serde_json::Value = serde_json::from_str(&error.expect("it failed")).unwrap();
        assert_eq!(report["kind"], "source");
        assert_eq!(report["connector_code"], "test_failure");
    });
}

#[test]
fn the_first_subscription_tells_the_run_from_its_first_event() {
    embedded::with(|py| {
        let run = started(py, &memory_document("first", 5), memory_host());
        let events = run.events(py).unwrap();
        let first = events.__next__(py).unwrap().expect("an event");
        let first: serde_json::Value = serde_json::from_str(&first).unwrap();
        assert_eq!(first["event"]["seq"], 1);
        assert_eq!(first["event"]["kind"], "attempt_started");
    });
}

#[test]
fn events_end_with_the_run_gap_free() {
    embedded::with(|py| {
        let run = started(py, &memory_document("evented", 300), memory_host());
        let events = run.events(py).unwrap();
        let mut seqs = Vec::new();
        while let Some(item) = events.__next__(py).unwrap() {
            let item: serde_json::Value = serde_json::from_str(&item).unwrap();
            seqs.push(
                item["event"]["seq"]
                    .as_u64()
                    .expect("no event is lost here"),
            );
        }
        let expected: Vec<u64> = (1..=seqs.len() as u64).collect();
        assert_eq!(seqs, expected);
    });
}

#[test]
fn a_status_says_where_the_run_stands() {
    embedded::with(|py| {
        let run = endless(py, "status");
        let status: serde_json::Value = serde_json::from_str(&run.status(py).unwrap()).unwrap();
        assert!(status.get("phase").is_some(), "{status}");
        run.cancel(py).unwrap();
    });
}

#[test]
fn a_run_that_ended_still_answers_after_a_cancel() {
    embedded::with(|py| {
        let run = started(py, &memory_document("late-cancel", 3), memory_host());
        run.wait(py).expect("it ends");
        run.cancel(py).unwrap();
        let status: serde_json::Value = serde_json::from_str(&run.status(py).unwrap()).unwrap();
        assert_eq!(status["phase"]["ended"]["status"], "succeeded", "{status}");
        let again = run.wait(py).expect("its outcome stays");
        assert!(again.1.is_none());
    });
}

#[test]
fn a_first_interrupt_stops_after_commit_and_a_second_cancels() {
    embedded::with(|py| {
        let (commands, mut asked) = tokio::sync::mpsc::unbounded_channel();
        let mut stopping = false;
        let interrupt = || PyKeyboardInterrupt::new_err(());
        let first = interrupted(py, &commands, &mut stopping, interrupt());
        assert!(matches!(first, Interrupted::Continue));
        assert!(matches!(
            asked.try_recv(),
            Ok(Command::Stop(rdlt::StopMode::AfterCommit))
        ));
        let second = interrupted(py, &commands, &mut stopping, interrupt());
        assert!(matches!(second, Interrupted::Raise(_)));
        assert!(matches!(asked.try_recv(), Ok(Command::Cancel)));
        let mut fresh = false;
        let other = interrupted(py, &commands, &mut fresh, PyValueError::new_err(()));
        assert!(
            matches!(other, Interrupted::Raise(_)),
            "only Ctrl-C stops after commit"
        );
        assert!(matches!(asked.try_recv(), Ok(Command::Cancel)));
    });
}

#[test]
fn a_stop_after_commit_ends_the_run_stopped() {
    embedded::with(|py| {
        let run = endless(py, "after_commit");
        run.stop(py, "after_commit").unwrap();
        let (outcome, error) = run.wait(py).expect("it ends");
        assert_eq!(error, None, "{outcome}");
        let outcome: serde_json::Value = serde_json::from_str(&outcome).unwrap();
        assert_eq!(outcome["report"]["status"], "stopped");
    });
}

#[test]
fn a_stop_now_ends_the_run_cancelled() {
    embedded::with(|py| {
        let run = endless(py, "now");
        run.stop(py, "now").unwrap();
        let (outcome, error) = run.wait(py).expect("it ends");
        let outcome: serde_json::Value = serde_json::from_str(&outcome).unwrap();
        assert_eq!(outcome["report"]["status"], "cancelled");
        let report: serde_json::Value = serde_json::from_str(&error.expect("it was cut")).unwrap();
        assert_eq!(report["kind"], "cancelled");
    });
}

#[test]
fn a_stop_names_a_mode_that_exists() {
    embedded::with(|py| {
        let run = started(py, &memory_document("modes", 1), memory_host());
        let refused = run.stop(py, "sideways").unwrap_err();
        assert!(refused.is_instance_of::<PyValueError>(py));
        assert_eq!(
            refused.value(py).str().unwrap().to_string(),
            "no such stop mode"
        );
    });
}

#[test]
fn dropping_every_handle_cancels_the_run() {
    embedded::with(|py| {
        let run = endless(py, "dropped");
        let mut ended = run.ended();
        drop(run);
        let runtime = runtime::get(py).unwrap();
        let closed = py.detach(|| {
            let _entered = runtime.enter();
            runtime.block_on(tokio::time::timeout(
                Duration::from_secs(30),
                ended.changed(),
            ))
        });
        assert!(
            matches!(closed, Ok(Err(_))),
            "the driver ended without an outcome: {closed:?}"
        );
    });
}

#[test]
fn a_detached_run_finishes_though_every_handle_is_dropped() {
    embedded::with(|py| {
        let run = endless(py, "detached");
        let mut ended = run.ended();
        run.detach();
        drop(run);
        let runtime = runtime::get(py).unwrap();
        let settled = py.detach(|| {
            let _entered = runtime.enter();
            let waiting =
                tokio::time::timeout(Duration::from_secs(30), ended.wait_for(Option::is_some));
            matches!(runtime.block_on(waiting), Ok(Ok(_)))
        });
        assert!(settled, "the detached run finished and told its outcome");
    });
}

#[test]
fn a_subscriber_keeps_its_run_going_until_it_is_dropped_too() {
    embedded::with(|py| {
        let run = started(py, &memory_document("subscribed", 300), memory_host());
        let events = run.events(py).unwrap();
        let mut ended = run.ended();
        drop(run);
        let mut items = 0;
        while let Some(item) = events.__next__(py).unwrap() {
            let item: serde_json::Value = serde_json::from_str(&item).unwrap();
            items += 1;
            if item["event"]["kind"].get("run_ended").is_some() {
                break;
            }
        }
        assert!(items > 0, "the run told nothing");
        let runtime = runtime::get(py).unwrap();
        let settled = py.detach(|| {
            let _entered = runtime.enter();
            let waiting =
                tokio::time::timeout(Duration::from_secs(30), ended.wait_for(Option::is_some));
            matches!(runtime.block_on(waiting), Ok(Ok(_)))
        });
        assert!(settled, "a subscriber kept the run going to its end");
    });
}

#[test]
fn a_cancelled_run_answers_a_stopped_error() {
    embedded::with(|py| {
        let run = endless(py, "cancelled");
        run.cancel(py).unwrap();
        let raised = run.wait(py).unwrap_err();
        assert_eq!(
            raised.get_type(py).name().unwrap().to_string(),
            "StoppedError"
        );
        let status = run.status(py).unwrap_err();
        assert_eq!(
            status.get_type(py).name().unwrap().to_string(),
            "StoppedError"
        );
    });
}
