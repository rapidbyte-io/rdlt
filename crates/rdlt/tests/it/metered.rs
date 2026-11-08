//! A run's metrics: what it committed by pipeline and stream, and gauges of its status, labelled
//! by pipeline and stream alone.

use std::sync::Arc;

use metrics::{
    Counter, Gauge, GaugeFn, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use parking_lot::Mutex;
use rdlt::{RunStatus, StopMode};

use crate::events::committed;
use crate::support::{memory_host, memory_pipeline, within};

#[tokio::test]
async fn a_run_counts_what_it_committed_and_samples_its_status() {
    let debugging = DebuggingRecorder::new();
    let snapshotter = debugging.snapshotter();
    let _recording = metrics::set_default_local_recorder(&debugging);
    let rows: Vec<serde_json::Value> = (0..20).map(|id| serde_json::json!({ "id": id })).collect();
    let pipeline = memory_pipeline(
        "metered",
        "metrics-metered",
        &serde_json::Value::Array(rows),
    )
    .commit(rdlt::Commit::new().rows(5))
    .build(&memory_host())
    .await
    .expect("built");
    let outcome = within(pipeline.start()).await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    let taken = snapshotter.snapshot().into_vec();
    let value = |name: &str, stream: Option<&str>| {
        taken.iter().find_map(|(key, _, _, value)| {
            let key = key.key();
            let labelled = match stream {
                Some(stream) => key
                    .labels()
                    .any(|label| label.key() == "stream" && label.value() == stream),
                None => key.labels().all(|label| label.key() != "stream"),
            };
            (key.name() == name && labelled).then_some(value)
        })
    };
    assert_eq!(
        value("rdlt_rows_committed_total", Some("users")),
        Some(&DebugValue::Counter(outcome.report.rows))
    );
    assert_eq!(
        value("rdlt_commits_total", None),
        Some(&DebugValue::Counter(outcome.report.commits))
    );
    assert!(value("rdlt_commit_seconds", None).is_some());
    assert!(value("rdlt_commit_flush_seconds", None).is_some());
    assert!(value("rdlt_commit_publish_seconds", None).is_some());
    assert!(value("rdlt_commit_ack_seconds", None).is_some());
    assert!(
        value("rdlt_partitions_reading", None).is_some(),
        "the status is sampled"
    );
    for (key, ..) in &taken {
        for label in key.key().labels() {
            assert!(matches!(label.key(), "pipeline" | "stream"), "{key:?}");
        }
    }
}

/// A recorder that keeps every value a gauge is set to, in order: reading a debugging recorder's
/// gauge takes the value from it.
#[derive(Clone, Default)]
struct Sets(Arc<Mutex<Vec<(String, f64)>>>);

struct SetGauge(String, Sets);

impl GaugeFn for SetGauge {
    fn increment(&self, _: f64) {}

    fn decrement(&self, _: f64) {}

    fn set(&self, value: f64) {
        self.1.0.lock().push((self.0.clone(), value));
    }
}

impl Recorder for Sets {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, _: &Key, _: &Metadata<'_>) -> Counter {
        Counter::noop()
    }

    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::from_arc(Arc::new(SetGauge(key.name().to_owned(), self.clone())))
    }

    fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}

#[tokio::test]
async fn a_gauge_reads_nothing_once_its_run_is_over() {
    let sets = Sets::default();
    let _recording = metrics::set_default_local_recorder(&sets);
    // Rows enough to still be read when the run is stopped.
    let rows: Vec<serde_json::Value> = (0..200_000)
        .map(|id| serde_json::json!({ "id": id }))
        .collect();
    let source = rdlt::Side::new(rdlt::Connector::new("io.rapidbyte.memory"))
        .config(serde_json::json!({ "streams": { "users": rows }, "page_size": 1 }));
    let destination = rdlt::Side::new(rdlt::Connector::new("io.rapidbyte.memory"))
        .config(serde_json::json!({ "store": "metrics-gauged" }));
    let pipeline = rdlt::Pipeline::builder("gauged", source, destination)
        .stream(rdlt::Stream::new("users"))
        .commit(rdlt::Commit::new().rows(1))
        .build(&memory_host())
        .await
        .expect("built");
    let mut run = pipeline.start();
    let mut events = run.events();
    committed(&mut run, &mut events).await;
    // Wait for a sample that finds a partition reading, then stop: the next sample is a second
    // off, so the last one would stay the value of a run that is over without a final sample.
    within(async {
        while !sets
            .0
            .lock()
            .contains(&("rdlt_partitions_reading".to_owned(), 1.0))
        {
            tokio::select! {
                biased;
                outcome = &mut run => panic!("the run ended: {outcome:?}"),
                () = tokio::time::sleep(std::time::Duration::from_millis(5)) => {}
            }
        }
    })
    .await;
    run.stop(StopMode::Now);
    let outcome = within(run).await;
    assert_eq!(
        outcome.status(),
        RunStatus::Cancelled,
        "{:?}",
        outcome.error
    );
    let sets = sets.0.lock();
    let reading: Vec<f64> = sets
        .iter()
        .filter(|(name, _)| name == "rdlt_partitions_reading")
        .map(|(_, value)| *value)
        .collect();
    assert!(
        reading.contains(&1.0),
        "a partition read during the run: {reading:?}"
    );
    assert_eq!(
        reading.last(),
        Some(&0.0),
        "the last sample is of a run that is over"
    );
}
