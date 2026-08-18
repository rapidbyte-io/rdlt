//! A misbehaving connector at the SPI boundary must produce a TYPED error,
//! never a hang: pushing Arrow batches on a stream not declared
//! `structured` is a contract violation, and the host must surface it even
//! while the source sits parked on the byte budget mid-push.

use arrow::array::RecordBatch;
use async_trait::async_trait;
use rdlt_connector::error::SourceError;
use rdlt_connector::source::{ReadRequest, Source, StreamSpec};
use rdlt_connector::spec::ConnectorSpec;
use rdlt_core::error::Error;
use rdlt_engine::config::Config;
use rdlt_engine::engine::Engine;
use rdlt_testkit::fixtures::batch_of;
use rdlt_testkit::memory;

struct ArrowOnUnstructured;

fn batch() -> RecordBatch {
    batch_of(&[1, 2, 3])
}

#[async_trait]
impl Source for ArrowOnUnstructured {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("misbehaving", "0.0.0")
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        // NOT declared structured — pushing Arrow violates the contract.
        Ok(vec![StreamSpec::new("s")])
    }

    async fn read(&self, mut request: ReadRequest) -> Result<(), SourceError> {
        // First push violates the contract; keep pushing so the source is
        // parked on the byte budget when the host breaks out — the exact
        // shape that used to await a reader that could never finish.
        loop {
            if request.out.arrow(batch()).await.is_err() {
                return Ok(());
            }
        }
    }
}

/// A source that trips a typed ingest refusal and then parks between frames
/// (the wire-adapter shape: waiting on its connector process, never touching
/// the channel again). Holds an `Arc` the test can probe: if the host leaks
/// the reader task after the refusal, the task's `Arc<dyn Source>` keeps
/// this alive and the probe still upgrades after the run has returned.
struct RefusalThenParked {
    /// Held, never read: its whole job is to be kept alive (or not) by
    /// whoever still owns this source when the run is over.
    _alive: std::sync::Arc<()>,
}

#[async_trait]
impl Source for RefusalThenParked {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("refusal-then-parked", "0.0.0")
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        Ok(vec![StreamSpec::new("s")])
    }

    async fn read(&self, mut request: ReadRequest) -> Result<(), SourceError> {
        // One legal-size slab carrying one root past the row cap: the host
        // refuses it typed, mid-loop.
        let cap = rdlt_connector::channel::MAX_RECORD_BATCH_ROWS;
        let mut slab = Vec::with_capacity((cap + 1) * 2);
        for _ in 0..=cap {
            slab.extend_from_slice(b"0\n");
        }
        let _ = request.out.raw_json(slab.into()).await;
        // Then park forever — only the host's bounded abort can reap this.
        std::future::pending::<()>().await;
        Ok(())
    }
}

/// An ingest refusal must not leak the reader: every error exit of the push
/// loop still closes the channel and reaps the spawned reader task within
/// the bounded grace, or an embedder re-running on a schedule accumulates a
/// parked task (holding the source and its channel) per refused run.
#[tokio::test(flavor = "multi_thread")]
async fn a_typed_refusal_still_reaps_the_reader_task() {
    let alive = std::sync::Arc::new(());
    let probe = std::sync::Arc::downgrade(&alive);
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        Engine::new(
            Config::new("refusal-cleanup"),
            RefusalThenParked { _alive: alive },
            memory::Destination::new(),
        )
        .run(),
    )
    .await
    .expect("the run must terminate, not hang");
    let err = outcome.expect_err("the row-cap refusal is an error");
    assert!(
        err.to_string().contains("row cap"),
        "the typed refusal surfaces: {err}"
    );
    assert!(
        probe.upgrade().is_none(),
        "the reader task must be gone after the run — a live probe means the \
         parked reader still holds its Arc<dyn Source>"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn arrow_on_unstructured_stream_fails_typed_and_terminates() {
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        Engine::new(
            Config::new("contract"),
            ArrowOnUnstructured,
            memory::Destination::new(),
        )
        .run(),
    )
    .await
    .expect("the run must terminate, not hang");
    let err = outcome.expect_err("contract violation is an error");
    assert!(
        matches!(err, Error::Source { .. }) && err.to_string().contains("structured"),
        "typed contract error expected: {err}"
    );
}
