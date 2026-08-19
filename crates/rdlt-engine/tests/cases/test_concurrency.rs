//! The read-concurrency bound: discovery may declare many streams, but
//! only `max_concurrent_streams` of them read at once (default 16), the
//! knob admits more, and streams queued past the bound hold nothing the
//! commit barrier could wait on.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use rdlt_connector::error::SourceError;
use rdlt_connector::source::{ReadRequest, Source, StreamSpec};
use rdlt_connector::spec::ConnectorSpec;
use rdlt_core::commit::CommitPolicy;
use rdlt_core::cursor::Cursor;
use rdlt_engine::config::Config;
use rdlt_engine::engine::Engine;
use rdlt_testkit::memory;
use serde_json::json;

/// Concurrency observed from inside `read`: the current count and the
/// peak it ever reached.
#[derive(Default)]
struct Gauge {
    current: AtomicUsize,
    peak: AtomicUsize,
}

impl Gauge {
    fn enter(&self) -> usize {
        let now = self.current.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        now
    }
    fn exit(&self) {
        self.current.fetch_sub(1, Ordering::SeqCst);
    }
    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }
}

/// A source declaring `streams` streams whose reads each push one row,
/// checkpoint, dwell long enough to overlap, and record the concurrency
/// they observed. `hold_until_peak` makes every read WAIT inside the
/// gauge until the peak reaches that count — reads finish only once
/// that much concurrency demonstrably existed, so an over-tight bound
/// turns the test into a loud timeout instead of a silent pass.
struct Gauged {
    streams: usize,
    gauge: Arc<Gauge>,
    hold_until_peak: Option<usize>,
}

#[async_trait]
impl Source for Gauged {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("gauged", "0.0.0")
    }
    async fn check(&self) -> Result<(), SourceError> {
        Ok(())
    }
    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        Ok((0..self.streams)
            .map(|index| StreamSpec::new(format!("s{index}")))
            .collect())
    }
    async fn read(&self, mut request: ReadRequest) -> Result<(), SourceError> {
        self.gauge.enter();
        let _ = request.out.rows([json!({"id": 1})]).await;
        let _ = request.out.checkpoint(Cursor::new(json!("done"))).await;
        match self.hold_until_peak {
            Some(peak) => {
                while self.gauge.peak() < peak {
                    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                }
            }
            None => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
        }
        self.gauge.exit();
        Ok(())
    }
}

/// The default bound: 32 declared streams, at most 16 ever read at
/// once. The ceiling is structural (16 semaphore permits), the gauge
/// only observes it — and every stream still completes.
#[tokio::test]
async fn the_default_bounds_concurrent_reads_to_sixteen() {
    let gauge = Arc::new(Gauge::default());
    let source = Gauged {
        streams: 32,
        gauge: Arc::clone(&gauge),
        hold_until_peak: None,
    };
    let report = Engine::new(Config::new("bounded"), source, memory::Destination::new())
        .run()
        .await
        .expect("the run completes");
    assert!(
        gauge.peak() <= 16,
        "at most 16 concurrent reads under the default: peak {}",
        gauge.peak()
    );
    assert_eq!(report.tables.len(), 32, "every stream still loads");
}

/// The knob is real: raised to 32, more than 16 reads run at once.
/// Every read HOLDS until the peak reaches 17, so this passing is proof
/// of admission past the old bound — under a 16-slot pool it would time
/// out loudly instead.
#[tokio::test]
async fn a_raised_knob_admits_more_than_sixteen_readers() {
    let gauge = Arc::new(Gauge::default());
    let source = Gauged {
        streams: 32,
        gauge: Arc::clone(&gauge),
        hold_until_peak: Some(17),
    };
    let run = Engine::new(
        Config::new("raised").with_max_concurrent_streams(32),
        source,
        memory::Destination::new(),
    )
    .run();
    let report = tokio::time::timeout(std::time::Duration::from_secs(30), run)
        .await
        .expect("17 concurrent readers must be admitted well within the deadline")
        .expect("the run completes");
    assert!(gauge.peak() >= 17, "peak {} proves admission", gauge.peak());
    assert_eq!(report.tables.len(), 32);
}

/// The commit barrier cannot deadlock on queued streams: 20 streams
/// against the 16-slot default with a commit-per-checkpoint policy —
/// commits fire while streams still wait for a slot (a queued stream
/// holds no rows and owes no checkpoint), and the run completes with
/// every row landed.
#[tokio::test]
async fn commits_proceed_while_streams_queue_past_the_bound() {
    let gauge = Arc::new(Gauge::default());
    let source = Gauged {
        streams: 20,
        gauge: Arc::clone(&gauge),
        hold_until_peak: None,
    };
    let run = Engine::new(
        Config::new("barrier").with_commit_policy(CommitPolicy::every_checkpoints(1)),
        source,
        memory::Destination::new(),
    )
    .run();
    let report = tokio::time::timeout(std::time::Duration::from_secs(30), run)
        .await
        .expect("queued streams must not deadlock the commit barrier")
        .expect("the run completes");
    assert!(
        report.commits >= 2,
        "commits fired mid-run, not only at the end: {}",
        report.commits
    );
    let rows: u64 = report.tables.values().map(|t| t.rows).sum();
    assert_eq!(rows, 20, "one row per stream, all landed");
}
