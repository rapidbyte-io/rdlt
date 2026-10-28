use std::sync::Arc;

use rdlt_connector::{
    BoxFuture, Capabilities, Catalog, CommitKey, Cursor, Destination, Epoch, OpenContext,
    OpenedSession, PartitionId, PartitionSink, PipelineId, ReadRequest, Receipt, Result, Source,
    StreamName, StreamState,
};

use super::{Engine, Ledger, StopMode};
use crate::config::EngineConfig;
use crate::env::SystemEnv;
use crate::plan::{PipelinePlan, StreamPlan};
use crate::report::{AttemptLog, AttemptRecord, Pending};

fn engine() -> Engine {
    let config = EngineConfig::builder().lanes(3).build().unwrap();
    Engine::new(config, Arc::new(SystemEnv::one_core()))
}

/// A connector the tests never poll a run far enough to use.
struct Idle(Capabilities);

impl Source for Idle {
    fn check(&self) -> BoxFuture<'_, Result<()>> {
        unreachable!("the run is never polled")
    }

    fn discover(&self) -> BoxFuture<'_, Result<Catalog>> {
        unreachable!("the run is never polled")
    }

    fn plan<'a>(
        &'a self,
        _: &'a StreamName,
        _: &'a StreamState,
    ) -> BoxFuture<'a, Result<rdlt_connector::PartitionPlan>> {
        unreachable!("the run is never polled")
    }

    fn read(&self, _: ReadRequest, _: PartitionSink) -> BoxFuture<'_, Result<()>> {
        unreachable!("the run is never polled")
    }

    fn committed<'a>(
        &'a self,
        _: &'a StreamName,
        _: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, Result<()>> {
        unreachable!("the run is never polled")
    }
}

impl Destination for Idle {
    fn capabilities(&self) -> &Capabilities {
        &self.0
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        unreachable!("the run is never polled")
    }

    fn open<'a>(&'a self, _: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        unreachable!("the run is never polled")
    }
}

#[test]
fn engines_and_handles_describe_themselves() {
    let engine = engine();
    let rendered = format!("{engine:?}");
    assert!(rendered.starts_with("Engine"), "{rendered}");
    assert!(rendered.contains("lanes: Some(3)"), "{rendered}");
    let plan = PipelinePlan::new(
        PipelineId::parse("p").unwrap(),
        [StreamPlan::new(StreamName::new("s").unwrap())],
    )
    .unwrap();
    let idle = Arc::new(Idle(Capabilities::minimal()));
    let handle = engine.run(plan, Arc::clone(&idle) as _, idle);
    assert_eq!(format!("{handle:?}"), "RunHandle { .. }");
    assert!(format!("{:?}", handle.control()).starts_with("RunControl"));
}

#[test]
fn stop_modes_are_distinct() {
    assert_ne!(StopMode::AfterCommit, StopMode::Now);
}

/// The key of commit `seq` of the session opened at `epoch`.
fn key(epoch: u64, seq: u64) -> CommitKey {
    CommitKey {
        epoch: Epoch(epoch),
        seq,
    }
}

/// The receipt of the commit keyed `key`, of `rows` rows, as the destination keeps it.
fn landed(key: CommitKey, rows: u64) -> Receipt {
    Receipt {
        key,
        committed_at: std::time::UNIX_EPOCH,
        rows,
        bytes: rows * 10,
    }
}

/// An attempt whose open found `landed` landed at `epoch`, or that opened nothing, and which
/// failed with the commit keyed `pending` in flight.
fn attempt(opened: Option<(u64, Option<Receipt>)>, pending: Option<CommitKey>) -> AttemptRecord {
    let streams = std::collections::BTreeMap::from([(
        StreamName::new("orders").unwrap(),
        crate::report::StreamReport {
            rows: 3,
            commits: 1,
            ..crate::report::StreamReport::default()
        },
    )]);
    AttemptRecord {
        started_at: std::time::UNIX_EPOCH,
        ended_at: std::time::UNIX_EPOCH,
        log: AttemptLog {
            opened: opened.map(|(epoch, landed)| (Epoch(epoch), landed)),
            pending: pending.map(|key| Pending {
                key,
                streams: streams.clone(),
            }),
            ..AttemptLog::default()
        },
        error: None,
    }
}

#[test]
fn a_lost_answer_is_credited_once_the_next_open_finds_its_key_landed() {
    let mut ledger = Ledger::new(PipelineId::parse("orders").unwrap());
    ledger
        .record(attempt(Some((2, None)), Some(key(2, 1))), true)
        .unwrap();
    // The destination counted the rows it published; the report takes its count.
    let found = landed(key(2, 1), 4);
    ledger
        .record(attempt(Some((3, Some(found))), None), false)
        .unwrap();
    let report = ledger.report();
    assert_eq!((report.commits, report.rows, report.bytes), (1, 4, 40));
    assert_eq!(report.streams["orders"].rows, 3);
    assert_eq!(
        (report.attempts[0].epoch, report.attempts[0].commits),
        (Some(Epoch(2)), 1),
        "credited to the attempt whose epoch its key names"
    );
}

#[test]
fn a_commit_in_flight_whose_key_did_not_land_is_never_credited() {
    for found in [None, Some(landed(key(2, 0), 9)), Some(landed(key(1, 7), 9))] {
        let mut ledger = Ledger::new(PipelineId::parse("orders").unwrap());
        ledger
            .record(attempt(Some((2, None)), Some(key(2, 1))), true)
            .unwrap();
        ledger
            .record(attempt(Some((3, found.clone())), None), false)
            .unwrap();
        let report = ledger.report();
        assert_eq!((report.commits, report.rows), (0, 0), "{found:?}");
    }
}

#[test]
fn a_commit_in_flight_waits_past_an_attempt_that_opened_nothing() {
    let mut ledger = Ledger::new(PipelineId::parse("orders").unwrap());
    ledger
        .record(attempt(Some((2, None)), Some(key(2, 1))), true)
        .unwrap();
    ledger.record(attempt(None, None), true).unwrap();
    ledger
        .record(attempt(Some((3, Some(landed(key(2, 1), 4)))), None), false)
        .unwrap();
    let report = ledger.report();
    assert_eq!((report.attempted, report.commits, report.rows), (3, 1, 4));
}
