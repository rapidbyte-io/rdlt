//! The reference connectors, served in this process and certified through the protocol.

use std::fmt::Write as _;
use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::types::Int8Type;
use arrow_array::{ArrayRef, Int8Array, ListArray, RecordBatch};
use rdlt_certify::{Outcome, Probe, Target, certify_destination, certify_source};
use rdlt_connector::serve::Served;
use rdlt_connector::testing::BARRIER_SLACK;
use rdlt_connector::{
    BoxFuture, Catalog, ConnectContext, ConnectorErrorKind, ConnectorSpec, Cursor, PartitionId,
    PartitionPlan, PartitionSink, Push, ReadRequest, Source, SourceEvent, SourceFactory,
    StreamName, StreamState, TableRef, acknowledging_source_factory, destination_factory,
    partition_channel, source_factory,
};
use rdlt_connector_reference::{
    ChangesSource, FilesSource, GeneratorSource, MemoryDestination, MemorySource, published,
};
use rdlt_wire::Limits;
use serde_json::json;

use crate::killed::{SETTLED_LATE, read_as_killed};

struct MemoryProbe(&'static str);

impl Probe for MemoryProbe {
    fn published<'a>(
        &'a self,
        table: &'a TableRef,
    ) -> BoxFuture<'a, rdlt_connector::Result<Vec<RecordBatch>>> {
        let batches = published(self.0, &table.name);
        Box::pin(async move { Ok(batches) })
    }
}

#[tokio::test]
async fn the_generator_served_in_process_is_certified_through_the_protocol() {
    let target = Target::served(Served::new().with_source(source_factory::<GeneratorSource>()))
        .credit_watch(crate::BRIEF);
    let config = json!({
        "seed": 7,
        "streams": [{ "name": "events", "rows": 57, "partitions": 3, "batch_rows": 5 }],
    });
    let report = certify_source(&target, config).await;
    // A source this small ends before a kill lands: its kill clause is not observed.
    assert_eq!(crate::unobserved(&report), ["K-SOURCE"], "{report}");
    for id in [
        "P-HANDSHAKE",
        "P-ORDER",
        "P-ROLE",
        "P-LIMITS",
        "P-HEARTBEAT",
        "P-MALFORMED",
        "P-CREDIT",
        "S-RESUME",
        "S-BARRIER",
    ] {
        assert_eq!(report.outcome(id), Some(&Outcome::Passed), "{id}: {report}");
    }
}

#[tokio::test]
async fn the_memory_destination_served_in_process_is_certified_through_the_protocol() {
    let target =
        Target::served(Served::new().with_destination(destination_factory::<MemoryDestination>()));
    let report = certify_destination(
        &target,
        json!({ "store": "certify_served" }),
        &MemoryProbe("certify_served"),
    )
    .await;
    report.assert_passed();
    assert_eq!(
        report
            .outcome("P-CREDIT")
            .map(|outcome| matches!(outcome, Outcome::Inapplicable(_))),
        Some(true),
        "{report}"
    );
    for id in ["D-FENCE", "K-DESTINATION"] {
        assert_eq!(report.outcome(id), Some(&Outcome::Passed), "{id}: {report}");
    }
}

#[tokio::test]
async fn a_source_served_in_process_killed_as_it_loads_resumes_where_it_was() {
    let target = Target::served(Served::new().with_source(source_factory::<GeneratorSource>()))
        .credit_watch(crate::BRIEF)
        .kill_seed(SETTLED_LATE);
    let report = certify_source(&target, read_as_killed(5)).await;
    // Every clause that applies was seen to be met, the kill clause among them.
    report.assert_passed();
    assert_eq!(
        report.outcome("K-SOURCE"),
        Some(&Outcome::Passed),
        "{report}"
    );
    // Served in this process, the source is cut off, not killed, and the report says so.
    assert!(
        report
            .note("K-SOURCE")
            .is_some_and(|note| note.starts_with("cut: "))
    );
}

#[tokio::test]
async fn a_source_holding_more_than_a_kill_clause_loads_leaves_it_unobserved() {
    let target = Target::served(Served::new().with_source(source_factory::<GeneratorSource>()))
        .credit_watch(crate::BRIEF)
        .kill_seed(SETTLED_LATE);
    // A hundred rows more than a kill clause loads.
    let config = json!({
        "seed": 5,
        "streams": [{ "name": "events", "rows": 100_100, "partitions": 1, "batch_rows": 500 }],
    });
    let report = certify_source(&target, config).await;
    let outcome = report.outcome("K-SOURCE");
    assert!(matches!(outcome, Some(Outcome::Unobserved(_))), "{report}");
    assert_eq!(report.failures().count(), 0, "{report}");
}

#[tokio::test]
async fn a_source_holding_all_a_kill_clause_loads_is_beyond_it_once_a_kill_repeats_a_row() {
    let target = Target::served(Served::new().with_source(source_factory::<GeneratorSource>()))
        .credit_watch(crate::BRIEF)
        .kill_seed(SETTLED_LATE);
    // Exactly what a kill clause loads: a load never killed writes each row once, and one
    // killed writes again what its kills left uncommitted.
    let config = json!({
        "seed": 5,
        "streams": [{ "name": "events", "rows": 100_000, "partitions": 1, "batch_rows": 500 }],
    });
    let report = certify_source(&target, config).await;
    let outcome = report.outcome("K-SOURCE");
    assert!(matches!(outcome, Some(Outcome::Unobserved(_))), "{report}");
    assert_eq!(report.failures().count(), 0, "{report}");
}

#[tokio::test]
async fn a_source_read_before_any_kill_lands_proves_nothing_of_kills() {
    let target = Target::served(Served::new().with_source(source_factory::<GeneratorSource>()))
        .credit_watch(crate::BRIEF);
    let config = json!({
        "seed": 5,
        "streams": [{ "name": "events", "rows": 300, "partitions": 2, "batch_rows": 5 }],
    });
    let report = certify_source(&target, config).await;
    assert!(
        matches!(report.outcome("K-SOURCE"), Some(Outcome::Unobserved(_))),
        "{report}"
    );
}

#[tokio::test]
async fn a_destination_certified_again_in_its_store_is_killed_into_tables_of_its_own() {
    let target =
        Target::served(Served::new().with_destination(destination_factory::<MemoryDestination>()));
    for _ in 0..2 {
        let report = certify_destination(
            &target,
            json!({ "store": "certify_again" }),
            &MemoryProbe("certify_again"),
        )
        .await;
        assert_eq!(
            report.outcome("K-DESTINATION"),
            Some(&Outcome::Passed),
            "{report}"
        );
    }
}

#[tokio::test]
async fn the_memory_source_served_in_process_is_certified_through_the_protocol() {
    let target = Target::served(Served::new().with_source(source_factory::<MemorySource>()))
        .credit_watch(crate::BRIEF);
    let config =
        json!({ "streams": { "users": [{"id": 1}, {"id": 2}, {"id": 3}] }, "page_size": 1 });
    let report = certify_source(&target, config).await;
    assert_eq!(crate::unobserved(&report), ["K-SOURCE"], "{report}");
}

#[tokio::test]
async fn a_kill_timeout_bounds_the_kill_clauses_alone() {
    let target =
        Target::served(Served::new().with_destination(destination_factory::<MemoryDestination>()))
            .kill_timeout(std::time::Duration::ZERO);
    let report = certify_destination(
        &target,
        json!({ "store": "certify_hurried" }),
        &MemoryProbe("certify_hurried"),
    )
    .await;
    assert!(
        matches!(report.outcome("K-DESTINATION"), Some(Outcome::Failed(_))),
        "{report}"
    );
    assert_eq!(
        report.outcome("D-COMMIT"),
        Some(&Outcome::Passed),
        "{report}"
    );
}

#[tokio::test]
async fn a_change_source_served_in_process_tells_where_it_stands_through_the_protocol() {
    let served = Served::new().with_source(acknowledging_source_factory::<ChangesSource>());
    let target = Target::served(served).credit_watch(crate::BRIEF);
    let config = json!({
        "seed": 5,
        "streams": [{ "name": "accounts", "keys": 9, "changes": 6, "batch_rows": 2 }],
        "slot": "certified_over_the_wire",
    });
    let report = certify_source(&target, config).await;
    // Its changes are few: they end before a kill lands.
    assert_eq!(crate::unobserved(&report), ["K-SOURCE"], "{report}");
    assert_eq!(report.outcome("S-ACK"), Some(&Outcome::Passed), "{report}");
}

#[tokio::test]
async fn a_change_source_that_cannot_read_again_breaks_no_clause_through_the_protocol() {
    let served = Served::new().with_source(acknowledging_source_factory::<ChangesSource>());
    let target = Target::served(served).credit_watch(crate::BRIEF);
    let config = json!({
        "seed": 5,
        "streams": [{
            "name": "accounts", "keys": 9, "changes": 6, "batch_rows": 2, "replayable": false,
        }],
        "slot": "certified_forgetting_over_the_wire",
    });
    let report = certify_source(&target, config).await;
    // `S-ACK` told its slot where the changes stand, which a load from no cursor is before.
    assert_eq!(crate::unobserved(&report), ["K-SOURCE"], "{report}");
    assert_eq!(report.outcome("S-ACK"), Some(&Outcome::Passed), "{report}");
    let Some(Outcome::Unobserved(reason)) = report.outcome("K-SOURCE") else {
        panic!("K-SOURCE was observed: {report}");
    };
    assert!(
        reason.contains("stream accounts: its source cannot read it again"),
        "{reason}"
    );
}

#[tokio::test]
async fn s_ack_does_not_apply_through_the_protocol_to_a_source_that_tells_nothing() {
    let target = Target::served(Served::new().with_source(source_factory::<GeneratorSource>()))
        .credit_watch(crate::BRIEF);
    let config = json!({
        "seed": 7,
        "streams": [{ "name": "events", "rows": 5, "partitions": 1, "batch_rows": 5 }],
    });
    let report = certify_source(&target, config).await;
    assert!(
        matches!(report.outcome("S-ACK"), Some(Outcome::Inapplicable(_))),
        "{report}"
    );
}

#[tokio::test]
async fn a_read_back_probe_shows_nothing_of_its_configuration_or_of_what_its_connector_said() {
    use rdlt_connector::readable_destination_factory;
    let served =
        Served::new().with_destination(readable_destination_factory::<MemoryDestination>());
    let target = Target::served(served);
    // A field the destination does not know: its handshake fails, saying so.
    let config = serde_json::json!({ "store": "probe_debug", "password": "hunter2-SECRET" });
    let probe = rdlt_certify::read_back(&target, &config)
        .await
        .expect("a probe whose read-backs fail");
    let shown = format!("{probe:?} {probe:#?}");
    assert!(shown.contains("ReadBackProbe"), "{shown}");
    for hidden in ["hunter2", "password", "probe_debug", "failed"] {
        assert!(!shown.contains(hidden), "{hidden}: {shown}");
    }
}

/// A source that answers the barrier pending as its read starts with a checkpoint, then sends
/// what it reads and checkpoints again only at its end, whatever barrier is raised meanwhile.
///
/// It relays the generator's read of one partition, whose cursors name the next row to read: its
/// checkpoint at the end is the generator's last, moved past the rows sent after it.
struct Opening(Box<dyn SourceFactory>);

struct OpeningSource(Box<dyn Source>);

impl SourceFactory for Opening {
    fn spec(&self) -> &ConnectorSpec {
        self.0.spec()
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, rdlt_connector::Result<Box<dyn Source>>> {
        Box::pin(async move {
            let source = self.0.connect(config, context).await?;
            Ok(Box::new(OpeningSource(source)) as Box<dyn Source>)
        })
    }
}

impl Source for OpeningSource {
    fn check(&self) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        self.0.check()
    }

    fn discover(&self) -> BoxFuture<'_, rdlt_connector::Result<Catalog>> {
        self.0.discover()
    }

    fn plan<'a>(
        &'a self,
        stream: &'a StreamName,
        state: &'a StreamState,
    ) -> BoxFuture<'a, rdlt_connector::Result<PartitionPlan>> {
        self.0.plan(stream, state)
    }

    fn read(
        &self,
        request: ReadRequest,
        mut sink: PartitionSink,
    ) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        Box::pin(async move {
            let (inner, mut feed) = partition_channel(NonZeroUsize::MIN);
            if let Some(barrier) = sink.pending_barrier() {
                feed.request_checkpoint(barrier);
            }
            let relay = async {
                let (mut opened, mut last, mut since) = (false, None, 0);
                while let Some(event) = feed.recv().await {
                    let sent = match event {
                        SourceEvent::Checkpoint { cursor, answers } if !opened => {
                            opened = true;
                            sink.send(SourceEvent::Checkpoint { cursor, answers }).await
                        }
                        SourceEvent::Checkpoint { cursor, .. } => {
                            (last, since) = (Some(cursor), 0);
                            Ok(())
                        }
                        SourceEvent::Push(push) => {
                            if let Push::Arrow(batch) = &push {
                                since += batch.num_rows();
                            }
                            sink.send(SourceEvent::Push(push)).await
                        }
                        event => sink.send(event).await,
                    };
                    if sent.is_err() {
                        feed.stop();
                        return stopped(sent);
                    }
                }
                let Some(cursor) = last else {
                    return Ok(());
                };
                let mut position: serde_json::Value = cursor.decode(cursor.version())?;
                let next = position["next"].as_u64().unwrap_or(0);
                position["next"] = json!(next + u64::try_from(since).expect("a count"));
                let cursor = Cursor::encode(cursor.version(), &position)?;
                let answers = sink.pending_barrier();
                stopped(sink.send(SourceEvent::Checkpoint { cursor, answers }).await)
            };
            let (read, relayed) = tokio::join!(self.0.read(request, inner), relay);
            read.and(relayed)
        })
    }

    fn committed<'a>(
        &'a self,
        stream: &'a StreamName,
        cursors: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, rdlt_connector::Result<()>> {
        self.0.committed(stream, cursors)
    }
}

/// What a relayed send leaves of the read: one the host stopped ends cleanly, as a stopped read
/// does.
fn stopped(sent: rdlt_connector::Result<()>) -> rdlt_connector::Result<()> {
    match sent {
        Err(error) if error.kind() == ConnectorErrorKind::Stopped => Ok(()),
        other => other,
    }
}

/// Rows of reads `S-BARRIER` judges a correct source on, in batches of five: one too short for
/// the barrier raised during it to be judged, the shortest that has it judged, and a longer one.
///
/// The barrier is raised as the read's first push, its second event, is taken, and a read of
/// `batches`, a checkpoint after each but the last, has its last push at event
/// `2 * batches - 1`: it is judged once that comes more than `BARRIER_SLACK` events later, and
/// the pieces its batches are cut into, which batches this small add none of.
pub(crate) fn barrier_rows() -> [u64; 3] {
    let batches = (BARRIER_SLACK + 3).div_ceil(2);
    let shortest = 5 * u64::try_from(batches).expect("a count");
    [50, shortest, shortest + 45]
}

/// Writes a stream whose first record holds 256 KiB, and the 60 after it an id each, under
/// `root`: a read of it, a record a push, sends a large frame and small ones after.
pub(crate) fn large_then_small(root: &std::path::Path) {
    let large = format!("{{\"id\": 0, \"text\": \"{}\"}}\n", "x".repeat(256 << 10));
    let small = (1..=60).fold(String::new(), |mut small, id| {
        writeln!(small, "{{\"id\": {id}}}").expect("a string takes a line");
        small
    });
    std::fs::write(root.join("items.jsonl"), large + &small).expect("the stream is written");
}

/// One partition of the generator's rows in batches of five, a checkpoint after each.
pub(crate) fn batches_of_five(rows: u64) -> serde_json::Value {
    json!({ "seed": 7, "streams": [{ "name": "events", "rows": rows, "batch_rows": 5 }] })
}

#[tokio::test]
async fn a_source_served_in_process_passes_s_barrier_however_far_ahead_of_the_host_it_reads() {
    // A read that ends before the source sees the barrier raised during it; the shortest whose
    // barrier is judged; and one that reads on and answers it, which a source granted the host's
    // usual credit reads past unseen.
    for rows in barrier_rows() {
        let served = || Served::new().with_source(source_factory::<GeneratorSource>());
        let target = Target::served(served()).credit_watch(crate::BRIEF);
        // Options of a certifier's own leave its reads' credit as the certification grants it.
        let optioned = Target::served(served()).options(rdlt_host::Options::default());
        for target in [target, optioned.credit_watch(crate::BRIEF)] {
            let report = certify_source(&target, batches_of_five(rows)).await;
            assert_eq!(
                report.outcome("S-BARRIER"),
                Some(&Outcome::Passed),
                "{rows}: {report}"
            );
        }
    }
}

#[tokio::test]
async fn a_source_served_in_process_answering_a_barrier_only_as_its_read_starts_fails_s_barrier() {
    let opening = Opening(source_factory::<GeneratorSource>());
    let target =
        Target::served(Served::new().with_source(Box::new(opening))).credit_watch(crate::BRIEF);
    let report = certify_source(&target, batches_of_five(400)).await;
    let failed: Vec<&str> = report.failures().map(|failed| failed.clause.id).collect();
    assert_eq!(failed, ["S-BARRIER"], "{report}");
    let Some(Outcome::Failed(reason)) = report.outcome("S-BARRIER") else {
        panic!("{report}");
    };
    assert!(reason.contains("barrier 2"), "{reason}");
}

/// A source of the generator's streams that reads every partition as its batch, sent
/// [`LISTED_BATCHES`] times, kept ready so it sends them as fast as the host takes them.
///
/// It is correct: it checkpoints where a barrier is pending before a push, and after each push
/// but the last, each checkpoint answering the barrier pending.
struct Listed(Box<dyn SourceFactory>, RecordBatch);

struct ListedSource(Box<dyn Source>, RecordBatch);

/// Batches each partition of a [`Listed`] source reads.
const LISTED_BATCHES: u64 = 9;

impl SourceFactory for Listed {
    fn spec(&self) -> &ConnectorSpec {
        self.0.spec()
    }

    fn connect(
        &self,
        config: serde_json::Value,
        context: ConnectContext,
    ) -> BoxFuture<'_, rdlt_connector::Result<Box<dyn Source>>> {
        Box::pin(async move {
            let source = self.0.connect(config, context).await?;
            Ok(Box::new(ListedSource(source, self.1.clone())) as Box<dyn Source>)
        })
    }
}

impl Source for ListedSource {
    fn check(&self) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        self.0.check()
    }

    fn discover(&self) -> BoxFuture<'_, rdlt_connector::Result<Catalog>> {
        self.0.discover()
    }

    fn plan<'a>(
        &'a self,
        stream: &'a StreamName,
        state: &'a StreamState,
    ) -> BoxFuture<'a, rdlt_connector::Result<PartitionPlan>> {
        self.0.plan(stream, state)
    }

    fn read(
        &self,
        request: ReadRequest,
        mut sink: PartitionSink,
    ) -> BoxFuture<'_, rdlt_connector::Result<()>> {
        Box::pin(async move {
            let start = match &request.cursor {
                Some(cursor) => cursor.decode::<serde_json::Value>(1)?["next"]
                    .as_u64()
                    .unwrap_or(0),
                None => 0,
            };
            let checkpoint = |next: u64, sink: &PartitionSink| {
                let cursor = Cursor::encode(1, &json!({ "next": next }))?;
                let answers = sink.pending_barrier();
                Ok::<_, rdlt_connector::ConnectorError>(SourceEvent::Checkpoint { cursor, answers })
            };
            for next in start..LISTED_BATCHES {
                if sink.pending_barrier().is_some() {
                    let answering = checkpoint(next, &sink)?;
                    stopped(sink.send(answering).await)?;
                }
                let push = SourceEvent::Push(Push::Arrow(self.1.clone()));
                stopped(sink.send(push).await)?;
                if next + 1 < LISTED_BATCHES {
                    let sealing = checkpoint(next + 1, &sink)?;
                    stopped(sink.send(sealing).await)?;
                }
            }
            Ok(())
        })
    }

    fn committed<'a>(
        &'a self,
        stream: &'a StreamName,
        cursors: &'a [(PartitionId, Cursor)],
    ) -> BoxFuture<'a, rdlt_connector::Result<()>> {
        self.0.committed(stream, cursors)
    }
}

/// What `S-BARRIER` finds of a [`Listed`] source of `batch`, served to a host of `limits`.
async fn listed_under(batch: RecordBatch, limits: Limits) -> Option<Outcome> {
    let options = rdlt_host::Options {
        limits,
        ..rdlt_host::Options::default()
    };
    let listed = Listed(source_factory::<GeneratorSource>(), batch);
    let target = Target::served(Served::new().with_source(Box::new(listed)))
        .options(options)
        .credit_watch(crate::BRIEF);
    let config = json!({ "seed": 7, "streams": [{ "name": "events", "rows": 1 }] });
    let report = certify_source(&target, config).await;
    report.outcome("S-BARRIER").cloned()
}

#[tokio::test]
async fn a_source_served_under_a_small_value_limit_passes_s_barrier() {
    // Each batch of 64000 rows of a list and its 64 items goes as four pieces of a mebivalue: the
    // last push is event 44, and the 16 events after the first batch, all the serving end's
    // queue holds, go before the source can see the barrier raised at its first piece.
    let rows = (0..64_000).map(|_| Some(vec![Some(1_i8); 64]));
    let items: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int8Type, _, _>(rows));
    let batch = RecordBatch::try_from_iter([("items", items)]).expect("a batch");
    let limits = Limits {
        batch_values: rdlt_wire::limits::MIN_BATCH_VALUES,
        ..Limits::default()
    };
    assert_eq!(listed_under(batch, limits).await, Some(Outcome::Passed));
}

#[tokio::test]
async fn a_source_served_under_a_small_row_limit_passes_s_barrier() {
    // Each batch of 4096 rows goes as four pieces of 1024 rows: the read is as long as that
    // cut by values, and as much of it goes before the source can see the barrier.
    let items: ArrayRef = Arc::new(Int8Array::from(vec![1_i8; 4096]));
    let batch = RecordBatch::try_from_iter([("items", items)]).expect("a batch");
    let limits = Limits {
        batch_rows: rdlt_wire::limits::MIN_BATCH_ROWS,
        ..Limits::default()
    };
    assert_eq!(listed_under(batch, limits).await, Some(Outcome::Passed));
}

#[tokio::test]
async fn a_source_served_in_process_whose_first_push_is_large_passes_s_barrier() {
    // Its 61 pushes, a checkpoint after each but the last, end at the read's event 121: far past
    // where the barrier raised as its first push is taken is judged.
    let root = tempfile::tempdir().expect("a temporary directory");
    large_then_small(root.path());
    let target = Target::served(Served::new().with_source(source_factory::<FilesSource>()))
        .credit_watch(crate::BRIEF);
    let config = json!({ "root": root.path(), "batch_rows": 1 });
    let report = certify_source(&target, config).await;
    assert_eq!(
        report.outcome("S-BARRIER"),
        Some(&Outcome::Passed),
        "{report}"
    );
}
