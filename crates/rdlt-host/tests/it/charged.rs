//! What decoding a remote connector's answers holds is charged to the engine's memory budget
//! before it is decoded, and a connector keeping to the limits the budget admits is refused
//! nothing.

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rdlt_connector::wire::v1;
use rdlt_connector::{
    Admission, BoxFuture, Partition, Permit, PipelineId, ReadRequest, Source as _, SourceEvent,
    StreamName, admitted_partition_channel,
};
use rdlt_engine::{CommitPolicy, Engine, EngineConfig, PipelinePlan, RunStatus, StreamPlan};
use rdlt_host::Options;
use rdlt_wire::bounded::{Charge, Held};
use rdlt_wire::limits::Class;
use rdlt_wire::prost::Message as _;

use crate::support::{memory_destination, memory_source};

/// The memory source's configuration: `count` streams of one row each, under long names.
fn streams(count: usize) -> serde_json::Value {
    let streams: serde_json::Map<_, _> = (0..count)
        .map(|index| {
            (
                format!("s{index:0>150}"),
                serde_json::json!([{ "id": index }]),
            )
        })
        .collect();
    serde_json::json!({ "streams": streams })
}

/// The first stream of the memory source.
fn first() -> StreamName {
    StreamName::new(format!("s{:0>150}", 0)).expect("a stream name")
}

/// The bytes the catalog of the memory source of `count` streams takes on the wire.
async fn catalog_bytes(count: usize) -> usize {
    let source = memory_source(streams(count), &Options::default()).await;
    let catalog = source.discover().await.expect("the catalog is discovered");
    v1::Catalog::from(&catalog).encoded_len()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_catalog_at_the_least_budget_s_limit_is_charged_before_it_is_decoded_and_taken() {
    let config = EngineConfig::builder()
        .memory(EngineConfig::least_memory(16))
        .commit(CommitPolicy::new(None, Some(1), None).expect("a row threshold"))
        .build()
        .expect("the least memory is valid");
    let limits = config.limits();
    let limit = usize::try_from(limits.catalog_bytes).expect("a size");
    // As many streams as the catalog limit holds, less one.
    let one = catalog_bytes(2).await - catalog_bytes(1).await;
    let count = limit / one - 1;
    let wire = catalog_bytes(count).await;
    assert!(wire <= limit && wire + 2 * one > limit, "{wire} of {limit}");
    let options = Options {
        limits,
        ..Options::default()
    };
    let source = memory_source(streams(count), &options).await;
    let destination = memory_destination("charged", &options).await;
    let first = StreamName::new(format!("s{:0>150}", 0)).expect("a stream name");
    let plan = PipelinePlan::new(
        PipelineId::parse("charged").unwrap(),
        [StreamPlan::new(first)],
    )
    .expect("a plan");
    let env = rdlt_engine::SystemEnv::new(
        rdlt_engine::RayonPool::new(NonZeroUsize::MIN).expect("a pool"),
    );
    let outcome = Engine::new(config, Arc::new(env))
        .run(plan, Arc::new(source), Arc::new(destination))
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    // The catalog was charged at what its scan counts, no less than its bytes: one row and its
    // cursor alone hold far less.
    let peak = usize::try_from(outcome.report.peak_memory).expect("a size");
    assert!(
        peak >= wire,
        "the budget's peak, {peak} bytes, is under the catalog's {wire}"
    );
}

/// Charges answers' messages at once, counting those held and each of a frame.
#[derive(Default)]
struct Tally {
    held: AtomicUsize,
    frames: AtomicUsize,
    /// How many were held as each event a read sent was admitted.
    seen: Mutex<Vec<usize>>,
}

struct Charged(Arc<Tally>);

impl Drop for Charged {
    fn drop(&mut self) {
        self.0.held.fetch_sub(1, Ordering::SeqCst);
    }
}

struct Charging(Arc<Tally>);

impl Charge for Charging {
    fn charge(&self, class: Class, _: usize) -> rdlt_wire::bounded::Charging {
        self.0.held.fetch_add(1, Ordering::SeqCst);
        if class == Class::Data {
            self.0.frames.fetch_add(1, Ordering::SeqCst);
        }
        let charged = Charged(Arc::clone(&self.0));
        Box::pin(async move { Ok(Box::new(charged) as Held) })
    }
}

struct Watching(Arc<Tally>);

impl Admission for Watching {
    fn admit<'a>(
        &'a self,
        _: &'a SourceEvent,
    ) -> BoxFuture<'a, rdlt_connector::Result<Option<Permit>>> {
        let held = self.0.held.load(Ordering::SeqCst);
        self.0.seen.lock().expect("not poisoned").push(held);
        Box::pin(async { Ok(None) })
    }

    fn charge(&self, _: u64) -> rdlt_connector::Result<Permit> {
        Ok(Box::new(()))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn each_frame_is_charged_and_released_before_its_event_waits_for_room() {
    let source = memory_source(streams(1), &Options::default()).await;
    let tally = Arc::new(Tally::default());
    let watching = Arc::new(Watching(Arc::clone(&tally)));
    let (sink, mut feed) = admitted_partition_channel(NonZeroUsize::MIN, watching);
    let request = ReadRequest::new(first(), Partition::single(), None);
    let charge = Arc::new(Charging(Arc::clone(&tally)));
    let read = tokio::spawn(rdlt_wire::bounded::charging(charge, async move {
        source.read(request, sink).await
    }));
    while feed.recv().await.is_some() {}
    read.await.expect("the read runs").expect("the read ends");
    assert!(
        tally.frames.load(Ordering::SeqCst) > 0,
        "frames are charged"
    );
    let seen = tally.seen.lock().expect("not poisoned").clone();
    assert!(!seen.is_empty(), "events are admitted");
    assert!(
        seen.iter().all(|held| *held == 0),
        "a charge held as an event was admitted: {seen:?}"
    );
    assert_eq!(
        tally.held.load(Ordering::SeqCst),
        0,
        "every charge is released"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_writer_releases_each_answer_s_charge_once_it_is_decoded() {
    use rdlt_connector::{
        Destination as _, LoadId, OpenContext, SchemaVersion, SegmentId, TablePath, TableRef,
    };
    let tally = Arc::new(Tally::default());
    let charge = Arc::new(Charging(Arc::clone(&tally)));
    let destination = memory_destination("charged_writes", &Options::default()).await;
    let held = Arc::clone(&tally);
    rdlt_wire::bounded::charging(charge, async move {
        let context = OpenContext {
            pipeline: PipelineId::parse("charged").unwrap(),
            load_id: LoadId::from_parts(std::time::UNIX_EPOCH, 1),
        };
        let mut opened = destination.open(&context).await.expect("the session opens");
        let table = TableRef {
            path: TablePath::new(["items"]).expect("a valid table path"),
            name: Arc::from("items"),
            version: SchemaVersion(1),
            generation: None,
            merge: None,
        };
        let mut writer = opened.session.writer(&table).await.expect("a writer opens");
        let ids = arrow_array::Int64Array::from(vec![1_i64, 2, 3]);
        let batch = arrow_array::RecordBatch::try_from_iter([("id", Arc::new(ids) as _)]).unwrap();
        writer
            .write(SegmentId(1), batch)
            .await
            .expect("the write is taken");
        let stats = writer.flush().await.expect("the flush is answered");
        assert_eq!(stats.rows, 3);
        // The flush's answer has been decoded: its charge is no longer held.
        assert_eq!(
            held.held.load(Ordering::SeqCst),
            0,
            "an answer's charge is still held"
        );
    })
    .await;
    assert!(tally.seen.lock().expect("not poisoned").is_empty());
}
