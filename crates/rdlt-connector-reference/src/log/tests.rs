use std::num::NonZeroUsize;
use std::time::Duration;

use rdlt_connector::{
    ConnectContext, ConnectorErrorKind, Cursor, Partition, PartitionId, Push, ReadRequest, Source,
    SourceEvent, StreamName, StreamState, partition_channel, source_factory,
};
use serde_json::{Value, json};

use super::{LogSource, Offset, message};

fn events() -> StreamName {
    StreamName::new("events").expect("a valid stream")
}

fn p(index: u32) -> PartitionId {
    PartitionId::parse(format!("p{index}")).expect("a valid partition")
}

fn config(stream: &Value, group: &str) -> Value {
    json!({ "seed": 3, "group": group, "streams": [stream] })
}

async fn connect(stream: &Value, group: &str) -> Box<dyn Source> {
    source_factory::<LogSource>()
        .connect(config(stream, group), ConnectContext::new())
        .await
        .expect("the source connects")
}

fn offset(next: u64) -> Cursor {
    Cursor::encode(1, &Offset { next }).expect("an offset encodes")
}

/// What a read sent: the offsets it pushed and its checkpoints.
#[derive(Debug, Default)]
struct Sent {
    offsets: Vec<u64>,
    checkpoints: Vec<u64>,
}

/// A read of partition 0 from `cursor`, which follows the log until `stop_after` has passed where
/// that is set, and how it ended.
async fn read(
    source: &dyn Source,
    cursor: Option<Cursor>,
    stop_after: Option<Duration>,
) -> (rdlt_connector::Result<()>, Sent) {
    let (sink, mut feed) = partition_channel(NonZeroUsize::new(64).expect("not zero"));
    let partition = Partition::new(p(0)).unbounded();
    let request = ReadRequest::new(events(), partition, cursor).following(stop_after.is_some());
    let reading = source.read(request, sink);
    let collect = async {
        let mut sent = Sent::default();
        let stop = tokio::time::sleep(stop_after.unwrap_or(Duration::MAX));
        tokio::pin!(stop);
        let mut stopping = stop_after.is_some();
        loop {
            tokio::select! {
                biased;
                () = &mut stop, if stopping => {
                    stopping = false;
                    feed.stop();
                }
                event = feed.recv() => match event {
                    Some(SourceEvent::Push(push)) => sent.offsets.extend(pushed(&push)),
                    Some(SourceEvent::Checkpoint { cursor, .. }) => {
                        let next = cursor.decode::<Offset>(1).expect("an offset").next;
                        sent.checkpoints.push(next);
                    }
                    Some(_) => {}
                    None => break,
                },
            }
        }
        sent
    };
    tokio::join!(reading, collect)
}

fn pushed(push: &Push) -> Vec<u64> {
    let Push::Json(bytes) = push else {
        panic!("the log pushes JSON rows");
    };
    let rows: Vec<Value> = serde_json::from_slice(bytes).expect("a JSON array");
    rows.iter()
        .map(|row| row["offset"].as_u64().expect("an offset"))
        .collect()
}

#[tokio::test(start_paused = true)]
async fn a_read_that_does_not_follow_ends_at_the_head_it_started_at() {
    let stream = json!({ "name": "events", "partitions": 1, "messages": 25, "per_second": 10 });
    let source = connect(&stream, "at_the_head").await;
    let (read, sent) = read(source.as_ref(), None, None).await;
    read.expect("the read ends");
    assert_eq!(sent.offsets, (0..25).collect::<Vec<_>>());
    assert_eq!(sent.checkpoints, [10, 20, 25]);
}

#[tokio::test(start_paused = true)]
async fn a_following_read_reads_messages_as_they_arrive_until_asked_to_stop() {
    let stream = json!({ "name": "events", "partitions": 1, "messages": 2, "per_second": 5 });
    let source = connect(&stream, "following").await;
    let (read, sent) = read(source.as_ref(), None, Some(Duration::from_millis(1100))).await;
    read.expect("a stopped read ends cleanly");
    // Two messages at first, then five a second: seven within the second and a tenth.
    assert_eq!(sent.offsets, (0..7).collect::<Vec<_>>());
    assert_eq!(sent.checkpoints.last(), Some(&7));
}

#[tokio::test(start_paused = true)]
async fn a_log_that_never_grows_is_followed_until_the_read_is_asked_to_stop() {
    let stream = json!({ "name": "events", "partitions": 1, "messages": 3 });
    let source = connect(&stream, "still").await;
    let (read, sent) = read(source.as_ref(), None, Some(Duration::from_secs(3600))).await;
    read.expect("a stopped read ends cleanly");
    assert_eq!(sent.offsets, [0, 1, 2]);
}

#[tokio::test(start_paused = true)]
async fn partitions_added_later_are_planned_once_they_appear() {
    let stream = json!({
        "name": "events", "partitions": 1, "messages": 1,
        "partitions_later": 2, "later_after_ms": 500,
    });
    let source = connect(&stream, "growing").await;
    let planned = || async {
        let plan = source.plan(&events(), &StreamState::default()).await;
        let plan = plan.expect("the stream plans");
        plan.partitions
            .iter()
            .map(|partition| (partition.id().clone(), partition.is_unbounded()))
            .collect::<Vec<_>>()
    };
    assert_eq!(planned().await, [(p(0), true)]);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(planned().await, [(p(0), true), (p(1), true), (p(2), true)]);
}

#[tokio::test(start_paused = true)]
async fn a_log_that_forgets_refuses_to_read_before_its_committed_offset() {
    let stream = json!({ "name": "events", "partitions": 1, "messages": 8, "replayable": false });
    let source = connect(&stream, "forgetting").await;
    source
        .committed(&events(), &[(p(0), offset(5))])
        .await
        .expect("the offset commits");
    let (read_before, _) = read(source.as_ref(), Some(offset(4)), None).await;
    let refused = read_before.expect_err("a read before the committed offset is refused");
    assert_eq!(refused.kind(), ConnectorErrorKind::Transient);
    let (read_from, sent) = read(source.as_ref(), Some(offset(5)), None).await;
    read_from.expect("a read from the committed offset succeeds");
    assert_eq!(sent.offsets, [5, 6, 7]);
}

#[tokio::test(start_paused = true)]
async fn a_group_keeps_each_partitions_committed_offset_beyond_a_connection_never_back() {
    let stream = json!({ "name": "events", "partitions": 2, "messages": 8 });
    let source = connect(&stream, "kept").await;
    source
        .committed(&events(), &[(p(0), offset(6)), (p(1), offset(2))])
        .await
        .expect("the offsets commit");
    source
        .committed(&events(), &[(p(0), offset(3))])
        .await
        .expect("an older offset commits");
    let factory = source_factory::<LogSource>();
    let (_, reader) = factory
        .connect_acknowledging(config(&stream, "kept"), ConnectContext::new())
        .await
        .expect("the source connects with its reader");
    for (partition, next) in [(0, 6), (1, 2)] {
        let told = reader.acknowledged(&events(), &p(partition)).await;
        assert_eq!(told.expect("the group answers"), Some(offset(next)));
    }
}

#[test]
fn each_message_is_its_seed_stream_partition_and_offset_alone() {
    let value = message(3, "events", &p(0), 7);
    assert_eq!(value, message(3, "events", &p(0), 7));
    for other in [
        message(4, "events", &p(0), 7),
        message(3, "orders", &p(0), 7),
        message(3, "events", &p(1), 7),
        message(3, "events", &p(0), 8),
    ] {
        assert_ne!(value, other);
    }
}
