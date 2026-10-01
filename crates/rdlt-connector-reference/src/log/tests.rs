use std::num::NonZeroUsize;
use std::time::Duration;

use rdlt_connector::{
    ConnectContext, ConnectorErrorKind, Cursor, Partition, PartitionId, Push, ReadRequest, Source,
    SourceEvent, StreamName, StreamState, acknowledging_source_factory, partition_channel,
    source_factory,
};
use serde_json::{Value, json};

use super::{LogSource, Logged, LoggedStream, Offset, message};
use crate::limits::{MAX_MESSAGE_ROWS, MAX_PARTITIONS};

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

/// What a read sent: the offsets it pushed, its checkpoints, how far behind it said it was, and
/// how often it asked for a new plan.
#[derive(Debug, Default)]
struct Sent {
    offsets: Vec<u64>,
    checkpoints: Vec<u64>,
    behind: Vec<u64>,
    replans: usize,
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
                    Some(SourceEvent::Behind { records }) => sent.behind.push(records),
                    Some(SourceEvent::Replan) => sent.replans += 1,
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
    let factory = acknowledging_source_factory::<LogSource>();
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

#[tokio::test]
async fn a_stream_without_partitions_or_rows_to_a_batch_is_refused() {
    for (partitions, batch_rows) in [(0, 10), (1, 0)] {
        let stream = json!({
            "name": "events", "partitions": partitions, "messages": 1, "batch_rows": batch_rows,
        });
        let refused = source_factory::<LogSource>()
            .connect(config(&stream, "refused"), ConnectContext::new())
            .await
            .err()
            .expect("the configuration is refused");
        assert_eq!(refused.kind(), ConnectorErrorKind::Config, "{stream}");
    }
}

#[test]
fn a_partition_count_at_its_limit_neither_wraps_nor_panics() {
    let stream: LoggedStream = serde_json::from_value(json!({
        "name": "events", "partitions": u32::MAX, "messages": 3,
        "partitions_later": u32::MAX, "partitions_retired": 1,
    }))
    .expect("a valid stream");
    assert_eq!(Logged(stream).partitions(Duration::ZERO), u32::MAX - 1);
}

#[tokio::test(start_paused = true)]
async fn a_batch_at_its_limit_neither_wraps_nor_panics() {
    let stream = json!({
        "name": "events", "partitions": 1, "messages": 3, "batch_rows": MAX_MESSAGE_ROWS,
    });
    let source = connect(&stream, "a_whole_batch").await;
    let (read, sent) = read(source.as_ref(), Some(offset(1)), None).await;
    read.expect("the read ends");
    assert_eq!(sent.offsets, [1, 2]);
}

#[tokio::test(start_paused = true)]
async fn a_log_keeps_its_newest_messages_and_refuses_to_resume_from_one_it_dropped() {
    let stream = json!({ "name": "events", "partitions": 1, "messages": 10, "retention": 4 });
    let source = connect(&stream, "retained").await;
    let (from_the_start, sent) = read(source.as_ref(), None, None).await;
    from_the_start.expect("a read from the start reads what the log keeps");
    assert_eq!(sent.offsets, [6, 7, 8, 9]);
    let (kept, sent) = read(source.as_ref(), Some(offset(7)), None).await;
    kept.expect("a read from a kept offset succeeds");
    assert_eq!(sent.offsets, [7, 8, 9]);
    let (earliest, sent) = read(source.as_ref(), Some(offset(6)), None).await;
    earliest.expect("a read from the earliest kept offset succeeds");
    assert_eq!(sent.offsets, [6, 7, 8, 9]);
    let (dropped, _) = read(source.as_ref(), Some(offset(3)), None).await;
    let lost = dropped.expect_err("a read from a dropped offset fails");
    assert_eq!(lost.code(), Some(rdlt_connector::RETENTION_LOST));
}

#[tokio::test(start_paused = true)]
async fn a_read_says_how_far_behind_the_head_it_is_at_each_checkpoint() {
    let stream = json!({ "name": "events", "partitions": 1, "messages": 25 });
    let source = connect(&stream, "behind").await;
    let (read, sent) = read(source.as_ref(), None, None).await;
    read.expect("the read ends");
    assert_eq!(sent.behind, [15, 5, 0]);
}

#[tokio::test(start_paused = true)]
async fn a_following_read_of_the_first_partition_asks_for_a_plan_as_partitions_are_added() {
    let stream = json!({
        "name": "events", "partitions": 1, "messages": 1,
        "partitions_later": 1, "later_after_ms": 500,
    });
    let source = connect(&stream, "signalled").await;
    let (read, sent) = read(source.as_ref(), None, Some(Duration::from_secs(2))).await;
    read.expect("a stopped read ends cleanly");
    assert_eq!(sent.replans, 1);
}

#[tokio::test(start_paused = true)]
async fn a_group_on_disk_keeps_its_committed_offsets_for_its_next_process() {
    let dir = crate::scratch::tempdir().expect("a temporary directory");
    let path = dir.path().join("events.group");
    let stream = json!({ "name": "events", "partitions": 1, "messages": 8 });
    let mut kept = config(&stream, "unused");
    kept["group_path"] = json!(path);
    let source = source_factory::<LogSource>()
        .connect(kept, ConnectContext::new())
        .await
        .expect("the source connects");
    source
        .committed(&events(), &[(p(0), offset(5))])
        .await
        .expect("the offset commits");
    let found: crate::kept::Kept<u64> = crate::kept::Kept::at(&path).expect("the group reads");
    assert_eq!(found.position("events", &p(0)), Some(5));
}

#[tokio::test]
async fn a_stream_of_more_partitions_or_messages_a_batch_than_a_source_holds_is_refused() {
    let most = u32::try_from(MAX_PARTITIONS).unwrap();
    let at_limits = json!({
        "name": "events", "partitions": most - 1, "partitions_later": 1, "messages": 1,
        "batch_rows": MAX_MESSAGE_ROWS,
    });
    connect(&at_limits, "at_limits").await;
    for (partitions, later, batch_rows) in [
        (most + 1, 0, 1),
        (most, 1, 1),
        (u32::MAX, u32::MAX, 1),
        (1, 0, MAX_MESSAGE_ROWS + 1),
    ] {
        let stream = json!({
            "name": "events", "partitions": partitions, "partitions_later": later,
            "messages": 1, "batch_rows": batch_rows,
        });
        let refused = source_factory::<LogSource>()
            .connect(config(&stream, "past_limits"), ConnectContext::new())
            .await
            .err()
            .expect("past a limit");
        assert_eq!(refused.kind(), ConnectorErrorKind::Config, "{stream}");
        assert_eq!(refused.code(), Some("limit_exceeded"), "{stream}");
    }
}

#[test]
fn the_last_offset_a_number_holds_arrives_later_never_at_once() {
    let stream: LoggedStream = serde_json::from_value(json!({
        "name": "events", "partitions": 1, "messages": 0, "per_second": 1,
    }))
    .expect("a valid stream");
    let wait = Logged(stream).arrives(u64::MAX, Duration::ZERO);
    assert!(wait.expect("the log grows") > Duration::from_secs(3600));
}

#[tokio::test(start_paused = true)]
async fn an_offset_of_a_partition_the_stream_never_has_is_not_committed() {
    let stream = json!({
        "name": "events", "partitions": 2, "partitions_later": 1, "messages": 8,
    });
    let source = connect(&stream, "members").await;
    source
        .committed(&events(), &[(p(2), offset(4))])
        .await
        .expect("a partition the stream gains");
    for partition in ["p3", "p03", "p", "snapshot-0", "p4294967296"] {
        let partition = PartitionId::parse(partition).expect("a valid partition");
        let refused = source
            .committed(
                &events(),
                &[(p(0), offset(7)), (partition.clone(), offset(4))],
            )
            .await
            .expect_err("no such partition");
        assert_eq!(refused.kind(), ConnectorErrorKind::Data, "{partition}");
    }
    // A commit naming a partition the stream never has kept none of its offsets.
    let factory = source_factory::<LogSource>();
    let (_, reader) = factory
        .connect_acknowledging(config(&stream, "members"), ConnectContext::new())
        .await
        .expect("the source connects with its reader");
    let told = reader.acknowledged(&events(), &p(0)).await;
    assert_eq!(told.expect("the group answers"), None);
}

#[tokio::test]
async fn a_group_is_kept_only_in_a_file_named_whole_and_as_a_group_s() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let inside = |name: &str| dir.path().join(name);
    let stream = json!({ "name": "events", "partitions": 1, "messages": 8 });
    let paths = [
        std::path::PathBuf::from("events.group"),
        std::path::PathBuf::from("./events.group"),
        inside("nested/../events.group"),
        inside("./events.group"),
        inside("events.slot"),
        inside("events"),
        inside("server.key"),
        inside(".group"),
    ];
    for path in paths {
        let mut kept = config(&stream, "unused");
        kept["group_path"] = json!(path);
        let refused = source_factory::<LogSource>()
            .connect(kept, ConnectContext::new())
            .await
            .err()
            .unwrap_or_else(|| panic!("{} is refused", path.display()));
        assert_eq!(refused.kind(), ConnectorErrorKind::Config);
        assert_eq!(refused.code(), Some("keeper_path_invalid"), "{refused}");
    }
}

#[tokio::test]
async fn a_log_that_forgets_names_the_group_it_keeps_its_offsets_in() {
    let forgets = json!({ "name": "events", "partitions": 1, "messages": 8, "replayable": false });
    let serves_again = json!({ "name": "other", "partitions": 1, "messages": 8 });
    let unnamed = json!({ "seed": 3, "streams": [serves_again, forgets] });
    let refused = source_factory::<LogSource>()
        .connect(unnamed.clone(), ConnectContext::new())
        .await
        .err()
        .expect("no group is named");
    assert_eq!(refused.kind(), ConnectorErrorKind::Config);
    assert_eq!(refused.code(), Some("keeper_unnamed"));
    let dir = tempfile::tempdir().expect("a temporary directory");
    let named = [
        ("group", json!("named")),
        ("group_path", json!(dir.path().join("events.group"))),
    ];
    for (field, value) in named {
        let mut config = unnamed.clone();
        config[field] = value;
        let connected = source_factory::<LogSource>()
            .connect(config, ConnectContext::new())
            .await;
        connected.unwrap_or_else(|error| panic!("{field} names the group: {error}"));
    }
    // A log that serves its messages again may share the default group.
    let config = json!({ "seed": 3, "streams": [serves_again] });
    let connected = source_factory::<LogSource>()
        .connect(config, ConnectContext::new())
        .await;
    connected.expect("the default group");
}
