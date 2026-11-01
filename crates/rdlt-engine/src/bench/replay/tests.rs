use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use bytes::Bytes;
use proptest::prelude::*;
use rdlt_connector::testing::certify_source;
use rdlt_connector::{
    ConnectContext, ConnectorErrorKind, Partition, PartitionId, PipelineId, ReadMode, ReadRequest,
    SourceEvent, StreamName, StreamState, partition_channel,
};
use serde_json::value::RawValue;
use serde_json::{Value, json};

use super::{Replay, connect_replay, following};
use crate::bench::runner::streams;
use crate::bench::{
    CHUNK_BYTES, Corpus, Replayed, ipc_sink, null_sink, register, replay, replay_config,
    replay_factory, rows_pushed, shred, split_replay_config, stream_name,
};
use crate::env::{Env, InlineEnv};
use crate::{Engine, EngineConfig, PipelinePlan, StreamPlan, SystemEnv};

#[tokio::test]
async fn replayed_batches_pass_through_the_engine_into_the_sink() {
    let batches: Vec<RecordBatch> = (0..3)
        .map(|batch| {
            let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(batch * 10..batch * 10 + 10));
            RecordBatch::try_from_iter([("id", ids)]).unwrap()
        })
        .collect();
    let engine = Engine::new(EngineConfig::default(), Arc::new(SystemEnv::one_core()));
    let plan = PipelinePlan::new(
        PipelineId::parse("replay").unwrap(),
        [StreamPlan::new(StreamName::new("events").unwrap())],
    )
    .unwrap();
    let outcome = engine
        .run(
            plan,
            replay("replay", Arc::new(Replayed::Batches(batches))).await,
            ipc_sink().await,
        )
        .await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(outcome.report.rows, 30);
}

#[tokio::test]
async fn a_replay_is_shared_by_the_sources_it_connects_never_copied() {
    let replayed = Arc::new(Replayed::Batches(ten_row_batches(3)));
    register("shared", Arc::clone(&replayed));
    let sources = [
        connect_replay(replay_config("shared")).await,
        connect_replay(replay_config("shared")).await,
    ];
    // Held here, by the registry and by each source.
    assert_eq!(Arc::strong_count(&replayed), 2 + sources.len());
}

#[tokio::test]
async fn the_replay_source_resumes_after_each_checkpoint() {
    let batches: Vec<RecordBatch> = (0..3)
        .map(|batch| {
            let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(batch * 10..batch * 10 + 10));
            RecordBatch::try_from_iter([("id", ids)]).unwrap()
        })
        .collect();
    replay("certified", Arc::new(Replayed::Batches(batches))).await;
    certify_source::<Replay>(json!({ "name": "certified" }))
        .await
        .assert_passed();
}

/// `count` batches of ten rows, their ids running on from one batch to the next.
fn ten_row_batches(count: i64) -> Vec<RecordBatch> {
    (0..count)
        .map(|batch| {
            let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(batch * 10..batch * 10 + 10));
            RecordBatch::try_from_iter([("id", ids)]).unwrap()
        })
        .collect()
}

#[tokio::test]
async fn a_split_replay_plans_its_partitions_and_pushes_every_batch_once() {
    register("split", Arc::new(Replayed::Batches(ten_row_batches(8))));
    let four = NonZeroUsize::new(4).unwrap();
    let source = replay_factory()
        .connect(
            split_replay_config("split", four, NonZeroUsize::MIN),
            ConnectContext::new(),
        )
        .await
        .unwrap();
    let stream = StreamName::new("events").unwrap();
    let planned = source.plan(&stream, &StreamState::default()).await.unwrap();
    assert_eq!(planned.partitions.len(), 4);
    let engine = Engine::new(EngineConfig::default(), Arc::new(SystemEnv::one_core()));
    let plan = PipelinePlan::new(
        PipelineId::parse("split").unwrap(),
        [StreamPlan::new(stream)],
    )
    .unwrap();
    let outcome = engine.run(plan, Arc::from(source), ipc_sink().await).await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(outcome.report.rows, 80);
}

#[tokio::test]
async fn a_split_replay_resumes_each_partition_after_each_checkpoint() {
    register(
        "certified-split",
        Arc::new(Replayed::Batches(ten_row_batches(7))),
    );
    certify_source::<Replay>(json!({ "name": "certified-split", "partitions": 3 }))
        .await
        .assert_passed();
}

#[tokio::test]
async fn a_replay_of_several_streams_pushes_every_batch_once_across_them() {
    // Seven batches over three streams of two partitions: shares that do not divide.
    register("streams", Arc::new(Replayed::Batches(ten_row_batches(7))));
    let (two, three) = (NonZeroUsize::new(2).unwrap(), NonZeroUsize::new(3).unwrap());
    let source = replay_factory()
        .connect(
            split_replay_config("streams", two, three),
            ConnectContext::new(),
        )
        .await
        .unwrap();
    let engine = Engine::new(EngineConfig::default(), Arc::new(SystemEnv::one_core()));
    let plan = PipelinePlan::new(PipelineId::parse("streams").unwrap(), streams(three)).unwrap();
    let outcome = engine.run(plan, Arc::from(source), ipc_sink().await).await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(outcome.report.rows, 70);
    let names: Vec<&str> = outcome.report.streams.keys().map(String::as_str).collect();
    assert_eq!(names, ["events", "events_1", "events_2"]);
}

#[tokio::test]
async fn a_replay_of_several_streams_resumes_each_partition_after_each_checkpoint() {
    register(
        "certified-streams",
        Arc::new(Replayed::Batches(ten_row_batches(7))),
    );
    let config = json!({ "name": "certified-streams", "partitions": 3, "streams": 2 });
    certify_source::<Replay>(config).await.assert_passed();
}

/// What a read of `config`'s one partition sends: `None` for a push, the cursor of a checkpoint.
async fn read_one(config: Value) -> Vec<Option<usize>> {
    read_partition(config, Partition::single()).await
}

/// What a read of `config`'s `partition` sends: `None` for a push, the cursor of a checkpoint.
async fn read_partition(config: Value, partition: Partition) -> Vec<Option<usize>> {
    let source = replay_factory()
        .connect(config, ConnectContext::new())
        .await
        .unwrap();
    let (sink, mut feed) = partition_channel(NonZeroUsize::new(64).unwrap());
    let request = ReadRequest::new(stream_name(0), partition, None);
    source.read(request, sink).await.unwrap();
    let mut sent = Vec::new();
    while let Some(event) = feed.recv().await {
        match event {
            SourceEvent::Push(_) => sent.push(None),
            SourceEvent::Checkpoint { cursor, .. } => {
                sent.push(Some(serde_json::from_slice(cursor.bytes()).unwrap()));
            }
            other => panic!("a replay sends pushes and checkpoints alone, not {other:?}"),
        }
    }
    sent
}

#[tokio::test]
async fn a_replay_checkpoints_after_every_kth_push_of_a_partition_and_after_its_last() {
    register(
        "every-third",
        Arc::new(Replayed::Batches(ten_row_batches(7))),
    );
    let config = json!({ "name": "every-third", "every": 3 });
    let sent = read_one(config).await;
    let pushed = [None, None, None];
    let expected: Vec<Option<usize>> =
        [&pushed[..], &[Some(3)], &pushed, &[Some(6), None, Some(7)]].concat();
    assert_eq!(sent, expected);
    let each = read_one(json!({ "name": "every-third" })).await;
    let checkpoints: Vec<usize> = each.into_iter().flatten().collect();
    assert_eq!(checkpoints, (1..=7).collect::<Vec<_>>());
    // Nine pushes over two partitions: the second's share is pushes 5 to 9.
    register(
        "every-third-split",
        Arc::new(Replayed::Batches(ten_row_batches(9))),
    );
    let config = json!({ "name": "every-third-split", "partitions": 2, "every": 3 });
    let second = Partition::new(PartitionId::parse("1").unwrap());
    let sent = read_partition(config, second).await;
    let checkpoints: Vec<usize> = sent.iter().flatten().copied().collect();
    assert_eq!((sent.len(), checkpoints), (7, vec![7, 9]));
}

#[tokio::test]
async fn a_replay_checkpointing_every_few_pushes_resumes_after_each_checkpoint() {
    register(
        "certified-every",
        Arc::new(Replayed::Batches(ten_row_batches(11))),
    );
    let config = json!({ "name": "certified-every", "partitions": 2, "every": 3 });
    certify_source::<Replay>(config).await.assert_passed();
}

#[test]
fn the_first_stream_is_events_and_the_rest_are_numbered() {
    assert_eq!(stream_name(0), StreamName::new("events").unwrap());
    assert_eq!(stream_name(3), StreamName::new("events_3").unwrap());
}

#[tokio::test]
async fn a_split_replay_refuses_a_partition_it_never_planned() {
    register("unplanned", Arc::new(Replayed::Batches(ten_row_batches(4))));
    let two = NonZeroUsize::new(2).unwrap();
    let source = replay_factory()
        .connect(
            split_replay_config("unplanned", two, NonZeroUsize::MIN),
            ConnectContext::new(),
        )
        .await
        .unwrap();
    for id in ["2", "x"] {
        let (sink, _feed) = partition_channel(NonZeroUsize::new(16).unwrap());
        let partition = Partition::new(PartitionId::parse(id).unwrap());
        let request = ReadRequest::new(StreamName::new("events").unwrap(), partition, None);
        let error = source.read(request, sink).await.unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::Config, "{id}");
    }
}

#[tokio::test]
async fn replayed_json_pushes_pass_through_the_engine_into_the_null_sink() {
    let pushes = vec![
        Bytes::from_static(b"{\"id\":1}\n{\"id\":2}\n"),
        Bytes::from_static(b"{\"id\":3}\n"),
    ];
    let replayed = Replayed::Json(pushes);
    assert_eq!(replayed.pushes(), 2);
    let engine = Engine::new(EngineConfig::default(), Arc::new(SystemEnv::one_core()));
    let plan = PipelinePlan::new(
        PipelineId::parse("json").unwrap(),
        [StreamPlan::new(StreamName::new("events").unwrap())],
    )
    .unwrap();
    let outcome = engine
        .run(
            plan,
            replay("json", Arc::new(replayed)).await,
            null_sink().await,
        )
        .await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(outcome.report.rows, 3);
    assert_eq!(outcome.report.commits, 1);
}

#[test]
fn rows_of_compact_lines_pushed_through_the_sdk_are_pushed_as_those_lines() {
    let lines = Corpus::Nested.rows(1_000, std::num::NonZeroU64::new(300).unwrap());
    assert_eq!(lines.len(), 4);
    assert_eq!(rows_pushed(lines.clone()), lines);
}

/// A JSON object a few levels deep, its strings holding line ends, quotes and backslashes.
fn json_object() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        (-1e6..1e6_f64).prop_map(Value::from),
        "[a\n\r\t\"\\\\ \u{e9}\u{2028}]{0,6}".prop_map(Value::from),
    ];
    let value = leaf.prop_recursive(3, 24, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            prop::collection::btree_map("[a-c]", inner, 0..4)
                .prop_map(|fields| Value::Object(fields.into_iter().collect())),
        ]
    });
    prop::collection::btree_map("[a-d]", value, 0..4)
        .prop_map(|fields| Value::Object(fields.into_iter().collect()))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn rows_pushed_through_the_sdk_hold_a_record_a_line_and_shred_as_the_same_rows_in_an_array(
        rows in prop::collection::vec((json_object(), any::<bool>()), 1..8),
    ) {
        let texts: Vec<String> = rows
            .iter()
            .map(|(row, printed)| {
                if *printed {
                    serde_json::to_string_pretty(row).unwrap()
                } else {
                    row.to_string()
                }
            })
            .collect();
        let pushed = rows_pushed(vec![Bytes::from(texts.join("\n"))]);
        let [lines] = pushed.as_slice() else {
            panic!("one call of rows makes one push: {pushed:?}");
        };
        prop_assert!(lines.ends_with(b"\n"), "{lines:?}");
        let ends = memchr::memchr_iter(b'\n', lines).count();
        prop_assert_eq!(ends, rows.len());
        let records: Vec<&RawValue> = texts
            .iter()
            .map(|text| serde_json::from_str(text).unwrap())
            .collect();
        let array = Bytes::from(serde_json::to_vec(&records).unwrap());
        prop_assert_eq!(
            shred(std::slice::from_ref(lines), CHUNK_BYTES),
            shred(&[array], CHUNK_BYTES)
        );
    }
}

#[test]
fn a_replay_of_rows_printed_over_several_lines_pushes_each_row() {
    let text = "{\n  \"id\": 1\n}\n{\n  \"id\": 2,\n  \"tags\": [\n    \"a\"\n  ]\n}";
    let pushed = rows_pushed(vec![Bytes::from(text)]);
    let rows = shred(&pushed, CHUNK_BYTES).unwrap();
    assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
}

#[tokio::test]
async fn replayed_rows_pass_through_the_engine_a_push_of_rows_each() {
    let lines = vec![
        Bytes::from_static(b"{\"id\":1}\n{\"id\":2}\n"),
        Bytes::from_static(b"{\"id\":3}"),
    ];
    let replayed = Replayed::Rows(lines);
    assert_eq!(replayed.pushes(), 2);
    let engine = Engine::new(EngineConfig::default(), Arc::new(SystemEnv::one_core()));
    let plan = PipelinePlan::new(
        PipelineId::parse("rows").unwrap(),
        [StreamPlan::new(StreamName::new("events").unwrap())],
    )
    .unwrap();
    let outcome = engine
        .run(
            plan,
            replay("rows", Arc::new(replayed)).await,
            null_sink().await,
        )
        .await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(outcome.report.rows, 3);
}

#[tokio::test]
async fn a_replay_of_rows_that_are_not_json_fails_its_read() {
    for text in [&b"{\"id\":1}\nnot json"[..], &[0xff, b'\n']] {
        register(
            "broken-rows",
            Arc::new(Replayed::Rows(vec![Bytes::copy_from_slice(text)])),
        );
        let source = replay_factory()
            .connect(replay_config("broken-rows"), ConnectContext::new())
            .await
            .unwrap();
        let (sink, _feed) = partition_channel(NonZeroUsize::new(4).unwrap());
        let request = ReadRequest::new(stream_name(0), Partition::single(), None);
        let error = source.read(request, sink).await.unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::Data);
    }
}

/// What a followed read of the first partition of `config`'s stream numbered `stream` sends,
/// each event with when it arrived since the read started: `None` for a push, the cursor of a
/// checkpoint; the read following where `follow` says.
struct Followed {
    sent: Vec<(Duration, Option<usize>)>,
    /// Whether the read was still waiting an hour after its last event.
    waited: bool,
}

/// Reads as [`Followed`] says, stopping the read an hour after its last event if it is still
/// reading, and checks that it returns without an error.
async fn read_followed(config: Value, stream: usize, follow: bool) -> Followed {
    let source = replay_factory()
        .connect(config, ConnectContext::new())
        .await
        .unwrap();
    let (sink, mut feed) = partition_channel(NonZeroUsize::new(64).unwrap());
    let partition = Partition::single().unbounded();
    let request = ReadRequest::new(stream_name(stream), partition, None).following(follow);
    let clock = InlineEnv;
    let started = clock.instant();
    let since = || clock.instant().saturating_duration_since(started);
    let read = source.read(request, sink);
    tokio::pin!(read);
    let (mut sent, mut waited, mut ended) = (Vec::new(), false, None);
    loop {
        tokio::select! {
            biased;
            // Events first, so each the read sent is seen before its end.
            event = feed.recv() => match event {
                Some(SourceEvent::Push(_)) => sent.push((since(), None)),
                Some(SourceEvent::Checkpoint { cursor, .. }) => {
                    let cursor = serde_json::from_slice(cursor.bytes()).unwrap();
                    sent.push((since(), Some(cursor)));
                }
                Some(other) => panic!("a replay sends pushes and checkpoints alone, not {other:?}"),
                None => break,
            },
            read = &mut read, if ended.is_none() => ended = Some(read),
            () = clock.sleep(Duration::from_hours(1)), if ended.is_none() => {
                assert!(!waited, "a stopped read ends within an hour");
                waited = true;
                feed.stop();
            }
        }
    }
    match ended {
        Some(read) => read.unwrap(),
        None => read.await.unwrap(),
    }
    Followed { sent, waited }
}

#[tokio::test(start_paused = true)]
async fn a_followed_replay_pushes_its_share_a_pace_apart_then_waits_until_stopped() {
    register("followed", Arc::new(Replayed::Batches(ten_row_batches(6))));
    let two = NonZeroUsize::new(2).unwrap();
    let pace = Duration::from_secs(1);
    let config = following(
        split_replay_config("followed", NonZeroUsize::MIN, two),
        pace,
    );
    // The second of two streams pushes the second half of six pushes.
    let read = read_followed(config, 1, true).await;
    let second = |seconds| Duration::from_secs(seconds);
    let expected = [
        (second(0), None),
        (second(0), Some(4)),
        (second(1), None),
        (second(1), Some(5)),
        (second(2), None),
        (second(2), Some(6)),
    ];
    assert_eq!(read.sent, expected);
    assert!(read.waited, "a followed read waits until it is stopped");
}

#[tokio::test(start_paused = true)]
async fn a_followed_replay_not_asked_to_follow_returns_once_its_share_is_pushed() {
    register(
        "unfollowed",
        Arc::new(Replayed::Batches(ten_row_batches(3))),
    );
    let pace = Duration::from_secs(1);
    let config = following(replay_config("unfollowed"), pace);
    let read = read_followed(config, 0, false).await;
    let checkpoints: Vec<(Duration, usize)> = read
        .sent
        .iter()
        .filter_map(|(at, cursor)| cursor.map(|cursor| (*at, cursor)))
        .collect();
    let second = |seconds| Duration::from_secs(seconds);
    assert_eq!(
        checkpoints,
        [(second(0), 1), (second(1), 2), (second(2), 3)]
    );
    assert!(
        !read.waited,
        "a read not asked to follow returns once caught up"
    );
}

#[tokio::test]
async fn a_followed_replay_plans_one_unbounded_partition_a_stream_read_incrementally() {
    register(
        "planned-followed",
        Arc::new(Replayed::Batches(ten_row_batches(4))),
    );
    let two = NonZeroUsize::new(2).unwrap();
    let config = split_replay_config("planned-followed", NonZeroUsize::MIN, two);
    let source = replay_factory()
        .connect(
            following(config, Duration::from_secs(1)),
            ConnectContext::new(),
        )
        .await
        .unwrap();
    let catalog = source.discover().await.unwrap();
    for index in 0..2 {
        let stream = stream_name(index);
        let spec = catalog.get(&stream).unwrap();
        assert_eq!(spec.read_modes(), [ReadMode::Incremental]);
        assert!(spec.is_replayable());
        let planned = source.plan(&stream, &StreamState::default()).await.unwrap();
        assert_eq!(planned.partitions, [Partition::single().unbounded()]);
    }
}

#[tokio::test(start_paused = true)]
async fn a_followed_replay_resumes_after_each_checkpoint() {
    register(
        "certified-followed",
        Arc::new(Replayed::Batches(ten_row_batches(5))),
    );
    let config = following(replay_config("certified-followed"), Duration::from_secs(1));
    certify_source::<Replay>(config).await.assert_passed();
}

#[tokio::test(start_paused = true)]
async fn a_followed_replay_stopped_between_its_pushes_returns_at_once() {
    register(
        "stopped-followed",
        Arc::new(Replayed::Batches(ten_row_batches(3))),
    );
    let config = following(replay_config("stopped-followed"), Duration::from_hours(1));
    let source = replay_factory()
        .connect(config, ConnectContext::new())
        .await
        .unwrap();
    let (sink, mut feed) = partition_channel(NonZeroUsize::new(64).unwrap());
    let partition = Partition::single().unbounded();
    let request = ReadRequest::new(stream_name(0), partition, None).following(true);
    let clock = InlineEnv;
    let read = source.read(request, sink);
    tokio::pin!(read);
    let mut events = 0;
    while events < 2 {
        tokio::select! {
            biased;
            // The first push and its checkpoint come before the read could end.
            event = feed.recv() => {
                assert!(event.is_some(), "the first push and its checkpoint are sent");
                events += 1;
            }
            read = &mut read => panic!("the read ended before it was stopped: {read:?}"),
            () = clock.sleep(Duration::from_secs(1)) => panic!("the first push comes at once"),
        }
    }
    feed.stop();
    tokio::select! {
        biased;
        // The read's end wins: one stopped while it waits for its next push returns at once.
        read = &mut read => read.unwrap(),
        () = clock.sleep(Duration::from_secs(1)) => panic!("a stopped read returns at once"),
    }
    assert!(
        feed.recv().await.is_none(),
        "nothing is pushed once stopped"
    );
}
