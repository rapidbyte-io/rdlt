use std::io::Cursor;
use std::num::{NonZeroU16, NonZeroUsize};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_ipc::reader::StreamReader;
use bytes::Bytes;
use proptest::prelude::*;
use rdlt_connector::testing::certify_source;
use rdlt_connector::{
    ConnectContext, ConnectorErrorKind, Partition, PartitionId, PipelineId, ReadRequest, SegmentId,
    SourceEvent, StreamName, StreamState, TableWriter, WriteStats, partition_channel,
};
use serde_json::value::RawValue;
use serde_json::{Value, json};

use super::replay::Replay;
use super::runner::streams;
use super::{ADMITTED, CHUNK_BYTES, Corpus, PUSH_BYTES, Parse};
use super::{
    Refused, Replayed, SinkWriter, Sinking, check_json, ipc_sink, json_column, normalize,
    null_sink, pruned, register, replay, replay_config, replay_factory, rows_pushed, shred,
    shred_counted, shred_on, shred_with, sink_factory, split_replay_config, stream_name,
};
use crate::compute::Cores;
use crate::env::Env;
use crate::error::codes;
use crate::{Engine, EngineConfig, PipelinePlan, StreamPlan, SystemEnv};

#[test]
fn shredding_inline_and_on_a_pool_gives_the_same_batches() {
    let pushes = [
        Bytes::from_static(b"{\"a\":1}\n{\"a\":2}"),
        Bytes::from_static(b"[{\"a\":3}]"),
    ];
    let inline = shred(&pushes, 8).unwrap();
    let cores = Cores::new(NonZeroUsize::new(2).unwrap());
    let env: Arc<dyn Env> = Arc::new(SystemEnv::try_new(cores).unwrap());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let pooled = runtime.block_on(shred_on(&env, &pushes, 8)).unwrap();
    assert_eq!(inline, pooled);
    assert_eq!(inline.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
}

#[test]
fn shredding_each_push_alone_shreds_every_row_of_the_corpus() {
    let pushes = Corpus::Nested.pushes(2 * PUSH_BYTES + PUSH_BYTES / 2);
    assert!(pushes.len() > 1);
    assert!(pushes.last().unwrap().len() < pushes[0].len());
    let whole: usize = shred(&pushes, CHUNK_BYTES)
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    let alone: usize = pushes
        .iter()
        .flat_map(|push| shred(std::slice::from_ref(push), CHUNK_BYTES).unwrap())
        .map(|batch| batch.num_rows())
        .sum();
    assert_eq!(alone, whole);
}

#[test]
fn counting_a_shred_counts_one_parse_a_chunk_of_plain_rows() {
    let pushes = Corpus::Nested.pushes(4 << 20);
    let counts = shred_counted(&pushes, CHUNK_BYTES).unwrap();
    assert_eq!(counts.parsed, 4);
    assert_eq!((counts.tripped, counts.rebuilt, counts.exact), (0, 0, 0));
}

/// `batches` as one batch.
fn joined(batches: &[RecordBatch]) -> RecordBatch {
    arrow_select::concat::concat_batches(&batches[0].schema(), batches).unwrap()
}

#[test]
fn shredding_any_way_the_engine_may_gives_the_batches_it_gives() {
    // Each cliff corpus drifts, trips its allowance or needs exact numbers, over several chunks.
    for corpus in Corpus::CLIFFS {
        let pushes = corpus.pushes(1 << 20);
        let engine = joined(&shred(&pushes, 64 << 10).unwrap());
        assert!(engine.num_rows() > 1000, "{}", corpus.name());
        for (chunk_bytes, admitted, parse) in [
            (64 << 10, ADMITTED, Parse::Fast),
            (1 << 20, 1 << 20, Parse::Fast),
            (64 << 10, 1 << 20, Parse::Exact),
            (64 << 10, ADMITTED, Parse::Exact),
        ] {
            let shredded = joined(&shred_with(&pushes, chunk_bytes, admitted, parse).unwrap());
            assert_eq!(
                shredded,
                engine,
                "{} {chunk_bytes} {admitted} {parse:?}",
                corpus.name()
            );
        }
    }
}

#[test]
fn a_way_of_shredding_takes_its_allowance_and_parse() {
    let fast = super::limits(7, Parse::Fast);
    assert_eq!((fast.admitted, fast.exact), (7, false));
    let exact = super::limits(ADMITTED, Parse::Exact);
    assert_eq!((exact.admitted, exact.exact), (ADMITTED, true));
    assert_eq!(exact.columns, rdlt_wire::limits::SCHEMA_COLUMNS);
}

#[test]
fn a_refusal_carries_the_errors_code_and_message() {
    let refused = shred(&[Bytes::from_static(b"[1]")], 8).unwrap_err();
    assert_eq!(
        refused,
        Refused {
            code: codes::JSON_NOT_OBJECT.as_str(),
            message: "a record is not a JSON object".to_owned(),
        }
    );
    assert_eq!(
        shred(&[Bytes::from_static(b"[")], 8).unwrap_err().code,
        codes::JSON_INVALID.as_str()
    );
}

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
            replay("replay", Replayed::Batches(batches)).await,
            ipc_sink().await,
        )
        .await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(outcome.report.rows, 30);
}

#[tokio::test]
async fn the_replay_source_resumes_after_each_checkpoint() {
    let batches: Vec<RecordBatch> = (0..3)
        .map(|batch| {
            let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(batch * 10..batch * 10 + 10));
            RecordBatch::try_from_iter([("id", ids)]).unwrap()
        })
        .collect();
    replay("certified", Replayed::Batches(batches)).await;
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
    register("split", Replayed::Batches(ten_row_batches(8)));
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
    register("certified-split", Replayed::Batches(ten_row_batches(7)));
    certify_source::<Replay>(json!({ "name": "certified-split", "partitions": 3 }))
        .await
        .assert_passed();
}

#[tokio::test]
async fn a_replay_of_several_streams_pushes_every_batch_once_across_them() {
    // Seven batches over three streams of two partitions: shares that do not divide.
    register("streams", Replayed::Batches(ten_row_batches(7)));
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
    register("certified-streams", Replayed::Batches(ten_row_batches(7)));
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
    register("every-third", Replayed::Batches(ten_row_batches(7)));
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
    register("every-third-split", Replayed::Batches(ten_row_batches(9)));
    let config = json!({ "name": "every-third-split", "partitions": 2, "every": 3 });
    let second = Partition::new(PartitionId::parse("1").unwrap());
    let sent = read_partition(config, second).await;
    let checkpoints: Vec<usize> = sent.iter().flatten().copied().collect();
    assert_eq!((sent.len(), checkpoints), (7, vec![7, 9]));
}

#[tokio::test]
async fn a_replay_checkpointing_every_few_pushes_resumes_after_each_checkpoint() {
    register("certified-every", Replayed::Batches(ten_row_batches(11)));
    let config = json!({ "name": "certified-every", "partitions": 2, "every": 3 });
    certify_source::<Replay>(config).await.assert_passed();
}

#[test]
fn the_first_stream_is_events_and_the_rest_are_numbered() {
    assert_eq!(stream_name(0), StreamName::new("events").unwrap());
    assert_eq!(stream_name(3), StreamName::new("events_3").unwrap());
}

#[tokio::test]
async fn a_sink_declares_the_writers_its_configuration_names_and_one_otherwise() {
    let factory = sink_factory();
    let one = factory
        .connect(Sinking::Ipc.config(), ConnectContext::new())
        .await
        .unwrap();
    assert_eq!(one.capabilities().max_parallel_writers, NonZeroU16::MIN);
    let sixteen = NonZeroU16::new(16).unwrap();
    let config = Sinking::Ipc.writing(sixteen);
    let many = factory
        .connect(config, ConnectContext::new())
        .await
        .unwrap();
    assert_eq!(many.capabilities().max_parallel_writers, sixteen);
}

#[tokio::test]
async fn a_split_replay_refuses_a_partition_it_never_planned() {
    register("unplanned", Replayed::Batches(ten_row_batches(4)));
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
async fn the_sink_encodes_each_batch_and_reports_what_it_staged() {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..10));
    let batch = RecordBatch::try_from_iter([("id", ids)]).unwrap();
    let mut writer = SinkWriter::new(Arc::default(), Sinking::Ipc);
    let encoded = writer.encode(&batch).unwrap().to_vec();
    let decoded = StreamReader::try_new(Cursor::new(&encoded), None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(decoded, std::slice::from_ref(&batch));
    writer.write(SegmentId(1), batch.clone()).await.unwrap();
    writer.write(SegmentId(1), batch).await.unwrap();
    let staged = WriteStats {
        rows: 20,
        bytes: 2 * encoded.len() as u64,
    };
    assert_eq!(writer.flush().await.unwrap(), staged);
    assert_eq!(writer.flush().await.unwrap(), WriteStats::default());
}

#[tokio::test]
async fn the_null_sink_counts_what_it_stages_and_encodes_nothing() {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..10));
    let batch = RecordBatch::try_from_iter([("id", ids)]).unwrap();
    let staged = Arc::default();
    let mut writer = SinkWriter::new(Arc::clone(&staged), Sinking::Discard);
    writer.write(SegmentId(1), batch.clone()).await.unwrap();
    writer.write(SegmentId(2), batch).await.unwrap();
    let flushed = WriteStats { rows: 20, bytes: 0 };
    assert_eq!(writer.flush().await.unwrap(), flushed);
    assert_eq!(
        *staged.lock(),
        [(SegmentId(1), 10), (SegmentId(2), 10)]
            .into_iter()
            .collect()
    );
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
        .run(plan, replay("json", replayed).await, null_sink().await)
        .await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(outcome.report.rows, 3);
    assert_eq!(outcome.report.commits, 1);
}

#[test]
fn normalizing_a_shredded_batch_names_each_table_by_its_path() {
    let push = Bytes::from_static(br#"{"id":1,"items":[{"sku":"a","tags":["x"]}]}"#);
    let batches = shred(&[push], 1 << 20).unwrap();
    let parts = normalize(&batches[0], 8, &["id"]).unwrap();
    let paths: Vec<Vec<String>> = parts.iter().map(|(path, _)| path.clone()).collect();
    assert_eq!(
        paths,
        [
            Vec::new(),
            vec!["items".to_owned()],
            vec!["items".to_owned(), "tags".to_owned()],
        ]
    );
    assert!(parts.iter().all(|(_, batch)| batch.num_rows() == 1));
}

#[test]
fn pruning_a_normalized_batch_drops_each_dropped_roots_children_with_it() {
    let push = Bytes::from_static(
        b"{\"id\":0,\"items\":[{\"tags\":[\"x\"]}]}\n{\"id\":1,\"items\":[{\"tags\":[\"y\",\"z\"]}]}",
    );
    let batches = shred(&[push], 1 << 20).unwrap();
    // Root 0 goes with its item and its tag; root 1, its item and its two tags are left.
    assert_eq!(
        pruned(&batches[0], 8, NonZeroUsize::new(2).unwrap()).unwrap(),
        4
    );
}

#[test]
fn a_sample_log_is_the_frames_each_encoded_alone_after_the_preamble() {
    use crate::wal::frame::{frames, limits, preamble};

    let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let batch = RecordBatch::try_from_iter([("id", ids)]).expect("a valid batch");
    let log = super::sample_log(batch);
    let limits = limits(EngineConfig::default().memory().get());
    let mut expected = preamble().to_vec();
    for frame in frames(&log, limits).expect("the log's frames read") {
        expected.extend_from_slice(&frame.encode().expect("the frame encodes"));
    }
    assert_eq!(log, expected);
}

#[test]
fn a_sample_log_reads_back_with_its_one_commit_and_a_garbled_one_is_refused() {
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let batch = RecordBatch::try_from_iter([("id", ids)]).expect("a valid batch");
    let log = super::sample_log(batch);
    assert_eq!(super::scan_log(&log), Ok(1));
    // Garble the commit frame's payload, then make its checksum match, as a bug would.
    let mut garbled = log.clone();
    // Past the chunk's preamble.
    let mut offset = 14;
    let mut frames = Vec::new();
    while offset + 9 <= garbled.len() {
        let len = u32::from_le_bytes(
            garbled[offset + 1..offset + 5]
                .try_into()
                .expect("four bytes"),
        );
        frames.push((
            garbled[offset],
            offset,
            usize::try_from(len).expect("a length"),
        ));
        offset += 9 + usize::try_from(len).expect("a length");
    }
    let (_, at, len) = *frames
        .iter()
        .find(|(kind, _, _)| *kind == 5)
        .expect("a commit frame");
    garbled[at + 9] = b'!';
    // The checksum covers the frame's kind and length too.
    let crc = crc32c::crc32c_append(
        crc32c::crc32c(&garbled[at..at + 5]),
        &garbled[at + 9..at + 9 + len],
    );
    garbled[at + 5..at + 9].copy_from_slice(&crc.to_le_bytes());
    let refused = super::scan_log(&garbled).expect_err("the commit does not decode");
    assert_eq!(refused.code, codes::WAL_UNREADABLE.as_str());
}

#[test]
fn a_log_another_load_wrote_is_refused_as_foreign() {
    use crate::wal::frame::{self, End, Frame, Header};
    let other = rdlt_connector::LoadId::from_parts(std::time::UNIX_EPOCH, 2);
    let header = Frame::Header(Header {
        pipeline: PipelineId::parse("fuzzed").expect("a valid pipeline"),
        load: other,
        chunk: 0,
        epoch: rdlt_connector::Epoch(1),
        opened: None,
        origin: other,
    });
    let mut log = frame::preamble().to_vec();
    for frame in [header, Frame::End(End::default())] {
        log.extend_from_slice(&frame.encode().expect("the frame encodes"));
    }
    let refused = super::scan_log(&log).expect_err("another load's log");
    assert_eq!(
        refused.code,
        codes::WAL_FOREIGN.as_str(),
        "{}",
        refused.message
    );
}

#[test]
fn a_column_of_json_checks_with_escapes_where_asked_and_a_broken_value_is_refused() {
    for escaped in [false, true] {
        let batch = json_column(1_000, escaped);
        assert_eq!(batch.num_rows(), 1_000);
        check_json(&batch).unwrap();
    }
    let escaped = json_column(8, true);
    let values = escaped.column(0).as_string::<i32>();
    assert!(values.iter().flatten().any(|value| value.contains('\\')));
    let column = arrow_array::StringArray::from(vec![r#"{"a":"\x01"}"#]);
    let schema = escaped.schema();
    let refused = RecordBatch::try_new(schema, vec![Arc::new(column)]).unwrap();
    assert_eq!(
        check_json(&refused).unwrap_err().code,
        codes::JSON_INVALID.as_str()
    );
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
fn lines_that_are_not_objects_are_refused_the_first_an_array_as_invalid_json() {
    // A first line that is an array reads as an array with more after it.
    for (lines, code) in [
        ("[1,2]\n[3]\n", codes::JSON_INVALID),
        ("{\"a\":1}\n[1]\n", codes::JSON_NOT_OBJECT),
        ("\"a\"\n{\"a\":1}\n", codes::JSON_NOT_OBJECT),
    ] {
        let push = Bytes::from_static(lines.as_bytes());
        let refused = shred(&[push], CHUNK_BYTES).unwrap_err();
        assert_eq!(refused.code, code.as_str(), "{lines}");
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
        .run(plan, replay("rows", replayed).await, null_sink().await)
        .await;
    assert!(outcome.error.is_none(), "{:?}", outcome.error);
    assert_eq!(outcome.report.rows, 3);
}

#[tokio::test]
async fn a_replay_of_rows_that_are_not_json_fails_its_read() {
    for text in [&b"{\"id\":1}\nnot json"[..], &[0xff, b'\n']] {
        register(
            "broken-rows",
            Replayed::Rows(vec![Bytes::copy_from_slice(text)]),
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
