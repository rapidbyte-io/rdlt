use std::io::Cursor;
use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_ipc::reader::StreamReader;
use bytes::Bytes;
use rdlt_connector::testing::certify_source;
use rdlt_connector::{PipelineId, SegmentId, StreamName, TableWriter, WriteStats};
use serde_json::json;

use super::connectors::Replay;
use super::{Refused, SinkWriter, ipc_sink, normalize, replay, shred, shred_on};
use crate::compute::{Cores, RayonPool};
use crate::{Engine, EngineConfig, PipelinePlan, StreamPlan, SystemEnv};

#[test]
fn shredding_inline_and_on_a_pool_gives_the_same_batches() {
    let pushes = [
        Bytes::from_static(b"{\"a\":1}\n{\"a\":2}"),
        Bytes::from_static(b"[{\"a\":3}]"),
    ];
    let inline = shred(&pushes, 8).unwrap();
    let pool =
        RayonPool::try_new(Cores::new(NonZeroUsize::new(3).unwrap(), NonZeroUsize::MIN)).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let pooled = runtime.block_on(shred_on(&pool, &pushes, 8)).unwrap();
    assert_eq!(inline, pooled);
    assert_eq!(inline.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
}

#[test]
fn a_refusal_carries_the_errors_code_and_message() {
    let refused = shred(&[Bytes::from_static(b"[1]")], 8).unwrap_err();
    assert_eq!(
        refused,
        Refused {
            code: "json_not_object",
            message: "a record is not a JSON object".to_owned(),
        }
    );
    assert_eq!(
        shred(&[Bytes::from_static(b"[")], 8).unwrap_err().code,
        "json_invalid"
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
        .run(plan, replay("replay", batches).await, ipc_sink().await)
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
    replay("certified", batches).await;
    certify_source::<Replay>(json!({ "name": "certified" }))
        .await
        .assert_passed();
}

#[tokio::test]
async fn the_sink_encodes_each_batch_and_reports_what_it_staged() {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..10));
    let batch = RecordBatch::try_from_iter([("id", ids)]).unwrap();
    let mut writer = SinkWriter::new(Arc::default());
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
    assert_eq!(refused.code, "wal_unreadable");
}
