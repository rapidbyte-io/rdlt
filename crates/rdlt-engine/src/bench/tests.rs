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
use super::{Refused, SinkWriter, ipc_sink, replay, shred, shred_on};
use crate::compute::RayonPool;
use crate::{Engine, EngineConfig, PipelinePlan, StreamPlan, SystemEnv};

#[test]
fn shredding_inline_and_on_a_pool_gives_the_same_batches() {
    let pushes = [
        Bytes::from_static(b"{\"a\":1}\n{\"a\":2}"),
        Bytes::from_static(b"[{\"a\":3}]"),
    ];
    let inline = shred(&pushes, 8).unwrap();
    let pool = RayonPool::new(NonZeroUsize::new(2).unwrap()).unwrap();
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
    let pool = RayonPool::new(NonZeroUsize::new(2).unwrap()).unwrap();
    let engine = Engine::new(EngineConfig::default(), Arc::new(SystemEnv::new(pool)));
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
