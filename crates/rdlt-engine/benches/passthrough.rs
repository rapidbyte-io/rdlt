//! Arrow passthrough (spec §21.1): the engine against a bare loop writing the same batches to the
//! same destination; the gate is at most 10 % overhead.

use std::hint::black_box;
use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use rdlt_connector::{PipelineId, SegmentId, StreamName, TableWriter};
use rdlt_engine::bench::{SinkWriter, ipc_sink, replay};
use rdlt_engine::{
    CommitPolicy, Engine, EngineConfig, PipelinePlan, RayonPool, StreamPlan, SystemEnv,
};

/// Rows per batch: about 8 MiB of ten mixed columns.
const ROWS: i64 = 80_000;
/// Batches per run.
const BATCHES: usize = 64;

fn batch(offset: i64) -> RecordBatch {
    let ids = offset..offset + ROWS;
    let int = |factor: i64| -> ArrayRef {
        Arc::new(Int64Array::from_iter_values(
            ids.clone().map(|id| id * factor),
        ))
    };
    let float = |factor: f64| -> ArrayRef {
        Arc::new(Float64Array::from_iter_values(
            ids.clone()
                .map(|id| f64::from(u32::try_from(id).unwrap_or(0)) * factor),
        ))
    };
    let text = |prefix: &str| -> ArrayRef {
        Arc::new(StringArray::from_iter_values(
            ids.clone().map(|id| format!("{prefix}-{id:08}")),
        ))
    };
    let at: ArrayRef = Arc::new(
        TimestampMicrosecondArray::from_iter_values(
            ids.clone().map(|id| 1_790_000_000_000_000 + id),
        )
        .with_timezone("UTC"),
    );
    let flag: ArrayRef = Arc::new(BooleanArray::from_iter(
        ids.clone().map(|id| Some(id % 3 == 0)),
    ));
    let small: ArrayRef = Arc::new(Int32Array::from_iter_values(
        ids.clone().map(|id| i32::try_from(id % 1000).unwrap_or(0)),
    ));
    RecordBatch::try_from_iter([
        ("id", int(1)),
        ("a", int(7)),
        ("b", int(13)),
        ("x", float(0.5)),
        ("y", float(1.25)),
        ("name", text("user")),
        ("city", text("city")),
        ("at", at),
        ("flag", flag),
        ("n", small),
    ])
    .expect("equal-length columns make a batch")
}

fn passthrough(c: &mut Criterion) {
    let batches: Vec<RecordBatch> = (0..BATCHES)
        .map(|index| batch(i64::try_from(index).unwrap_or(0) * ROWS))
        .collect();
    let bytes: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a runtime starts");
    let mut group = c.benchmark_group("passthrough");
    group.sample_size(10);
    group.throughput(Throughput::Bytes(u64::try_from(bytes).unwrap_or(u64::MAX)));
    group.bench_function("bare", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let mut writer = SinkWriter::new(Arc::default());
                for batch in &batches {
                    writer
                        .write(SegmentId(1), batch.clone())
                        .await
                        .expect("the sink writes");
                }
                black_box(writer.flush().await.expect("the sink flushes"))
            })
        });
    });
    let config = EngineConfig::builder()
        .memory(1 << 30)
        .commit(CommitPolicy::new(None, None, Some(1 << 40)).expect("a valid policy"))
        .build()
        .expect("a valid configuration");
    let pool = RayonPool::new(NonZeroUsize::new(4).expect("four")).expect("the pool starts");
    let engine = Engine::new(config, Arc::new(SystemEnv::new(pool)));
    let plan = PipelinePlan::new(
        PipelineId::parse("passthrough").expect("a valid id"),
        [StreamPlan::new(
            StreamName::new("events").expect("a valid name"),
        )],
    )
    .expect("a valid plan");
    group.bench_function("engine", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let source = replay("passthrough", batches.clone()).await;
                let outcome = engine.run(plan.clone(), source, ipc_sink().await).await;
                assert!(outcome.error.is_none(), "{:?}", outcome.error);
                black_box(outcome.report.rows)
            })
        });
    });
    group.finish();
}

criterion_group!(benches, passthrough);
criterion_main!(benches);
