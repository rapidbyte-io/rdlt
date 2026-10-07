use std::num::NonZeroUsize;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;

use std::sync::Arc;

use rdlt_connector::ConnectContext;

use super::Passthrough;
use crate::Cores;
use crate::bench::{Replayed, null_sink, register, replay_config, replay_factory};

fn ids(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column(0)
        .as_primitive::<Int64Type>()
        .values()
        .to_vec()
}

#[test]
fn the_bare_loop_and_the_engine_each_move_every_row_on_every_run() {
    let cores = Cores::from_count(NonZeroUsize::new(2).unwrap());
    let passthrough = Passthrough::try_new(cores, 3, 10).unwrap();
    let all: Vec<i64> = passthrough.batches().iter().flat_map(ids).collect();
    assert_eq!(all, (0..30).collect::<Vec<_>>());
    assert_eq!(passthrough.rows(), 30);
    let bytes = crate::bench::logical_bytes(passthrough.batches());
    assert_eq!(Passthrough::bytes(3, 10), bytes);
    for _ in 0..2 {
        assert_eq!(passthrough.bare_loop(), 30);
        assert_eq!(passthrough.engine_run(), 30);
    }
}

#[test]
fn the_engine_moves_the_batches_from_any_source_into_any_destination() {
    let cores = Cores::from_count(NonZeroUsize::new(2).unwrap());
    let passthrough = Passthrough::try_new(cores, 2, 10).unwrap();
    register(
        "anywhere",
        Replayed::Batches(passthrough.batches().to_vec()),
    );
    let (source, destination) = passthrough.block_on(async {
        let source = replay_factory()
            .connect(replay_config("anywhere"), ConnectContext::new())
            .await
            .unwrap();
        (Arc::from(source), null_sink().await)
    });
    assert_eq!(passthrough.run(source, destination), 20);
}
