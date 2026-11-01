use std::num::NonZeroUsize;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;

use std::sync::Arc;

use rdlt_connector::ConnectContext;

use super::Passthrough;
use crate::Cores;
use crate::bench::{
    Cadence, Cell, Replayed, null_sink, register, replay_config, replay_factory, written,
};

fn ids(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column(0)
        .as_primitive::<Int64Type>()
        .values()
        .to_vec()
}

#[test]
fn the_bare_loop_and_the_engine_each_move_every_row_on_every_run() {
    let cores = Cores::new(NonZeroUsize::new(2).unwrap());
    let cell = Cell {
        rows: 10,
        batches: 3,
        ..Cell::PAIRED
    };
    let passthrough = Passthrough::try_new(cores, cell).unwrap();
    let all: Vec<i64> = passthrough.batches().iter().flat_map(ids).collect();
    assert_eq!(all, (0..30).collect::<Vec<_>>());
    assert_eq!(passthrough.rows(), 30);
    let bytes = crate::bench::logical_bytes(passthrough.batches());
    assert_eq!(Passthrough::bytes(passthrough.cell()), bytes);
    for _ in 0..2 {
        assert_eq!(passthrough.bare_loop(), 30);
        assert_eq!(passthrough.engine_run(), 30);
    }
}

#[test]
fn the_engine_moves_the_batches_from_any_source_into_any_destination() {
    let cores = Cores::new(NonZeroUsize::new(2).unwrap());
    let cell = Cell {
        rows: 10,
        batches: 2,
        ..Cell::PAIRED
    };
    let passthrough = Passthrough::try_new(cores, cell).unwrap();
    register(
        "anywhere",
        Arc::new(Replayed::Batches(passthrough.batches().to_vec())),
    );
    let (source, destination) = passthrough.block_on(async {
        let source = replay_factory()
            .connect(replay_config("anywhere"), ConnectContext::new())
            .await
            .unwrap();
        (Arc::from(source), null_sink().await)
    });
    assert_eq!(passthrough.run(source, destination).rows, 20);
}

#[test]
fn a_batch_is_the_workload_s_batch_whose_ids_run_from_the_first() {
    let cores = Cores::new(NonZeroUsize::new(2).unwrap());
    let cell = Cell {
        rows: 5,
        batches: 2,
        ..Cell::PAIRED
    };
    let passthrough = Passthrough::try_new(cores, cell).unwrap();
    let batch = Passthrough::batch(5, 5);
    assert_eq!(batch, passthrough.batches()[1]);
    assert_eq!(batch.num_columns(), Passthrough::COLUMNS);
    assert_eq!(ids(&batch), [5, 6, 7, 8, 9]);
}

#[test]
fn every_chosen_cell_moves_every_row_through_its_partitions_and_streams() {
    let cores = Cores::new(NonZeroUsize::new(2).unwrap());
    for cell in Cell::chosen() {
        let passthrough = Passthrough::try_new(cores, cell.tested()).unwrap();
        // The report's rows are checked against the batches' inside.
        let report = passthrough.engine_report();
        assert_eq!(report.streams.len(), cell.streams.get(), "{}", cell.name());
        if cell.commit == Cadence::Sixteenths {
            assert!(report.counters.commits.size > 0, "{}", cell.name());
        }
    }
}

#[test]
fn sixteen_partitions_write_through_more_than_one_lane_of_the_matrix_s_sink() {
    let cores = Cores::new(NonZeroUsize::new(4).unwrap());
    let cell = Cell {
        partitions: NonZeroUsize::new(16).unwrap(),
        ..Cell::PAIRED
    };
    let report = Passthrough::try_new(cores, cell.tested())
        .unwrap()
        .engine_report();
    let written = report
        .counters
        .lanes
        .iter()
        .filter(|lane| !lane.writing.is_zero())
        .count();
    assert!(written > 1, "{written} lanes written");
}

#[test]
fn the_paired_cell_writes_through_one_lane_whatever_its_partitions() {
    let cores = Cores::new(NonZeroUsize::new(4).unwrap());
    let cell = Cell {
        partitions: NonZeroUsize::new(16).unwrap(),
        ..Cell::PAIRED
    }
    .tested();
    let report = Passthrough::paired(cores, cell).unwrap().engine_report();
    let written = report
        .counters
        .lanes
        .iter()
        .filter(|lane| !lane.writing.is_zero())
        .count();
    assert_eq!(written, 1);
}

#[test]
fn a_cell_checkpointed_every_few_pushes_writes_their_batches_together() {
    let cores = Cores::new(NonZeroUsize::new(2).unwrap());
    let cell = Cell {
        rows: 10,
        batches: 12,
        ..Cell::PAIRED
    };
    let writes = |cell: Cell| {
        let passthrough = Passthrough::try_new(cores, cell).unwrap();
        let before = written();
        assert_eq!(passthrough.engine_run(), 120);
        written() - before
    };
    assert_eq!(writes(cell), 12);
    assert_eq!(writes(cell.checkpointed(NonZeroUsize::new(6).unwrap())), 2);
    assert_eq!(writes(cell.checkpointed(NonZeroUsize::new(5).unwrap())), 3);
}
