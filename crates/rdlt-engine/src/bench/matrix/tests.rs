use std::collections::BTreeSet;
use std::num::{NonZeroU64, NonZeroUsize};

use super::{Cadence, Cell};
use crate::bench::Sinking;

fn count(count: usize) -> NonZeroUsize {
    NonZeroUsize::new(count).unwrap()
}

#[test]
fn every_cell_of_the_full_matrix_moves_the_same_rows_under_a_name_of_its_own() {
    let full = Cell::full();
    assert_eq!(full.len(), 38);
    for cell in &full {
        let rows = u64::from(cell.rows) * u64::from(cell.batches);
        assert_eq!(rows, u64::from(Cell::RUN_ROWS), "{}", cell.name());
    }
    let names: BTreeSet<String> = full.iter().map(|cell| cell.name()).collect();
    assert_eq!(names.len(), full.len());
    let chosen = Cell::chosen();
    assert_eq!(chosen.len(), 12);
    assert_eq!(chosen[0], Cell::PAIRED);
    assert!(chosen.iter().all(|cell| full.contains(cell)));
}

#[test]
fn a_cell_is_named_by_its_batches_partitions_streams_cadence_and_sink() {
    assert_eq!(Cell::PAIRED.name(), "80000x64/1p/1s/end");
    assert_eq!(Cell::PAIRED.commit_rows(), None);
    let cell = Cell::of(
        1_000,
        count(16),
        count(4),
        Cadence::Sixteenths,
        Sinking::Ipc,
    );
    assert_eq!(cell.name(), "1000x5120/16p/4s/16th");
    assert_eq!(cell.commit_rows(), NonZeroU64::new(320_000));
    let null = Cell::of(8_000, count(1), count(1), Cadence::End, Sinking::Discard);
    assert_eq!(null.name(), "8000x640/1p/1s/end/null");
}

#[test]
fn naming_full_in_any_argument_asks_for_the_full_matrix() {
    let args = |list: &[&str]| list.iter().map(|arg| (*arg).to_owned()).collect::<Vec<_>>();
    assert_eq!(
        Cell::asked(&args(&["bench", "--bench"])),
        ("matrix", Cell::chosen())
    );
    assert_eq!(
        Cell::asked(&args(&["bench", "--bench", "full"])),
        ("full", Cell::full())
    );
    let exact = args(&[
        "bench",
        "--bench",
        "--exact",
        "passthrough/full/1000x5120/16p/4s/end",
    ]);
    assert_eq!(Cell::asked(&exact).0, "full");
    assert_eq!(
        Cell::asked(&args(&["bench", "--list", "paired"])).0,
        "matrix"
    );
    assert_eq!(
        Cell::asked(&args(&[
            "/fullstack/target/release/deps/passthrough",
            "--bench"
        ]))
        .0,
        "matrix"
    );
}

#[test]
fn a_tested_cell_keeps_its_shape_in_two_small_batches_a_partition_of_a_stream() {
    let cell = Cell::of(80_000, count(16), count(4), Cadence::End, Sinking::Ipc);
    let tested = cell.tested();
    assert_eq!((tested.rows, tested.batches), (1_000, 128));
    assert_eq!(
        (tested.partitions, tested.streams),
        (cell.partitions, cell.streams)
    );
    let sixteenths = Cell {
        commit: Cadence::Sixteenths,
        ..Cell::PAIRED
    }
    .tested();
    assert_eq!(sixteenths.commit_rows(), NonZeroU64::new(125));
}
