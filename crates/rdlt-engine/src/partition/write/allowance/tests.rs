#![expect(
    clippy::disallowed_methods,
    reason = "tests drive tokio's paused clock and tasks directly"
)]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use rdlt_connector::cost::Rendering;
use rdlt_connector::{ColumnPath, StreamName, TableSchema};
use tokio_util::sync::CancellationToken;

use super::{Cutter, PartPiece};
use crate::budget::TooLarge;
use crate::error::{Error, ErrorKind};
use crate::normalize::{Shape, normalize};
use crate::table::{Incoming, LoweringPlan};

/// A unit of `rows` 8-byte integers, normalized and planned for a table of no columns yet.
fn unit(rows: i64) -> super::PlannedParts {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows));
    let batch = RecordBatch::try_from_iter([("id", ids)]).unwrap();
    let shape = Shape {
        max_depth: 8,
        whole: std::collections::BTreeSet::new(),
        key: Vec::new(),
    };
    let parts = normalize(&batch, &shape).unwrap();
    parts
        .into_iter()
        .map(|part| {
            let schema = TableSchema::from_arrow(&part.batch.schema()).unwrap();
            let paths: Vec<ColumnPath> = part.columns.clone();
            let incoming = Incoming::of(schema, paths, &[]);
            let plan = LoweringPlan::new(
                StreamName::new("events").unwrap(),
                crate::table::testing::view("t"),
                incoming,
                Vec::new(),
            );
            (0, part, Arc::new(plan))
        })
        .collect()
}

fn cut(cutter: Cutter, rows: i64) -> Vec<PartPiece> {
    cutter.cut(&Rendering::native(), unit(rows)).unwrap()
}

fn rows(pieces: &[PartPiece]) -> usize {
    pieces.iter().map(|piece| piece.batch.num_rows()).sum()
}

/// A part's piece never takes more than its allowance, which holds every piece whole.
fn unfit(large: TooLarge) -> Error {
    panic!(
        "a piece of {} passes its allowance of {}",
        large.asked, large.limit
    )
}

#[test]
fn parts_are_cut_into_pieces_within_a_piece_and_keep_their_rows_and_lineage() {
    let cutter = Cutter::new(1 << 20, 16 << 10);
    let pieces = cut(cutter, 10_000);
    assert!(pieces.len() > 4, "{} pieces", pieces.len());
    assert_eq!(rows(&pieces), 10_000);
    let mut first = 0;
    for piece in &pieces {
        assert!(piece.bytes <= 16 << 10, "{}", piece.bytes);
        assert_eq!(piece.lineage.row.len(), piece.batch.num_rows());
        assert_eq!(piece.lineage.root_row().len(), piece.batch.num_rows());
        let ids = piece.batch.column(0);
        let ids = ids.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ids.value(0), first);
        first += i64::try_from(piece.batch.num_rows()).unwrap();
    }
}

#[test]
fn a_row_beyond_what_the_parts_may_take_is_refused() {
    let one = cut(Cutter::new(1 << 20, 16 << 10), 1);
    let row = one[0].bytes;
    assert!(row >= 8, "{row}");
    // A row fits within exactly what lowering it takes, and not a byte less.
    assert_eq!(rows(&cut(Cutter::new(row, 16 << 10), 3)), 3);
    let Err(refused) = Cutter::new(row - 1, 16 << 10).cut(&Rendering::native(), unit(3)) else {
        panic!("a row was cut within less than it takes");
    };
    assert_eq!(refused.limit, row - 1);
    assert!(refused.expanded > row - 1, "{}", refused.expanded);
}

#[test]
fn an_allowance_holds_every_piece_whole_and_never_more_than_is_available() {
    for (available, piece, count) in [
        (1_u64 << 20, 16_u64 << 10, 10_000_i64),
        (20 << 10, 16 << 10, 10_000),
        (1 << 20, 16 << 10, 3),
        (3 << 10, 1, 500),
    ] {
        let cutter = Cutter::new(available, piece);
        let Ok(pieces) = cutter.cut(&Rendering::native(), unit(count)) else {
            continue;
        };
        let allowance = cutter.allowance(&pieces);
        let what = format!("{available} available in pieces of {piece}");
        assert!(allowance <= available, "{what}: {allowance}");
        for piece in &pieces {
            assert!(
                piece.bytes <= allowance,
                "{what}: a piece of {}",
                piece.bytes
            );
        }
        // Few rows take what they take, and no more.
        let all: u64 = pieces.iter().map(|piece| piece.bytes).sum();
        assert!(allowance <= all, "{what}: {allowance} of {all}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_piece_waits_for_those_before_it_to_be_written_and_presses_for_it() {
    let budget = crate::budget::budget(1 << 20);
    let cutter = Cutter::new(1 << 20, 16 << 10);
    let pieces = cut(cutter, 10_000);
    let allowance = cutter.allowance(&pieces);
    assert_eq!(allowance, 32 << 10);
    let lent = budget.acquire_working(allowance).await.unwrap().lend();
    // Two pieces are lowered at once: one lowered while the other is written.
    let first = lent.take(pieces[0].bytes).await.unwrap();
    let second = lent.take(pieces[1].bytes).await.unwrap();
    let free = allowance - pieces[0].bytes - pieces[1].bytes;
    assert!(
        pieces[2].bytes > free,
        "the third piece fits beside the first two"
    );
    let third = lent.take(pieces[2].bytes);
    tokio::pin!(third);
    let mut pressure = budget.pressure();
    tokio::select! {
        biased;
        _ = &mut third => panic!("three pieces pass the allowance"),
        () = pressure.raised() => {}
        () = tokio::time::sleep(Duration::from_secs(1)) => panic!("no lane was pressed"),
    }
    // The first piece written, the next is taken.
    drop(first);
    let third = third.await.unwrap();
    tokio::select! {
        biased;
        () = pressure.raised() => panic!("nothing waits any more"),
        () = tokio::time::sleep(Duration::from_secs(1)) => {}
    }
    drop((second, third));
    assert!(
        lent.try_take(allowance).is_some(),
        "each piece gave its part back"
    );
}

#[tokio::test(start_paused = true)]
async fn a_piece_waiting_for_its_allowance_stops_when_the_attempt_is_cancelled() {
    let budget = crate::budget::budget(1 << 20);
    let cancel = CancellationToken::new();
    let cutter = Cutter::new(16 << 10, 16 << 10);
    let pieces = cut(cutter, 10_000);
    let allowance = cutter.allowance(&pieces);
    assert!(
        pieces[0].bytes + pieces[1].bytes > allowance,
        "one piece at a time"
    );
    let lent = budget.acquire_working(allowance).await.unwrap().lend();
    let held = lent.try_take(pieces[0].bytes).unwrap();
    let waiting = super::super::taking(&cancel, &lent, pieces[1].bytes, unfit);
    let (waiting, ()) = tokio::join!(waiting, async {
        tokio::time::sleep(Duration::from_secs(5)).await;
        cancel.cancel();
    });
    assert_eq!(
        waiting.err().map(|error| error.kind()),
        Some(ErrorKind::Cancelled)
    );
    // The take let go took nothing, and presses no lane.
    let mut pressure = budget.pressure();
    tokio::select! {
        biased;
        () = pressure.raised() => panic!("nothing waits any more"),
        () = tokio::time::sleep(Duration::from_secs(1)) => {}
    }
    assert!(lent.try_take(allowance - pieces[0].bytes).is_some());
    drop(held);
}
