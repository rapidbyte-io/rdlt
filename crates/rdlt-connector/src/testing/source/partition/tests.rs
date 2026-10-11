use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use bytes::Bytes;

use super::Rows;
use crate::sink::Push;
use crate::testing::limits::{RENDERED_BYTES, YIELD_BYTES};
use crate::testing::render::Rendering;

/// The output of `future`, and how many times it yielded before it.
fn polled<F: Future>(future: F) -> (F::Output, usize) {
    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    let mut yields = 0;
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return (output, yields),
            Poll::Pending => yields += 1,
        }
    }
}

/// The rows `pushes` hold, each as many times as they hold it.
fn rows(pushes: &[Push]) -> Rows {
    let mut rows = Rows::default();
    let mut rendering = Rendering::new(RENDERED_BYTES);
    polled(rows.add(&mut rendering, pushes.iter())).0.unwrap();
    rows
}

fn arrow(ids: &[i64]) -> Push {
    let ids: ArrayRef = Arc::new(Int64Array::from(ids.to_vec()));
    Push::Arrow(RecordBatch::try_from_iter([("id", ids)]).unwrap())
}

fn json(text: &'static str) -> Push {
    Push::Json(Bytes::from_static(text.as_bytes()))
}

#[test]
fn rows_differ_by_each_row_pushed_more_or_fewer_times() {
    let cases = [
        (arrow(&[1, 2]), arrow(&[1, 2, 2, 3])),
        (json("[1, 2]"), json("[1, 2, 2, 3]")),
    ];
    for (once, more) in cases {
        let (once, more) = (rows(&[once]), rows(&[more]));
        assert_eq!(once.difference(&rows(&[])), Some((2, 0)));
        assert_eq!(once.difference(&more), Some((0, 2)));
        assert_eq!(more.difference(&once), Some((2, 0)));
    }
    // However the rows were split into pushes.
    let split = rows(&[arrow(&[2]), arrow(&[1, 2]), arrow(&[3])]);
    assert_eq!(rows(&[arrow(&[1, 2, 2, 3])]).difference(&split), None);
}

#[test]
fn a_json_push_is_counted_in_pieces_between_which_it_yields() {
    for (pieces, yields) in [(0, 0), (1, 1), (2, 2)] {
        // Rows of two bytes each, a piece of them and one row more.
        let pushed = pieces * YIELD_BYTES / 2 + 1;
        let text = format!("[{}]", vec!["0"; pushed].join(","));
        let (mut counted, mut rendering) = (Rows::default(), Rendering::new(RENDERED_BYTES));
        let (read, yielded) = polled(counted.json(&mut rendering, text.as_bytes()));
        assert!(read.is_ok(), "{pieces}");
        assert_eq!(yielded, yields, "{pieces}");
        assert_eq!(counted.0.get("0"), Some(&pushed), "{pieces}");
    }
}
