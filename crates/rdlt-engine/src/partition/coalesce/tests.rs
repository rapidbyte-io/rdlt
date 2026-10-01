use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use bytes::Bytes;
use rdlt_connector::Permit;

use super::{Coalescer, Flushed, Pushed, Unit};
use crate::config::BatchPolicy;

fn policy(bytes: u64, rows: u64) -> BatchPolicy {
    BatchPolicy::new(bytes, rows, Duration::from_secs(1), 1 << 20).unwrap()
}

fn json(text: &str) -> Pushed {
    Pushed::Json(Bytes::from(text.to_owned()))
}

fn ints(values: &[i64]) -> Pushed {
    let column: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
    Pushed::Arrow(RecordBatch::try_from_iter([("a", column)]).unwrap())
}

fn texts(values: &[&str]) -> Pushed {
    let column: ArrayRef = Arc::new(StringArray::from(values.to_vec()));
    Pushed::Arrow(RecordBatch::try_from_iter([("a", column)]).unwrap())
}

/// A permit tagged `tag`, holding as many bytes as `pushed` takes.
fn permit(pushed: &Pushed, tag: u8) -> (u64, Permit) {
    let bytes = match pushed {
        Pushed::Json(json) => json.len(),
        Pushed::Arrow(batch) => batch.get_array_memory_size(),
    };
    (u64::try_from(bytes).unwrap(), Box::new(tag))
}

/// Adds `pushed` to `coalescer` under a permit tagged `tag`.
fn add(coalescer: &mut Coalescer, pushed: Pushed, tag: u8, now: Instant) -> Vec<Flushed> {
    let held = permit(&pushed, tag);
    coalescer.add(pushed, held, now)
}

/// A moment to measure from; the coalescer never reads the clock itself.
#[expect(
    clippy::disallowed_methods,
    reason = "tests pass the coalescer its instants"
)]
fn start() -> Instant {
    Instant::now()
}

/// The tags of `flushed`'s permits and how many pushes it holds.
fn summary(flushed: &Flushed) -> (Vec<u8>, usize) {
    let tags = flushed
        .permits
        .iter()
        .map(|permit| *permit.downcast_ref::<u8>().unwrap())
        .collect();
    let pushes = match &flushed.unit {
        Unit::Json(pushes) => pushes.len(),
        Unit::Arrow(batches) => batches.len(),
    };
    (tags, pushes)
}

#[test]
fn pushes_are_gathered_until_they_reach_the_target_bytes() {
    let mut coalescer = Coalescer::new(policy(20, 1000));
    let now = start();
    assert!(add(&mut coalescer, json("{\"a\":1}\n"), 1, now).is_empty());
    let flushed = add(&mut coalescer, json("{\"a\":2}\n{\"a\":3}\n"), 2, now);
    assert_eq!(
        flushed.iter().map(summary).collect::<Vec<_>>(),
        [(vec![1, 2], 2)]
    );
    assert_eq!(flushed[0].bytes, 24);
    assert!(coalescer.flush().is_none());
}

#[test]
fn arrow_pushes_are_gathered_until_they_reach_the_row_limit() {
    let mut coalescer = Coalescer::new(policy(1 << 20, 3));
    let now = start();
    assert!(add(&mut coalescer, ints(&[1, 2]), 1, now).is_empty());
    let flushed = add(&mut coalescer, ints(&[3]), 2, now);
    assert_eq!(
        flushed.iter().map(summary).collect::<Vec<_>>(),
        [(vec![1, 2], 2)]
    );
}

#[test]
fn a_push_that_cannot_join_the_gathered_ones_flushes_them_first() {
    let mut coalescer = Coalescer::new(policy(1 << 20, 1000));
    let now = start();
    assert!(add(&mut coalescer, ints(&[1]), 1, now).is_empty());
    let flushed = add(&mut coalescer, texts(&["x"]), 2, now);
    assert_eq!(
        flushed.iter().map(summary).collect::<Vec<_>>(),
        [(vec![1], 1)]
    );
    let flushed = add(&mut coalescer, json("{\"a\":1}"), 3, now);
    assert_eq!(
        flushed.iter().map(summary).collect::<Vec<_>>(),
        [(vec![2], 1)]
    );
    let flushed = coalescer.flush().unwrap();
    assert!(matches!(flushed.unit, Unit::Json(_)));
    assert_eq!(summary(&flushed), (vec![3], 1));
}

#[test]
fn the_deadline_is_the_first_gathered_push_plus_the_latency() {
    let mut coalescer = Coalescer::new(policy(1 << 20, 1000));
    let first = start();
    assert_eq!(coalescer.deadline(), None);
    add(&mut coalescer, json("{}"), 1, first);
    add(
        &mut coalescer,
        json("{}"),
        2,
        first + Duration::from_millis(300),
    );
    assert_eq!(coalescer.deadline(), Some(first + Duration::from_secs(1)));
    coalescer.flush();
    assert_eq!(coalescer.deadline(), None);
}

#[test]
fn json_pushes_count_only_their_bytes_toward_the_limits() {
    let mut coalescer = Coalescer::new(policy(1 << 20, 1));
    let now = start();
    assert!(add(&mut coalescer, json("{\"a\":1}\n{\"a\":2}"), 1, now).is_empty());
    assert!(add(&mut coalescer, json("{\"a\":3}"), 2, now).is_empty());
    assert_eq!(summary(&coalescer.flush().unwrap()), (vec![1, 2], 2));
}
