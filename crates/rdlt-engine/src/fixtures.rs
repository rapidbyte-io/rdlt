//! Batches the benches and the heap-peak tests share: mixed columns of any width, and a batch of
//! each kind a lowering plan prepares.

pub(crate) mod lowering;
#[cfg(test)]
mod tests;

use std::ops::Range;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};

/// The names of the ten mixed columns, in the order each width takes them.
const MIXED: [&str; 10] = ["id", "a", "b", "x", "y", "name", "city", "at", "flag", "n"];

/// `rows` rows of `columns` columns whose ids run from `first`: three 64-bit integers, two
/// floats, two short strings, a timestamp, a boolean and a 32-bit integer, then those ten kinds
/// again in turn, each round's named after its kind and round and its values shifted by it.
///
/// # Panics
///
/// Panics where `columns` is zero.
pub(crate) fn events(first: i64, rows: u32, columns: usize) -> RecordBatch {
    assert!(columns > 0, "a batch has a column");
    let ids = first..first + i64::from(rows);
    let columns = (0..columns).map(|index| {
        let (round, kind) = (index / MIXED.len(), index % MIXED.len());
        let name = match round {
            0 => MIXED[kind].to_owned(),
            _ => format!("{}_{round}", MIXED[kind]),
        };
        let round = i64::try_from(round).expect("a round fits in 64 bits");
        (name, mixed(kind, round, ids.clone()))
    });
    RecordBatch::try_from_iter(columns).expect("equal-length columns make a batch")
}

/// The column of the mixed `kind` in `round` for the rows of `ids`.
fn mixed(kind: usize, round: i64, ids: Range<i64>) -> ArrayRef {
    let int = |factor: i64| -> ArrayRef {
        Arc::new(Int64Array::from_iter_values(
            ids.clone().map(|id| id * factor + round),
        ))
    };
    #[expect(clippy::cast_precision_loss, reason = "a round is far below 2^52")]
    let shift = round as f64;
    let float = |factor: f64| -> ArrayRef {
        Arc::new(Float64Array::from_iter_values(ids.clone().map(|id| {
            f64::from(u32::try_from(id).unwrap_or(0)) * factor + shift
        })))
    };
    let text = |prefix: &str| -> ArrayRef {
        let prefix = match round {
            0 => prefix.to_owned(),
            _ => format!("{prefix}{round}"),
        };
        Arc::new(StringArray::from_iter_values(
            ids.clone().map(|id| format!("{prefix}-{id:08}")),
        ))
    };
    match kind {
        0 => int(1),
        1 => int(7),
        2 => int(13),
        3 => float(0.5),
        4 => float(1.25),
        5 => text("user"),
        6 => text("city"),
        7 => Arc::new(
            TimestampMicrosecondArray::from_iter_values(
                ids.map(|id| 1_790_000_000_000_000 + id + round),
            )
            .with_timezone("UTC"),
        ),
        8 => Arc::new(BooleanArray::from_iter(
            ids.map(|id| Some((id + round) % 3 == 0)),
        )),
        _ => Arc::new(Int32Array::from_iter_values(
            ids.map(|id| i32::try_from((id + round) % 1000).unwrap_or(0)),
        )),
    }
}
