//! The bound clause: what a transform allocates on a batch and on the parts the engine cuts.

use arrow_array::RecordBatch;
use rdlt_connector::cost::Allocations;
use rdlt_connector::testing::{Outcome, Reason};
use rdlt_engine::Transform;

use super::{Breach, outcome, run};

/// `batch` whole, in halves, and its first row alone: the sizes the engine runs a transform on.
fn cuts(batch: &RecordBatch) -> Vec<(&'static str, RecordBatch)> {
    let rows = batch.num_rows();
    let mut cuts = vec![("whole batch", batch.clone())];
    if rows >= 2 {
        let half = rows / 2;
        cuts.extend([
            ("first half", batch.slice(0, half)),
            ("second half", batch.slice(half, rows - half)),
            ("first row", batch.slice(0, 1)),
        ]);
    }
    cuts
}

pub(super) fn bounded(transform: &dyn Transform, taken: &[&RecordBatch]) -> Outcome {
    let mut declared = false;
    let mut measured = false;
    let checked = taken.iter().try_for_each(|batch| {
        let Some(bound) = transform.bound(batch.schema_ref()) else {
            return Ok(());
        };
        declared = true;
        measured |= batch.num_rows() > 0;
        for (part, cut) in cuts(batch) {
            let made = run(transform, &cut)?;
            let mut held = Allocations::of(&cut);
            held.add(&RecordBatch::new_empty(made.output.schema()));
            let mut returned = Allocations::default();
            let mut allocated = returned.add_beyond(&made.output, &held);
            if let Some(keep) = &made.keep {
                allocated = allocated.saturating_add(returned.add_array_beyond(keep, &held));
            }
            let limit = bound.of(&cut);
            if allocated > limit {
                return Err(Breach(format!(
                    "on the {part} it allocated {allocated} bytes, beyond the {limit} it declares"
                )));
            }
        }
        Ok(())
    });
    match checked {
        Ok(()) if !declared => {
            Outcome::Inapplicable(Reason::new("the transform declares no bound"))
        }
        Ok(()) if !measured => Outcome::Unobserved(Reason::new(
            "no input with rows has a schema the transform declares a bound for",
        )),
        other => outcome(other),
    }
}
