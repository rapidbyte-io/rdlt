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
    let checked = taken.iter().try_for_each(|batch| {
        let Some(bound) = transform.bound(batch.schema_ref()) else {
            return Ok(());
        };
        declared = true;
        for (part, cut) in cuts(batch) {
            let made = run(transform, &cut)?;
            let held = Allocations::of(&cut);
            let keep = made.keep.map_or(0, |keep| {
                Allocations::default().add_array_beyond(&keep, &held)
            });
            let allocated = rdlt_engine::allocated_beyond(&made.output, &cut).saturating_add(keep);
            let limit = bound.of(&cut);
            if allocated > limit {
                return Err(Breach(format!(
                    "on the {part} it allocated {allocated} bytes, beyond the {limit} it declares"
                )));
            }
        }
        Ok(())
    });
    if checked.is_ok() && !declared {
        return Outcome::Inapplicable(Reason::new("the transform declares no bound"));
    }
    outcome(checked)
}
