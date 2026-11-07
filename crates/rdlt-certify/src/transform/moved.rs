//! The clauses that move one input column's rows and watch what changes: `T-LINEAGE` and
//! `T-READS`.

use std::collections::BTreeSet;

use arrow_array::RecordBatch;
use arrow_select::take::take;
use rdlt_connector::testing::{Outcome, Reason};
use rdlt_engine::Transform;

use super::{Breach, Order, UNREAD, alike, outcome, run};

/// What a check that moves input columns has seen across the batches.
#[derive(Default)]
struct Seen {
    declared: bool,
    /// The columns, by name, the declaration leaves out: each must be moved to show it.
    wanted: BTreeSet<String>,
    moved: BTreeSet<String>,
}

impl Seen {
    fn outcome(&self, checked: Result<(), Breach>, undeclared: &str) -> Outcome {
        match checked {
            Err(breach) => outcome(Err(breach)),
            Ok(()) if !self.declared => Outcome::Inapplicable(Reason::new(undeclared)),
            Ok(()) => match self.wanted.difference(&self.moved).next() {
                Some(name) => Outcome::Unobserved(Reason::new(format!(
                    "no input has rows of column {name} that differ when moved, \
                     to show what changes with it"
                ))),
                None => Outcome::Passed,
            },
        }
    }

    /// `batch` with the rows of its column `input` moved, or none where moving them changes
    /// nothing; either way the column is noted as one to show.
    fn moved(&mut self, batch: &RecordBatch, input: usize) -> Result<Option<RecordBatch>, Breach> {
        let name = batch.schema_ref().field(input).name().clone();
        self.wanted.insert(name.clone());
        if batch.num_rows() < 2 {
            return Ok(None);
        }
        let indices = Order::Shift.indices(batch.num_rows());
        let mut columns = batch.columns().to_vec();
        let shifted = take(columns[input].as_ref(), &indices, None)?;
        if shifted.as_ref() == columns[input].as_ref() {
            return Ok(None);
        }
        columns[input] = shifted;
        self.moved.insert(name);
        Ok(Some(RecordBatch::try_new(batch.schema(), columns)?))
    }
}

pub(super) fn lineaged(transform: &dyn Transform, taken: &[&RecordBatch]) -> Outcome {
    let mut seen = Seen::default();
    let checked = taken.iter().try_for_each(|batch| {
        let Some(lineage) = transform.lineage(batch.schema_ref()) else {
            return Ok(());
        };
        seen.declared = true;
        let output = run(transform, batch)?.output;
        let inputs = batch.num_columns();
        let named = lineage.iter().flatten().all(|column| *column < inputs);
        if lineage.len() != output.num_columns() || !named {
            return Err(Breach::new(
                "its lineage does not give each output column input columns its input has",
            ));
        }
        for input in 0..inputs {
            if lineage.iter().all(|reads| reads.contains(&input)) {
                continue;
            }
            let Some(shuffled) = seen.moved(batch, input)? else {
                continue;
            };
            let again = run(transform, &shuffled)?.output;
            alike(&output, &again)?;
            for (column, reads) in lineage.iter().enumerate() {
                if !reads.contains(&input)
                    && again.column(column).as_ref() != output.column(column).as_ref()
                {
                    return Err(Breach(format!(
                        "output column {column} changes with input column {input}, \
                         which its lineage does not name"
                    )));
                }
            }
        }
        Ok(())
    });
    seen.outcome(checked, "the transform declares no lineage")
}

pub(super) fn read(transform: &dyn Transform, taken: &[&RecordBatch]) -> Outcome {
    let mut seen = Seen::default();
    let checked = taken.iter().try_for_each(|batch| {
        let Some(reads) = transform
            .keep_reads(batch.schema_ref())
            .filter(|_| transform.filters())
        else {
            return Ok(());
        };
        seen.declared = true;
        if reads.iter().any(|column| *column >= batch.num_columns()) {
            return Err(Breach::new(
                "the columns its filter declares it reads are not its input's",
            ));
        }
        let kept = transform.keep(batch)?;
        for input in (0..batch.num_columns()).filter(|input| !reads.contains(input)) {
            let Some(shuffled) = seen.moved(batch, input)? else {
                continue;
            };
            if transform.keep(&shuffled)? != kept {
                return Err(Breach(format!(
                    "its filter's answer changes with input column {input}, \
                     which it declares it does not read"
                )));
            }
        }
        Ok(())
    });
    seen.outcome(checked, UNREAD)
}
