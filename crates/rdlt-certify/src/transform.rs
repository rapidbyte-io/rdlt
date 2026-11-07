//! Certifying a transform: the `T` clauses, checked in process on the batches it is given.

mod bound;
mod moved;
#[cfg(test)]
mod tests;

use std::collections::BTreeSet;

use arrow_array::cast::AsArray as _;
use arrow_array::{BooleanArray, RecordBatch, UInt64Array};
use arrow_schema::ArrowError;
use arrow_select::concat::concat_batches;
use arrow_select::take::{take, take_record_batch};
use rdlt_connector::testing::{Clause, ClauseResult, Outcome, Reason, Report};
use rdlt_engine::{Transform, TransformError};

const KEYLESS: &str = "the inputs name no key and no change time";
const UNREAD: &str = "the transform does not filter, or declares no columns its filter reads";

/// The clauses [`certify_transform`] checks, in order.
pub const TRANSFORM_CLAUSES: &[Clause] = &[
    Clause {
        id: "T-REPLAY",
        statement: "a transform applied twice to one batch gives equal output and keeps equal rows",
        unless: "",
    },
    Clause {
        id: "T-ROWWISE",
        statement: "a transform's output and kept rows for two batches joined, or for one batch \
                    cut in halves, are those for each part, joined, so it carries nothing from \
                    one batch to the next and works row by row",
        unless: "",
    },
    Clause {
        id: "T-KEYS",
        statement: "the key columns and the change time pass a transform as they arrive: the \
                    same name, type and values",
        unless: KEYLESS,
    },
    Clause {
        id: "T-SCHEMA",
        statement: "each batch a transform returns matches the output schema it declares for \
                    its input, metadata included",
        unless: "",
    },
    Clause {
        id: "T-ROWS",
        statement: "a transform's apply keeps its input's rows, their count and their order, \
                    and its filter answers one value a row, in the same order",
        unless: "",
    },
    Clause {
        id: "T-BOUND",
        statement: "the buffers a transform returns, its output and its filter's answer, \
                    beyond its input stay within the bound it declares; scratch memory it frees \
                    before returning is not measured",
        unless: "the transform declares no bound",
    },
    Clause {
        id: "T-LINEAGE",
        statement: "the lineage a transform declares names, for each output column, input \
                    columns its input has, and every input column the output column changes \
                    with",
        unless: "the transform declares no lineage",
    },
    Clause {
        id: "T-READS",
        statement: "the input columns a transform's filter declares it reads are every column \
                    its answer changes with",
        unless: UNREAD,
    },
];

/// What a transform is certified on: batches, and the columns that pass it as they arrive.
///
/// The batches hold data columns alone, as the engine gives them to a transform, with no change
/// columns. Each runs alone, and cut in halves; each two in a row that share a schema run
/// joined. A clause sees only what the batches show: a dependence hidden behind a function
/// that is constant on them goes unseen, and a clause whose check needs rows or columns that
/// vary says it was not observed.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TransformInputs {
    /// The batches.
    pub batches: Vec<RecordBatch>,
    /// The key's top-level columns.
    pub keys: Vec<String>,
    /// The change time's top-level column, where the stream has one.
    pub change_time: Option<String>,
}

/// Checks every `T` clause of `transform` on `inputs`, in clause order; a batch whose schema
/// the transform refuses is left out of every clause.
pub fn certify_transform(transform: &dyn Transform, inputs: &TransformInputs) -> Report {
    let taken: Vec<&RecordBatch> = inputs
        .batches
        .iter()
        .filter(|batch| transform.output(batch.schema_ref()).is_ok())
        .collect();
    let rows: usize = taken.iter().map(|batch| batch.num_rows()).sum();
    let results = TRANSFORM_CLAUSES
        .iter()
        .map(|clause| {
            let outcome = if taken.is_empty() {
                refused(clause.id, transform, inputs)
            } else {
                match checked(clause.id, transform, inputs, &taken) {
                    Outcome::Passed if rows == 0 => {
                        Outcome::Unobserved(Reason::new("no input holds a row"))
                    }
                    other => other,
                }
            };
            ClauseResult {
                clause: *clause,
                outcome,
                note: None,
            }
        })
        .collect();
    Report {
        connector: transform.name().to_owned(),
        results,
    }
}

/// The outcome of a clause when the transform refuses every input's schema: not applicable
/// where the declarations say so, else not observed.
fn refused(id: &str, transform: &dyn Transform, inputs: &TransformInputs) -> Outcome {
    if id == "T-KEYS" && inputs.keys.is_empty() && inputs.change_time.is_none() {
        Outcome::Inapplicable(Reason::new(KEYLESS))
    } else if id == "T-READS" && !transform.filters() {
        Outcome::Inapplicable(Reason::new(UNREAD))
    } else {
        Outcome::Unobserved(Reason::new("the transform refuses every input's schema"))
    }
}

/// Why a transform breaks a clause, in words that quote no value.
struct Breach(String);

impl Breach {
    fn new(reason: &str) -> Self {
        Self(reason.to_owned())
    }
}

impl From<TransformError> for Breach {
    fn from(error: TransformError) -> Self {
        Self(format!("it failed: {error}"))
    }
}

impl From<ArrowError> for Breach {
    fn from(error: ArrowError) -> Self {
        Self(error.to_string())
    }
}

/// What a transform made of one batch: what its filter keeps, where it filters, and its output.
struct Made {
    keep: Option<BooleanArray>,
    output: RecordBatch,
}

fn run(transform: &dyn Transform, batch: &RecordBatch) -> Result<Made, Breach> {
    let keep = if transform.filters() {
        Some(transform.keep(batch)?)
    } else {
        None
    };
    let output = transform.apply(batch.clone())?;
    Ok(Made { keep, output })
}

/// Fails where two outputs of one transform have other columns or schema metadata than each
/// other.
fn alike(one: &RecordBatch, other: &RecordBatch) -> Result<(), Breach> {
    let (one, other) = (one.schema_ref(), other.schema_ref());
    if one.fields() == other.fields() && one.metadata() == other.metadata() {
        Ok(())
    } else {
        Err(Breach::new(
            "the columns or schema metadata it returns vary with the rows it is given",
        ))
    }
}

fn outcome(checked: Result<(), Breach>) -> Outcome {
    match checked {
        Ok(()) => Outcome::Passed,
        Err(Breach(reason)) => Outcome::Failed(Reason::new(reason)),
    }
}

/// The outcome of a check that compared something, or `unseen` where it compared nothing.
fn compared(checked: Result<bool, Breach>, unseen: &str) -> Outcome {
    match checked {
        Ok(true) => Outcome::Passed,
        Ok(false) => Outcome::Unobserved(Reason::new(unseen)),
        Err(breach) => outcome(Err(breach)),
    }
}

fn checked(
    id: &str,
    transform: &dyn Transform,
    inputs: &TransformInputs,
    taken: &[&RecordBatch],
) -> Outcome {
    match id {
        "T-REPLAY" => outcome(replayed(transform, taken)),
        "T-ROWWISE" => compared(
            rowwise(transform, taken),
            "no input has two rows, or two in a row sharing a schema, to cut or join",
        ),
        "T-KEYS" => keyed(transform, inputs, taken),
        "T-SCHEMA" => outcome(schemed(transform, taken)),
        "T-ROWS" => compared(
            rowed(transform, taken),
            "no input has three rows to show order",
        ),
        "T-BOUND" => bound::bounded(transform, taken),
        "T-LINEAGE" => moved::lineaged(transform, taken),
        "T-READS" => moved::read(transform, taken),
        _ => Outcome::Failed(Reason::new(format!("no check for {id}"))),
    }
}

fn replayed(transform: &dyn Transform, taken: &[&RecordBatch]) -> Result<(), Breach> {
    for batch in taken {
        let (one, two) = (run(transform, batch)?, run(transform, batch)?);
        if one.keep != two.keep || one.output != two.output {
            return Err(Breach::new(
                "applied twice to one batch, it gave different output",
            ));
        }
    }
    Ok(())
}

/// Fails unless what `transform` makes of `whole` is what it makes of `parts`, joined.
fn joined(
    transform: &dyn Transform,
    whole: &RecordBatch,
    parts: &[RecordBatch],
) -> Result<(), Breach> {
    let all = run(transform, whole)?;
    let each = parts
        .iter()
        .map(|part| run(transform, part))
        .collect::<Result<Vec<_>, _>>()?;
    for made in &each {
        alike(&all.output, &made.output)?;
    }
    let outputs = concat_batches(
        all.output.schema_ref(),
        each.iter().map(|made| &made.output),
    )?;
    if outputs != all.output {
        return Err(Breach::new(
            "its output for batches joined is not its outputs for each, joined",
        ));
    }
    let kept = each.iter().filter_map(|made| made.keep.as_ref());
    if let Some(keep) = &all.keep
        && !keep.iter().eq(kept.flat_map(BooleanArray::iter))
    {
        return Err(Breach::new(
            "its filter's answers for batches joined are not its answers for each, joined",
        ));
    }
    Ok(())
}

fn rowwise(transform: &dyn Transform, taken: &[&RecordBatch]) -> Result<bool, Breach> {
    let mut seen = false;
    for pair in taken.windows(2) {
        if pair[0].schema() == pair[1].schema() {
            let whole = concat_batches(pair[0].schema_ref(), pair.iter().copied())?;
            joined(transform, &whole, &[pair[0].clone(), pair[1].clone()])?;
            seen |= whole.num_rows() > 0;
        }
    }
    for batch in taken {
        let rows = batch.num_rows();
        if rows >= 2 {
            let half = rows / 2;
            joined(
                transform,
                batch,
                &[batch.slice(0, half), batch.slice(half, rows - half)],
            )?;
            seen = true;
        }
    }
    Ok(seen)
}

fn keyed(transform: &dyn Transform, inputs: &TransformInputs, taken: &[&RecordBatch]) -> Outcome {
    let named: Vec<&str> = inputs
        .keys
        .iter()
        .map(String::as_str)
        .chain(inputs.change_time.as_deref())
        .collect();
    if named.is_empty() {
        return Outcome::Inapplicable(Reason::new(KEYLESS));
    }
    let mut seen = BTreeSet::new();
    let checked = taken.iter().try_for_each(|batch| {
        let made = run(transform, batch)?;
        for name in &named {
            let Some(arrived) = batch.column_by_name(name).filter(|_| batch.num_rows() > 0) else {
                continue;
            };
            seen.insert(*name);
            match made.output.column_by_name(name) {
                Some(left) if left.as_ref() == arrived.as_ref() => {}
                _ => return Err(Breach(format!("column {name} does not pass as it arrives"))),
            }
        }
        Ok(())
    });
    let unseen: Vec<&str> = named
        .into_iter()
        .filter(|name| !seen.contains(name))
        .collect();
    match checked {
        Ok(()) if !unseen.is_empty() => Outcome::Unobserved(Reason::new(format!(
            "no input has rows of a column the inputs name: {}",
            unseen.join(", ")
        ))),
        other => outcome(other),
    }
}

fn schemed(transform: &dyn Transform, taken: &[&RecordBatch]) -> Result<(), Breach> {
    for batch in taken {
        let declared = transform.output(batch.schema_ref())?;
        let output = run(transform, batch)?.output;
        if output.schema_ref().fields() != declared.fields()
            || !declared.contains(output.schema_ref())
        {
            return Err(Breach::new(
                "it returned a batch unlike the schema it declares",
            ));
        }
    }
    Ok(())
}

/// A reordering of rows that a transform keeping its rows' order commutes with.
///
/// From three rows, no reordering but the identity commutes with both of these.
#[derive(Clone, Copy)]
enum Order {
    /// The first row moves last.
    Shift,
    /// The first two rows trade places.
    Swap,
}

impl Order {
    const ALL: [Self; 2] = [Self::Shift, Self::Swap];

    fn indices(self, rows: usize) -> UInt64Array {
        let rows = u64::try_from(rows).unwrap_or(u64::MAX);
        match self {
            Self::Shift => (1..rows).chain(0..1).collect(),
            Self::Swap => (0..rows)
                .map(|row| match row {
                    0 => 1,
                    1 => 0,
                    row => row,
                })
                .collect(),
        }
    }

    fn batch(self, batch: &RecordBatch) -> Result<RecordBatch, Breach> {
        Ok(take_record_batch(batch, &self.indices(batch.num_rows()))?)
    }

    fn keep(self, keep: &BooleanArray) -> Result<BooleanArray, Breach> {
        let moved = take(keep, &self.indices(keep.len()), None)?;
        Ok(moved.as_boolean().clone())
    }
}

/// Whether every batch kept its rows, and one at least showed they keep their order.
fn rowed(transform: &dyn Transform, taken: &[&RecordBatch]) -> Result<bool, Breach> {
    let mut shown = false;
    for batch in taken {
        let made = run(transform, batch)?;
        let rows = batch.num_rows();
        if made.output.num_rows() != rows {
            return Err(Breach(format!(
                "it returned {} rows for {rows}",
                made.output.num_rows()
            )));
        }
        if made.keep.as_ref().is_some_and(|keep| keep.len() != rows) {
            return Err(Breach(format!(
                "its filter answered another count of values than {rows} rows"
            )));
        }
        if rows < 3 {
            continue;
        }
        shown = true;
        for order in Order::ALL {
            let again = run(transform, &order.batch(batch)?)?;
            alike(&made.output, &again.output)?;
            let keep = made
                .keep
                .as_ref()
                .map(|keep| order.keep(keep))
                .transpose()?;
            if again.output != order.batch(&made.output)? || again.keep != keep {
                return Err(Breach::new("it does not keep its rows' order"));
            }
        }
    }
    Ok(shown)
}
