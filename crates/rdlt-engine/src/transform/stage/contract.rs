//! Each transform held to what it declares: its lineage and the columns its filter reads, the
//! changes it can carry, its filter's answer, and its output's rows, schema and bound.

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_schema::{Schema, SchemaRef};
use arrow_select::filter;

use super::changes::Changes;
use super::{Breach, Refusal};
use crate::transform::{Bound, Transform, allocated_beyond};

/// What a transform declares of batches of one schema.
#[derive(Debug)]
pub(super) struct Declared {
    /// What `keep` and `apply` allocate at most, where it says.
    pub(super) bound: Option<Bound>,
    /// Each output column's input columns, where it says.
    pub(super) lineage: Option<Vec<Vec<usize>>>,
    /// The input columns its filter reads, where it filters and says.
    keep_reads: Option<Vec<usize>>,
}

/// What `transform` declares of batches of `input`, whose output is `output`, refusing a
/// lineage or the columns its filter reads where they name columns `input` lacks.
pub(super) fn declares(
    transform: &dyn Transform,
    input: &Schema,
    output: &Schema,
) -> Result<Declared, Refusal> {
    let bound = transform.bound(input);
    let lineage = transform.lineage(input);
    let keep_reads = transform
        .filters()
        .then(|| transform.keep_reads(input))
        .flatten();
    let inputs = input.fields().len();
    let breach = |breach| Refusal::Contract {
        transform: transform.name().to_owned(),
        bounded: bound.is_some(),
        breach,
    };
    if let Some(lineage) = &lineage {
        let fits = lineage.len() == output.fields().len()
            && lineage.iter().flatten().all(|column| *column < inputs);
        if !fits {
            return Err(breach(Breach::Lineage));
        }
    }
    if keep_reads.iter().flatten().any(|column| *column >= inputs) {
        return Err(breach(Breach::Reads));
    }
    Ok(Declared {
        bound,
        lineage,
        keep_reads,
    })
}

/// Refuses `changes` where they flag columns unchanged that `transform` cannot carry: it
/// declares no lineage, or it filters reading a flagged column, whose value the source did not
/// send.
pub(super) fn carried(
    transform: &dyn Transform,
    declared: &Declared,
    changes: &Changes,
) -> Result<(), Refusal> {
    if !changes.flags_any_of(None) {
        return Ok(());
    }
    let name = || transform.name().to_owned();
    if declared.lineage.is_none() {
        return Err(Refusal::Partial { transform: name() });
    }
    if transform.filters() && changes.flags_any_of(declared.keep_reads.as_deref()) {
        return Err(Refusal::Unsent { transform: name() });
    }
    Ok(())
}

/// What `transform` keeps of `data`, a null keeping nothing.
pub(super) fn kept(
    transform: &dyn Transform,
    bounded: bool,
    data: &RecordBatch,
) -> Result<BooleanArray, Refusal> {
    let keep = transform.keep(data).map_err(|error| Refusal::Failed {
        transform: transform.name().to_owned(),
        error,
    })?;
    if keep.len() != data.num_rows() {
        let breach = Breach::Keep {
            rows: data.num_rows(),
            answered: keep.len(),
        };
        let transform = transform.name().to_owned();
        return Err(Refusal::Contract {
            transform,
            bounded,
            breach,
        });
    }
    Ok(if keep.null_count() > 0 {
        filter::prep_null_mask_filter(&keep)
    } else {
        keep
    })
}

/// What `transform` makes of `data`: checked against `data`'s rows and the `schema` it
/// declared, holding the chain's copy of it, and against the bound it declares.
pub(super) fn applied(
    transform: &dyn Transform,
    declared: &Declared,
    data: &RecordBatch,
    schema: &SchemaRef,
) -> Result<RecordBatch, Refusal> {
    let name = || transform.name().to_owned();
    let output = transform
        .apply(data.clone())
        .map_err(|error| Refusal::Failed {
            transform: name(),
            error,
        })?;
    let breach = |breach| Refusal::Contract {
        transform: name(),
        bounded: declared.bound.is_some(),
        breach,
    };
    if output.num_rows() != data.num_rows() {
        let (input, output) = (data.num_rows(), output.num_rows());
        return Err(breach(Breach::Rows { input, output }));
    }
    if output.schema_ref().fields() != schema.fields() {
        return Err(breach(Breach::Schema));
    }
    let output = output
        .with_schema(Arc::clone(schema))
        .map_err(|_| breach(Breach::Schema))?;
    if let Some(bound) = &declared.bound {
        let (allocated, bound) = (allocated_beyond(&output, data), bound.of(data));
        if allocated > bound {
            return Err(Refusal::Bound {
                transform: name(),
                allocated,
                bound,
            });
        }
    }
    Ok(output)
}
