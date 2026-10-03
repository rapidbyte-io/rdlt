//! Values their column cannot hold — refused by the column's conversion, or a change time no
//! version can begin at — which the column's schema policy decides row by row: refused with the
//! batch, the row dropped, or the value nulled.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch};
use arrow_schema::{ArrowError, Schema};

use super::{LoweringPlan, Source};
use crate::policy::SchemaPolicy;
use crate::table::convert::{convert, decoded};
use crate::table::temporal::micros_at;

/// Incoming columns, by their place in a batch, each converted to the type its column holds.
pub(super) type Converted = Vec<(usize, ArrayRef)>;

/// What checking the columns a schema policy decides found: the rows of each holding a value its
/// column cannot hold, and the others converted whole, which lowering takes as they are.
#[derive(Default)]
pub(super) struct Checked {
    unheld: Vec<(usize, BooleanArray)>,
    pub(super) converted: Converted,
}

impl LoweringPlan {
    /// Checks the incoming columns of `batch` whose schema policy is `policy`: the rows of each
    /// that hold a value its column cannot hold, where some do, and each other converted.
    pub(super) fn unheld(
        &self,
        batch: &RecordBatch,
        policy: SchemaPolicy,
    ) -> Result<Checked, ArrowError> {
        let mut checked = Checked::default();
        let unheld = &mut checked.unheld;
        let following = |index: usize| self.policies.get(index) == Some(&policy);
        for (column, source) in self.view.model.columns.iter().zip(&self.sources) {
            let Source::Incoming(index, from) = source else {
                continue;
            };
            if source.is_null() || !following(*index) {
                continue;
            }
            match refused(batch.column(*index), from, column.logical_type())? {
                Check::Converted(converted) => checked.converted.push((*index, converted)),
                Check::Refused(rows) => unheld.push((*index, rows)),
                Check::Failed => {}
            }
        }
        if let Some(index) = self.change_time().filter(|index| following(*index))
            && let Some(rows) = unbegun(batch.column(index))?
        {
            // A value refused on both counts is one value its column cannot hold.
            match unheld.iter_mut().find(|(refused, _)| *refused == index) {
                Some((_, refused)) => *refused = either(refused, &rows),
                None => unheld.push((index, rows)),
            }
            // The column changes, so what it converted to is not what lowering takes.
            checked
                .converted
                .retain(|(converted, _)| *converted != index);
        }
        Ok(checked)
    }

    /// The incoming column holding a history table's change times, where it has one.
    pub(super) fn change_time(&self) -> Option<usize> {
        let column = self.view.meta.history.as_ref()?.change_time.as_deref()?;
        let fields = self.incoming.schema.fields();
        fields.iter().position(|field| field.name() == column)
    }

    /// Whether a change time a version cannot begin at is the batch's to refuse: unless the
    /// change time's schema policy discards it.
    pub(super) fn change_time_policy(&self) -> SchemaPolicy {
        self.change_time()
            .and_then(|index| self.policies.get(index).copied())
            .unwrap_or_default()
    }

    /// Which rows of `batch` the policies of its columns keep, and the columns those policies
    /// decide converted, each whose every value was held.
    ///
    /// No row holding a value its column cannot hold is kept where the column's policy drops
    /// rows; `None` keeps every row.
    pub(super) fn held(
        &self,
        batch: &RecordBatch,
    ) -> Result<(Option<BooleanArray>, Converted), ArrowError> {
        let Checked { unheld, converted } = self.unheld(batch, SchemaPolicy::DiscardRow)?;
        if unheld.is_empty() {
            return Ok((None, converted));
        }
        let kept = (0..batch.num_rows())
            .map(|row| Some(!unheld.iter().any(|(_, rows)| rows.value(row))))
            .collect();
        Ok((Some(kept), converted))
    }

    /// `batch` with each value its column cannot hold nulled, where the column's policy discards
    /// values, how many it nulled, and the columns those policies decide that it left as they
    /// were, converted.
    pub(super) fn nulled(
        &self,
        batch: RecordBatch,
    ) -> Result<(RecordBatch, u64, Converted), ArrowError> {
        let Checked { unheld, converted } = self.unheld(&batch, SchemaPolicy::DiscardValue)?;
        if unheld.is_empty() {
            return Ok((batch, 0, converted));
        }
        let mut columns = batch.columns().to_vec();
        let mut fields: Vec<_> = batch.schema().fields().iter().cloned().collect();
        let mut nulled = 0;
        for (index, rows) in unheld {
            nulled += rows.true_count() as u64;
            columns[index] = nulled_at(&columns[index], &rows)?;
            let field = fields[index].as_ref().clone();
            let field = field.with_data_type(columns[index].data_type().clone());
            fields[index] = Arc::new(field.with_nullable(true));
        }
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            batch.schema().metadata().clone(),
        ));
        Ok((RecordBatch::try_new(schema, columns)?, nulled, converted))
    }
}

/// `array` with the values of the rows `rows` names null.
pub(crate) fn nulled_at(array: &ArrayRef, rows: &BooleanArray) -> Result<ArrayRef, ArrowError> {
    arrow_select::nullif::nullif(decoded(array)?.as_ref(), rows)
}

/// What converting a column whose schema policy decides its values row by row found.
pub(crate) enum Check {
    /// Every value converted, to this.
    Converted(ArrayRef),
    /// The rows holding a value the column cannot hold, each refused alone.
    Refused(BooleanArray),
    /// The column failed whole, though no row alone is refused: lowering refuses it.
    Failed,
}

/// The rows `one` or `other` names.
fn either(one: &BooleanArray, other: &BooleanArray) -> BooleanArray {
    one.iter()
        .zip(other.iter())
        .map(|(one, other)| Some(one == Some(true) || other == Some(true)))
        .collect()
}

/// `array`, of `from`, as a column of `to` holds it, or the rows it cannot hold: those whose
/// conversion alone is refused.
pub(crate) fn refused(
    array: &ArrayRef,
    from: &rdlt_connector::LogicalType,
    to: &rdlt_connector::LogicalType,
) -> Result<Check, ArrowError> {
    if let Ok(converted) = convert(array, from, to) {
        return Ok(Check::Converted(converted));
    }
    let array = decoded(array)?;
    let rows: BooleanArray = (0..array.len())
        .map(|row| Some(array.is_valid(row) && convert(&array.slice(row, 1), from, to).is_err()))
        .collect();
    Ok(if rows.true_count() > 0 {
        Check::Refused(rows)
    } else {
        Check::Failed
    })
}

/// Which rows of `times`, a change time, no version can begin at: a null, or a time an `i64` of
/// microseconds cannot hold; `None` where every row can, or the column holds no times, which
/// the batch's refusal names.
fn unbegun(times: &ArrayRef) -> Result<Option<BooleanArray>, ArrowError> {
    let times = decoded(times)?;
    let instants = matches!(
        times.data_type(),
        arrow_schema::DataType::Timestamp(..)
            | arrow_schema::DataType::Date32
            | arrow_schema::DataType::Date64
    );
    if !instants {
        return Ok(None);
    }
    let rows: BooleanArray = (0..times.len())
        .map(|row| Some(times.is_null(row) || micros_at(times.as_ref(), row).is_none()))
        .collect();
    Ok((rows.true_count() > 0).then_some(rows))
}
