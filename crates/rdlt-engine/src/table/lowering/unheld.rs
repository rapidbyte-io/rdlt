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

impl LoweringPlan {
    /// The incoming columns whose schema policy is `policy`, each with the rows of `batch` that
    /// hold a value its column cannot hold, where some do.
    pub(super) fn unheld(
        &self,
        batch: &RecordBatch,
        policy: SchemaPolicy,
    ) -> Result<Vec<(usize, BooleanArray)>, ArrowError> {
        let mut unheld = Vec::new();
        let following = |index: usize| self.policies.get(index) == Some(&policy);
        for (column, source) in self.view.model.columns.iter().zip(&self.sources) {
            let Source::Incoming(index, from) = source else {
                continue;
            };
            if source.is_null() || !following(*index) {
                continue;
            }
            if let Some(rows) = refused(batch.column(*index), from, column.logical_type())? {
                unheld.push((*index, rows));
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
        }
        Ok(unheld)
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

    /// Which rows of `batch` the policies of its columns keep: none holding a value its column
    /// cannot hold where the column's policy drops rows; `None` where they keep every row.
    pub(super) fn held(&self, batch: &RecordBatch) -> Result<Option<BooleanArray>, ArrowError> {
        let unheld = self.unheld(batch, SchemaPolicy::DiscardRow)?;
        if unheld.is_empty() {
            return Ok(None);
        }
        let kept = (0..batch.num_rows())
            .map(|row| Some(!unheld.iter().any(|(_, rows)| rows.value(row))))
            .collect();
        Ok(Some(kept))
    }

    /// `batch` with each value its column cannot hold nulled, where the column's policy discards
    /// values, and how many it nulled.
    pub(super) fn nulled(&self, batch: RecordBatch) -> Result<(RecordBatch, u64), ArrowError> {
        let unheld = self.unheld(&batch, SchemaPolicy::DiscardValue)?;
        if unheld.is_empty() {
            return Ok((batch, 0));
        }
        let mut columns = batch.columns().to_vec();
        let mut fields: Vec<_> = batch.schema().fields().iter().cloned().collect();
        let mut nulled = 0;
        for (index, rows) in unheld {
            nulled += rows.true_count() as u64;
            let values = decoded(&columns[index])?;
            columns[index] = arrow_select::nullif::nullif(values.as_ref(), &rows)?;
            let field = fields[index].as_ref().clone();
            let field = field.with_data_type(columns[index].data_type().clone());
            fields[index] = Arc::new(field.with_nullable(true));
        }
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            batch.schema().metadata().clone(),
        ));
        Ok((RecordBatch::try_new(schema, columns)?, nulled))
    }
}

/// The rows `one` or `other` names.
fn either(one: &BooleanArray, other: &BooleanArray) -> BooleanArray {
    one.iter()
        .zip(other.iter())
        .map(|(one, other)| Some(one == Some(true) || other == Some(true)))
        .collect()
}

/// Which rows of `array`, of `from`, a column of `to` cannot hold: those whose conversion alone
/// is refused; `None` where the array converts, or no row alone is refused.
fn refused(
    array: &ArrayRef,
    from: &rdlt_connector::LogicalType,
    to: &rdlt_connector::LogicalType,
) -> Result<Option<BooleanArray>, ArrowError> {
    if convert(array, from, to).is_ok() {
        return Ok(None);
    }
    let array = decoded(array)?;
    let rows: BooleanArray = (0..array.len())
        .map(|row| Some(array.is_valid(row) && convert(&array.slice(row, 1), from, to).is_err()))
        .collect();
    Ok((rows.true_count() > 0).then_some(rows))
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
