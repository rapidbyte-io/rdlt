//! Values their column cannot hold — refused by the column's conversion, or a change time no
//! version can begin at — which the column's schema policy decides row by row: refused with the
//! batch, the row dropped, or the value nulled.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch};
use arrow_buffer::BooleanBufferBuilder;
use arrow_schema::{ArrowError, Schema};

use super::{LoweringPlan, Source};
use crate::policy::SchemaPolicy;
use crate::table::convert::{convert, decoded};
use crate::table::temporal::micros_at;

/// Incoming columns, by their place in a batch, each converted to the type its column holds
/// where its schema policy decided it.
#[derive(Default)]
pub(super) struct Converted(Vec<Option<ArrayRef>>);

/// What checking the columns a schema policy decides found: the rows of each holding a value its
/// column cannot hold, and the others converted whole, which lowering takes as they are.
#[derive(Default)]
pub(super) struct Checked {
    unheld: Vec<(usize, BooleanArray)>,
    pub(super) converted: Converted,
}

impl Converted {
    /// The conversion of the incoming column at `index`, where its policy made one.
    pub(super) fn get(&self, index: usize) -> Option<&ArrayRef> {
        self.0.get(index).and_then(Option::as_ref)
    }

    /// Keeps `array` as the conversion of the incoming column at `index` of a batch of
    /// `columns` columns.
    fn set(&mut self, columns: usize, index: usize, array: ArrayRef) {
        if self.0.is_empty() {
            self.0.resize(columns, None);
        }
        self.0[index] = Some(array);
    }

    /// Forgets the conversion of each incoming column at `indices`.
    pub(super) fn forget(&mut self, indices: impl IntoIterator<Item = usize>) {
        for index in indices {
            if let Some(slot) = self.0.get_mut(index) {
                *slot = None;
            }
        }
    }

    /// These and `other`'s conversions, of columns that two policies decide apart.
    pub(super) fn extend(&mut self, other: Self) {
        if self.0.is_empty() {
            *self = other;
            return;
        }
        for (slot, other) in self.0.iter_mut().zip(other.0) {
            if other.is_some() {
                *slot = other;
            }
        }
    }

    /// Forgets each conversion that is a column of `whole` as it is, which then goes with
    /// `whole`: the places of those it forgot.
    pub(super) fn release(&mut self, whole: &RecordBatch) -> Vec<usize> {
        let mut own = Vec::new();
        for (index, slot) in self.0.iter_mut().enumerate() {
            if slot
                .as_ref()
                .is_some_and(|array| Arc::ptr_eq(array, whole.column(index)))
            {
                *slot = None;
                own.push(index);
            }
        }
        own
    }

    /// Each conversion of a batch's columns, of the rows `keep` keeps, which are `kept`: those
    /// at `own`, which [`Converted::release`] forgot as the batch's own columns, are `kept`'s,
    /// and any other is filtered.
    pub(super) fn filter(
        &mut self,
        own: &[usize],
        kept: &RecordBatch,
        keep: &BooleanArray,
    ) -> Result<(), ArrowError> {
        for slot in &mut self.0 {
            if let Some(array) = slot.as_ref() {
                *slot = Some(arrow_select::filter::filter(array.as_ref(), keep)?);
            }
        }
        for index in own {
            self.0[*index] = Some(Arc::clone(kept.column(*index)));
        }
        Ok(())
    }
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
                Check::Converted(converted) => {
                    checked
                        .converted
                        .set(batch.num_columns(), *index, converted);
                }
                Check::Refused(rows) => unheld.push((*index, rows)),
            }
        }
        if let Some(index) = self.change_time.filter(|index| following(*index))
            && let Some(rows) = unbegun(batch.column(index))?
        {
            // A value refused on both counts is one value its column cannot hold.
            match unheld.iter_mut().find(|(refused, _)| *refused == index) {
                Some((_, refused)) => *refused = either(refused, &rows),
                None => unheld.push((index, rows)),
            }
            // The column changes, so what it converted to is not what lowering takes.
            checked.converted.forget([index]);
        }
        Ok(checked)
    }

    /// Whether a change time a version cannot begin at is the batch's to refuse: unless the
    /// change time's schema policy discards it.
    pub(super) fn change_time_policy(&self) -> SchemaPolicy {
        self.change_time
            .and_then(|index| self.policies.get(index).copied())
            .unwrap_or_default()
    }

    /// Which rows of `batch` the policies of its columns keep, the columns those policies decide
    /// converted, each whose every value was held, and how many rows each column drops.
    ///
    /// No row holding a value its column cannot hold is kept where the column's policy drops
    /// rows; `None` keeps every row.
    pub(super) fn held(&self, batch: &RecordBatch) -> Result<Held, ArrowError> {
        let Checked { unheld, converted } = self.unheld(batch, SchemaPolicy::DiscardRow)?;
        let refused = unheld
            .iter()
            .map(|(_, rows)| rows.values().clone())
            .reduce(|left, right| &left | &right);
        let kept = refused.map(|refused| BooleanArray::new(!&refused, None));
        let dropping = unheld.iter().map(|(index, rows)| (*index, count(rows)));
        Ok((kept, converted, dropping.collect()))
    }

    /// `batch` with each value its column cannot hold nulled, where the column's policy discards
    /// values, how many it nulled of each column, and the columns those policies decide that it
    /// left as they were, converted.
    pub(super) fn nulled(&self, batch: RecordBatch) -> Result<Nulled, ArrowError> {
        let Checked { unheld, converted } = self.unheld(&batch, SchemaPolicy::DiscardValue)?;
        if unheld.is_empty() {
            return Ok((batch, Vec::new(), converted));
        }
        let mut columns = batch.columns().to_vec();
        let mut fields: Vec<_> = batch.schema().fields().iter().cloned().collect();
        let mut nulled = Vec::with_capacity(unheld.len());
        for (index, rows) in unheld {
            nulled.push((index, count(&rows)));
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

/// Which rows of a batch its columns' policies keep, its columns those policies decide
/// converted, and how many rows each column whose policy drops rows drops, by its place.
pub(super) type Held = (Option<BooleanArray>, Converted, Vec<(usize, u64)>);

/// A batch with the values its columns cannot hold nulled, how many each column nulled, by its
/// place, and the columns the policies decide that it left as they were, converted.
pub(super) type Nulled = (RecordBatch, Vec<(usize, u64)>, Converted);

/// How many rows `rows` names.
fn count(rows: &BooleanArray) -> u64 {
    u64::try_from(rows.true_count()).unwrap_or(u64::MAX)
}

/// `array` with the values of the rows `rows` names null.
pub(crate) fn nulled_at(array: &ArrayRef, rows: &BooleanArray) -> Result<ArrayRef, ArrowError> {
    arrow_select::nullif::nullif(decoded(array)?.as_ref(), rows)
}

/// What converting a column whose schema policy decides its values row by row found.
pub(crate) enum Check {
    /// Every value converted, to this.
    Converted(ArrayRef),
    /// The rows holding a value the column cannot hold, each refused alone; where none is, the
    /// column refused whole is refused with its batch as it is lowered.
    Refused(BooleanArray),
}

/// The rows `one` or `other` names; neither mask holds a null.
fn either(one: &BooleanArray, other: &BooleanArray) -> BooleanArray {
    BooleanArray::new(one.values() | other.values(), None)
}

/// `array`, of `from`, as a column of `to` holds it, or the rows it cannot hold: found by
/// converting halves of it, and the halves of a half that fails, down to a row alone.
///
/// Every check a conversion makes is of each row, so a half fails when a row of it does, and a
/// conversion refused by no row alone leaves every row unmarked.
pub(crate) fn refused(
    array: &ArrayRef,
    from: &rdlt_connector::LogicalType,
    to: &rdlt_connector::LogicalType,
) -> Result<Check, ArrowError> {
    if let Ok(converted) = convert(array, from, to) {
        return Ok(Check::Converted(converted));
    }
    let array = decoded(array)?;
    let mut rows = BooleanBufferBuilder::new(array.len());
    rows.append_n(array.len(), false);
    // Ranges of rows whose conversion failed, each `(offset, length)`.
    let mut failed = vec![(0, array.len())];
    while let Some((offset, length)) = failed.pop() {
        if length < 2 {
            if length == 1 && array.is_valid(offset) {
                rows.set_bit(offset, true);
            }
            continue;
        }
        let half = length / 2;
        for (start, length) in [(offset, half), (offset + half, length - half)] {
            // What a half converts to is dropped at once, so one half is held at a time.
            if convert(&array.slice(start, length), from, to).is_err() {
                failed.push((start, length));
            }
        }
    }
    Ok(Check::Refused(BooleanArray::new(rows.finish(), None)))
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
