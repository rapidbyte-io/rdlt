//! Columns of JSON whose values a table's column of another type holds in part: each value it
//! holds goes there, read into its type, and only the others take a variant or the policy, so
//! one value of another kind changes where no other value goes.
//!
//! A value fits where its own type, as the shredder reads a value, joins into the column's; a
//! value that would widen the column goes with the others, since the column's type is fixed
//! once planned.

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, StringArray};
use arrow_buffer::BooleanBuffer;
use arrow_schema::ArrowError;
use rdlt_connector::LogicalType;

use super::super::convert::{convert, normalize};
use super::super::resolve::{Rest, Route};
use crate::shred::values;

#[cfg(test)]
mod tests;

/// The split columns of a plan: each's position among the incoming columns, its own column's
/// type and where its other values go.
#[derive(Clone, Debug, Default)]
pub(super) struct Splits(Vec<(usize, LogicalType, Rest)>);

/// Each split column of a batch by its position, in ascending order: its JSON text, plain, and
/// which of its values its own column holds, null where the column holds none, and the join of
/// those values' types.
#[derive(Clone, Debug, Default)]
pub(super) struct Fitted(Vec<(usize, StringArray, BooleanArray, values::Joined)>);

impl Splits {
    /// The split columns `routes` names, whose own columns are of `types`, the model's.
    pub(super) fn of(routes: &[Route], types: &[LogicalType]) -> Self {
        let splits = routes
            .iter()
            .enumerate()
            .filter_map(|(index, route)| match route {
                Route::Split { own, rest } => Some((index, types[*own].clone(), *rest)),
                _ => None,
            });
        Self(splits.collect())
    }

    /// Which values of each split column of `batch` its own column holds; of only those whose
    /// other values drop their rows where `dropping`.
    pub(super) fn fit(&self, batch: &RecordBatch, dropping: bool) -> Result<Fitted, ArrowError> {
        let splits = self
            .0
            .iter()
            .filter(|(_, _, rest)| !dropping || *rest == Rest::DiscardRows);
        let fitted = splits.map(|(index, own, _)| {
            let texts = texts(batch.column(*index))?;
            let (fits, joined) = values::fitting(&texts, own);
            Ok((*index, texts, fits, joined))
        });
        Ok(Fitted(fitted.collect::<Result<_, ArrowError>>()?))
    }

    /// Which rows of a batch of `rows` rows hold a value of a split column whose other values
    /// drop their rows, as `fitted` says: false for those, where any are.
    pub(super) fn kept(&self, fitted: &Fitted, rows: usize) -> Option<BooleanArray> {
        let dropping: Vec<&BooleanArray> = self
            .0
            .iter()
            .filter(|(_, _, rest)| *rest == Rest::DiscardRows)
            .filter_map(|(index, _, _)| fitted.mask(*index))
            .collect();
        if dropping.is_empty() {
            return None;
        }
        let rows = BooleanBuffer::new_set(rows);
        let kept = dropping.iter().fold(rows, |kept, fits| {
            // A null fit holds no value, so its row is kept.
            let fits = match fits.nulls() {
                Some(nulls) => &!nulls.inner() | fits.values(),
                None => fits.values().clone(),
            };
            &kept & &fits
        });
        Some(BooleanArray::new(kept, None))
    }

    /// The values of `batch`'s split columns whose own column does not hold them and the policy
    /// discards, as `fitted` says.
    pub(super) fn discarded(&self, batch: &RecordBatch, fitted: &Fitted) -> u64 {
        let discarding = self
            .0
            .iter()
            .filter(|(_, _, rest)| *rest == Rest::DiscardValues);
        discarding
            .filter_map(|(index, _, _)| {
                let fits = fitted.mask(*index)?;
                let others =
                    (0..batch.num_rows()).filter(|row| fits.is_valid(*row) && !fits.value(*row));
                Some(u64::try_from(others.count()).unwrap_or(u64::MAX))
            })
            .fold(0, u64::saturating_add)
    }
}

impl Fitted {
    fn split(
        &self,
        index: usize,
    ) -> Result<(&StringArray, &BooleanArray, &values::Joined), ArrowError> {
        self.0
            .binary_search_by_key(&index, |(fitted, ..)| *fitted)
            .ok()
            .map(|found| {
                let (_, texts, fits, joined) = &self.0[found];
                (texts, fits, joined)
            })
            .ok_or_else(|| {
                ArrowError::ComputeError("a split column's values were not fitted".to_owned())
            })
    }

    fn mask(&self, index: usize) -> Option<&BooleanArray> {
        self.split(index).ok().map(|(_, fits, _)| fits)
    }

    /// The texts, masks and joins of the rows `kept` keeps: where it drops any, the kept values
    /// are joined again, since a dropped value can change how the others read.
    pub(super) fn filtered(self, kept: &BooleanArray) -> Result<Self, ArrowError> {
        let fitted = self.0.into_iter().map(|(index, texts, fits, joined)| {
            let rows = texts.len();
            let texts = arrow_select::filter::filter(&texts, kept)?;
            let texts = texts.as_string::<i32>().clone();
            let fits = arrow_select::filter::filter(&fits, kept)?;
            let fits = fits.as_boolean().clone();
            let joined = if texts.len() < rows {
                values::rejoined(&texts, &fits)
                    .map_err(|error| ArrowError::ExternalError(Box::new(error)))?
            } else {
                joined
            };
            Ok((index, texts, fits, joined))
        });
        Ok(Self(fitted.collect::<Result<_, ArrowError>>()?))
    }

    /// The values of the split column at `index` in its batch that its own column of `own`
    /// holds, read into that type; the others null.
    pub(super) fn own(&self, index: usize, own: &LogicalType) -> Result<ArrayRef, ArrowError> {
        let (texts, fits, joined) = self.split(index)?;
        let (values, read) = values::read(texts, fits, joined)
            .map_err(|error| ArrowError::ExternalError(Box::new(error)))?;
        convert(&values, &read, own)
    }

    /// The values of the split column at `index` in its batch that its own column does not
    /// hold, as JSON text; the others null.
    pub(super) fn rest(&self, index: usize) -> Result<ArrayRef, ArrowError> {
        let (texts, fits, _) = self.split(index)?;
        arrow_select::nullif::nullif(texts, fits)
    }
}

/// The JSON text `column` holds, plain.
fn texts(column: &ArrayRef) -> Result<StringArray, ArrowError> {
    Ok(normalize(column, &LogicalType::Json)?
        .as_string::<i32>()
        .clone())
}

/// What lowering `column`, of JSON text, takes into a table column of `to`, of another type,
/// stored as text where `as_text`: its values the column holds read and lowered into it, and
/// the others as text.
#[cfg(test)]
pub(crate) fn lowered(
    column: &ArrayRef,
    to: &LogicalType,
    as_text: bool,
) -> Result<(ArrayRef, ArrayRef), ArrowError> {
    let batch = RecordBatch::try_from_iter([("c", std::sync::Arc::clone(column))])?;
    let splits = Splits(vec![(0, to.clone(), Rest::DiscardValues)]);
    let fitted = splits.fit(&batch, false)?;
    let own = fitted.own(0, to)?;
    let own = if as_text {
        super::super::convert::text(&own, to)?
    } else {
        own
    };
    Ok((own, fitted.rest(0)?))
}
