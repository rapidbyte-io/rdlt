//! Columns of JSON whose values a table's column of another type holds in part: each value it
//! holds goes there, read into its type, and only the others take a variant or the policy, so
//! one value of another kind changes where no other value goes.
//!
//! A value fits where its own type, as the shredder reads a value, joins into the column's; a
//! value that would widen the column goes with the others, since the column's type is fixed
//! once planned.

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, StringArray};
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
/// which of its values its own column holds, null where the column holds none.
#[derive(Clone, Debug, Default)]
pub(super) struct Fitted(Vec<(usize, StringArray, BooleanArray)>);

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
            let fits = values::fitting(&texts, own);
            Ok((*index, texts, fits))
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
        let kept: BooleanArray = (0..rows)
            .map(|row| {
                Some(
                    dropping
                        .iter()
                        .all(|fits| fits.is_null(row) || fits.value(row)),
                )
            })
            .collect();
        Some(kept)
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
    fn split(&self, index: usize) -> Result<(&StringArray, &BooleanArray), ArrowError> {
        self.0
            .binary_search_by_key(&index, |(fitted, _, _)| *fitted)
            .ok()
            .map(|found| {
                let (_, texts, fits) = &self.0[found];
                (texts, fits)
            })
            .ok_or_else(|| {
                ArrowError::ComputeError("a split column's values were not fitted".to_owned())
            })
    }

    fn mask(&self, index: usize) -> Option<&BooleanArray> {
        self.split(index).ok().map(|(_, fits)| fits)
    }

    /// The texts and masks of the rows `kept` keeps.
    pub(super) fn filtered(self, kept: &BooleanArray) -> Result<Self, ArrowError> {
        let fitted = self.0.into_iter().map(|(index, texts, fits)| {
            let texts = arrow_select::filter::filter(&texts, kept)?;
            let fits = arrow_select::filter::filter(&fits, kept)?;
            Ok((
                index,
                texts.as_string::<i32>().clone(),
                fits.as_boolean().clone(),
            ))
        });
        Ok(Self(fitted.collect::<Result<_, ArrowError>>()?))
    }

    /// The values of the split column at `index` in its batch that its own column of `own`
    /// holds, read into that type; the others null.
    pub(super) fn own(&self, index: usize, own: &LogicalType) -> Result<ArrayRef, ArrowError> {
        let (texts, fits) = self.split(index)?;
        let (values, read) = values::read(texts, fits)
            .map_err(|error| ArrowError::ExternalError(Box::new(error)))?;
        convert(&values, &read, own)
    }

    /// The values of the split column at `index` in its batch that its own column does not
    /// hold, as JSON text; the others null.
    pub(super) fn rest(&self, index: usize) -> Result<ArrayRef, ArrowError> {
        let (texts, fits) = self.split(index)?;
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
