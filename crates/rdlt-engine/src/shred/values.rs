//! Values of JSON text read as the shredder reads a record's: whether each fits a column's type,
//! and those that fit built into a column, their numbers exact.
//!
//! A column of JSON whose table holds the column as another type sends there each value that
//! type holds; this is how it finds them, and reads them.

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;

use arrow_array::{Array, ArrayRef, BooleanArray, StringArray};
use rdlt_connector::LogicalType;

use super::ShredError;
use super::build::Column;
use super::meter::{Columns, Meter};
use super::observe::Observed;
use super::observing::Look;
use super::visit::{Context, Value};
use crate::json::{Reader, Token};

/// A parse of single values, which a lowering piece has reserved what it builds for.
fn context() -> Context {
    Context::new(Meter::reserved(), Columns::new(u64::MAX))
}

/// What the JSON value `text` is observed as, its numbers exact.
fn observed(text: &str, context: &Context) -> Result<Observed, ShredError> {
    let mut node = Observed::Null;
    let look = Look {
        node: &mut node,
        context,
        depth: 1,
    };
    super::exact::visit(text.as_bytes(), look, context).map_err(|error| {
        context
            .fault()
            .unwrap_or_else(|| ShredError::Invalid(format!("a value of JSON text: {error}")))
    })?;
    Ok(node)
}

/// Whether a column of `column` holds the value `observed` as it is: its type joins into the
/// column's, or it is an integer a 64-bit float holds exactly and the column is of such floats.
fn holds(column: &LogicalType, observed: &Observed) -> bool {
    let exact = *observed == Observed::Int { exact: true };
    column.join(&observed.logical_type()) == *column || (*column == LogicalType::Float64 && exact)
}

/// Which values of `texts`, JSON text, a column of `column` holds: null for a null.
///
/// A value the shredder would refuse, as a number beyond a float's range is, fits no column:
/// JSON text keeps it, as it keeps an object repeating a key.
pub(crate) fn fitting(texts: &StringArray, column: &LogicalType) -> BooleanArray {
    let nested = matches!(column, LogicalType::Struct(_) | LogicalType::List(_));
    texts
        .iter()
        .map(|text| {
            let text = text?;
            let fits = observed(text, &context())
                .is_ok_and(|observed| observed != Observed::Null && holds(column, &observed));
            Some(fits && !(nested && repeats_a_key(text)))
        })
        .collect()
}

/// Whether an object in the JSON value `text`, at any depth, repeats a key.
fn repeats_a_key(text: &str) -> bool {
    let mut reader = Reader::new(text);
    let mut open: Vec<Option<BTreeSet<String>>> = Vec::new();
    while let Ok(Some(token)) = reader.next() {
        match token {
            Token::BeginObject => open.push(Some(BTreeSet::new())),
            Token::BeginArray => open.push(None),
            Token::EndObject | Token::EndArray => {
                open.pop();
            }
            Token::Key(key) => {
                if let Some(Some(keys)) = open.last_mut()
                    && !keys.insert(key.into_owned())
                {
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}

/// The values of `texts` that `taken` names, read into a column typed by their join, the others
/// null: the column and its type.
///
/// # Errors
///
/// Where a value is not JSON, nests past the limit, or repeats a key, or a bug stops the build.
pub(crate) fn read(
    texts: &StringArray,
    taken: &BooleanArray,
) -> Result<(ArrayRef, LogicalType), ShredError> {
    let context = context();
    let named = |row: usize| texts.is_valid(row) && taken.is_valid(row) && taken.value(row);
    let mut joined = Observed::Null;
    for row in (0..texts.len()).filter(|row| named(*row)) {
        joined.join(&observed(texts.value(row), &context)?);
    }
    let unbuilt = |_| ShredError::Internal("a reserved build ran out of room".to_owned());
    let mut column = Column::new(&joined, 0, texts.len(), &context.meter).map_err(unbuilt)?;
    for row in 0..texts.len() {
        if !named(row) {
            column.null(&context.meter).map_err(unbuilt)?;
            continue;
        }
        let value = Value {
            column: &mut column,
            context: &context,
            depth: 1,
            capacity: texts.len(),
        };
        super::exact::visit(texts.value(row).as_bytes(), value, &context).map_err(|error| {
            context
                .fault()
                .unwrap_or_else(|| ShredError::Invalid(format!("a value of JSON text: {error}")))
        })?;
    }
    Ok((column.finish()?, joined.logical_type()))
}
