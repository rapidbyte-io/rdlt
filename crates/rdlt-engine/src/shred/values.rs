//! Values of JSON text read as the shredder reads a record's: whether each fits a column's type,
//! and those that fit built into a column, their numbers exact.
//!
//! A column of JSON whose table holds the column as another type sends there each value that
//! type holds; this is how it finds them, and reads them.

#[cfg(test)]
mod tests;

use arrow_array::{Array, ArrayRef, BooleanArray, StringArray};
use rdlt_connector::LogicalType;

use super::ShredError;
use super::build::Column;
use super::keys::ObjectKeys;
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

/// Whether a column of `column`, nullable where `nullable` says, holds one value observed as
/// `observed`: a value of its type.
///
/// A null fits a nullable column. An object fits a struct holding each of its fields, by name in
/// any order, a field it lacks being null. An array fits a list whose items hold its items, and
/// may be null where it holds nulls. An integer of at most 2⁵³ in magnitude fits a column of
/// 64-bit floats, which hold it exactly. Any other value fits a column whose type its own type
/// joins into.
fn fits(column: &LogicalType, observed: &Observed, nullable: bool) -> bool {
    match (column, observed) {
        (_, Observed::Null) => nullable,
        (LogicalType::Struct(fields), Observed::Object(shape)) => {
            let held = shape.fields().iter().all(|(name, value)| {
                let field = fields.iter().find(|field| field.name() == &**name);
                field.is_some_and(|field| fits(field.logical_type(), value, field.is_nullable()))
            });
            let lacking = fields.iter().all(|field| {
                field.is_nullable()
                    || shape
                        .get(field.name())
                        .is_some_and(|value| *value != Observed::Null)
            });
            held && lacking
        }
        // An array's items are observed together, so whether one is null is not known: a list
        // of items that may not be null holds none of them.
        (LogicalType::List(item), Observed::Array(items, count)) => {
            *count == 0 || (item.is_nullable() && fits(item.logical_type(), items, true))
        }
        (LogicalType::Float64, Observed::Int { exact: true }) => true,
        (LogicalType::Struct(_) | LogicalType::List(_), _) => false,
        (column, observed) => column.join(&observed.logical_type()) == *column,
    }
}

/// Which values of `texts`, JSON text, a column of `column` holds: null for a null.
///
/// A value the shredder would refuse, as a number beyond a float's range is, fits no column:
/// JSON text keeps it, as it keeps an object repeating a key. A table's column that a column of
/// JSON is split into is never a merge key, so it holds nulls.
pub(crate) fn fitting(texts: &StringArray, column: &LogicalType) -> BooleanArray {
    // Each value is parsed as a shredding job is, sure of its stack at the nesting limit.
    super::job(|| {
        let nested = matches!(column, LogicalType::Struct(_) | LogicalType::List(_));
        texts
            .iter()
            .map(|text| {
                let text = text?;
                let fits =
                    observed(text, &context()).is_ok_and(|observed| fits(column, &observed, true));
                Some(fits && !(nested && repeats_a_key(text)))
            })
            .collect()
    })
}

/// Whether an object in the JSON value `text`, at any depth, repeats a key.
fn repeats_a_key(text: &str) -> bool {
    let mut reader = Reader::new(text);
    let mut open: Vec<Option<ObjectKeys<'_>>> = Vec::new();
    while let Ok(Some(token)) = reader.next() {
        match token {
            Token::BeginObject => open.push(Some(ObjectKeys::default())),
            Token::BeginArray => open.push(None),
            Token::EndObject | Token::EndArray => {
                open.pop();
            }
            Token::Key(key) => {
                if let Some(Some(keys)) = open.last_mut()
                    && keys.note(key).is_err()
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
    super::job(|| read_values(texts, taken))
}

/// The values of `texts` that `taken` names, read as [`read`] reads them.
fn read_values(
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
