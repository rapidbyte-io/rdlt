//! Values of JSON text read as the shredder reads a record's: whether each fits a column's type,
//! and those that fit built into a column, their numbers exact.
//!
//! A column of JSON whose table holds the column as another type sends there each value that
//! type holds; this is how it finds them, and reads them. Each value is observed once, to fit
//! it, a token at a time and as the type guides, and the join of those that fit is kept to read
//! them; where rows are dropped between, the kept ones are observed again to join them alone.

#[cfg(test)]
mod tests;

use arrow_array::{Array, ArrayRef, BooleanArray, StringArray};
use rdlt_connector::LogicalType;

use super::ShredError;
use super::build::Column;
use super::keys::OpenKeys;
use super::meter::{Columns, Meter};
use super::observe::Observed;
use super::observing::Look;
use super::visit::{Context, Value};
use crate::json::{Reader, Token};

#[cfg(test)]
thread_local! {
    /// How many values of JSON text this thread observed, which tests read.
    pub(crate) static OBSERVED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The join of what the values a column of JSON sends its own column were observed as, the type
/// they are read into, and that column's type, which guided observing them.
#[derive(Clone, Debug)]
pub(crate) struct Joined {
    observed: Observed,
    column: LogicalType,
}

impl Default for Joined {
    fn default() -> Self {
        Self {
            observed: Observed::Null,
            column: LogicalType::Null,
        }
    }
}

/// A parse of single values, which a lowering piece has reserved what it builds for.
fn context() -> Context {
    Context::new(Meter::reserved(), Columns::new(u64::MAX))
}

/// What the JSON value `text` is observed as, its numbers exact, as `column`, the type it is
/// fitted to, guides: a part `column` holds as JSON as JSON, nothing below it, and the value no
/// further than a key its struct lacks or a container its type holds none of.
fn observed(text: &str, column: &LogicalType, context: &Context) -> Result<Observed, ShredError> {
    #[cfg(test)]
    OBSERVED.with(|observed| observed.set(observed.get() + 1));
    let mut node = Observed::Null;
    let look = Look {
        node: &mut node,
        context,
        depth: 1,
        target: Some(column),
    };
    super::tokens::visit(text, look, context).map_err(|error| {
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

/// Which values of `texts`, JSON text, a column of `column` holds, null for a null, and the join
/// of what those values were observed as.
///
/// A value the shredder would refuse, as a number beyond a float's range is, fits no column:
/// JSON text keeps it, as it keeps an object repeating a key. A table's column that a column of
/// JSON is split into is never a merge key, so it holds nulls.
pub(crate) fn fitting(texts: &StringArray, column: &LogicalType) -> (BooleanArray, Joined) {
    // Each value is parsed as a shredding job is, sure of its stack at the nesting limit.
    super::job(|| {
        let nested = matches!(column, LogicalType::Struct(_) | LogicalType::List(_));
        let mut joined = Observed::Null;
        let held: BooleanArray = texts
            .iter()
            .map(|text| {
                let text = text?;
                let held = match observed(text, column, &context()) {
                    Ok(observed)
                        if fits(column, &observed, true) && !(nested && repeats_a_key(text)) =>
                    {
                        joined.join(&observed);
                        true
                    }
                    _ => false,
                };
                Some(held)
            })
            .collect();
        let joined = Joined {
            observed: joined,
            column: column.clone(),
        };
        (held, joined)
    })
}

/// Whether an object in the JSON value `text`, at any depth, repeats a key.
fn repeats_a_key(text: &str) -> bool {
    let mut reader = Reader::new(text);
    let meter = Meter::reserved();
    let mut open = OpenKeys::default();
    // Each key as its escapes read, one after another.
    let mut keys = String::new();
    while let Ok(Some(token)) = reader.next() {
        match token {
            // A key or an object the meter has no room for counts as a repeat: the value stays
            // JSON text.
            Token::BeginObject => {
                if open.open(&meter).is_err() {
                    return true;
                }
            }
            Token::EndObject => {
                if open.close(&keys).is_some() {
                    return true;
                }
            }
            Token::Key(key) => {
                let start = keys.len();
                keys.push_str(&key.unescaped());
                if open.key(start..keys.len(), &meter).is_err() {
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}

/// The join of what the values of `texts` that `taken` names are observed as, guided by the
/// column `joined` was fitted to, for values some rows of which were dropped since `fitting`
/// joined them.
///
/// # Errors
///
/// Where a value is not JSON or nests past the limit.
pub(crate) fn rejoined(
    texts: &StringArray,
    taken: &BooleanArray,
    joined: &Joined,
) -> Result<Joined, ShredError> {
    super::job(|| {
        let context = context();
        let named = |row: &usize| texts.is_valid(*row) && taken.is_valid(*row) && taken.value(*row);
        let mut observed_join = Observed::Null;
        for row in (0..texts.len()).filter(named) {
            observed_join.join(&observed(texts.value(row), &joined.column, &context)?);
        }
        Ok(Joined {
            observed: observed_join,
            column: joined.column.clone(),
        })
    })
}

/// The values of `texts` that `taken` names, read into a column typed by `joined`, the join of
/// their types as `fitting` found it, or `rejoined` after rows were dropped, the others null: the
/// column and its type.
///
/// # Errors
///
/// Where a value is not JSON, nests past the limit, or repeats a key, or a bug stops the build.
pub(crate) fn read(
    texts: &StringArray,
    taken: &BooleanArray,
    joined: &Joined,
) -> Result<(ArrayRef, LogicalType), ShredError> {
    super::job(|| read_values(texts, taken, &joined.observed))
}

/// The values of `texts` that `taken` names, read as [`read`] reads them.
fn read_values(
    texts: &StringArray,
    taken: &BooleanArray,
    joined: &Observed,
) -> Result<(ArrayRef, LogicalType), ShredError> {
    let context = context();
    let named = |row: usize| texts.is_valid(row) && taken.is_valid(row) && taken.value(row);
    let unbuilt = |_| ShredError::Internal("a reserved build ran out of room".to_owned());
    let mut column = Column::new(joined, 0, texts.len(), &context.meter).map_err(unbuilt)?;
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
