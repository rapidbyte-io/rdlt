//! Values of JSON text read as the shredder reads a record's: whether each fits a column's type,
//! and those that fit built into a column, their numbers exact.
//!
//! A column of JSON whose table holds the column as another type sends there each value that
//! type holds; this is how it finds them, and reads them. Each value is observed once, to fit
//! it, a token at a time and as the type guides, and the join of those that fit is kept to read
//! them; where rows are dropped between, the kept ones are observed again to join them alone.

#[cfg(test)]
mod tests;

use std::borrow::Cow;

use arrow_array::{Array, ArrayRef, BooleanArray, StringArray};
use rdlt_connector::LogicalType;
use rdlt_wire::limits::NESTING_DEPTH;

use super::ShredError;
use super::build::Column;
use super::meter::{Columns, Meter};
use super::observe::Observed;
use super::observing::Look;
use super::visit::{Context, Value};
use crate::json::{Reader, Text, Token};

#[cfg(test)]
thread_local! {
    /// How many values of JSON text this thread observed, which tests read.
    pub(crate) static OBSERVED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// How many values of JSON text this thread checked apart for a key an object repeats,
    /// which tests read.
    pub(crate) static CHECKED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
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
        // A key an object repeats is refused as the value is observed, but in a part held as
        // JSON, which is checked apart.
        let checked = holds_json(column);
        let mut joined = Observed::Null;
        let held: BooleanArray = texts
            .iter()
            .map(|text| {
                let text = text?;
                let held = match observed(text, column, &context()) {
                    Ok(observed)
                        if fits(column, &observed, true) && !(checked && repeats_a_key(text)) =>
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

/// Whether `column` holds a part as JSON at any depth.
fn holds_json(column: &LogicalType) -> bool {
    match column {
        LogicalType::Json => true,
        LogicalType::Struct(fields) => fields.iter().any(|field| holds_json(field.logical_type())),
        LogicalType::List(item) => holds_json(item.logical_type()),
        _ => false,
    }
}

/// Objects a value may hold open at once: no more than its containers may nest.
#[expect(
    clippy::cast_possible_truncation,
    reason = "the nesting limit is a few dozen levels"
)]
const OPEN: usize = NESTING_DEPTH as usize;

/// Whether an object in the JSON value `text`, at any depth, repeats a key, however escaped.
///
/// The keys of the objects open are noted by where each starts in `text`, four bytes a key, with
/// room made once for as many as `text` may hold, and read again to be compared when their
/// object closes: only an escaped key is copied, two at a time.
fn repeats_a_key(text: &str) -> bool {
    #[cfg(test)]
    CHECKED.with(|checked| checked.set(checked.get() + 1));
    // Each key is followed by a colon, and takes four bytes at the least: `"":0`.
    let most = memchr::memchr_iter(b':', text.as_bytes())
        .count()
        .min(text.len() / 4);
    let mut starts: Vec<u32> = Vec::with_capacity(most);
    // Where each open object's keys start among `starts`, the innermost last.
    let mut objects = [0_usize; OPEN];
    let mut open = 0;
    let mut reader = Reader::new(text);
    while let Ok(Some(token)) = reader.next() {
        match token {
            Token::BeginObject => {
                let Some(first) = objects.get_mut(open) else {
                    return true;
                };
                *first = starts.len();
                open += 1;
            }
            Token::EndObject => {
                open = open.saturating_sub(1);
                let first = objects.get(open).copied().unwrap_or_default();
                if repeated(text, starts.get_mut(first..).unwrap_or_default()) {
                    return true;
                }
                starts.truncate(first);
            }
            Token::Key(key) => {
                // A value of a column of text is shorter than 2 GiB.
                let Ok(start) = u32::try_from(key.start_in(text)) else {
                    return true;
                };
                starts.push(start);
            }
            _ => {}
        }
    }
    false
}

/// Whether two of `starts`, where the keys of one object start in `text`, start keys that read
/// alike, their escapes read.
fn repeated(text: &str, starts: &mut [u32]) -> bool {
    let key = |start: &u32| {
        let start = usize::try_from(*start).unwrap_or_default();
        Text::read(text, start).map_or(Cow::Borrowed(""), |(key, _)| key.unescaped())
    };
    starts.sort_unstable_by(|one, other| key(one).cmp(&key(other)));
    starts.windows(2).any(|pair| key(&pair[0]) == key(&pair[1]))
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
