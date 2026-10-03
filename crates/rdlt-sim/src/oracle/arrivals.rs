//! The types a stream's batch columns arrive as, which schema resolution decides by: an Arrow
//! batch's column has its shape's type, however many of its values are null; a JSON push's is
//! inferred from every value it holds.

#[cfg(test)]
mod tests;

use rdlt_connector::{DecimalType, LogicalType};
use rdlt_testkit::drawn::Scalar;
use rdlt_testkit::held::held;

use crate::workload::{Row, SimStream};

/// Integers whose magnitude is at most this, 2⁵³, are exact as a 64-bit float.
const EXACT_IN_FLOAT: u64 = 1 << 53;

/// The type one batch's column arrives as.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum Arrival {
    /// A type the model knows; `Int64` holds an integer a 64-bit float would round.
    Typed(LogicalType),
    /// 64-bit integers every one of which a 64-bit float holds exactly.
    ExactInt,
    /// A pushed object or array, whose inferred type the model leaves open: a container type,
    /// or `Json` where containers of both kinds meet.
    Container,
}

impl Arrival {
    /// The type a column holding both `self` and `other` arrives as: exact integers joined to
    /// floats are floats, as the shredder joins them.
    fn join(self, other: Self) -> Self {
        let float =
            |logical: &LogicalType| matches!(logical, LogicalType::Float32 | LogicalType::Float64);
        match (self, other) {
            (Self::ExactInt, Self::ExactInt | Self::Typed(LogicalType::Null))
            | (Self::Typed(LogicalType::Null), Self::ExactInt) => Self::ExactInt,
            (Self::ExactInt, Self::Typed(logical)) | (Self::Typed(logical), Self::ExactInt)
                if float(&logical) =>
            {
                Self::Typed(LogicalType::Float64)
            }
            (Self::ExactInt, other) | (other, Self::ExactInt) => {
                Self::Typed(LogicalType::Int64).join(other)
            }
            (Self::Typed(left), Self::Typed(right)) => Self::Typed(left.join(&right)),
            (Self::Container, Self::Typed(LogicalType::Null) | Self::Container)
            | (Self::Typed(LogicalType::Null), Self::Container) => Self::Container,
            // A container joins a scalar as `Json`.
            (Self::Container, Self::Typed(_)) | (Self::Typed(_), Self::Container) => {
                Self::Typed(LogicalType::Json)
            }
        }
    }

    /// Whether a column of `column` holds every value arriving as `self`; `None` where the model
    /// cannot say.
    pub(super) fn fits(&self, column: &LogicalType) -> Option<bool> {
        match self {
            Self::Typed(logical) => Some(column.join(logical) == *column),
            // A column of 64-bit floats takes them cast.
            Self::ExactInt => {
                Some(column.join(&LogicalType::Int64) == *column || *column == LogicalType::Float64)
            }
            Self::Container if *column == LogicalType::Json => Some(true),
            Self::Container => match column {
                LogicalType::Struct(_) | LogicalType::List(_) => None,
                _ => Some(false),
            },
        }
    }

    /// Whether the column arrives with no type, holding only nulls: a JSON push's column of
    /// nulls, which is no change.
    pub(super) fn is_null(&self) -> bool {
        *self == Self::Typed(LogicalType::Null)
    }

    /// The arrival a declared schema's column of `logical` makes as a run plans: a schema holds no
    /// values, so its integers are exact.
    pub(super) fn declared(logical: &LogicalType) -> Self {
        if *logical == LogicalType::Int64 {
            Self::ExactInt
        } else {
            Self::Typed(logical.clone())
        }
    }

    /// The type the column arrives as, where the model knows it.
    pub(super) fn logical(&self) -> Option<LogicalType> {
        match self {
            Self::Typed(logical) => Some(logical.clone()),
            Self::ExactInt => Some(LogicalType::Int64),
            Self::Container => None,
        }
    }

    /// Whether it holds an integer a 64-bit float would round.
    pub(super) fn rounds(&self) -> bool {
        *self == Self::Typed(LogicalType::Int64)
    }

    /// The arrival of 64-bit integers `values`, exact where every one is.
    fn integers(mut values: impl Iterator<Item = i64>) -> Self {
        if values.all(|value| value.unsigned_abs() <= EXACT_IN_FLOAT) {
            Self::ExactInt
        } else {
            Self::Typed(LogicalType::Int64)
        }
    }
}

/// The type drift column `column` arrives as in the batch holding `row`, or `None` where the
/// batch lacks the column.
pub(super) fn arrival(stream: &SimStream, row: &Row, column: usize) -> Option<Arrival> {
    let partition = usize::try_from(row.partition).unwrap_or(0);
    let shape = stream.drift[column].shapes[partition][row.delivered].as_ref()?;
    let offset = usize::try_from(row.offset).unwrap_or(0);
    let rows = stream.rows(partition, row.delivered);
    let batch = stream
        .batches(partition, row.delivered)
        .into_iter()
        .find(|batch| batch.contains(&offset))?;
    if !stream.json {
        return Some(typed(&shape.logical, &rows[batch], column));
    }
    rows[batch]
        .iter()
        .filter_map(|row| row.extras[column].as_ref())
        .map(pushed)
        .reduce(Arrival::join)
}

/// The arrival of an Arrow column of `logical` holding `rows`' values of drift column `column`:
/// its type, and for 64-bit integers, whether a float holds every one exactly.
fn typed(logical: &LogicalType, rows: &[Row], column: usize) -> Arrival {
    if *logical != LogicalType::Int64 {
        return Arrival::Typed(logical.clone());
    }
    Arrival::integers(rows.iter().filter_map(|row| match &row.extras[column] {
        Some(Scalar::Int(value)) => Some(*value),
        _ => None,
    }))
}

/// The widest type drift column `column` may arrive as in the batch the engine writes `row` in:
/// the join of every push it may shred together with `row`'s, or `None` where none holds the
/// column.
///
/// An Arrow batch's column has one type whatever the engine gathers it with, though whether its
/// integers are exact is judged over every batch gathered with it.
pub(super) fn widest(stream: &SimStream, row: &Row, column: usize) -> Option<Arrival> {
    let partition = usize::try_from(row.partition).unwrap_or(0);
    let offset = usize::try_from(row.offset).unwrap_or(0);
    let rows = stream.rows(partition, row.delivered);
    let span = &rows[stream.span(partition, row.delivered, offset)];
    if !stream.json {
        let shape = stream.drift[column].shapes[partition][row.delivered].as_ref()?;
        return Some(typed(&shape.logical, span, column));
    }
    span.iter()
        .filter_map(|row| row.extras[column].as_ref())
        .map(pushed)
        .reduce(Arrival::join)
}

/// Whether a column arriving as `arrival`, JSON text, is one whose own column, of `own`, takes
/// each value it holds alone: only the others take a variant or the policy.
pub(super) fn splits(arrival: Option<&Arrival>, own: &LogicalType) -> bool {
    *own != LogicalType::Json && arrival.and_then(Arrival::logical) == Some(LogicalType::Json)
}

/// Whether a column of `own` holds `row`'s value of drift column `column` alone; `None` where the
/// model cannot say.
pub(super) fn alone(
    stream: &SimStream,
    row: &Row,
    column: usize,
    own: &LogicalType,
) -> Option<bool> {
    match row.extras[column].as_ref()? {
        value if stream.json => pushed(value).fits(own),
        Scalar::Json(value) => Some(held(value, own).is_some()),
        _ => None,
    }
}

/// The type the engine infers for `value`, pushed in JSON: a float JSON cannot hold is pushed as
/// its name.
pub(super) fn pushed(value: &Scalar) -> Arrival {
    Arrival::Typed(match value {
        Scalar::Null => LogicalType::Null,
        Scalar::Bool(_) => LogicalType::Bool,
        Scalar::Int(value) => return Arrival::integers(std::iter::once(*value)),
        Scalar::Decimal(digits) => return whole(digits),
        Scalar::Float64(float) if float.is_finite() => LogicalType::Float64,
        Scalar::Float64(_) | Scalar::Utf8(_) => LogicalType::Utf8,
        _ => return Arrival::Container,
    })
}

/// The arrival of the whole number `digits`, pushed as JSON: the narrowest type the engine reads it
/// as, a 64-bit integer, a decimal of 20, 38 or 76 digits, or else JSON text.
fn whole(digits: &str) -> Arrival {
    if let Ok(value) = digits.parse::<i64>() {
        return Arrival::integers(std::iter::once(value));
    }
    let decimal = |precision| {
        Arrival::Typed(LogicalType::Decimal(
            DecimalType::new(precision, 0).expect("whole decimals of 76 digits or fewer are valid"),
        ))
    };
    let width = digits.trim_start_matches('-').len();
    if digits.parse::<u64>().is_ok() {
        decimal(20)
    } else if width <= 38 {
        decimal(38)
    } else if width <= 76 {
        decimal(76)
    } else {
        Arrival::Typed(LogicalType::Json)
    }
}

/// The type drift column `column` of `stream` keeps while its policy never changes it: the
/// declared type, or the hinted one where the plan hints it.
pub(super) fn fixed(stream: &SimStream, column: usize) -> Option<LogicalType> {
    let drift = &stream.drift[column];
    let declared = drift.declared.as_ref()?;
    Some(drift.hint.clone().unwrap_or_else(|| declared.clone()))
}

/// Every type drift column `column` arrived as in `rows`, the rows delivered so far, each batch's
/// once.
pub(super) fn all(stream: &SimStream, rows: &[Row], column: usize) -> Vec<Arrival> {
    let mut arrivals: Vec<Arrival> = Vec::new();
    for row in rows {
        if let Some(arrival) = arrival(stream, row, column)
            && !arrivals.contains(&arrival)
        {
            arrivals.push(arrival);
        }
    }
    arrivals
}
