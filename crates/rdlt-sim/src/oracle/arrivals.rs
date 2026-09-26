//! The types a stream's batch columns arrive as, which schema resolution decides by: an Arrow
//! batch's column has its shape's type, however many of its values are null; a JSON push's is
//! inferred from every value it holds.

#[cfg(test)]
mod tests;

use rdlt_connector::LogicalType;
use rdlt_testkit::drawn::Scalar;

use crate::workload::{Row, SimStream};

/// The type one batch's column arrives as.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum Arrival {
    /// A type the model knows.
    Typed(LogicalType),
    /// A pushed object or array, whose inferred type the model leaves open: a container type,
    /// or `Json` where containers of both kinds meet.
    Container,
}

impl Arrival {
    /// The type a column holding both `self` and `other` arrives as.
    fn join(self, other: Self) -> Self {
        match (self, other) {
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
}

/// The type drift column `column` arrives as in the batch holding `row`, or `None` where the
/// batch lacks the column.
pub(super) fn arrival(stream: &SimStream, row: &Row, column: usize) -> Option<Arrival> {
    let partition = usize::try_from(row.partition).unwrap_or(0);
    let shape = stream.drift[column].shapes[partition][row.delivered].as_ref()?;
    if !stream.json {
        return Some(Arrival::Typed(shape.logical.clone()));
    }
    let offset = usize::try_from(row.offset).unwrap_or(0);
    let rows = stream.rows(partition, row.delivered);
    let batch = stream
        .batches(partition, row.delivered)
        .into_iter()
        .find(|batch| batch.contains(&offset))?;
    rows[batch]
        .iter()
        .filter_map(|row| row.extras[column].as_ref())
        .map(pushed)
        .reduce(Arrival::join)
}

/// The widest type drift column `column` may arrive as in the batch the engine writes `row` in:
/// the join of every push it may shred together with `row`'s, or `None` where none holds the
/// column.
///
/// An Arrow batch's column has one type whatever the engine gathers it with.
pub(super) fn widest(stream: &SimStream, row: &Row, column: usize) -> Option<Arrival> {
    if !stream.json {
        return arrival(stream, row, column);
    }
    let partition = usize::try_from(row.partition).unwrap_or(0);
    let offset = usize::try_from(row.offset).unwrap_or(0);
    let rows = stream.rows(partition, row.delivered);
    rows[stream.span(partition, row.delivered, offset)]
        .iter()
        .filter_map(|row| row.extras[column].as_ref())
        .map(pushed)
        .reduce(Arrival::join)
}

/// The type the engine infers for `value`, pushed in JSON: a float JSON cannot hold is pushed as
/// its name.
pub(super) fn pushed(value: &Scalar) -> Arrival {
    Arrival::Typed(match value {
        Scalar::Null => LogicalType::Null,
        Scalar::Bool(_) => LogicalType::Bool,
        Scalar::Int(_) => LogicalType::Int64,
        Scalar::Float64(float) if float.is_finite() => LogicalType::Float64,
        Scalar::Float64(_) | Scalar::Utf8(_) => LogicalType::Utf8,
        _ => return Arrival::Container,
    })
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
