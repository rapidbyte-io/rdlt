//! Values as the source sent them, and what each must mean once stored.

use rdlt_connector::LogicalType;
use rdlt_testkit::canon::{self, Canon};
use rdlt_testkit::decode;
use rdlt_testkit::drawn::Scalar;
use rdlt_testkit::held;

/// A value the source sent, which its cell must mean exactly.
#[derive(Clone, Debug)]
pub(in crate::oracle) enum Sent {
    /// A value of an Arrow batch's column, of the column's type.
    Typed(Scalar, LogicalType),
    /// A value of a JSON push, as its JSON text, whose type the engine infers.
    Json(String),
}

impl Sent {
    /// What the value means once held by a column of `column`.
    pub(in crate::oracle) fn meaning(&self, column: &LogicalType) -> Canon {
        match self {
            Self::Typed(value, source) => canon::canonical_into(value, source, column),
            Self::Json(text) => decode::json_as(text, column),
        }
    }

    /// Whether a column of `column` holds the value cast, as a column of 64-bit floats holds a
    /// 64-bit integer a float holds exactly, and a column of another type a value of JSON its
    /// type holds.
    pub(in crate::oracle) fn cast_exactly(&self, column: &LogicalType) -> bool {
        match (self, column) {
            (Self::Typed(Scalar::Int(value), LogicalType::Int64), LogicalType::Float64) => {
                value.unsigned_abs() <= 1 << 53
            }
            // A value of a column of JSON whose own column holds it is read into its type.
            (Self::Typed(Scalar::Json(value), LogicalType::Json), column) => {
                held::held(value, column).is_some()
            }
            _ => false,
        }
    }

    /// Whether the value, pushed as a JSON integer, is one a column of `column`, of floats, would
    /// round: the engine never puts such a value in one.
    pub(in crate::oracle) fn rounds_into(&self, column: &LogicalType) -> bool {
        let Self::Json(text) = self else {
            return false;
        };
        let digits = text.strip_prefix('-').unwrap_or(text);
        if digits.is_empty() || !digits.bytes().all(|digit| digit.is_ascii_digit()) {
            return false;
        }
        let exact: u128 = match column {
            LogicalType::Float64 => 1 << 53,
            LogicalType::Float32 => 1 << 24,
            _ => return false,
        };
        !digits
            .parse::<u128>()
            .is_ok_and(|magnitude| magnitude <= exact)
    }

    /// The type the source sent the value as, where it says.
    pub(in crate::oracle) fn source(&self) -> Option<&LogicalType> {
        match self {
            Self::Typed(_, source) => Some(source),
            Self::Json(_) => None,
        }
    }
}
