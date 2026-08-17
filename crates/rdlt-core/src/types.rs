//! Logical types and the widening lattice.
//!
//! `widen` is the pure join used by schema inference. Its laws — commutativity,
//! idempotence, associativity (order-insensitivity), and monotonicity — are
//! enforced by the crate's property tests. Types move only *upward*; there is
//! deliberately **no** `Float64 → Decimal` edge (NaN/±Inf and the exponent range
//! don't fit — that edge would be a silent-corruption bug).

use serde::{Deserialize, Serialize};

/// Maximum decimal precision we represent (128-bit decimal).
pub const DECIMAL_MAX_PRECISION: u8 = 38;

/// The engine's logical column types, mapped to Arrow physically and to destination
/// types via the SPI's `destination::Capabilities`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LogicalType {
    /// True or false.
    Bool,
    /// 64-bit signed integer. A value beyond its range widens the column rather
    /// than wrapping.
    Int64,
    /// 64-bit float. Reached from `Int64` only where the integer is exactly
    /// representable (within ±2^53); otherwise the column widens further.
    Float64,
    /// Fixed-point decimal. Never produced by JSON inference; enters via per-column
    /// hints or Arrow-native sources.
    Decimal {
        /// Total significant digits, at most [`DECIMAL_MAX_PRECISION`].
        precision: u8,
        /// Digits after the point.
        scale: u8,
    },
    /// Text. Absorbs every textable type through canonical rendering, which makes
    /// it the practical meeting point for mixed-type columns.
    Utf8,
    /// Opaque bytes. Not producible from JSON, and deliberately not textable —
    /// inventing an encoding silently would be a corruption bug.
    Binary,
    /// An instant with a timezone offset.
    TimestampTz,
    /// A date and time with no offset.
    TimestampNaive,
    /// A calendar date.
    Date,
    /// A time of day.
    Time,
    /// A UUID.
    Uuid,
    /// The typed escape hatch: undecomposable values are preserved verbatim here,
    /// never dropped, never exploded into variant columns. Top of the lattice.
    Json,
}

use LogicalType::*;

/// Least upper bound of two logical types on the widening lattice.
///
/// Structure: `Json` is the top; `Utf8` absorbs every textable type (everything except
/// `Binary` and `Json`); the numeric chains are `Int64 → Float64 → Utf8` and
/// `Int64 → Decimal → Utf8`, with `Float64 ⊔ Decimal = Utf8`.
///
/// This is the *type-level* join. The engine additionally value-checks
/// conversions at shred time (`Int64 → Float64` is exact only within ±2^53) and
/// escalates an inexact value's column further along the lattice.
pub fn widen(a: LogicalType, b: LogicalType) -> LogicalType {
    if a == b {
        return a;
    }
    match (a, b) {
        // Top absorbs everything.
        (Json, _) | (_, Json) => Json,
        // Binary is textable only via encodings we refuse to invent silently.
        (Binary, _) | (_, Binary) => Json,
        // Decimal ⊔ Decimal: max integer digits, max scale, bounded by 128-bit precision.
        (
            Decimal {
                precision: p1,
                scale: s1,
            },
            Decimal {
                precision: p2,
                scale: s2,
            },
        ) => join_decimals(p1, s1, p2, s2),
        // Int64 fits Decimal(>=19, 0); widen the decimal to cover it.
        (Int64, Decimal { precision, scale }) | (Decimal { precision, scale }, Int64) => {
            join_decimals(precision, scale, 19, 0)
        }
        (Int64, Float64) | (Float64, Int64) => Float64,
        // Everything textable meets at Utf8 (canonical renderings).
        _ => Utf8,
    }
}

fn join_decimals(p1: u8, s1: u8, p2: u8, s2: u8) -> LogicalType {
    let int_digits = (p1.saturating_sub(s1)).max(p2.saturating_sub(s2));
    let scale = s1.max(s2);
    match int_digits.checked_add(scale) {
        Some(precision) if precision <= DECIMAL_MAX_PRECISION => Decimal { precision, scale },
        // A decimal that no longer fits 128 bits widens to canonical text.
        _ => Utf8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The Binary arm meets everything at Json, and the order is strict:
    /// a join that lands on the wider side must not also land on the narrower.
    #[test]
    fn binary_meets_anything_at_json_and_order_is_strict() {
        assert_eq!(widen(Binary, Int64), Json);
        assert_eq!(widen(Utf8, Binary), Json);
        assert_eq!(widen(Int64, Float64), Float64);
        assert_ne!(widen(Float64, Int64), Int64, "narrowing is NOT widening");
    }

    #[test]
    fn no_float_to_decimal_edge() {
        // The silent-corruption edge must not exist: Float64 ⊔ Decimal = Utf8.
        assert_eq!(
            widen(
                Float64,
                Decimal {
                    precision: 10,
                    scale: 2
                }
            ),
            Utf8
        );
    }

    #[test]
    fn int64_joins_decimal_by_widening_integer_digits() {
        assert_eq!(
            widen(
                Int64,
                Decimal {
                    precision: 10,
                    scale: 2
                }
            ),
            Decimal {
                precision: 21,
                scale: 2
            }
        );
    }

    #[test]
    fn decimal_overflow_escalates_to_text() {
        assert_eq!(
            widen(
                Decimal {
                    precision: 38,
                    scale: 0
                },
                Decimal {
                    precision: 38,
                    scale: 10
                }
            ),
            Utf8
        );
    }
}
