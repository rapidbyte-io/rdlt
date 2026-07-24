//! Logical types and the widening lattice.
//!
//! `widen` is the pure join used by schema inference. Its laws — commutativity,
//! idempotence, associativity (order-insensitivity), and monotonicity — are enforced by
//! property tests in `tests/lattice_laws.rs`. Types move only *upward*; there is
//! deliberately **no** `Float64 → Decimal` edge (NaN/±Inf and the exponent range don't
//! fit — that edge would be a silent-corruption bug).

use serde::{Deserialize, Serialize};

/// Maximum decimal precision we represent (128-bit decimal).
pub const DECIMAL_MAX_PRECISION: u8 = 38;

/// The engine's logical column types, mapped to Arrow physically and to destination
/// types via `DestCapabilities`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LogicalType {
    Bool,
    Int64,
    Float64,
    /// Fixed-point decimal. Never produced by JSON inference; enters via per-column
    /// hints or Arrow-native sources.
    Decimal {
        precision: u8,
        scale: u8,
    },
    Utf8,
    Binary,
    TimestampTz,
    TimestampNaive,
    Date,
    Time,
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
/// Note this is the *type-level* join. Conversions are additionally value-checked at
/// shred time (`Int64 → Float64` is exact only within ±2^53); an inexact value
/// escalates the column further along the lattice. See `int64_fits_in_f64`.
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

/// `true` iff `v` converts to `f64` exactly. Values outside ±2^53 do not; the shredder
/// must escalate the column instead of silently rounding.
pub fn int64_fits_in_f64(v: i64) -> bool {
    const EXACT: i64 = 1 << 53;
    (-EXACT..=EXACT).contains(&v)
}

/// `a ⊑ b` in the widening order (i.e. `b` can hold everything `a` can).
pub fn is_widening_of(a: LogicalType, b: LogicalType) -> bool {
    widen(a, b) == b
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_meets_anything_at_json_and_order_is_strict() {
        // Mutation-report closure: the Binary arm and is_widening_of's negative
        // case were untested.
        use LogicalType::*;
        assert_eq!(widen(Binary, Int64), Json);
        assert_eq!(widen(Utf8, Binary), Json);
        assert!(is_widening_of(Int64, Float64));
        assert!(!is_widening_of(Float64, Int64), "narrowing is NOT widening");
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

    #[test]
    fn f64_exactness_boundary() {
        assert!(int64_fits_in_f64(1 << 53));
        assert!(!int64_fits_in_f64((1 << 53) + 1));
        assert!(int64_fits_in_f64(-(1 << 53)));
        assert!(!int64_fits_in_f64(-(1 << 53) - 1));
    }
}
