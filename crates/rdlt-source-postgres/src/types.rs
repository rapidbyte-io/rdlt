//! Postgres type → engine type mapping (contract:
//! `specs/005-postgres-source/contracts/type-mapping.md`).
//!
//! Every reflected column gets a `MappedType`: how the SELECT projects it
//! (`SelectPolicy` — policy conversions happen SERVER-side so the binary COPY
//! stream only ever carries the lossless decode set), which Arrow type the
//! decoder builds (the structured path derives logical types from Arrow,
//! engine clause E7), and how the wire bytes decode. Nothing falls through to
//! inference: unknown types take the textual fallback, visibly.

use arrow_schema::{DataType, TimeUnit};

/// Stable pg_type OIDs for the lossless decode set (`pg_type.dat`, unchanged
/// across all supported PG versions).
pub(crate) mod oid {
    pub const BOOL: u32 = 16;
    pub const BYTEA: u32 = 17;
    pub const NAME: u32 = 19;
    pub const INT8: u32 = 20;
    pub const INT2: u32 = 21;
    pub const INT4: u32 = 23;
    pub const TEXT: u32 = 25;
    pub const JSON: u32 = 114;
    pub const FLOAT4: u32 = 700;
    pub const FLOAT8: u32 = 701;
    pub const BPCHAR: u32 = 1042;
    pub const VARCHAR: u32 = 1043;
    pub const DATE: u32 = 1082;
    pub const TIME: u32 = 1083;
    pub const TIMESTAMP: u32 = 1114;
    pub const TIMESTAMPTZ: u32 = 1184;
    pub const NUMERIC: u32 = 1700;
    pub const UUID: u32 = 2950;
    pub const JSONB: u32 = 3802;
}

/// How `sqlgen` projects the column inside `COPY (SELECT …)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SelectPolicy {
    /// The bare quoted column — its binary wire format is decoded natively.
    Direct,
    /// `col::text` — the canonical-text policy rows (enum, interval, money,
    /// unconstrained numeric, textual fallback…).
    CastText,
    /// `to_jsonb(col)::text` — arrays / composites / ranges.
    CastJsonbText,
}

/// How the decoder interprets the field's wire bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Decode {
    Bool,
    Int2,
    Int4,
    Int8,
    Float4,
    Float8,
    /// NBASE-10000 numeric → i128 at the declared precision/scale.
    Decimal { precision: u8, scale: u8 },
    /// UTF-8 text payload (text family, json, every ::text projection).
    Utf8,
    /// jsonb wire payload: 1 version byte, then UTF-8 JSON text.
    JsonbText,
    /// 16-byte uuid → 36-char lowercase canonical text.
    UuidText,
    Bytea,
    /// µs since PG epoch (2000-01-01); rebased; ±infinity saturates.
    Timestamp { tz: bool },
    /// days since PG epoch; rebased; ±infinity saturates.
    Date,
    /// µs since midnight.
    Time,
}

/// The complete per-column plan the reflection layer emits.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MappedType {
    pub select: SelectPolicy,
    pub decode: Decode,
    pub arrow: DataType,
    /// Accepted for `cursor.column` (contract "Cursor-capable types").
    pub cursor_capable: bool,
    /// True for the [documented-lossy] policy rows — surfaced in the run
    /// report so representation changes are visible, never silent.
    pub documented_lossy: bool,
}

/// Shape facts reflection extracts from `pg_type` for mapping decisions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PgTypeInfo {
    pub oid: u32,
    /// `pg_type.typtype`: b=base, e=enum, d=domain (pre-resolved by
    /// reflection to its base — never reaches here), c=composite, r=range,
    /// m=multirange, p=pseudo.
    pub typtype: char,
    /// `pg_type.typcategory`: 'A' = array.
    pub typcategory: char,
    /// `pg_attribute.atttypmod` (numeric precision/scale live here).
    pub typmod: i32,
}

/// numeric typmod → (precision, scale); None for unconstrained (`typmod=-1`).
fn numeric_precision_scale(typmod: i32) -> Option<(i32, i32)> {
    if typmod < 4 {
        return None;
    }
    let packed = typmod - 4;
    // Scale is stored in the low 16 bits; PG ≥ 15 allows negative scales,
    // stored two's-complement in those bits.
    let precision = (packed >> 16) & 0xFFFF;
    let scale = ((packed & 0x7FF) ^ 1024) - 1024;
    Some((precision, scale))
}

fn lossless(select: SelectPolicy, decode: Decode, arrow: DataType, cursor: bool) -> MappedType {
    MappedType {
        select,
        decode,
        arrow,
        cursor_capable: cursor,
        documented_lossy: false,
    }
}

fn text_policy(select: SelectPolicy) -> MappedType {
    MappedType {
        select,
        decode: Decode::Utf8,
        arrow: DataType::Utf8,
        cursor_capable: false,
        documented_lossy: true,
    }
}

/// The contract's binding mapping. Total: every input maps; the last arm is
/// the textual fallback (visible, documented — never inference).
pub(crate) fn map_type(info: &PgTypeInfo) -> MappedType {
    use SelectPolicy::{CastJsonbText, CastText, Direct};

    // Policy shapes first: arrays, composites, ranges → canonical JSON text.
    if info.typcategory == 'A' || matches!(info.typtype, 'c' | 'r' | 'm') {
        return text_policy(CastJsonbText);
    }
    // Enum labels → text.
    if info.typtype == 'e' {
        return text_policy(CastText);
    }

    match info.oid {
        oid::BOOL => lossless(Direct, Decode::Bool, DataType::Boolean, false),
        oid::INT2 => lossless(Direct, Decode::Int2, DataType::Int64, true),
        oid::INT4 => lossless(Direct, Decode::Int4, DataType::Int64, true),
        oid::INT8 => lossless(Direct, Decode::Int8, DataType::Int64, true),
        oid::FLOAT4 => lossless(Direct, Decode::Float4, DataType::Float64, false),
        oid::FLOAT8 => lossless(Direct, Decode::Float8, DataType::Float64, false),
        oid::NUMERIC => match numeric_precision_scale(info.typmod) {
            Some((p, s)) if (1..=38).contains(&p) && s >= 0 && s <= p => {
                let (precision, scale) = (p as u8, s as u8);
                lossless(
                    Direct,
                    Decode::Decimal { precision, scale },
                    DataType::Decimal128(precision, scale as i8),
                    true,
                )
            }
            // Unconstrained, oversized, or negative-scale numeric: canonical
            // text — no precision loss, ever [documented-lossy].
            _ => text_policy(CastText),
        },
        oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME => {
            lossless(Direct, Decode::Utf8, DataType::Utf8, true)
        }
        oid::BYTEA => lossless(Direct, Decode::Bytea, DataType::Binary, false),
        oid::TIMESTAMPTZ => lossless(
            Direct,
            Decode::Timestamp { tz: true },
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        oid::TIMESTAMP => lossless(
            Direct,
            Decode::Timestamp { tz: false },
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        oid::DATE => lossless(Direct, Decode::Date, DataType::Date32, true),
        oid::TIME => lossless(
            Direct,
            Decode::Time,
            DataType::Time64(TimeUnit::Microsecond),
            true,
        ),
        // uuid: 36-char lowercase canonical text (structured-path constraint —
        // Arrow carries no uuid type). Lossless in value, cursor-capable.
        oid::UUID => lossless(Direct, Decode::UuidText, DataType::Utf8, true),
        // json arrives as its text; jsonb carries a leading version byte.
        oid::JSON => lossless(Direct, Decode::Utf8, DataType::Utf8, false),
        oid::JSONB => lossless(Direct, Decode::JsonbText, DataType::Utf8, false),
        // Everything else — interval, timetz, money, inet/cidr/macaddr,
        // xml, tsvector, …: the textual fallback [documented-lossy].
        _ => text_policy(CastText),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base(oid: u32, typmod: i32) -> PgTypeInfo {
        PgTypeInfo {
            oid,
            typtype: 'b',
            typcategory: 'X',
            typmod,
        }
    }

    // Contract "Scalar mappings (lossless)" — one assertion per row.
    #[test]
    fn lossless_rows() {
        let cases: &[(u32, DataType, Decode)] = &[
            (oid::BOOL, DataType::Boolean, Decode::Bool),
            (oid::INT2, DataType::Int64, Decode::Int2),
            (oid::INT4, DataType::Int64, Decode::Int4),
            (oid::INT8, DataType::Int64, Decode::Int8),
            (oid::FLOAT4, DataType::Float64, Decode::Float4),
            (oid::FLOAT8, DataType::Float64, Decode::Float8),
            (oid::TEXT, DataType::Utf8, Decode::Utf8),
            (oid::VARCHAR, DataType::Utf8, Decode::Utf8),
            (oid::BPCHAR, DataType::Utf8, Decode::Utf8),
            (oid::NAME, DataType::Utf8, Decode::Utf8),
            (oid::BYTEA, DataType::Binary, Decode::Bytea),
            (oid::DATE, DataType::Date32, Decode::Date),
            (
                oid::TIME,
                DataType::Time64(TimeUnit::Microsecond),
                Decode::Time,
            ),
            (oid::UUID, DataType::Utf8, Decode::UuidText),
            (oid::JSON, DataType::Utf8, Decode::Utf8),
            (oid::JSONB, DataType::Utf8, Decode::JsonbText),
        ];
        for (o, arrow, decode) in cases {
            let m = map_type(&base(*o, -1));
            assert_eq!(&m.arrow, arrow, "oid {o}");
            assert_eq!(&m.decode, decode, "oid {o}");
            assert_eq!(m.select, SelectPolicy::Direct, "oid {o}");
            assert!(!m.documented_lossy, "oid {o}");
        }
        let tz = map_type(&base(oid::TIMESTAMPTZ, -1));
        assert_eq!(
            tz.arrow,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        let naive = map_type(&base(oid::TIMESTAMP, -1));
        assert_eq!(naive.arrow, DataType::Timestamp(TimeUnit::Microsecond, None));
    }

    #[test]
    fn numeric_constrained_is_decimal() {
        // numeric(10,2): typmod = ((10 << 16) | 2) + 4
        let typmod = ((10 << 16) | 2) + 4;
        let m = map_type(&base(oid::NUMERIC, typmod));
        assert_eq!(m.arrow, DataType::Decimal128(10, 2));
        assert_eq!(
            m.decode,
            Decode::Decimal {
                precision: 10,
                scale: 2
            }
        );
        assert!(m.cursor_capable);
    }

    // Contract "Policy mappings" — representation changes, values survive.
    #[test]
    fn policy_rows() {
        // Unconstrained numeric → text.
        let m = map_type(&base(oid::NUMERIC, -1));
        assert_eq!(m.select, SelectPolicy::CastText);
        assert!(m.documented_lossy);
        // Oversized precision numeric(50, 2) → text.
        let typmod = ((50 << 16) | 2) + 4;
        assert_eq!(map_type(&base(oid::NUMERIC, typmod)).select, SelectPolicy::CastText);
        // Negative scale (PG15+) → text.
        let typmod = ((10 << 16) | ((-2i32) & 0x7FF)) + 4;
        assert_eq!(map_type(&base(oid::NUMERIC, typmod)).select, SelectPolicy::CastText);
        // Enum → label text.
        let e = map_type(&PgTypeInfo { oid: 99_999, typtype: 'e', typcategory: 'E', typmod: -1 });
        assert_eq!(e.select, SelectPolicy::CastText);
        // Array → canonical JSON text.
        let a = map_type(&PgTypeInfo { oid: 1007, typtype: 'b', typcategory: 'A', typmod: -1 });
        assert_eq!(a.select, SelectPolicy::CastJsonbText);
        // Composite / range / multirange → canonical JSON text.
        for tt in ['c', 'r', 'm'] {
            let m = map_type(&PgTypeInfo { oid: 88_888, typtype: tt, typcategory: 'C', typmod: -1 });
            assert_eq!(m.select, SelectPolicy::CastJsonbText, "typtype {tt}");
        }
        // Textual fallback: interval (1186), money (790), inet (869), timetz (1266).
        for o in [1186, 790, 869, 1266] {
            let m = map_type(&base(o, -1));
            assert_eq!(m.select, SelectPolicy::CastText, "oid {o}");
            assert_eq!(m.arrow, DataType::Utf8, "oid {o}");
            assert!(m.documented_lossy, "oid {o}");
        }
    }

    #[test]
    fn cursor_capability_matches_contract() {
        for (o, capable) in [
            (oid::INT8, true),
            (oid::TEXT, true),
            (oid::UUID, true),
            (oid::TIMESTAMPTZ, true),
            (oid::DATE, true),
            (oid::TIME, true),
            (oid::BOOL, false),
            (oid::FLOAT8, false),
            (oid::BYTEA, false),
            (oid::JSONB, false),
        ] {
            assert_eq!(map_type(&base(o, -1)).cursor_capable, capable, "oid {o}");
        }
    }
}
