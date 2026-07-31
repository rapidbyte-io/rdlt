//! THE Postgres type rulebook — one closed [`Kind`] vocabulary, dispatched
//! over exhaustively by every face of the crate that touches a value:
//!
//! - [`map`] — catalog type (+ optional hint) → [`Kind`] and its projection
//! - [`binary`] — COPY BINARY wire bytes → values (and the stream decoder)
//! - [`text`] — Postgres TEXT output forms → values (CDC tuples, literals)
//! - [`literal`] — values → injection-safe SQL literals (resume predicates)
//! - [`builder`] — the one Arrow column builder both input faces feed
//!
//! A new kind is therefore a compiler-forced edit in every face: no face
//! carries a `_` arm over [`Kind`], so adding a variant refuses to compile
//! until every conversion says what it does.

pub(crate) mod binary;
pub(crate) mod builder;
#[cfg(feature = "destination")]
pub(crate) mod encode;
pub(crate) mod literal;
pub(crate) mod map;
pub(crate) mod text;

use arrow_schema::{DataType, TimeUnit};

/// How a value travels and lands: the closed decode vocabulary. Every
/// reflected or hinted column resolves to exactly one kind; the wire
/// decoder, the text parser, the literal renderer and the Arrow builder all
/// dispatch over it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Bool,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    /// NBASE-10000 numeric → i128 at the declared precision/scale.
    Decimal {
        precision: u8,
        scale: u8,
    },
    /// UTF-8 text payload (text family, json, every `::text` projection).
    Text,
    /// jsonb payload: one version byte on the wire, then UTF-8 JSON text.
    Jsonb,
    /// 16 wire bytes → 36-char lowercase canonical text (Arrow carries no
    /// uuid type; the structured path derives logical types from Arrow).
    Uuid,
    Bytea,
    /// µs since epoch, UTC-labeled.
    TimestampTz,
    /// µs since epoch, no zone.
    TimestampNaive,
    /// Days since epoch.
    Date,
    /// µs since midnight.
    Time,
}

impl Kind {
    /// The Arrow type this kind lands as — total, so schema assembly can
    /// never disagree with decoding.
    pub(crate) fn arrow(&self) -> DataType {
        match self {
            Kind::Bool => DataType::Boolean,
            Kind::Int16 | Kind::Int32 | Kind::Int64 => DataType::Int64,
            Kind::Float32 | Kind::Float64 => DataType::Float64,
            Kind::Decimal { precision, scale } => DataType::Decimal128(*precision, *scale as i8),
            Kind::Text | Kind::Jsonb | Kind::Uuid => DataType::Utf8,
            Kind::Bytea => DataType::Binary,
            Kind::TimestampTz => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            Kind::TimestampNaive => DataType::Timestamp(TimeUnit::Microsecond, None),
            Kind::Date => DataType::Date32,
            Kind::Time => DataType::Time64(TimeUnit::Microsecond),
        }
    }
}

/// One column as the decode faces see it: its name, its kind, and whether
/// the source declares it NOT NULL (a NULL on such a column is schema
/// drift, refused rather than nulled).
#[derive(Debug, Clone)]
pub(crate) struct Column {
    pub(crate) name: String,
    pub(crate) kind: Kind,
    pub(crate) not_null: bool,
}

/// A typed scalar value — the vocabulary cursors and predicates speak.
/// Ordering is the cursor ordering (same-variant comparisons only; the
/// column type is fixed per stream).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Scalar {
    /// int2/int4/int8.
    Int(i64),
    /// Constrained numeric: the scale-preserved scaled integer + its scale.
    Decimal { scaled: i128, scale: u8 },
    /// Text family.
    Text(String),
    /// Canonical lowercase-hex uuid text (byte order == PG uuid order).
    Uuid(String),
    /// µs since Unix epoch, UTC.
    TimestampTz(i64),
    /// µs since Unix epoch, no zone.
    TimestampNaive(i64),
    /// Days since Unix epoch.
    Date(i32),
    /// µs since midnight.
    Time(i64),
}
