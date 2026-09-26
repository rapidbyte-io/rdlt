//! Logical types, schemas, identifiers, cursors and times on the wire.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::{Invalid, narrow, required, v1};
use crate::cursor::Cursor;
use crate::id::{StreamName, TablePath};
use crate::limits::MAX_NESTING_DEPTH;
use crate::schema::{ColumnPath, TableSchema};
use crate::types::{DecimalType, Field, Fields, LogicalType, TimeUnit, TypeKind};

impl From<TimeUnit> for v1::TimeUnit {
    fn from(unit: TimeUnit) -> Self {
        match unit {
            TimeUnit::Second => Self::Second,
            TimeUnit::Millisecond => Self::Millisecond,
            TimeUnit::Microsecond => Self::Microsecond,
            TimeUnit::Nanosecond => Self::Nanosecond,
        }
    }
}

/// The time unit `value` names.
pub(super) fn time_unit(value: i32) -> Result<TimeUnit, Invalid> {
    match v1::TimeUnit::try_from(value) {
        Ok(v1::TimeUnit::Second) => Ok(TimeUnit::Second),
        Ok(v1::TimeUnit::Millisecond) => Ok(TimeUnit::Millisecond),
        Ok(v1::TimeUnit::Microsecond) => Ok(TimeUnit::Microsecond),
        Ok(v1::TimeUnit::Nanosecond) => Ok(TimeUnit::Nanosecond),
        Ok(v1::TimeUnit::Unspecified) | Err(_) => Err(Invalid::Unknown("time unit")),
    }
}

impl From<&LogicalType> for v1::LogicalType {
    fn from(logical: &LogicalType) -> Self {
        let mut nodes = Vec::new();
        push(logical, "", false, &mut nodes);
        Self { nodes }
    }
}

/// Pushes the nodes of `logical`, the type of a field `name` that may hold nulls if `nullable`,
/// in pre-order.
fn push(logical: &LogicalType, name: &str, nullable: bool, nodes: &mut Vec<v1::TypeNode>) {
    use v1::type_node::Kind;
    let unit = |unit: &TimeUnit| v1::TimeUnit::from(*unit) as i32;
    let kind = match logical {
        LogicalType::Null => Kind::Null(v1::Unit {}),
        LogicalType::Bool => Kind::Bool(v1::Unit {}),
        LogicalType::Int8 => Kind::Int8(v1::Unit {}),
        LogicalType::Int16 => Kind::Int16(v1::Unit {}),
        LogicalType::Int32 => Kind::Int32(v1::Unit {}),
        LogicalType::Int64 => Kind::Int64(v1::Unit {}),
        LogicalType::Float32 => Kind::Float32(v1::Unit {}),
        LogicalType::Float64 => Kind::Float64(v1::Unit {}),
        LogicalType::Decimal(decimal) => Kind::Decimal(v1::Decimal {
            precision: u32::from(decimal.precision()),
            scale: u32::from(decimal.scale()),
        }),
        LogicalType::Utf8 => Kind::Utf8(v1::Unit {}),
        LogicalType::Binary => Kind::Binary(v1::Unit {}),
        LogicalType::Date => Kind::Date(v1::Unit {}),
        LogicalType::Time(time) => Kind::Time(unit(time)),
        LogicalType::Timestamp(time, zone) => Kind::Timestamp(v1::Timestamp {
            unit: unit(time),
            zone: zone.as_deref().map(ToOwned::to_owned),
        }),
        LogicalType::Duration(time) => Kind::Duration(unit(time)),
        LogicalType::Uuid => Kind::Uuid(v1::Unit {}),
        LogicalType::Json => Kind::Json(v1::Unit {}),
        LogicalType::Struct(fields) => {
            Kind::Struct(u32::try_from(fields.iter().count()).unwrap_or(u32::MAX))
        }
        LogicalType::List(_) => Kind::List(v1::Unit {}),
    };
    nodes.push(v1::TypeNode {
        name: name.to_owned(),
        nullable,
        kind: Some(kind),
    });
    match logical {
        LogicalType::Struct(fields) => {
            for field in fields.iter() {
                push(
                    field.logical_type(),
                    field.name(),
                    field.is_nullable(),
                    nodes,
                );
            }
        }
        LogicalType::List(item) => {
            push(item.logical_type(), item.name(), item.is_nullable(), nodes);
        }
        _ => {}
    }
}

impl TryFrom<v1::LogicalType> for LogicalType {
    type Error = Invalid;

    fn try_from(logical: v1::LogicalType) -> Result<Self, Invalid> {
        let mut nodes = logical.nodes.into_iter();
        let (_, _, decoded) = node(&mut nodes, 1)?;
        if nodes.next().is_some() {
            return Err(Invalid::OutOfRange("type nodes"));
        }
        Ok(decoded)
    }
}

/// The next node's field name, nullability and type, its subtree read from `nodes`; it sits
/// `depth` levels deep, and no node may sit deeper than the nesting limit.
fn node(
    nodes: &mut impl Iterator<Item = v1::TypeNode>,
    depth: u64,
) -> Result<(String, bool, LogicalType), Invalid> {
    use v1::type_node::Kind;
    if depth > MAX_NESTING_DEPTH {
        return Err(Invalid::OutOfRange("nesting depth"));
    }
    let next = required("type node", nodes.next())?;
    let child = |nodes: &mut _| {
        let (name, nullable, logical) = node(nodes, depth + 1)?;
        Ok::<_, Invalid>(Field::new(name, logical, nullable))
    };
    let logical = match required("logical type", next.kind)? {
        Kind::Null(_) => LogicalType::Null,
        Kind::Bool(_) => LogicalType::Bool,
        Kind::Int8(_) => LogicalType::Int8,
        Kind::Int16(_) => LogicalType::Int16,
        Kind::Int32(_) => LogicalType::Int32,
        Kind::Int64(_) => LogicalType::Int64,
        Kind::Float32(_) => LogicalType::Float32,
        Kind::Float64(_) => LogicalType::Float64,
        Kind::Decimal(decimal) => {
            let precision = narrow("decimal precision", decimal.precision)?;
            let scale = narrow("decimal scale", decimal.scale)?;
            let decimal = DecimalType::new(precision, scale)
                .map_err(|error| Invalid::rejected("decimal", error))?;
            LogicalType::Decimal(decimal)
        }
        Kind::Utf8(_) => LogicalType::Utf8,
        Kind::Binary(_) => LogicalType::Binary,
        Kind::Date(_) => LogicalType::Date,
        Kind::Time(unit) => LogicalType::Time(time_unit(unit)?),
        Kind::Timestamp(timestamp) => {
            LogicalType::Timestamp(time_unit(timestamp.unit)?, timestamp.zone.map(Arc::from))
        }
        Kind::Duration(unit) => LogicalType::Duration(time_unit(unit)?),
        Kind::Uuid(_) => LogicalType::Uuid,
        Kind::Json(_) => LogicalType::Json,
        Kind::Struct(count) => {
            let fields = (0..count)
                .map(|_| child(nodes))
                .collect::<Result<Vec<_>, _>>()?;
            let fields =
                Fields::new(fields).map_err(|error| Invalid::rejected("struct fields", error))?;
            LogicalType::Struct(fields)
        }
        Kind::List(_) => LogicalType::List(Box::new(child(nodes)?)),
    };
    Ok((next.name, next.nullable, logical))
}

impl From<&Field> for v1::Field {
    fn from(field: &Field) -> Self {
        Self {
            name: field.name().to_owned(),
            r#type: Some(v1::LogicalType::from(field.logical_type())),
            nullable: field.is_nullable(),
        }
    }
}

impl TryFrom<v1::Field> for Field {
    type Error = Invalid;

    fn try_from(field: v1::Field) -> Result<Self, Invalid> {
        let logical = LogicalType::try_from(required("field type", field.r#type)?)?;
        Ok(Self::new(field.name, logical, field.nullable))
    }
}

impl From<&TableSchema> for v1::TableSchema {
    fn from(schema: &TableSchema) -> Self {
        Self {
            fields: schema.fields().iter().map(v1::Field::from).collect(),
        }
    }
}

impl TryFrom<v1::TableSchema> for TableSchema {
    type Error = Invalid;

    fn try_from(schema: v1::TableSchema) -> Result<Self, Invalid> {
        let fields = schema
            .fields
            .into_iter()
            .map(Field::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Self::new(fields).map_err(|error| Invalid::rejected("table schema", error))
    }
}

impl From<TypeKind> for v1::TypeKind {
    fn from(kind: TypeKind) -> Self {
        match kind {
            TypeKind::Null => Self::Null,
            TypeKind::Bool => Self::Bool,
            TypeKind::Int8 => Self::Int8,
            TypeKind::Int16 => Self::Int16,
            TypeKind::Int32 => Self::Int32,
            TypeKind::Int64 => Self::Int64,
            TypeKind::Float32 => Self::Float32,
            TypeKind::Float64 => Self::Float64,
            TypeKind::Decimal => Self::Decimal,
            TypeKind::Utf8 => Self::Utf8,
            TypeKind::Binary => Self::Binary,
            TypeKind::Date => Self::Date,
            TypeKind::Time => Self::Time,
            TypeKind::Timestamp => Self::Timestamp,
            TypeKind::Duration => Self::Duration,
            TypeKind::Uuid => Self::Uuid,
            TypeKind::Json => Self::Json,
            TypeKind::Struct => Self::Struct,
            TypeKind::List => Self::List,
        }
    }
}

/// The type kind `value` names.
pub(super) fn type_kind(value: i32) -> Result<TypeKind, Invalid> {
    use v1::TypeKind as Wire;
    Ok(
        match Wire::try_from(value).map_err(|_| Invalid::Unknown("type kind"))? {
            Wire::Unspecified => return Err(Invalid::Unknown("type kind")),
            Wire::Null => TypeKind::Null,
            Wire::Bool => TypeKind::Bool,
            Wire::Int8 => TypeKind::Int8,
            Wire::Int16 => TypeKind::Int16,
            Wire::Int32 => TypeKind::Int32,
            Wire::Int64 => TypeKind::Int64,
            Wire::Float32 => TypeKind::Float32,
            Wire::Float64 => TypeKind::Float64,
            Wire::Decimal => TypeKind::Decimal,
            Wire::Utf8 => TypeKind::Utf8,
            Wire::Binary => TypeKind::Binary,
            Wire::Date => TypeKind::Date,
            Wire::Time => TypeKind::Time,
            Wire::Timestamp => TypeKind::Timestamp,
            Wire::Duration => TypeKind::Duration,
            Wire::Uuid => TypeKind::Uuid,
            Wire::Json => TypeKind::Json,
            Wire::Struct => TypeKind::Struct,
            Wire::List => TypeKind::List,
        },
    )
}

impl From<&ColumnPath> for v1::ColumnPath {
    fn from(path: &ColumnPath) -> Self {
        Self {
            segments: path.segments().map(ToOwned::to_owned).collect(),
        }
    }
}

impl TryFrom<v1::ColumnPath> for ColumnPath {
    type Error = Invalid;

    fn try_from(path: v1::ColumnPath) -> Result<Self, Invalid> {
        Self::new(path.segments).map_err(|error| Invalid::rejected("column path", error))
    }
}

impl From<&StreamName> for v1::StreamName {
    fn from(name: &StreamName) -> Self {
        Self {
            namespace: name.namespace().map(ToOwned::to_owned),
            name: name.name().to_owned(),
        }
    }
}

impl TryFrom<v1::StreamName> for StreamName {
    type Error = Invalid;

    fn try_from(name: v1::StreamName) -> Result<Self, Invalid> {
        let parsed = match name.namespace {
            Some(namespace) => Self::with_namespace(namespace, name.name),
            None => Self::new(name.name),
        };
        parsed.map_err(|error| Invalid::rejected("stream name", error))
    }
}

impl From<&TablePath> for v1::TablePath {
    fn from(path: &TablePath) -> Self {
        Self {
            segments: path.segments().map(ToOwned::to_owned).collect(),
        }
    }
}

impl TryFrom<v1::TablePath> for TablePath {
    type Error = Invalid;

    fn try_from(path: v1::TablePath) -> Result<Self, Invalid> {
        Self::new(path.segments).map_err(|error| Invalid::rejected("table path", error))
    }
}

impl From<&Cursor> for v1::Cursor {
    fn from(cursor: &Cursor) -> Self {
        Self {
            version: u32::from(cursor.version()),
            bytes: cursor.bytes().clone(),
        }
    }
}

impl TryFrom<v1::Cursor> for Cursor {
    type Error = Invalid;

    fn try_from(cursor: v1::Cursor) -> Result<Self, Invalid> {
        let version = narrow("cursor version", cursor.version)?;
        Self::new(version, cursor.bytes).map_err(|error| Invalid::rejected("cursor", error))
    }
}

/// `at` on the wire, clamped to the epoch if it is earlier.
pub(super) fn instant(at: SystemTime) -> v1::Instant {
    let since = at.duration_since(UNIX_EPOCH).unwrap_or_default();
    v1::Instant {
        seconds: i64::try_from(since.as_secs()).unwrap_or(i64::MAX),
        nanos: since.subsec_nanos(),
    }
}

/// The time `at` names.
pub(super) fn system_time(at: v1::Instant) -> Result<SystemTime, Invalid> {
    let seconds = u64::try_from(at.seconds).map_err(|_| Invalid::OutOfRange("instant"))?;
    if at.nanos >= 1_000_000_000 {
        return Err(Invalid::OutOfRange("instant nanoseconds"));
    }
    UNIX_EPOCH
        .checked_add(Duration::new(seconds, at.nanos))
        .ok_or(Invalid::OutOfRange("instant"))
}

/// `duration` on the wire.
pub(super) fn duration(duration: Duration) -> v1::Duration {
    v1::Duration {
        seconds: duration.as_secs(),
        nanos: duration.subsec_nanos(),
    }
}

/// The length of time `duration` names.
pub(super) fn std_duration(duration: v1::Duration) -> Result<Duration, Invalid> {
    if duration.nanos >= 1_000_000_000 {
        return Err(Invalid::OutOfRange("duration nanoseconds"));
    }
    Ok(Duration::new(duration.seconds, duration.nanos))
}
