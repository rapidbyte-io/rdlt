//! Drawn values as Arrow arrays, in any of the encodings a source may send a logical type in.

use std::sync::Arc;

use arrow_array::builder::{BinaryBuilder, StringBuilder};
use arrow_array::types::Int32Type;
use arrow_array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Decimal256Array, DictionaryArray,
    DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
    DurationSecondArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, ListArray, MapArray, RunArray, StructArray, Time32MillisecondArray,
    Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, new_null_array,
};
use arrow_buffer::{NullBuffer, OffsetBuffer, i256};
use arrow_schema::{DataType, Field as ArrowField, Fields as ArrowFields};
use rdlt_connector::{LogicalType, TimeUnit};

use super::Scalar;

/// The Arrow field metadata key naming an extension type.
const EXTENSION_NAME: &str = "ARROW:extension:name";

/// How a column's values travel in Arrow, beside the plain type of their logical type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Encoding {
    Plain,
    /// The unsigned integer type one size down: `UInt8` for `Int16` up to `UInt64` for
    /// `Decimal(20, 0)`.
    Unsigned,
    /// `Float16` for `Float32`.
    Half,
    Decimal32,
    Decimal64,
    Decimal256,
    /// `LargeUtf8`, `LargeBinary` or `LargeList`.
    Large,
    /// `Utf8View`, `BinaryView` or `ListView`.
    View,
    /// `LargeListView`.
    LargeView,
    Dictionary,
    RunEnd,
    /// `FixedSizeBinary` or `FixedSizeList` of this size.
    FixedSize(i32),
    Date64,
    /// A map of a list of key and value structs.
    Map,
}

/// A type as a batch's column holds it: its logical type, its encoding and, for structs and
/// lists, its fields' or item's.
#[derive(Clone, Debug)]
pub(crate) struct Shape {
    pub(crate) logical: LogicalType,
    pub(crate) encoding: Encoding,
    pub(crate) children: Vec<Shape>,
}

/// The Arrow field `name` of `shape` holding `array`: its extension type named where it has one.
#[expect(clippy::disallowed_types, reason = "Arrow field metadata is a HashMap")]
pub(crate) fn field(name: &str, shape: &Shape, array: &ArrayRef, nullable: bool) -> ArrowField {
    let field = ArrowField::new(name, array.data_type().clone(), nullable);
    let extension = match shape.logical {
        LogicalType::Uuid => Some("arrow.uuid"),
        LogicalType::Json => Some("arrow.json"),
        _ => None,
    };
    match extension {
        Some(extension) => field.with_metadata(std::collections::HashMap::from([(
            EXTENSION_NAME.to_owned(),
            extension.to_owned(),
        )])),
        None => field,
    }
}

/// `values`, of `shape`, as an Arrow array in its encoding.
pub(crate) fn array(shape: &Shape, values: &[&Scalar]) -> ArrayRef {
    let plain = plain(shape, values);
    encode(shape, plain)
}

/// Which of `values` are present.
fn present(values: &[&Scalar]) -> NullBuffer {
    NullBuffer::from_iter(values.iter().map(|value| **value != Scalar::Null))
}

/// `values` of `shape` in the plain Arrow type of its logical type, its children in theirs.
fn plain(shape: &Shape, values: &[&Scalar]) -> ArrayRef {
    use LogicalType as T;
    match &shape.logical {
        T::Null => new_null_array(&DataType::Null, values.len()),
        T::Bool => Arc::new(BooleanArray::from_iter(values.iter().map(
            |value| match value {
                Scalar::Bool(value) => Some(*value),
                _ => None,
            },
        ))),
        T::Int8 | T::Int16 | T::Int32 | T::Int64 => integers(&shape.logical, values),
        T::Float32 => Arc::new(Float32Array::from_iter(values.iter().map(
            |value| match value {
                Scalar::Float32(value) => Some(*value),
                _ => None,
            },
        ))),
        T::Float64 => Arc::new(Float64Array::from_iter(values.iter().map(
            |value| match value {
                Scalar::Float64(value) => Some(*value),
                _ => None,
            },
        ))),
        T::Decimal(decimal) => decimals(decimal.precision(), decimal.scale(), values),
        T::Utf8 | T::Json => strings(values),
        T::Binary => {
            let mut builder = BinaryBuilder::new();
            for value in values {
                match value {
                    Scalar::Binary(bytes) => builder.append_value(bytes),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        T::Date | T::Time(_) | T::Timestamp(..) | T::Duration(_) => temporal(shape, values),
        T::Uuid => Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                values.iter().map(|value| match value {
                    Scalar::Uuid(bytes) => Some(bytes.to_vec()),
                    _ => None,
                }),
                16,
            )
            .expect("16-byte values"),
        ),
        T::Struct(_) => structure(shape, values),
        T::List(_) => list(shape, values),
    }
}

/// `values` of `logical`, an integer type.
fn integers(logical: &LogicalType, values: &[&Scalar]) -> ArrayRef {
    use LogicalType as T;
    let ints = || {
        values.iter().map(|value| match value {
            Scalar::Int(value) => Some(*value),
            _ => None,
        })
    };
    let narrow = |value: i64| i32::try_from(value).expect("a drawn value in range");
    match logical {
        T::Int8 => {
            Arc::new(Int8Array::from_iter(ints().map(|value| {
                value.map(|value| i8::try_from(value).expect("in range"))
            })))
        }
        T::Int16 => {
            Arc::new(Int16Array::from_iter(ints().map(|value| {
                value.map(|value| i16::try_from(value).expect("in range"))
            })))
        }
        T::Int32 => Arc::new(Int32Array::from_iter(ints().map(|value| value.map(narrow)))),
        T::Int64 => Arc::new(Int64Array::from_iter(ints())),
        other => unreachable!("{other} is not an integer type"),
    }
}

/// `values` of `shape`, a date, time, timestamp or duration.
fn temporal(shape: &Shape, values: &[&Scalar]) -> ArrayRef {
    use LogicalType as T;
    let temporals = || {
        values.iter().map(|value| match value {
            Scalar::Temporal(value) => Some(*value),
            _ => None,
        })
    };
    let narrow = |value: i64| i32::try_from(value).expect("a drawn value in range");
    match &shape.logical {
        T::Date => Arc::new(Date32Array::from_iter(values.iter().map(
            |value| match value {
                Scalar::Date(days) => Some(*days),
                _ => None,
            },
        ))),
        T::Time(TimeUnit::Second) => Arc::new(Time32SecondArray::from_iter(
            temporals().map(|value| value.map(narrow)),
        )),
        T::Time(TimeUnit::Millisecond) => Arc::new(Time32MillisecondArray::from_iter(
            temporals().map(|value| value.map(narrow)),
        )),
        T::Time(TimeUnit::Microsecond) => Arc::new(Time64MicrosecondArray::from_iter(temporals())),
        T::Time(TimeUnit::Nanosecond) => Arc::new(Time64NanosecondArray::from_iter(temporals())),
        T::Timestamp(unit, zone) => timestamps(*unit, zone.clone(), temporals().collect()),
        T::Duration(unit) => durations(*unit, temporals().collect()),
        other => unreachable!("{other} is not temporal"),
    }
}

fn strings(values: &[&Scalar]) -> ArrayRef {
    let mut builder = StringBuilder::new();
    for value in values {
        match value {
            Scalar::Utf8(text) => builder.append_value(text),
            Scalar::Json(json) => builder.append_value(json.to_string()),
            _ => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}

fn decimals(precision: u8, scale: u8, values: &[&Scalar]) -> ArrayRef {
    let digits = values.iter().map(|value| match value {
        Scalar::Decimal(digits) => Some(digits.as_str()),
        _ => None,
    });
    let scale = i8::try_from(scale).expect("a small scale");
    if precision <= 38 {
        let values =
            digits.map(|digits| digits.map(|digits| digits.parse::<i128>().expect("digits")));
        Arc::new(
            Decimal128Array::from_iter(values)
                .with_precision_and_scale(precision, scale)
                .expect("a valid decimal type"),
        )
    } else {
        let values =
            digits.map(|digits| digits.map(|digits| i256::from_string(digits).expect("digits")));
        Arc::new(
            Decimal256Array::from_iter(values)
                .with_precision_and_scale(precision, scale)
                .expect("a valid decimal type"),
        )
    }
}

fn timestamps(unit: TimeUnit, zone: Option<Arc<str>>, values: Vec<Option<i64>>) -> ArrayRef {
    match unit {
        TimeUnit::Second => Arc::new(TimestampSecondArray::from(values).with_timezone_opt(zone)),
        TimeUnit::Millisecond => {
            Arc::new(TimestampMillisecondArray::from(values).with_timezone_opt(zone))
        }
        TimeUnit::Microsecond => {
            Arc::new(TimestampMicrosecondArray::from(values).with_timezone_opt(zone))
        }
        TimeUnit::Nanosecond => {
            Arc::new(TimestampNanosecondArray::from(values).with_timezone_opt(zone))
        }
    }
}

fn durations(unit: TimeUnit, values: Vec<Option<i64>>) -> ArrayRef {
    match unit {
        TimeUnit::Second => Arc::new(DurationSecondArray::from(values)),
        TimeUnit::Millisecond => Arc::new(DurationMillisecondArray::from(values)),
        TimeUnit::Microsecond => Arc::new(DurationMicrosecondArray::from(values)),
        TimeUnit::Nanosecond => Arc::new(DurationNanosecondArray::from(values)),
    }
}

/// `values`, structs of `shape`'s fields, as a struct array.
fn structure(shape: &Shape, values: &[&Scalar]) -> ArrayRef {
    let LogicalType::Struct(fields) = &shape.logical else {
        unreachable!("a struct shape")
    };
    let (fields, children): (Vec<ArrowField>, Vec<ArrayRef>) = fields
        .iter()
        .zip(&shape.children)
        .map(|(field, child)| {
            let inner: Vec<&Scalar> = values
                .iter()
                .map(|value| match value {
                    Scalar::Struct(members) => members
                        .iter()
                        .find(|(name, _)| name == field.name())
                        .map_or(&Scalar::Null, |(_, inner)| inner),
                    _ => &Scalar::Null,
                })
                .collect();
            let array = array(child, &inner);
            (
                super::arrays::field(field.name(), child, &array, field.is_nullable()),
                array,
            )
        })
        .unzip();
    Arc::new(StructArray::new(
        ArrowFields::from(fields),
        children,
        Some(present(values)),
    ))
}

/// `values`, lists of `shape`'s item, as a list array.
fn list(shape: &Shape, values: &[&Scalar]) -> ArrayRef {
    let LogicalType::List(item) = &shape.logical else {
        unreachable!("a list shape")
    };
    let items: Vec<&[Scalar]> = values
        .iter()
        .map(|value| match value {
            Scalar::List(items) => items.as_slice(),
            _ => &[],
        })
        .collect();
    let offsets = OffsetBuffer::from_lengths(items.iter().map(|items| items.len()));
    let flat: Vec<&Scalar> = items.iter().flat_map(|items| items.iter()).collect();
    let child = &shape.children[0];
    let inner = array(child, &flat);
    let field = field(item.name(), child, &inner, item.is_nullable());
    Arc::new(ListArray::new(
        Arc::new(field),
        offsets,
        inner,
        Some(present(values)),
    ))
}

/// `plain`, of `shape`, in `shape`'s encoding.
fn encode(shape: &Shape, plain: ArrayRef) -> ArrayRef {
    let cast = |to: DataType| arrow_cast::cast(&plain, &to).expect("a supported encoding");
    let list_field = || match plain.data_type() {
        DataType::List(field) => Arc::clone(field),
        other => unreachable!("a list encoding of {other}"),
    };
    match (shape.encoding, &shape.logical) {
        (Encoding::Plain, _) => plain,
        (Encoding::Unsigned, logical) => cast(match logical {
            LogicalType::Int16 => DataType::UInt8,
            LogicalType::Int32 => DataType::UInt16,
            LogicalType::Int64 => DataType::UInt32,
            _ => DataType::UInt64,
        }),
        (Encoding::Half, _) => cast(DataType::Float16),
        (Encoding::Decimal32, LogicalType::Decimal(d)) => cast(DataType::Decimal32(
            d.precision(),
            i8::try_from(d.scale()).expect("scale"),
        )),
        (Encoding::Decimal64, LogicalType::Decimal(d)) => cast(DataType::Decimal64(
            d.precision(),
            i8::try_from(d.scale()).expect("scale"),
        )),
        (Encoding::Decimal256, LogicalType::Decimal(d)) => cast(DataType::Decimal256(
            d.precision(),
            i8::try_from(d.scale()).expect("scale"),
        )),
        (Encoding::Large, LogicalType::Utf8 | LogicalType::Json) => cast(DataType::LargeUtf8),
        (Encoding::Large, LogicalType::Binary) => cast(DataType::LargeBinary),
        (Encoding::Large, LogicalType::List(_)) => cast(DataType::LargeList(list_field())),
        (Encoding::View, LogicalType::Utf8 | LogicalType::Json) => cast(DataType::Utf8View),
        (Encoding::View, LogicalType::Binary) => cast(DataType::BinaryView),
        (Encoding::View, LogicalType::List(_)) => cast(DataType::ListView(list_field())),
        (Encoding::LargeView, _) => cast(DataType::LargeListView(list_field())),
        (Encoding::FixedSize(size), LogicalType::Binary) => cast(DataType::FixedSizeBinary(size)),
        (Encoding::FixedSize(size), _) => cast(DataType::FixedSizeList(list_field(), size)),
        (Encoding::Date64, _) => cast(DataType::Date64),
        (Encoding::Dictionary, _) => dictionary(plain),
        (Encoding::RunEnd, _) => run_ends(&plain),
        (Encoding::Map, _) => map(&plain),
        (encoding, logical) => unreachable!("no {encoding:?} encoding of {logical}"),
    }
}

/// `values` as a dictionary with one key per row, null where the value is.
fn dictionary(values: ArrayRef) -> ArrayRef {
    let keys = Int32Array::from_iter((0..values.len()).map(|row| {
        values
            .is_valid(row)
            .then(|| i32::try_from(row).expect("a short batch"))
    }));
    Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).expect("valid keys"))
}

/// `values` run-end encoded, one run per row.
fn run_ends(values: &ArrayRef) -> ArrayRef {
    let ends = Int32Array::from_iter_values(
        (1..=values.len()).map(|end| i32::try_from(end).expect("a short batch")),
    );
    Arc::new(RunArray::<Int32Type>::try_new(&ends, values).expect("valid run ends"))
}

/// `list`, a list of key and value structs, as a map.
fn map(list: &ArrayRef) -> ArrayRef {
    let list = list.as_any().downcast_ref::<ListArray>().expect("a list");
    let entries = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("entries")
        .clone();
    let field = Arc::new(ArrowField::new(
        "entries",
        entries.data_type().clone(),
        false,
    ));
    Arc::new(
        MapArray::try_new(
            field,
            list.offsets().clone(),
            entries,
            list.nulls().cloned(),
            false,
        )
        .expect("a valid map"),
    )
}
