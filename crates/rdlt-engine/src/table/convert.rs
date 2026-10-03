//! Converting arrays between logical types and into the types a destination stores.
//!
//! Every conversion is exact: a column's type is the join of every type it received, so it holds
//! each incoming value as it is, and text or JSON renderings keep every value. A decimal beyond
//! the precision its type declares is no value of that type, and is refused.

mod decode;
mod encoders;

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Arc;

use arrow_array::builder::StringBuilder;
use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, GenericListArray, LargeListArray, ListArray,
    OffsetSizeTrait, StructArray, new_null_array,
};
use arrow_json::writer::{EncoderOptions, make_encoder};
use arrow_schema::{ArrowError, DataType, FieldRef};
use rdlt_connector::{Field, LogicalType, instants};

use super::temporal;
use encoders::Extensions;

/// The Arrow field metadata key naming an extension type.
const EXTENSION_NAME: &str = "ARROW:extension:name";

/// `array`, holding values of `from`, as `to`, which holds every value of `from`.
pub(crate) fn convert(
    array: &ArrayRef,
    from: &LogicalType,
    to: &LogicalType,
) -> Result<ArrayRef, ArrowError> {
    let array = &decoded(array)?;
    precise(array)?;
    if from == to {
        return normalize(array, to);
    }
    // A `Null` array has no validity buffer, so encoders would read its values as present.
    if *from == LogicalType::Null {
        return Ok(new_null_array(&to.to_arrow(), array.len()));
    }
    refuse_rounding(array, from, to)?;
    match (from, to) {
        (_, LogicalType::Json) => json(array, from),
        // Every temporal widening keeps each value the instant, time or duration it was.
        (LogicalType::Date | LogicalType::Timestamp(..), LogicalType::Timestamp(..))
        | (LogicalType::Time(_), LogicalType::Time(_))
        | (LogicalType::Duration(_), LogicalType::Duration(_)) => {
            instants::widened(array, &to.to_arrow())
        }
        // Nested dates stay wide until each meets its own target: a far `Date64` a `Date` column
        // refuses, JSON, text and timestamps hold.
        (LogicalType::Struct(from_fields), LogicalType::Struct(to_fields)) => {
            let source = normalize_to(array, &wide_dates(&from.to_arrow()))?;
            let source = source.as_struct();
            let arriving: BTreeMap<&str, (usize, &LogicalType)> = from_fields
                .iter()
                .enumerate()
                .map(|(index, field)| (field.name(), (index, field.logical_type())))
                .collect();
            let columns = to_fields
                .iter()
                .map(|field| match arriving.get(field.name()) {
                    Some((index, logical)) => {
                        convert(source.column(*index), logical, field.logical_type())
                    }
                    None => Ok(new_null_array(
                        &field.logical_type().to_arrow(),
                        source.len(),
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?;
            let DataType::Struct(fields) = to.to_arrow() else {
                unreachable!("struct types are Arrow structs")
            };
            Ok(Arc::new(StructArray::try_new(
                fields,
                columns,
                source.nulls().cloned(),
            )?))
        }
        (LogicalType::List(from_item), LogicalType::List(to_item)) => {
            let source = normalize_to(array, &wide_dates(&from.to_arrow()))?;
            list(source.as_list::<i32>(), from_item, to_item)
        }
        _ => cast(array, &to.to_arrow()),
    }
}

/// `array` in the plain Arrow type of `logical`: large, view and dictionary encodings, maps and
/// wider integer storage come out as the type the logical type names, and a `Date64` as the
/// days it is within.
pub(crate) fn normalize(array: &ArrayRef, logical: &LogicalType) -> Result<ArrayRef, ArrowError> {
    normalize_to(&temporal::dated(&unmapped(array)?)?, &logical.to_arrow())
}

/// `array` cast to `target`, with its maps unmapped first.
fn normalize_to(array: &ArrayRef, target: &DataType) -> Result<ArrayRef, ArrowError> {
    if array.data_type() == target {
        return Ok(Arc::clone(array));
    }
    cast(&unmapped(array)?, target)
}

/// `data_type` with its dates, at any depth, in a `Date64`, which holds every day a `Date32`
/// holds and the far ones only it does.
fn wide_dates(data_type: &DataType) -> DataType {
    let field = |field: &FieldRef| {
        Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(wide_dates(field.data_type())),
        )
    };
    match data_type {
        DataType::Date32 => DataType::Date64,
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(field).collect()),
        DataType::List(item) => DataType::List(field(item)),
        other => other.clone(),
    }
}

/// `array` holding only what its rows name: every dictionary and run-end encoding in it, at
/// any depth, decoded, every list view a list, and every list's items the ones its rows hold.
///
/// Arrow converts a nested array's items whether or not a row names them, and a dictionary's
/// values whether or not a key does, so what follows would cost what the array keeps alive, not
/// what its rows hold. Filtering rows out of an encoding also leaves their values in it, and a
/// value no row holds must not fail a conversion.
pub(crate) fn decoded(array: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    decode::decoded(array)
}

/// `array` with every map in it, at any depth, as the list of key and value structs it holds,
/// which Arrow casts where it casts no map; an array holding no map is returned as it is.
fn unmapped(array: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    Ok(match array.data_type() {
        DataType::Map(field, _) => {
            let map = array.as_map();
            let entries = unmapped(&(Arc::new(map.entries().clone()) as ArrayRef))?;
            let field = retyped(field, &entries);
            Arc::new(ListArray::try_new(
                field,
                map.offsets().clone(),
                entries,
                map.nulls().cloned(),
            )?)
        }
        DataType::Struct(fields) => {
            let array_of = array.as_struct();
            let columns = array_of
                .columns()
                .iter()
                .map(unmapped)
                .collect::<Result<Vec<_>, _>>()?;
            if columns
                .iter()
                .zip(array_of.columns())
                .all(|(new, old)| Arc::ptr_eq(new, old))
            {
                return Ok(Arc::clone(array));
            }
            let fields = fields
                .iter()
                .zip(&columns)
                .map(|(field, column)| retyped(field, column))
                .collect();
            Arc::new(StructArray::try_new(
                fields,
                columns,
                array_of.nulls().cloned(),
            )?)
        }
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(..) => lists(array)?,
        // Views of lists become plain lists first.
        DataType::ListView(field) => unmapped(&cast(array, &DataType::List(Arc::clone(field)))?)?,
        DataType::LargeListView(field) => {
            unmapped(&cast(array, &DataType::LargeList(Arc::clone(field)))?)?
        }
        _ => Arc::clone(array),
    })
}

/// `field` holding `values`' type.
pub(super) fn retyped(field: &FieldRef, values: &ArrayRef) -> FieldRef {
    Arc::new(
        field
            .as_ref()
            .clone()
            .with_data_type(values.data_type().clone()),
    )
}

/// `array`, a list, large list or fixed-size list, with every map in its items unmapped.
fn lists(array: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    let (field, old) = match array.data_type() {
        DataType::List(field) => (field, array.as_list::<i32>().values()),
        DataType::LargeList(field) => (field, array.as_list::<i64>().values()),
        DataType::FixedSizeList(field, _) => (field, array.as_fixed_size_list().values()),
        other => unreachable!("{other} is not a list"),
    };
    let values = unmapped(old)?;
    if Arc::ptr_eq(&values, old) {
        return Ok(Arc::clone(array));
    }
    let field = retyped(field, &values);
    let nulls = array.nulls().cloned();
    Ok(match array.data_type() {
        DataType::List(_) => {
            let offsets = array.as_list::<i32>().offsets().clone();
            Arc::new(ListArray::try_new(field, offsets, values, nulls)?)
        }
        DataType::LargeList(_) => {
            let offsets = array.as_list::<i64>().offsets().clone();
            Arc::new(LargeListArray::try_new(field, offsets, values, nulls)?)
        }
        _ => {
            let size = array.as_fixed_size_list().value_length();
            Arc::new(FixedSizeListArray::try_new(field, size, values, nulls)?)
        }
    })
}

/// Refuses converting `array`, of `from`, to `to` where that rounds: a plan casts 64-bit integers
/// to 64-bit floats only for batches a float holds exactly, so any other is refused, not rounded.
fn refuse_rounding(
    array: &ArrayRef,
    from: &LogicalType,
    to: &LogicalType,
) -> Result<(), ArrowError> {
    let floats = (from, to) == (&LogicalType::Int64, &LogicalType::Float64);
    if floats && super::exact::rounds(array) {
        return Err(ArrowError::CastError(
            "an integer a 64-bit float would round".to_owned(),
        ));
    }
    Ok(())
}

/// `array` as `target`, failing on any value `target` cannot represent instead of nulling it.
fn cast(array: &ArrayRef, target: &DataType) -> Result<ArrayRef, ArrowError> {
    let options = arrow_cast::CastOptions {
        safe: false,
        ..arrow_cast::CastOptions::default()
    };
    arrow_cast::cast_with_options(array, target, &options)
}

fn list<O: OffsetSizeTrait>(
    source: &GenericListArray<O>,
    from: &Field,
    to: &Field,
) -> Result<ArrayRef, ArrowError> {
    let values = convert(source.values(), from.logical_type(), to.logical_type())?;
    let DataType::List(field) = LogicalType::List(Box::new(to.clone())).to_arrow() else {
        unreachable!("list types are Arrow lists")
    };
    Ok(Arc::new(GenericListArray::<O>::try_new(
        field,
        source.offsets().clone(),
        values,
        source.nulls().cloned(),
    )?))
}

/// Each value of `array`, of `logical`, as JSON text; nulls stay null.
pub(crate) fn json(array: &ArrayRef, logical: &LogicalType) -> Result<ArrayRef, ArrowError> {
    let array = &decoded(array)?;
    precise(array)?;
    if *logical == LogicalType::Json {
        return normalize(array, logical);
    }
    // Dates go to JSON as `Date64`s, so a far one a `Date32` cannot hold is written too.
    let target = wide_dates(&logical.to_arrow());
    let field: FieldRef = Arc::new(
        Field::new("value", logical.clone(), true)
            .to_arrow()
            .with_data_type(target.clone()),
    );
    let array = normalize_to(array, &target)?;
    let options = EncoderOptions::default()
        .with_explicit_nulls(true)
        .with_encoder_factory(Arc::new(Extensions));
    let mut encoder = make_encoder(&field, array.as_ref(), &options)?;
    // Sized by what the cost model says the text takes at most, which the piece was charged.
    let mut builder = StringBuilder::with_capacity(array.len(), text_capacity(&array, true));
    let mut buffer = Vec::new();
    for row in 0..array.len() {
        if array.is_null(row) {
            builder.append_null();
            continue;
        }
        buffer.clear();
        encoder.encode(row, &mut buffer);
        builder.append_value(String::from_utf8_lossy(&buffer));
    }
    Ok(Arc::new(builder.finish()))
}

/// Each value of `array`, of `logical`, as text: UUIDs hyphenated, bytes in lower-case hex,
/// nested values and JSON as JSON text, anything else as Arrow renders it, or exactly where
/// Arrow cannot.
pub(crate) fn text(array: &ArrayRef, logical: &LogicalType) -> Result<ArrayRef, ArrowError> {
    let array = &decoded(array)?;
    precise(array)?;
    match logical {
        LogicalType::Struct(_) | LogicalType::List(_) | LogicalType::Json => json(array, logical),
        LogicalType::Uuid | LogicalType::Binary => {
            let array = normalize(array, logical)?;
            let mut builder =
                StringBuilder::with_capacity(array.len(), text_capacity(&array, false));
            for row in 0..array.len() {
                if array.is_null(row) {
                    builder.append_null();
                } else {
                    let bytes = match logical {
                        LogicalType::Uuid => array.as_fixed_size_binary().value(row),
                        _ => array.as_binary::<i32>().value(row),
                    };
                    builder.append_value(render_bytes(bytes, *logical == LogicalType::Uuid));
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => temporal::text(array),
    }
}

/// Refuses `array`, holding only what its rows name, where a decimal in it, at any depth, is
/// beyond the precision its type declares: Arrow builds and widens such a decimal unchecked, and
/// renders it cut to the precision.
fn precise(array: &ArrayRef) -> Result<(), ArrowError> {
    use arrow_array::types::{Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type};
    match array.data_type() {
        DataType::Decimal32(precision, _) => array
            .as_primitive::<Decimal32Type>()
            .validate_decimal_precision(*precision),
        DataType::Decimal64(precision, _) => array
            .as_primitive::<Decimal64Type>()
            .validate_decimal_precision(*precision),
        DataType::Decimal128(precision, _) => array
            .as_primitive::<Decimal128Type>()
            .validate_decimal_precision(*precision),
        DataType::Decimal256(precision, _) => array
            .as_primitive::<Decimal256Type>()
            .validate_decimal_precision(*precision),
        DataType::Struct(_) => array.as_struct().columns().iter().try_for_each(precise),
        DataType::List(_) => precise(array.as_list::<i32>().values()),
        DataType::LargeList(_) => precise(array.as_list::<i64>().values()),
        DataType::FixedSizeList(..) => precise(array.as_fixed_size_list().values()),
        DataType::Map(..) => precise(&(Arc::new(array.as_map().entries().clone()) as ArrayRef)),
        _ => Ok(()),
    }
}

/// Bytes: what a builder of the text of `array`'s values reserves, the most the cost model says
/// the text takes, so the builder never grows.
pub(super) fn text_capacity(array: &ArrayRef, json: bool) -> usize {
    let bytes = rdlt_connector::cost::text_bytes(array.as_ref(), json);
    // A bound no allocation could hold is not reserved: the builder then grows as it must.
    usize::try_from(bytes).unwrap_or(0)
}

/// `bytes` in lower-case hex; a UUID's 16 bytes in its hyphenated form.
pub(super) fn render_bytes(bytes: &[u8], uuid: bool) -> String {
    let mut rendered = String::with_capacity(bytes.len() * 2 + 4);
    for (index, byte) in bytes.iter().enumerate() {
        if uuid && matches!(index, 4 | 6 | 8 | 10) {
            rendered.push('-');
        }
        write!(rendered, "{byte:02x}").expect("writing to a string cannot fail");
    }
    rendered
}
