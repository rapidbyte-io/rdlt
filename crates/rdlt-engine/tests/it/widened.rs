//! Every widening the type lattice makes, as each destination applies it to what it already
//! holds: a value read back after its column widened denotes what it denoted before.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal128Type, Decimal256Type, Float64Type, Int64Type};
use arrow_array::{
    Array, ArrayRef, Decimal128Array, Decimal256Array, Float64Array, Int32Array, Int64Array,
    RecordBatch,
};
use arrow_buffer::i256;
use arrow_schema::{DataType, Field, Schema};
use rdlt_connector::{DecimalType, LogicalType, TimeUnit, instants};
use rdlt_engine::RunStatus;

use crate::support::batches::{BatchStream, batches};
use crate::support::targets::Target;
use crate::support::{commit_every, each, engine, pipeline, stream};

/// Rows each column holds before its widening.
const ROWS: usize = 6;

const UNITS: [TimeUnit; 4] = [
    TimeUnit::Second,
    TimeUnit::Millisecond,
    TimeUnit::Microsecond,
    TimeUnit::Nanosecond,
];

/// Every type of numbers or temporal values a column may take, and widen from.
fn types() -> Vec<LogicalType> {
    let decimal = |precision, scale| {
        LogicalType::Decimal(DecimalType::new(precision, scale).expect("a decimal type"))
    };
    let mut types = vec![
        LogicalType::Int8,
        LogicalType::Int16,
        LogicalType::Int32,
        LogicalType::Int64,
        LogicalType::Float32,
        LogicalType::Float64,
        decimal(5, 2),
        decimal(19, 4),
        decimal(39, 2),
        LogicalType::Date,
    ];
    for unit in UNITS {
        types.extend([LogicalType::Time(unit), LogicalType::Duration(unit)]);
        for zone in [None, Some("UTC"), Some("America/New_York"), Some("+05:30")] {
            types.push(LogicalType::Timestamp(unit, zone.map(Arc::from)));
        }
    }
    types
}

/// `ROWS` values of `logical`, at its edges and between, that every type it widens to holds.
fn values(logical: &LogicalType) -> ArrayRef {
    let data_type = logical.to_arrow();
    let cast = |values: ArrayRef| arrow_cast::cast(&values, &data_type).expect("a cast");
    match &data_type {
        DataType::Float32 | DataType::Float64 => cast(Arc::new(Float64Array::from(vec![
            0.0,
            0.1,
            -1.5,
            f64::from(f32::MAX),
            f64::from(f32::MIN_POSITIVE),
            16_777_216.0,
        ]))),
        DataType::Decimal128(precision, scale) => {
            let most = 10_i128.pow(u32::from(*precision)) - 1;
            let values = Decimal128Array::from(vec![0, 1, -1, 1_234, most, -most]);
            Arc::new(
                values
                    .with_precision_and_scale(*precision, *scale)
                    .expect("a decimal"),
            )
        }
        DataType::Decimal256(precision, scale) => {
            let most = i256::from_string(&"9".repeat(usize::from(*precision))).expect("digits");
            let values = vec![i256::ZERO, i256::ONE, i256::MINUS_ONE, most, -most, most];
            let values = Decimal256Array::from(values);
            Arc::new(
                values
                    .with_precision_and_scale(*precision, *scale)
                    .expect("a decimal"),
            )
        }
        DataType::Date32 | DataType::Time32(_) => cast(Arc::new(Int32Array::from(vec![
            -106_000, -1, 0, 1, 18_262, 86_399,
        ]))),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let (least, most) = match &data_type {
                DataType::Int8 => (i64::from(i8::MIN), i64::from(i8::MAX)),
                DataType::Int16 => (i64::from(i16::MIN), i64::from(i16::MAX)),
                DataType::Int32 => (i64::from(i32::MIN), i64::from(i32::MAX)),
                _ => (i64::MIN, i64::MAX),
            };
            cast(Arc::new(Int64Array::from(vec![least, -1, 0, 1, 100, most])))
        }
        // Seconds, the coarsest unit, a nanosecond still holds.
        _ => cast(Arc::new(Int64Array::from(vec![
            -9_000_000_000,
            -86_400_001,
            -1,
            0,
            1_577_854_800,
            9_000_000_000,
        ]))),
    }
}

/// What the value at `row` of `array`, as a destination read it back, denotes: an instant, time
/// of day or duration in nanoseconds, a number as the shortest decimal text of its exact value,
/// and anything else as Arrow shows it.
fn denoted(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return "null".to_owned();
    }
    if let Some(value) = instants::stored(array, row) {
        let nanos = instants::nanos(array.data_type(), i128::from(value)).expect("temporal");
        return format!("{nanos} ns");
    }
    let one = array.slice(row, 1);
    match array.data_type() {
        DataType::Decimal128(_, scale) => exact(
            &one.as_primitive::<Decimal128Type>().value(0).to_string(),
            *scale,
        ),
        DataType::Decimal256(_, scale) => exact(
            &one.as_primitive::<Decimal256Type>().value(0).to_string(),
            *scale,
        ),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            let wide = arrow_cast::cast(&one, &DataType::Float64).expect("a float");
            let value = wide.as_primitive::<Float64Type>().value(0);
            exact(&format!("{}", if value == 0.0 { 0.0 } else { value }), 0)
        }
        data_type if data_type.is_integer() => {
            let wide = arrow_cast::cast(&one, &DataType::Int64).expect("an integer");
            wide.as_primitive::<Int64Type>().value(0).to_string()
        }
        _ => {
            let shown = arrow_cast::display::ArrayFormatter::try_new(
                &one,
                &arrow_cast::display::FormatOptions::default(),
            )
            .expect("a formatter");
            shown.value(0).to_string()
        }
    }
}

/// `digits`, a number's text, divided by ten to the `scale`, without trailing zeros.
fn exact(digits: &str, scale: i8) -> String {
    let (sign, digits) = digits
        .strip_prefix('-')
        .map_or(("", digits), |rest| ("-", rest));
    let scale = usize::try_from(scale).expect("no negative scale");
    let text = if digits.contains('.') || scale == 0 {
        format!("{sign}{digits}")
    } else {
        let padded = format!("{digits:0>width$}", width = scale + 1);
        let (whole, fraction) = padded.split_at(padded.len() - scale);
        format!("{sign}{whole}.{fraction}")
    };
    if text.contains('.') {
        text.trim_end_matches('0').trim_end_matches('.').to_owned()
    } else {
        text
    }
}

/// What each column `c{index}` of the table `events` at `store` holds of each row, by id.
fn held(target: Target, store: &str) -> BTreeMap<(String, i64), String> {
    let mut held = BTreeMap::new();
    for batch in target.published(store, "events") {
        let ids = arrow_cast::cast(batch.column_by_name("id").expect("ids"), &DataType::Int64)
            .expect("integer ids");
        let schema = batch.schema();
        for (field, column) in schema.fields().iter().zip(batch.columns()) {
            if !field.name().starts_with('c') || field.name().contains("__") {
                continue;
            }
            for row in 0..batch.num_rows() {
                let id = ids.as_primitive::<Int64Type>().value(row);
                held.insert((field.name().clone(), id), denoted(column.as_ref(), row));
            }
        }
    }
    held
}

/// A batch of `ids` and a column `c{index}` for each of `columns`.
fn rows(ids: &[i64], columns: &[(usize, ArrayRef)]) -> RecordBatch {
    let mut fields = vec![Field::new("id", DataType::Int64, false)];
    let mut arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(ids.to_vec()))];
    for (index, column) in columns {
        fields.push(Field::new(
            format!("c{index}"),
            column.data_type().clone(),
            true,
        ));
        arrays.push(Arc::clone(column));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("a batch")
}

/// Loads `rows` into `store` at `target`, which must take them.
async fn load(target: Target, store: &str, rows: RecordBatch) {
    let source = batches(store, vec![BatchStream::new("events", vec![rows])]).await;
    let outcome = engine(commit_every(10))
        .run(
            pipeline(store, [stream("events")]),
            source,
            target.destination(store).await,
        )
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{target:?} {store}: {:?}",
        outcome.error
    );
}

#[tokio::test(start_paused = true)]
async fn every_value_reads_back_alike_after_its_column_widens_in_every_destination() {
    let types = types();
    // Each type's columns, each in a round of its own, widen to every type it joins to.
    let joins: Vec<Vec<LogicalType>> = types
        .iter()
        .map(|from| {
            let mut joins: Vec<LogicalType> = types
                .iter()
                .map(|other| from.join(other))
                .filter(|to| to != from && *to != LogicalType::Json)
                .collect();
            joins.dedup();
            joins.sort_by_key(|to| format!("{to:?}"));
            joins.dedup();
            joins
        })
        .collect();
    let rounds = joins.iter().map(Vec::len).max().unwrap_or(0);
    assert!(rounds > 10, "{rounds} rounds");
    each(Target::IN_PROCESS, |target| {
        let (types, joins) = (types.clone(), joins.clone());
        async move {
            for round in 0..rounds {
                let store = format!("widened_{round}");
                let widening: Vec<usize> = (0..types.len())
                    .filter(|index| joins[*index].len() > round)
                    .collect();
                let ids: Vec<i64> = (0..ROWS)
                    .map(|row| i64::try_from(row).expect("few"))
                    .collect();
                let narrow: Vec<(usize, ArrayRef)> = widening
                    .iter()
                    .map(|index| (*index, values(&types[*index])))
                    .collect();
                load(target, &store, rows(&ids, &narrow)).await;
                let before = held(target, &store);
                let wide: Vec<(usize, ArrayRef)> = widening
                    .iter()
                    .map(|index| (*index, values(&joins[*index][round]).slice(1, 1)))
                    .collect();
                load(target, &store, rows(&[1_000], &wide)).await;
                let after = held(target, &store);
                for (place, value) in &before {
                    let (column, _) = place;
                    let index: usize = column[1..].parse().expect("a column's place");
                    let widened = (&types[index], &joins[index][round]);
                    assert_eq!(
                        after.get(place),
                        Some(value),
                        "{target:?} {place:?} {widened:?}"
                    );
                }
            }
        }
    })
    .await;
}
