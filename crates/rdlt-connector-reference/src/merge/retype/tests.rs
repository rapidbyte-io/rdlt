use std::sync::Arc;

use arrow_array::builder::PrimitiveDictionaryBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowDictionaryKeyType, Decimal128Type, Decimal256Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
    UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeBinaryArray, LargeStringArray, ListArray, NullArray, RunArray, StringArray,
    StringViewArray, StructArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow_buffer::i256;
use arrow_schema::{DataType, Field, Fields, TimeUnit};

use super::{raw, retyped};

const UNITS: [TimeUnit; 4] = [
    TimeUnit::Second,
    TimeUnit::Millisecond,
    TimeUnit::Microsecond,
    TimeUnit::Nanosecond,
];

fn per_second(unit: TimeUnit) -> i128 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

fn time(unit: TimeUnit) -> DataType {
    match unit {
        TimeUnit::Second | TimeUnit::Millisecond => DataType::Time32(unit),
        _ => DataType::Time64(unit),
    }
}

/// One value of the temporal type `kind`, as that type stores it, or none where it cannot.
fn temporal(kind: &DataType, value: i64) -> Option<ArrayRef> {
    let wide: ArrayRef = Arc::new(Int64Array::from(vec![value]));
    let narrow = || -> Option<ArrayRef> {
        let value = i32::try_from(value).ok()?;
        Some(Arc::new(Int32Array::from(vec![value])))
    };
    // Reinterpreting an integer as a temporal value of its width changes no bit.
    let stored = match kind {
        DataType::Date32 | DataType::Time32(_) => narrow()?,
        _ => wide,
    };
    Some(arrow_cast::cast(&stored, kind).expect("integers are any temporal type's storage"))
}

/// Seconds the zone is ahead of UTC, for the fixed zones the tests place values in.
fn ahead(zone: Option<&str>) -> i128 {
    match zone {
        None | Some("UTC") => 0,
        Some("+05:30") => 19_800,
        Some("-08:00") => -28_800,
        Some("+0100") => 3_600,
        Some(other) => panic!("{other} is no zone of the tests"),
    }
}

/// Checks `retyped` of `value` from `from` to `to` against `expected`, the value `to` then holds:
/// none where the conversion must be refused.
fn check(from: &DataType, to: &DataType, value: i64, expected: Option<i128>) {
    let Some(stored) = temporal(from, value) else {
        return;
    };
    let outcome = retyped(&stored, to);
    let expected = expected.and_then(|expected| i64::try_from(expected).ok());
    let narrow = matches!(to, DataType::Time32(_));
    let expected = expected.filter(|expected| !narrow || i32::try_from(*expected).is_ok());
    match (outcome, expected) {
        (Ok(converted), Some(expected)) => {
            assert_eq!(converted.data_type(), to, "{from} {value}");
            assert_eq!(raw(&converted), [Some(expected)], "{from} to {to}: {value}");
        }
        (Err(_), None) => {}
        (outcome, expected) => panic!("{from} to {to}: {value} gave {outcome:?}, not {expected:?}"),
    }
}

/// Values at and around every bound a conversion by `factor` has.
fn edges(factor: i128) -> Vec<i64> {
    let mut edges = vec![0, 1, -1, 86_399, i64::MAX, i64::MIN];
    let around = [i128::from(i64::MAX) / factor, i128::from(i64::MIN) / factor];
    let narrow = [i128::from(i32::MAX) / factor, i128::from(i32::MIN) / factor];
    for bound in around.into_iter().chain(narrow) {
        for near in [bound - 1, bound, bound + 1] {
            edges.extend(i64::try_from(near));
        }
    }
    edges.extend([i64::from(i32::MAX), i64::from(i32::MIN)]);
    edges
}

#[test]
fn every_unit_widening_is_exact_or_refused() {
    type Of = fn(TimeUnit) -> DataType;
    let kinds: [Of; 3] = [
        |unit| DataType::Timestamp(unit, None),
        DataType::Duration,
        time,
    ];
    let mut checked = 0;
    for kind in kinds {
        for (coarse, from) in UNITS.into_iter().enumerate() {
            for (fine, to) in UNITS.into_iter().enumerate() {
                let factor = per_second(to) / per_second(from).max(1);
                for value in edges(factor.max(1)) {
                    // A coarser unit would round, so it is refused whatever the value.
                    let expected = (fine >= coarse).then(|| i128::from(value) * factor);
                    check(&kind(from), &kind(to), value, expected);
                    checked += 1;
                }
            }
        }
    }
    assert!(checked >= 3 * 16 * 18, "{checked}");
}

#[test]
fn a_wall_clock_time_or_a_date_is_placed_in_a_fixed_zone_exactly_or_refused() {
    let zones = [
        None,
        Some("UTC"),
        Some("+05:30"),
        Some("-08:00"),
        Some("+0100"),
    ];
    for zone in zones {
        let named = zone.map(Arc::<str>::from);
        for (coarse, from) in UNITS.into_iter().enumerate() {
            for to in UNITS.into_iter().skip(coarse) {
                let target = DataType::Timestamp(to, named.clone());
                let factor = per_second(to) / per_second(from);
                for value in edges(factor) {
                    let scaled = i128::from(value) * factor;
                    // The product itself must fit: the placing starts from it.
                    let expected = i64::try_from(scaled)
                        .ok()
                        .map(|_| scaled - ahead(zone) * per_second(to));
                    check(&DataType::Timestamp(from, None), &target, value, expected);
                }
            }
        }
        for to in UNITS {
            let target = DataType::Timestamp(to, named.clone());
            let per_day = 86_400 * per_second(to);
            for days in edges(per_day) {
                let local = i128::from(days) * per_day;
                let expected = i64::try_from(local)
                    .ok()
                    .map(|_| local - ahead(zone) * per_second(to));
                check(&DataType::Date32, &target, days, expected);
            }
        }
    }
}

#[test]
fn an_instant_keeps_its_value_under_another_zone_and_is_no_wall_clock_time() {
    let stored: ArrayRef =
        Arc::new(TimestampSecondArray::from(vec![Some(7), None]).with_timezone("+05:30"));
    let utc = DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
    let relabelled = retyped(&stored, &utc).unwrap();
    assert_eq!(relabelled.data_type(), &utc);
    assert_eq!(raw(&relabelled), [Some(7_000), None]);
    let naive = DataType::Timestamp(TimeUnit::Millisecond, None);
    assert!(retyped(&stored, &naive).is_err());
    // A zone no database of zones knows places nothing.
    let nowhere = DataType::Timestamp(TimeUnit::Second, Some("Nowhere/Land".into()));
    let wall: ArrayRef = Arc::new(TimestampSecondArray::from(vec![7]));
    assert!(retyped(&wall, &nowhere).is_err());
}

#[test]
fn a_wall_clock_time_in_a_named_zone_is_the_instant_it_names_there() {
    let warsaw = |unit| DataType::Timestamp(unit, Some("Europe/Warsaw".into()));
    // Winter, summer, a time the clocks skipped and one they repeated.
    let placed = [
        (1_705_320_000, 1_705_316_400),
        (1_719_835_200, 1_719_828_000),
        (1_711_852_200, 1_711_848_600),
        (1_729_996_200, 1_729_989_000),
    ];
    for (local, instant) in placed {
        let stored: ArrayRef = Arc::new(TimestampSecondArray::from(vec![local]));
        let seconds = retyped(&stored, &warsaw(TimeUnit::Second)).unwrap();
        assert_eq!(raw(&seconds), [Some(instant)], "{local}");
        let nanos = retyped(&stored, &warsaw(TimeUnit::Nanosecond)).unwrap();
        assert_eq!(raw(&nanos), [Some(instant * 1_000_000_000)], "{local}");
    }
    // A date is its midnight there.
    let day: ArrayRef = Arc::new(Date32Array::from(vec![19_737]));
    let midnight = retyped(&day, &warsaw(TimeUnit::Second)).unwrap();
    assert_eq!(raw(&midnight), [Some(19_737 * 86_400 - 3_600)]);
    // Beyond the years a calendar holds no named zone has an offset; a fixed one still does.
    let far: ArrayRef = Arc::new(TimestampSecondArray::from(vec![i64::MAX / 2]));
    assert!(retyped(&far, &warsaw(TimeUnit::Second)).is_err());
    let fixed = DataType::Timestamp(TimeUnit::Second, Some("+05:30".into()));
    assert_eq!(
        raw(&retyped(&far, &fixed).unwrap()),
        [Some(i64::MAX / 2 - 19_800)]
    );
}

#[test]
fn a_date_with_a_time_of_day_is_no_date() {
    let whole: ArrayRef = Arc::new(Date64Array::from(vec![Some(-86_400_000), None, Some(0)]));
    let seconds = DataType::Timestamp(TimeUnit::Second, None);
    assert_eq!(
        raw(&retyped(&whole, &seconds).unwrap()),
        [Some(-86_400), None, Some(0)]
    );
    for part in [1, -1, 86_399_999, -86_400_001] {
        let partial: ArrayRef = Arc::new(Date64Array::from(vec![part]));
        assert!(retyped(&partial, &seconds).is_err(), "{part}");
    }
}

/// One integer of each type that holds `value`.
fn integers(value: i128) -> Vec<ArrayRef> {
    let mut held: Vec<ArrayRef> = Vec::new();
    macro_rules! held {
        ($($native:ty => $array:ty),*) => {$(
            if let Ok(value) = <$native>::try_from(value) {
                held.push(Arc::new(<$array>::from(vec![value])));
            }
        )*};
    }
    held!(
        i8 => Int8Array, i16 => Int16Array, i32 => Int32Array, i64 => Int64Array,
        u8 => UInt8Array, u16 => UInt16Array, u32 => UInt32Array, u64 => UInt64Array
    );
    held
}

/// The integer a one-row integer column holds.
fn integer(array: &ArrayRef) -> i128 {
    let wide = arrow_cast::cast(array, &DataType::Decimal128(38, 0)).unwrap();
    wide.as_primitive::<Decimal128Type>().value(0)
}

#[test]
fn an_integer_becomes_another_integer_only_where_it_fits() {
    let bounds = [
        (DataType::Int8, i128::from(i8::MIN), i128::from(i8::MAX)),
        (DataType::Int16, i128::from(i16::MIN), i128::from(i16::MAX)),
        (DataType::Int32, i128::from(i32::MIN), i128::from(i32::MAX)),
        (DataType::Int64, i128::from(i64::MIN), i128::from(i64::MAX)),
        (DataType::UInt8, 0, i128::from(u8::MAX)),
        (DataType::UInt16, 0, i128::from(u16::MAX)),
        (DataType::UInt32, 0, i128::from(u32::MAX)),
        (DataType::UInt64, 0, i128::from(u64::MAX)),
    ];
    let values: Vec<i128> = bounds
        .iter()
        .flat_map(|(_, least, greatest)| [*least, *greatest, least + 1, greatest - 1])
        .chain([0, -1, 1])
        .collect();
    for value in values {
        for stored in integers(value) {
            for (to, least, greatest) in &bounds {
                let converted = retyped(&stored, to);
                if (*least..=*greatest).contains(&value) {
                    let converted = converted.unwrap();
                    assert_eq!(converted.data_type(), to);
                    assert_eq!(integer(&converted), value, "{value} to {to}");
                } else {
                    assert!(converted.is_err(), "{value} to {to}");
                }
            }
        }
    }
}

#[test]
fn floats_and_decimals_take_only_what_they_hold_exactly() {
    for value in [i128::from(i32::MIN), -1, 0, 255, i128::from(u32::MAX)] {
        for stored in integers(value) {
            let small = !matches!(stored.data_type(), DataType::Int64 | DataType::UInt64);
            let float = retyped(&stored, &DataType::Float64);
            if small {
                #[expect(clippy::cast_precision_loss, reason = "32 bits fit a float's 53")]
                let expected = value as f64;
                let float = float.unwrap();
                let held = float.as_primitive::<Float64Type>().value(0);
                assert_eq!(held.to_bits(), expected.to_bits());
            } else {
                assert!(float.is_err(), "{value} of {}", stored.data_type());
            }
            assert!(retyped(&stored, &DataType::Float32).is_err());
            let decimal = retyped(&stored, &DataType::Decimal128(20, 2)).unwrap();
            assert_eq!(
                decimal.as_primitive::<Decimal128Type>().value(0),
                value * 100
            );
            // Two digits before the point do not hold three.
            let tight = retyped(&stored, &DataType::Decimal128(4, 2));
            assert_eq!(tight.is_ok(), value.abs() < 100, "{value}");
        }
    }
    let floats = [1.5_f32, f32::NAN, -0.0, f32::INFINITY, f32::MIN_POSITIVE];
    let narrow: ArrayRef = Arc::new(Float32Array::from(floats.to_vec()));
    let wide = retyped(&narrow, &DataType::Float64).unwrap();
    let wide = wide.as_primitive::<Float64Type>();
    for (index, float) in floats.into_iter().enumerate() {
        assert_eq!(wide.value(index).to_bits(), f64::from(float).to_bits());
    }
    let double: ArrayRef = Arc::new(Float64Array::from(vec![1.5]));
    for lossy in [
        DataType::Float32,
        DataType::Int64,
        DataType::Decimal128(20, 2),
    ] {
        assert!(retyped(&double, &lossy).is_err(), "{lossy}");
    }
    let decimal: ArrayRef = Arc::new(
        arrow_array::Decimal128Array::from(vec![12_345])
            .with_precision_and_scale(10, 2)
            .unwrap(),
    );
    let wider = retyped(&decimal, &DataType::Decimal128(20, 4)).unwrap();
    assert_eq!(wider.as_primitive::<Decimal128Type>().value(0), 1_234_500);
    let widest = retyped(&decimal, &DataType::Decimal256(60, 4)).unwrap();
    assert_eq!(
        widest.as_primitive::<Decimal256Type>().value(0),
        i256::from_i128(1_234_500)
    );
    // A smaller scale would round, and a precision too small holds no such value.
    assert!(retyped(&decimal, &DataType::Decimal128(20, 1)).is_err());
    assert!(retyped(&decimal, &DataType::Decimal128(5, 4)).is_err());
}

#[test]
fn text_and_bytes_change_their_encoding_and_nothing_else() {
    let words = vec![Some("a"), None, Some("")];
    let texts: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(words.clone())),
        Arc::new(LargeStringArray::from(words.clone())),
        Arc::new(StringViewArray::from(words.clone())),
    ];
    let raw_bytes: Vec<Option<&[u8]>> = vec![Some(b"ab"), None, Some(b"cd")];
    let bytes: Vec<ArrayRef> = vec![
        Arc::new(BinaryArray::from(raw_bytes.clone())),
        Arc::new(LargeBinaryArray::from(raw_bytes.clone())),
        Arc::new(BinaryViewArray::from(raw_bytes.clone())),
        Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(raw_bytes.clone().into_iter(), 2)
                .unwrap(),
        ),
    ];
    let text_kinds = [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View];
    let byte_kinds = [
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
    ];
    for text in &texts {
        for to in &text_kinds {
            let converted = retyped(text, to).unwrap();
            let plain = arrow_cast::cast(&converted, &DataType::Utf8).unwrap();
            let held: Vec<Option<&str>> = plain.as_string::<i32>().iter().collect();
            assert_eq!(held, words, "{} to {to}", text.data_type());
        }
        for to in &byte_kinds {
            assert!(retyped(text, to).is_err(), "{} to {to}", text.data_type());
        }
        assert!(retyped(text, &DataType::Int64).is_err());
    }
    for stored in &bytes {
        for to in &byte_kinds {
            let converted = retyped(stored, to).unwrap();
            let plain = arrow_cast::cast(&converted, &DataType::Binary).unwrap();
            let held: Vec<Option<&[u8]>> = plain.as_binary::<i32>().iter().collect();
            assert_eq!(held, raw_bytes, "{} to {to}", stored.data_type());
        }
        for to in &text_kinds {
            assert!(
                retyped(stored, to).is_err(),
                "{} to {to}",
                stored.data_type()
            );
        }
    }
    // Bytes of a fixed width are bytes; bytes are of no fixed width.
    assert!(retyped(&bytes[0], &DataType::FixedSizeBinary(2)).is_err());
    let flags: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
    for other in [DataType::Int8, DataType::Utf8] {
        assert!(retyped(&flags, &other).is_err(), "{other}");
    }
    let number: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    for other in [DataType::Utf8, DataType::Boolean, DataType::Binary] {
        assert!(retyped(&number, &other).is_err(), "{other}");
    }
}

/// `values` dictionary-encoded with keys of `K`.
fn dictionary<K: ArrowDictionaryKeyType>(values: &[Option<i32>]) -> ArrayRef {
    let mut builder = PrimitiveDictionaryBuilder::<K, Int32Type>::new();
    for value in values {
        builder.append_option(*value);
    }
    Arc::new(builder.finish())
}

#[test]
fn an_encoded_column_converts_as_the_values_it_encodes() {
    let values = [Some(7), None, Some(-1), Some(7)];
    let encoded = [
        dictionary::<Int8Type>(&values),
        dictionary::<Int16Type>(&values),
        dictionary::<Int32Type>(&values),
        dictionary::<Int64Type>(&values),
        dictionary::<UInt8Type>(&values),
        dictionary::<UInt16Type>(&values),
        dictionary::<UInt32Type>(&values),
        dictionary::<UInt64Type>(&values),
    ];
    for column in encoded {
        let wide = retyped(&column, &DataType::Int64).unwrap();
        let held: Vec<Option<i64>> = wide.as_primitive::<Int64Type>().iter().collect();
        assert_eq!(held, [Some(7), None, Some(-1), Some(7)]);
        // The values themselves must fit, whatever encodes them.
        assert!(retyped(&column, &DataType::UInt8).is_err());
    }
    let seconds = TimestampSecondArray::from(vec![Some(5), None, Some(i64::MAX)]);
    let millis = DataType::Timestamp(TimeUnit::Millisecond, None);
    let runs: [ArrayRef; 3] = [
        Arc::new(RunArray::try_new(&Int16Array::from(vec![2, 3, 4]), &seconds).unwrap()),
        Arc::new(RunArray::try_new(&Int32Array::from(vec![2, 3, 4]), &seconds).unwrap()),
        Arc::new(RunArray::try_new(&Int64Array::from(vec![2, 3, 4]), &seconds).unwrap()),
    ];
    for run in runs {
        assert!(
            retyped(&run, &millis).is_err(),
            "the last run's value does not fit"
        );
        let fitting = run.slice(0, 3);
        let converted = retyped(&fitting, &millis).unwrap();
        let held: Vec<Option<i64>> = converted
            .as_primitive::<TimestampMillisecondType>()
            .iter()
            .collect();
        assert_eq!(held, [Some(5_000), Some(5_000), None]);
    }
    let nulls: ArrayRef = Arc::new(NullArray::new(3));
    let typed = retyped(&nulls, &millis).unwrap();
    assert_eq!((typed.data_type(), typed.null_count()), (&millis, 3));
    // A column already of its type is the column itself.
    let same: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    assert!(Arc::ptr_eq(
        &retyped(&same, &DataType::Int64).unwrap(),
        &same
    ));
}

fn field(name: &str, kind: DataType) -> Field {
    Field::new(name, kind, true)
}

#[test]
fn a_struct_converts_field_by_field() {
    let seconds = DataType::Timestamp(TimeUnit::Second, None);
    let nanos = DataType::Timestamp(TimeUnit::Nanosecond, None);
    let stored_fields = Fields::from(vec![field("n", DataType::Int32), field("at", seconds)]);
    let stored = |at: i64| -> ArrayRef {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(TimestampSecondArray::from(vec![Some(2), Some(at), None])),
        ];
        let nulls = arrow_buffer::NullBuffer::from(vec![true, false, true]);
        Arc::new(StructArray::new(
            stored_fields.clone(),
            columns,
            Some(nulls),
        ))
    };
    // The table's struct gained a field, and widened the two it had.
    let wider = DataType::Struct(Fields::from(vec![
        field("added", DataType::Utf8),
        field("at", nanos.clone()),
        field("n", DataType::Int64),
    ]));
    let converted = retyped(&stored(4), &wider).unwrap();
    let converted = converted.as_struct();
    assert_eq!(converted.data_type(), &wider);
    assert_eq!(
        (0..3).map(|row| converted.is_null(row)).collect::<Vec<_>>(),
        [false, true, false]
    );
    assert_eq!(converted.column(0).null_count(), 3);
    let at = converted
        .column(1)
        .as_primitive::<TimestampNanosecondType>();
    assert_eq!(
        at.iter().collect::<Vec<_>>(),
        [Some(2_000_000_000), Some(4_000_000_000), None]
    );
    let n = converted.column(2).as_primitive::<Int64Type>();
    assert_eq!(n.iter().collect::<Vec<_>>(), [Some(1), None, Some(3)]);
    // A value a field's wider type cannot hold fails the whole struct, under a null struct too.
    assert!(retyped(&stored(i64::MAX), &wider).is_err());
}

#[test]
fn a_list_converts_item_by_item_whatever_encodes_it() {
    let nanos = DataType::Timestamp(TimeUnit::Nanosecond, None);
    let days = Date32Array::from(vec![Some(1), None, Some(2), Some(3)]);
    let offsets = arrow_buffer::OffsetBuffer::new(vec![0, 2, 2, 4].into());
    let nulls = arrow_buffer::NullBuffer::from(vec![true, false, true]);
    let item = Arc::new(field("item", DataType::Date32));
    let stored: ArrayRef = Arc::new(ListArray::new(item, offsets, Arc::new(days), Some(nulls)));
    let zoned = DataType::Timestamp(TimeUnit::Second, Some("UTC".into()));
    let wider = DataType::List(Arc::new(field("item", zoned)));
    let check = |stored: &ArrayRef| {
        let converted = retyped(stored, &wider).unwrap();
        assert_eq!(converted.data_type(), &wider);
        let lists = converted.as_list::<i32>();
        assert_eq!(lists.null_count(), 1);
        assert!(lists.is_null(1));
        let first = lists.value(0);
        let last = lists.value(2);
        let seconds = |list: &ArrayRef| -> Vec<Option<i64>> {
            list.as_primitive::<TimestampSecondType>().iter().collect()
        };
        assert_eq!(seconds(&first), [Some(86_400), None]);
        assert_eq!(seconds(&last), [Some(172_800), Some(259_200)]);
    };
    check(&stored);
    // Every other encoding of a list is a list of the same items.
    let item = Arc::new(field("item", DataType::Date32));
    let large = arrow_cast::cast(&stored, &DataType::LargeList(Arc::clone(&item))).unwrap();
    let view = arrow_cast::cast(&stored, &DataType::ListView(Arc::clone(&item))).unwrap();
    let large_view = arrow_cast::cast(&stored, &DataType::LargeListView(item)).unwrap();
    for encoded in [large, view, large_view] {
        check(&encoded);
    }
    let far: ArrayRef = Arc::new(Date32Array::from(vec![i32::MAX]));
    let far = arrow_cast::cast(
        &far,
        &DataType::FixedSizeList(Arc::new(field("item", DataType::Date32)), 1),
    )
    .unwrap();
    let nanos = DataType::List(Arc::new(field("item", nanos)));
    assert!(retyped(&far, &nanos).is_err());
}

#[test]
fn an_id_or_a_sequence_kept_as_text_compares_as_the_bytes_it_is() {
    let text: ArrayRef = Arc::new(StringArray::from(vec![Some("0a"), Some("")]));
    let large: ArrayRef = Arc::new(LargeStringArray::from(vec![Some("0a"), Some("")]));
    let fixed: ArrayRef =
        Arc::new(FixedSizeBinaryArray::try_from_iter([b"0a", b"zz"].into_iter()).unwrap());
    for (stored, expected) in [
        (&text, [&b"0a"[..], b""]),
        (&large, [b"0a", b""]),
        (&fixed, [b"0a", b"zz"]),
    ] {
        let bytes = super::compared(stored).unwrap();
        let held: Vec<&[u8]> = bytes.as_binary::<i32>().iter().flatten().collect();
        assert_eq!(held, expected, "{}", stored.data_type());
    }
    // A number is no id or sequence.
    let number: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    assert!(super::compared(&number).is_err());
}
