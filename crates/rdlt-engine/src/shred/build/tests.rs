use super::{Column, Record, Scalar};
use crate::shred::meter::{BUILDER, Columns, KEY, Meter, Over};
use crate::shred::observe::{Observed, Shape};

fn columns() -> Columns {
    Columns::new(u64::MAX)
}

#[test]
fn a_column_a_row_adds_is_sized_for_the_whole_chunk_and_charged_for_it() {
    let meter = Meter::new(u64::MAX);
    let mut record = Record::empty(100);
    let position = record
        .position("a", 0, &columns(), &meter)
        .unwrap()
        .unwrap();
    let capacity = record.capacity();
    let column = record.field(position).unwrap();
    assert_eq!(column.scalar(Scalar::Int(1), capacity, &meter), Ok(true));
    record.end_row(1, &meter).unwrap();
    let Column::Int { builder, .. } = &record.columns[position] else {
        panic!("an integer column");
    };
    assert!(builder.capacity() >= 100, "{}", builder.capacity());
    // The field's entry and its builder's fixed parts; a value and its validity bit, for each of
    // the hundred rows.
    assert_eq!(meter.spent(), KEY + 1 + BUILDER + 100 * 8 + 13);
}

#[test]
fn a_builder_the_meter_has_no_room_for_is_never_made() {
    let meter = Meter::new(BUILDER + 100 * 8 + 13 - 1);
    let mut column = Column::Null(0);
    assert_eq!(column.scalar(Scalar::Int(1), 100, &meter), Err(Over));
    assert!(meter.tripped());
    assert!(matches!(column, Column::Null(0)), "the column is unchanged");
    let meter = Meter::new(BUILDER + 100 * 8 + 13);
    assert_eq!(column.scalar(Scalar::Int(1), 100, &meter), Ok(true));
    assert_eq!(meter.spent(), BUILDER + 100 * 8 + 13);
}

#[test]
fn rows_past_what_a_record_was_sized_for_are_charged_as_its_builders_double() {
    let meter = Meter::new(u64::MAX);
    let mut record = Record::empty(2);
    for row in 0..2 {
        let position = record
            .position("a", 0, &columns(), &meter)
            .unwrap()
            .unwrap();
        let capacity = record.capacity();
        let column = record.field(position).unwrap();
        assert_eq!(column.scalar(Scalar::Int(row), capacity, &meter), Ok(true));
        record.end_row(1, &meter).unwrap();
    }
    let sized = meter.spent();
    assert_eq!(sized, KEY + 1 + BUILDER + 2 * 8 + 1);
    // The third row doubles the builders to four rows, charged for the two they grow by: a
    // value and two bits, the column's validity and the record's, a row.
    let position = record
        .position("a", 0, &columns(), &meter)
        .unwrap()
        .unwrap();
    let column = record.field(position).unwrap();
    assert_eq!(column.scalar(Scalar::Int(3), 2, &meter), Ok(true));
    record.end_row(1, &meter).unwrap();
    assert_eq!(meter.spent() - sized, 2 * 8 + 1);
    assert_eq!(record.capacity(), 4);
    // A record with no room to grow trips its meter instead.
    let mut full = Record::empty(0);
    assert_eq!(full.end_row(0, &Meter::new(0)), Err(Over));
}

#[test]
fn text_past_what_its_builder_was_sized_for_is_charged_as_it_doubles() {
    let meter = Meter::new(u64::MAX);
    let mut column = Column::Null(0);
    // One row: the builder's fixed parts, an offset and validity, and eight bytes of text.
    assert_eq!(column.scalar(Scalar::Text("12345678"), 1, &meter), Ok(true));
    let made = BUILDER + 5 + 8;
    assert_eq!(meter.spent(), made);
    assert_eq!(column.scalar(Scalar::Text("abc"), 1, &meter), Ok(true));
    // Past the eight bytes it was sized for, it holds sixteen, charged for the eight more.
    assert_eq!(meter.spent(), made + 8);
    // Within what the growth made, nothing more; past it, it doubles again.
    assert_eq!(column.scalar(Scalar::Text("abcde"), 1, &meter), Ok(true));
    assert_eq!(meter.spent(), made + 8);
    assert_eq!(column.scalar(Scalar::Text("x"), 1, &meter), Ok(true));
    assert_eq!(meter.spent(), made + 8 + 16);
}

#[test]
fn a_list_s_items_past_its_room_are_charged_as_their_builder_doubles() {
    let meter = Meter::new(u64::MAX);
    let mut column =
        Column::new(&Observed::Array(Box::new(Observed::Null), 0), 0, 1, &meter).unwrap();
    let Column::List(list) = &mut column else {
        panic!("a list column");
    };
    let made = meter.spent();
    let (item, capacity) = list.item(&meter).unwrap();
    assert_eq!(item.scalar(Scalar::Int(1), capacity, &meter), Ok(true));
    let first = meter.spent() - made;
    // The item column is sized for one item, the list's row, beside its builder's fixed parts.
    assert_eq!(first, BUILDER + 8 + 1);
    let (item, capacity) = list.item(&meter).unwrap();
    assert_eq!(item.scalar(Scalar::Int(2), capacity, &meter), Ok(true));
    // The second doubles it to two, charged for the slot it grows by.
    assert_eq!(meter.spent() - made - first, 8 + 1);
    list.end_row(2).unwrap();
    assert_eq!(
        list.observed(),
        Observed::Array(Box::new(Observed::Int { exact: true }), 2)
    );
}

#[test]
fn widening_charges_the_wider_builders() {
    let meter = Meter::new(u64::MAX);
    let mut column = Column::Null(0);
    assert_eq!(column.scalar(Scalar::Int(1), 4, &meter), Ok(true));
    let ints = meter.spent();
    assert_eq!(column.scalar(Scalar::Huge(1 << 70), 4, &meter), Ok(true));
    assert_eq!(meter.spent() - ints, BUILDER + 4 * 16 + 1);
    assert_eq!(column.observed(), Observed::Huge);
    let Ok(array) = column.finish() else {
        panic!("a decimal column");
    };
    assert_eq!(array.len(), 2);
}

#[test]
fn a_record_built_against_a_shape_presizes_every_column() {
    let meter = Meter::new(u64::MAX);
    let mut shape = Shape::default();
    shape.push("a".into(), Observed::Float);
    shape.push("l".into(), Observed::Array(Box::new(Observed::Wide), 7));
    let record = Record::new(&shape, 3, &meter).unwrap();
    assert_eq!(record.capacity(), 3);
    // Three floats; three offsets and the first; seven 128-bit items; a validity bit each; and
    // each column's entry and builder's fixed parts.
    let fixed = (KEY + 1) * 2 + BUILDER * 3;
    assert_eq!(
        meter.spent(),
        fixed + (3 * 8 + 1) + (3 * 4 + 1 + 4) + (7 * 16 + 1)
    );
}

#[test]
fn a_column_of_booleans_is_charged_two_bits_a_row() {
    let meter = Meter::new(u64::MAX);
    Column::new(&Observed::Bool, 0, 100, &meter).unwrap();
    // A value's bit and a validity bit for each of the hundred rows.
    assert_eq!(meter.spent(), BUILDER + 25);
}

#[test]
fn a_list_s_booleans_past_its_room_are_charged_two_bits_an_item() {
    let meter = Meter::new(u64::MAX);
    let mut column =
        Column::new(&Observed::Array(Box::new(Observed::Bool), 8), 0, 1, &meter).unwrap();
    let Column::List(list) = &mut column else {
        panic!("a list column");
    };
    let made = meter.spent();
    let mut append = |items: usize| {
        for _ in 0..items {
            let (item, capacity) = list.item(&meter).unwrap();
            assert_eq!(item.scalar(Scalar::Bool(true), capacity, &meter), Ok(true));
        }
        meter.spent() - made
    };
    // The eight items it was sized for take nothing more.
    assert_eq!(append(8), 0);
    // The ninth doubles it to sixteen, charged a value's bit and a validity bit for the eight
    // it grows by; the seventeenth to thirty-two, for sixteen more.
    assert_eq!(append(1), 2);
    assert_eq!(append(7), 2);
    assert_eq!(append(1), 2 + 4);
}

#[test]
fn integers_widen_through_every_whole_decimal_charged_the_wider_builders() {
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Decimal128Type, Decimal256Type};
    let meter = Meter::new(u64::MAX);
    let mut column = Column::Null(0);
    let mut widened = |value: Scalar<'_>, observed: Observed| {
        let before = meter.spent();
        assert_eq!(column.scalar(value, 4, &meter), Ok(true));
        assert_eq!(column.observed(), observed);
        meter.spent() - before
    };
    widened(Scalar::Int(1), Observed::Int { exact: true });
    // Each widening makes builders sized for the four rows: sixteen bytes a decimal of 20 or 38
    // digits, thirty-two of 76, and a validity bit each.
    assert_eq!(
        widened(Scalar::Wide(u64::MAX), Observed::Wide),
        BUILDER + 4 * 16 + 1
    );
    assert_eq!(
        widened(Scalar::Huge(1 << 70), Observed::Huge),
        BUILDER + 4 * 16 + 1
    );
    let vast = arrow_buffer::i256::from_i128(i128::MAX).wrapping_mul(arrow_buffer::i256::from(4));
    assert_eq!(
        widened(Scalar::Vast(vast), Observed::Vast),
        BUILDER + 4 * 32 + 1
    );
    let Ok(array) = column.finish() else {
        panic!("a decimal column");
    };
    let decimals = array.as_primitive::<Decimal256Type>();
    let values: Vec<String> = (0..4).map(|row| decimals.value_as_string(row)).collect();
    assert_eq!(
        values,
        [
            "1".to_owned(),
            u64::MAX.to_string(),
            (1_i128 << 70).to_string(),
            vast.to_string()
        ]
    );
    // From 20 digits to 38 alone, the values are kept as well.
    let mut wide = Column::Null(0);
    assert_eq!(wide.scalar(Scalar::Wide(u64::MAX), 2, &meter), Ok(true));
    assert_eq!(wide.scalar(Scalar::Huge(-1 << 70), 2, &meter), Ok(true));
    let Ok(array) = wide.finish() else {
        panic!("a decimal column");
    };
    let decimals = array.as_primitive::<Decimal128Type>();
    assert_eq!(
        (decimals.value(0), decimals.value(1)),
        (i128::from(u64::MAX), -1 << 70)
    );
}
