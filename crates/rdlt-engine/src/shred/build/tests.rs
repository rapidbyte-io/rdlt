use super::{Column, Record, Scalar};
use crate::shred::meter::{Columns, Meter, Over};
use crate::shred::observe::{Observed, Shape};

fn columns() -> Columns {
    Columns::new(u64::MAX)
}

#[test]
fn a_column_a_row_adds_is_sized_for_the_whole_chunk_and_charged_for_it() {
    let meter = Meter::new(u64::MAX);
    let mut record = Record::empty(100);
    let position = record.position("a", 0, &columns()).unwrap();
    let capacity = record.capacity();
    let column = record.field(position).unwrap();
    assert_eq!(column.scalar(Scalar::Int(1), capacity, &meter), Ok(true));
    record.end_row(1, &meter).unwrap();
    let Column::Int { builder, .. } = &record.columns[position] else {
        panic!("an integer column");
    };
    assert!(builder.capacity() >= 100, "{}", builder.capacity());
    // A value and its validity bit, for each of the hundred rows.
    assert_eq!(meter.spent(), 100 * 8 + 13);
}

#[test]
fn a_builder_the_meter_has_no_room_for_is_never_made() {
    let meter = Meter::new(100 * 8 + 13 - 1);
    let mut column = Column::Null(0);
    assert_eq!(column.scalar(Scalar::Int(1), 100, &meter), Err(Over));
    assert!(meter.tripped());
    assert!(matches!(column, Column::Null(0)), "the column is unchanged");
    let meter = Meter::new(100 * 8 + 13);
    assert_eq!(column.scalar(Scalar::Int(1), 100, &meter), Ok(true));
    assert_eq!(meter.spent(), 100 * 8 + 13);
}

#[test]
fn rows_past_what_a_record_was_sized_for_are_charged_as_its_builders_double() {
    let meter = Meter::new(u64::MAX);
    let mut record = Record::empty(2);
    for row in 0..2 {
        let position = record.position("a", 0, &columns()).unwrap();
        let capacity = record.capacity();
        let column = record.field(position).unwrap();
        assert_eq!(column.scalar(Scalar::Int(row), capacity, &meter), Ok(true));
        record.end_row(1, &meter).unwrap();
    }
    let sized = meter.spent();
    assert_eq!(sized, 2 * 8 + 1);
    // The third row doubles the builders to four rows, charged whole: a value and two bits, the
    // column's validity and the record's, a row.
    let position = record.position("a", 0, &columns()).unwrap();
    let column = record.field(position).unwrap();
    assert_eq!(column.scalar(Scalar::Int(3), 2, &meter), Ok(true));
    record.end_row(1, &meter).unwrap();
    assert_eq!(meter.spent() - sized, 4 * 8 + 1);
    assert_eq!(record.capacity(), 4);
    // A record with no room to grow trips its meter instead.
    let mut full = Record::empty(0);
    assert_eq!(full.end_row(0, &Meter::new(0)), Err(Over));
}

#[test]
fn text_past_what_its_builder_was_sized_for_is_charged_as_it_doubles() {
    let meter = Meter::new(u64::MAX);
    let mut column = Column::Null(0);
    // One row: an offset and validity, and eight bytes of text.
    assert_eq!(column.scalar(Scalar::Text("12345678"), 1, &meter), Ok(true));
    assert_eq!(meter.spent(), 5 + 8);
    assert_eq!(column.scalar(Scalar::Text("abc"), 1, &meter), Ok(true));
    // Past the eight bytes it was sized for, it holds sixteen, all charged.
    assert_eq!(meter.spent(), 5 + 8 + 16);
    // Within what the growth made, nothing more; past it, it doubles again.
    assert_eq!(column.scalar(Scalar::Text("abcde"), 1, &meter), Ok(true));
    assert_eq!(meter.spent(), 5 + 8 + 16);
    assert_eq!(column.scalar(Scalar::Text("x"), 1, &meter), Ok(true));
    assert_eq!(meter.spent(), 5 + 8 + 16 + 32);
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
    // The item column is sized for one item, the list's row.
    assert_eq!(first, 8 + 1);
    let (item, capacity) = list.item(&meter).unwrap();
    assert_eq!(item.scalar(Scalar::Int(2), capacity, &meter), Ok(true));
    assert_eq!(meter.spent() - made - first, 2 * 8 + 1);
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
    assert_eq!(meter.spent() - ints, 4 * 16 + 1);
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
    // Three floats; three offsets and the first; seven 128-bit items; a validity bit each.
    assert_eq!(meter.spent(), (3 * 8 + 1) + (3 * 4 + 1 + 4) + (7 * 16 + 1));
}
