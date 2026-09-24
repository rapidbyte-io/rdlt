use super::{Column, Record, Scalar};

#[test]
fn a_column_a_row_adds_is_sized_for_the_whole_chunk() {
    let mut record = Record::empty(100);
    let position = record.position("a", 0);
    let capacity = record.capacity();
    let column = record.field(position).unwrap();
    assert!(column.scalar(Scalar::Int(1), capacity));
    record.end_row(1);
    let Column::Int { builder, .. } = &record.columns[position] else {
        panic!("an integer column");
    };
    assert!(builder.capacity() >= 100, "{}", builder.capacity());
}
