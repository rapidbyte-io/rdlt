use std::sync::Arc;

use arrow_array::cast::AsArray as _;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use proptest::prelude::*;

use super::{Literal, Op};
use crate::transform::{Transform, allocated_beyond};

fn people(rows: usize) -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(
        0..i64::try_from(rows).unwrap(),
    ));
    let notes: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..rows).map(|row| format!("note {row}")),
    ));
    RecordBatch::try_from_iter([("id", ids), ("note", notes)]).unwrap()
}

#[test]
fn a_rename_renames_top_level_columns_and_shares_their_arrays() {
    let input = people(4);
    let op = Op::rename([("note", "remark")]);
    let output = op.apply(input.clone()).unwrap();
    let names: Vec<&str> = output
        .schema_ref()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(names, ["id", "remark"]);
    assert!(Arc::ptr_eq(output.column(1), input.column(1)));
    assert_eq!(op.output(input.schema_ref()).unwrap(), output.schema());
}

#[test]
fn ops_pass_on_a_batch_lacking_the_columns_they_name() {
    let input = people(3);
    for op in [
        Op::rename([("absent", "other")]),
        Op::constant(Vec::<(String, Literal)>::new()),
    ] {
        let output = op.apply(input.clone()).unwrap();
        assert_eq!(output.schema(), input.schema());
        assert_eq!(output.num_rows(), 3);
    }
}

#[test]
fn a_rename_onto_a_column_the_batch_holds_is_refused_naming_it() {
    let refused = Op::rename([("note", "id")])
        .output(people(1).schema_ref())
        .unwrap_err();
    assert_eq!(refused.column(), Some("id"));
}

#[test]
fn a_constant_holds_its_literal_in_every_row() {
    let input = people(5);
    let cases = [
        (Literal::Text("eu".to_owned()), arrow_schema::DataType::Utf8),
        (Literal::Integer(7), arrow_schema::DataType::Int64),
        (Literal::Boolean(true), arrow_schema::DataType::Boolean),
    ];
    for (literal, data_type) in cases {
        let output = Op::constant([("region", literal.clone())])
            .apply(input.clone())
            .unwrap();
        let column = output.column_by_name("region").unwrap();
        assert_eq!(column.data_type(), &data_type);
        assert_eq!((column.len(), column.null_count()), (5, 0));
        match literal {
            Literal::Text(text) => assert!(
                column
                    .as_string::<i32>()
                    .iter()
                    .all(|v| v == Some(text.as_str()))
            ),
            Literal::Integer(value) => assert!(
                column
                    .as_primitive::<Int64Type>()
                    .values()
                    .iter()
                    .all(|v| *v == value)
            ),
            Literal::Boolean(value) => {
                assert_eq!(column.as_boolean().true_count(), if value { 5 } else { 0 });
            }
        }
    }
}

#[test]
fn an_op_reports_each_column_read_from_its_own_place_and_a_constant_from_none() {
    let input = people(2);
    let renamed = Op::rename([("note", "remark")]).lineage(input.schema_ref());
    assert_eq!(renamed, Some(vec![vec![0], vec![1]]));
    let constant = Op::constant([("c", Literal::Integer(1))]).lineage(input.schema_ref());
    assert_eq!(constant, Some(vec![vec![0], vec![1], vec![]]));
}

#[test]
fn a_constant_meeting_a_column_the_batch_holds_is_refused_naming_it() {
    let refused = Op::constant([("note", Literal::Integer(1))])
        .output(people(1).schema_ref())
        .unwrap_err();
    assert_eq!(refused.column(), Some("note"));
}

proptest! {
    #[test]
    fn renames_and_constants_allocate_within_their_bounds(rows in 0_usize..3000, text in "\\PC{0,64}") {
        let input = people(rows);
        let ops = [
            Op::rename([("note", "remark")]),
            Op::constant([("a", Literal::Text(text)), ("b", Literal::Integer(1)), ("c", Literal::Boolean(false))]),
        ];
        for op in ops {
            let bound = op.bound(input.schema_ref()).expect("an op declares its bound");
            let output = op.apply(input.clone()).unwrap();
            prop_assert!(allocated_beyond(&output, &input) <= bound.of(&input));
        }
    }
}
