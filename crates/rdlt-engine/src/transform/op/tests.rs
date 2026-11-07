use std::sync::Arc;

use arrow_array::cast::AsArray as _;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use proptest::prelude::*;
use rdlt_connector::ColumnPath;

use super::{Literal, Op};
use crate::transform::{Mask, Transform, allocated_beyond};

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
fn an_op_names_the_top_level_columns_it_reads_and_the_names_it_gives() {
    let nested = ColumnPath::new(["s", "inner"]).unwrap();
    let cases = [
        (
            Op::rename([("a", "b"), ("c", "d")]),
            vec!["a", "c"],
            vec!["b", "d"],
        ),
        (
            Op::constant([("x", Literal::Integer(1)), ("y", Literal::Boolean(true))]),
            vec![],
            vec!["x", "y"],
        ),
        (
            Op::mask([nested, ColumnPath::from("t")], Mask::Null),
            vec!["s", "t"],
            vec![],
        ),
    ];
    for (op, reads, writes) in cases {
        assert_eq!(
            (op.reads(), op.writes()),
            (strings(&reads), strings(&writes)),
            "{op:?}"
        );
    }
}

fn strings(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| (*name).to_owned()).collect()
}

#[test]
fn a_literal_takes_its_value_in_bytes_a_row() {
    let cases = [
        (Literal::Text("four".to_owned()), 4),
        (Literal::Integer(-1), 8),
        (Literal::Boolean(false), 1),
    ];
    for (literal, bytes) in cases {
        assert_eq!(literal.bytes(), bytes, "{literal:?}");
    }
}

#[test]
fn an_op_is_named_for_what_it_does() {
    let names = [
        (Op::rename([("a", "b")]), "rename"),
        (Op::constant([("a", Literal::Integer(1))]), "constant"),
        (Op::mask(["a"], Mask::Null), "mask"),
    ];
    for (op, name) in names {
        assert_eq!(op.name(), name);
    }
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

#[test]
fn a_constant_of_more_text_than_a_column_holds_is_refused_naming_it() {
    let options = arrow_array::RecordBatchOptions::new().with_row_count(Some(600_000));
    let rows = RecordBatch::try_new_with_options(
        Arc::new(arrow_schema::Schema::empty()),
        Vec::new(),
        &options,
    )
    .unwrap();
    let text = Literal::Text("x".repeat(4096));
    let refused = Op::constant([("note", text)]).apply(rows).unwrap_err();
    assert_eq!(refused.column(), Some("note"));
}

#[test]
fn text_room_is_what_32_bit_offsets_reach() {
    use super::text_room;
    let most = usize::try_from(i32::MAX).unwrap();
    assert_eq!(text_room(most, 1, "c"), Ok(most));
    assert_eq!(text_room(most + 1, 1, "c").unwrap_err().column(), Some("c"));
    assert!(text_room(usize::MAX, 2, "c").is_err());
    assert!(text_room(40_000_000, 64, "c").is_err());
}

#[test]
fn an_op_says_which_column_it_names_a_schema_lacks() {
    use arrow_schema::{DataType, Field, Fields, Schema};

    let inner = Fields::from(vec![Field::new("inner", DataType::Utf8, true)]);
    let schema = Schema::new(vec![
        Field::new("note", DataType::Utf8, true),
        Field::new("s", DataType::Struct(inner), true),
    ]);
    let path = |dotted: &str| ColumnPath::new(dotted.split('.')).unwrap();
    let cases = [
        (Op::rename([("note", "remark")]), None),
        (Op::rename([("nite", "remark")]), Some("nite")),
        (Op::rename([("note", "remark"), ("x", "y")]), Some("x")),
        (Op::constant([("c", Literal::Integer(1))]), None),
        (Op::mask([path("s.inner"), path("note")], Mask::Null), None),
        (
            Op::mask([path("note"), path("s.absent")], Mask::Null),
            Some("s.absent"),
        ),
        (Op::mask([path("absent")], Mask::Null), Some("absent")),
        (Op::mask([path("note.below")], Mask::Null), None),
    ];
    for (op, lacks) in cases {
        assert_eq!(op.lacks(&schema).as_deref(), lacks, "{op:?}");
    }
}
