use arrow_schema::{DataType, Field as ArrowField, Schema};

use super::{ColumnPath, EmptyColumnPath, SchemaError, TableSchema};
use crate::types::{Field, LogicalType, TypeError};

fn orders() -> TableSchema {
    TableSchema::new(vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("note", LogicalType::Utf8, true),
    ])
    .unwrap()
}

#[test]
fn schemas_round_trip_through_arrow() {
    let schema = orders();
    assert_eq!(TableSchema::from_arrow(&schema.to_arrow()).unwrap(), schema);
    assert_eq!(
        schema.field("note").unwrap().logical_type(),
        &LogicalType::Utf8
    );
    assert!(schema.field("missing").is_none());
}

#[test]
fn arrow_schemas_with_duplicate_or_unsupported_fields_are_refused() {
    let duplicate = Schema::new(vec![
        ArrowField::new("a", DataType::Int8, true),
        ArrowField::new("a", DataType::Int8, true),
    ]);
    assert_eq!(
        TableSchema::from_arrow(&duplicate),
        Err(SchemaError::Type(TypeError::DuplicateField {
            name: "a".to_owned()
        }))
    );
    let union = Schema::new(vec![ArrowField::new(
        "u",
        DataType::Union(
            arrow_schema::UnionFields::empty(),
            arrow_schema::UnionMode::Sparse,
        ),
        true,
    )]);
    assert!(matches!(
        TableSchema::from_arrow(&union),
        Err(SchemaError::Unsupported(_))
    ));
}

#[test]
fn column_paths_need_a_segment_and_display_with_dots() {
    assert_eq!(
        ColumnPath::new(["profile", "email"]).unwrap().to_string(),
        "profile.email"
    );
    assert_eq!(
        ColumnPath::from("id").segments().collect::<Vec<_>>(),
        ["id"]
    );
    assert_eq!(ColumnPath::new(Vec::<String>::new()), Err(EmptyColumnPath));
    assert!(serde_json::from_str::<ColumnPath>("[]").is_err());
}
