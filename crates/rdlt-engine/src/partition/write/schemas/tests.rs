use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use rdlt_connector::{ColumnPath, LogicalType};

use super::Schemas;

fn schema(doc: Field) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        doc,
    ]))
}

#[test]
fn a_schema_equal_to_the_last_one_is_converted_once() {
    let text = Field::new("doc", DataType::Utf8, true);
    let (first, again) = (schema(text.clone()), schema(text));
    assert!(!Arc::ptr_eq(&first, &again));
    let mut schemas = Schemas::default();
    let converted = schemas.of(&first).unwrap();
    assert_eq!(schemas.of(&again).unwrap(), converted);
    assert_eq!(schemas.of(&first).unwrap(), converted);
    assert_eq!(schemas.misses, 1);
    assert_eq!(
        converted.1[..],
        [ColumnPath::from("id"), ColumnPath::from("doc")]
    );
}

#[test]
fn a_schema_converted_last_is_shared_not_copied() {
    let text = Field::new("doc", DataType::Utf8, true);
    let (first, again) = (schema(text.clone()), schema(text));
    let mut schemas = Schemas::default();
    let (schema, paths) = schemas.of(&first).unwrap();
    let (peak, (shared, shared_paths)) =
        crate::cost::tests::peak_alone(|| schemas.of(&again).unwrap());
    assert_eq!(peak, 0, "a hit allocates nothing");
    assert!(Arc::ptr_eq(&schema, &shared));
    assert!(Arc::ptr_eq(&paths, &shared_paths));
}

#[test]
fn a_schema_differing_in_metadata_is_converted_again() {
    let text = Field::new("doc", DataType::Utf8, true);
    let extension = [("ARROW:extension:name".to_owned(), "arrow.json".to_owned())];
    let json = text.clone().with_metadata(extension.into());
    let mut schemas = Schemas::default();
    let (plain, _) = schemas.of(&schema(text)).unwrap();
    let (marked, _) = schemas.of(&schema(json)).unwrap();
    assert_eq!(
        plain.field("doc").unwrap().logical_type(),
        &LogicalType::Utf8
    );
    assert_eq!(
        marked.field("doc").unwrap().logical_type(),
        &LogicalType::Json
    );
    assert_eq!(schemas.misses, 2);
}

#[test]
fn a_schema_with_no_table_schema_is_refused_and_not_kept() {
    let union =
        arrow_schema::UnionFields::try_new([0], [Field::new("u", DataType::Utf8, true)]).unwrap();
    let refused = schema(Field::new(
        "doc",
        DataType::Union(union, arrow_schema::UnionMode::Sparse),
        true,
    ));
    let mut schemas = Schemas::default();
    assert!(schemas.of(&refused).is_err());
    assert!(schemas.of(&refused).is_err());
    assert_eq!(schemas.misses, 2);
}

#[test]
fn the_stage_s_chains_are_kept_for_the_input_schemas_met_last() {
    use rdlt_connector::StreamName;

    use crate::plan::StreamPlan;
    use crate::transform::stage::Stage;

    let plan = StreamPlan::new(StreamName::new("s").unwrap()).include_only(["id"]);
    let stage = Stage::new(&plan, &[], None).unwrap();
    let inputs: Vec<Arc<Schema>> = (0..=super::CHAINS)
        .map(|n| schema(Field::new(format!("doc{n}"), DataType::Utf8, true)))
        .collect();
    let mut schemas = Schemas::default();
    for input in &inputs {
        assert!(schemas.chain(input).is_none());
        schemas.keep_chain(Arc::new(stage.chain(input).unwrap()));
    }
    // The oldest was let go to keep the newest.
    assert!(schemas.chain(&inputs[0]).is_none());
    for input in &inputs[1..] {
        let kept = schemas
            .chain(input)
            .expect("a chain is kept for each newer schema");
        assert!(Arc::ptr_eq(kept.input(), input));
    }
    let equal = schema(Field::new(
        format!("doc{}", super::CHAINS),
        DataType::Utf8,
        true,
    ));
    let kept = schemas
        .chain(&equal)
        .expect("an equal schema finds its chain");
    assert!(Arc::ptr_eq(kept.input(), &inputs[super::CHAINS]));
}
