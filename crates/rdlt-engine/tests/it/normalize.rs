//! Normalized streams end to end: child tables, lineage, naming and replace.

use std::sync::Arc;

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, Int64Array, ListArray, RecordBatch, StructArray};
use arrow_schema::Field;
use rdlt_connector::{Field as RdltField, Fields, LogicalType, TableSchema};
use rdlt_connector_reference::{published, schema};
use rdlt_engine::{
    ErrorKind, Nested, RunOutcome, RunStatus, SchemaPolicy, SchemaSettings, StreamPlan, WriteMode,
};
use serde_json::{Value, json};

use crate::support::batches::{BatchStream, batches};
use crate::support::{commit_every, engine, memory, pipeline, published_json, stream};

fn normalized(name: &str) -> StreamPlan {
    stream(name).schema(SchemaSettings::new().nested(Nested::normalize()))
}

/// Loads `streams` from `store` as `plans` say, into the memory destination `store`.
async fn load(store: &str, streams: Vec<BatchStream>, plans: Vec<StreamPlan>) -> RunOutcome {
    let source = batches(store, streams).await;
    engine(commit_every(1))
        .run(pipeline(store, plans), source, memory(store).await)
        .await
}

fn succeeded(outcome: &RunOutcome) {
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
}

/// Each published row of `table` in `store`: its lineage column `column` as bytes, by row.
fn lineage(store: &str, table: &str, column: &str) -> Vec<Vec<u8>> {
    published(store, table)
        .iter()
        .flat_map(|batch| {
            let ids = batch
                .column_by_name(column)
                .unwrap_or_else(|| panic!("{table} has {column}"));
            let ids = ids.as_binary::<i32>();
            (0..ids.len())
                .map(|row| ids.value(row).to_vec())
                .collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test(start_paused = true)]
async fn arrays_land_in_child_tables_whose_rows_name_their_parents() {
    let push = [
        r#"{"id":1,"meta":{"a":2},"items":[{"sku":"x","tags":["p","q"]},{"sku":"y"}]}"#,
        r#"{"id":2,"items":[]}"#,
    ]
    .join("\n");
    let outcome = load(
        "normalized",
        vec![BatchStream::json("events", &[&push])],
        vec![normalized("events")],
    )
    .await;
    succeeded(&outcome);
    assert_eq!(
        published_json("normalized", "events"),
        [json!({"id": 1, "meta__a": 2}), json!({"id": 2})]
    );
    assert_eq!(
        published_json("normalized", "events__items"),
        [json!({"sku": "x"}), json!({"sku": "y"})]
    );
    assert_eq!(
        published_json("normalized", "events__items__tags"),
        [json!({"value": "p"}), json!({"value": "q"})]
    );
    let roots = lineage("normalized", "events", "_rdlt_id");
    let parents = lineage("normalized", "events__items", "_rdlt_parent_id");
    assert!(parents.iter().all(|parent| roots.contains(parent)));
    let items = lineage("normalized", "events__items", "_rdlt_id");
    let tags = lineage("normalized", "events__items__tags", "_rdlt_parent_id");
    assert!(tags.iter().all(|parent| items.contains(parent)));
    let tag_roots = lineage("normalized", "events__items__tags", "_rdlt_root_id");
    assert!(tag_roots.iter().all(|root| roots.contains(root)));
    let idx: Vec<i64> = published("normalized", "events__items__tags")
        .iter()
        .flat_map(|batch| {
            let idx = batch
                .column_by_name("_rdlt_idx")
                .expect("child rows have a position");
            idx.as_primitive::<Int64Type>().values().to_vec()
        })
        .collect();
    assert_eq!(idx, [0, 1]);
}

/// D4: child tables alias across parents.
#[tokio::test(start_paused = true)]
async fn child_tables_never_alias_across_parents() {
    let a = [r#"{"b__c":[1]}"#, r#"{"b":{"c":[2]}}"#].join("\n");
    let outcome = load(
        "d4",
        vec![
            BatchStream::json("a", &[&a]),
            BatchStream::json("a__b", &[r#"{"c":[3]}"#]),
        ],
        vec![normalized("a"), normalized("a__b")],
    )
    .await;
    succeeded(&outcome);
    let values = |table: &str| -> Vec<Value> {
        assert!(schema("d4", table).is_some(), "table {table} exists");
        published_json("d4", table)
    };
    assert_eq!(values("a__b_x5f_c"), [json!({"value": 1})], "the key b__c");
    assert_eq!(values("a__b__c"), [json!({"value": 2})], "the path b.c");
    assert_eq!(
        values("a_x5f_b__c"),
        [json!({"value": 3})],
        "stream a__b's c"
    );
}

#[tokio::test(start_paused = true)]
async fn an_array_first_seen_mid_run_adds_its_child_table() {
    let outcome = load(
        "mid_run",
        vec![BatchStream::json(
            "events",
            &[r#"{"id":1}"#, r#"{"id":2,"items":[{"sku":"z"}]}"#],
        )],
        vec![normalized("events")],
    )
    .await;
    succeeded(&outcome);
    assert_eq!(published_json("mid_run", "events").len(), 2);
    assert_eq!(
        published_json("mid_run", "events__items"),
        [json!({"sku": "z"})]
    );
}

#[tokio::test(start_paused = true)]
async fn replace_swaps_a_streams_child_tables_in_with_its_table() {
    let replace = || normalized("events").write(WriteMode::Replace);
    let first = r#"{"id":1,"items":[{"sku":"x"}]}"#;
    succeeded(
        &load(
            "replaced",
            vec![BatchStream::json("events", &[first])],
            vec![replace()],
        )
        .await,
    );
    assert_eq!(published_json("replaced", "events__items").len(), 1);
    succeeded(
        &load(
            "replaced",
            vec![BatchStream::json("events", &[r#"{"id":2}"#])],
            vec![replace()],
        )
        .await,
    );
    assert_eq!(published_json("replaced", "events"), [json!({"id": 2})]);
    assert!(
        published_json("replaced", "events__items").is_empty(),
        "the child table swaps in empty with its stream's"
    );
}

#[tokio::test(start_paused = true)]
async fn ids_are_the_same_whenever_the_same_row_loads() {
    let push = r#"{"id":1,"items":[{"sku":"x"}]}"#;
    let keyed = || BatchStream::json("events", &[push]).primary_key(&["id"]);
    succeeded(&load("stable", vec![keyed()], vec![normalized("events")]).await);
    succeeded(&load("stable", vec![keyed()], vec![normalized("events")]).await);
    let roots = lineage("stable", "events", "_rdlt_id");
    assert_eq!(roots.len(), 2);
    assert_eq!(roots[0], roots[1], "one key, one id, in every run");
    let items = lineage("stable", "events__items", "_rdlt_id");
    assert_eq!(items[0], items[1]);
    assert_eq!(roots[0].len(), 16, "ids are 16 bytes of xxh3-128");
}

#[tokio::test(start_paused = true)]
async fn containers_deeper_than_max_depth_land_as_json() {
    let shallow =
        stream("events").schema(SchemaSettings::new().nested(Nested::Normalize { max_depth: 1 }));
    let push = r#"{"meta":{"b":{"c":1}},"items":[{"sku":"x"}]}"#;
    succeeded(
        &load(
            "shallow",
            vec![BatchStream::json("events", &[push])],
            vec![shallow],
        )
        .await,
    );
    let rows = published_json("shallow", "events");
    assert_eq!(rows.len(), 1);
    let inner: Value = serde_json::from_str(rows[0]["meta__b"].as_str().expect("JSON text"))
        .expect("the object past depth 1 is JSON");
    assert_eq!(inner, json!({"c": 1}));
    let items = published_json("shallow", "events__items");
    let item: Value = serde_json::from_str(items[0]["value"].as_str().expect("JSON text"))
        .expect("the items past depth 1 are JSON");
    assert_eq!(item, json!({"sku": "x"}));
}

#[tokio::test(start_paused = true)]
async fn nested_arrow_batches_normalize_like_json() {
    let skus = Arc::new(arrow_array::StringArray::from(vec!["x", "y"])) as ArrayRef;
    let items = StructArray::try_from(vec![("sku", skus)]).expect("a struct of skus");
    let item = Arc::new(Field::new("item", items.data_type().clone(), true));
    let offsets = arrow_buffer::OffsetBuffer::from_lengths([2]);
    let list = ListArray::new(item, offsets, Arc::new(items), None);
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        ("items", Arc::new(list) as ArrayRef),
    ])
    .expect("a nested batch");
    succeeded(
        &load(
            "arrow_nested",
            vec![BatchStream::new("events", vec![batch])],
            vec![normalized("events")],
        )
        .await,
    );
    assert_eq!(published_json("arrow_nested", "events"), [json!({"id": 1})]);
    assert_eq!(
        published_json("arrow_nested", "events__items"),
        [json!({"sku": "x"}), json!({"sku": "y"})]
    );
}

#[tokio::test(start_paused = true)]
async fn a_normalized_stream_cannot_merge_or_drop_rows_yet() {
    let discard = SchemaSettings::new().policy(SchemaPolicy::DiscardRow);
    let cases = [
        (
            "normalize_merge_unsupported",
            normalized("events").write(WriteMode::Merge).key(["id"]),
        ),
        (
            "normalize_discard_row_unsupported",
            stream("events").schema(discard.nested(Nested::normalize())),
        ),
        (
            "normalize_discard_row_unsupported",
            normalized("events").column("id", discard),
        ),
    ];
    for (index, (code, plan)) in cases.into_iter().enumerate() {
        let outcome = load(
            &format!("{code}_{index}"),
            vec![BatchStream::json("events", &[r#"{"id":1}"#])],
            vec![plan],
        )
        .await;
        let error = outcome.error.expect("the run fails");
        assert_eq!(
            (error.kind(), error.code()),
            (ErrorKind::Config, Some(code))
        );
    }
}

#[tokio::test(start_paused = true)]
async fn streams_after_one_with_child_tables_load_their_own_tables() {
    let streams = || {
        vec![
            BatchStream::json("a", &[r#"{"id":1,"items":[{"sku":"x"}]}"#]),
            BatchStream::json("b", &[r#"{"id":2}"#]),
        ]
    };
    for _ in 0..2 {
        succeeded(
            &load(
                "siblings",
                streams(),
                vec![normalized("a"), normalized("b")],
            )
            .await,
        );
    }
    assert_eq!(
        published_json("siblings", "a"),
        [json!({"id": 1}), json!({"id": 1})]
    );
    assert_eq!(
        published_json("siblings", "b"),
        [json!({"id": 2}), json!({"id": 2})]
    );
    assert_eq!(published_json("siblings", "a__items").len(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_normalized_streams_report_counts_its_child_tables_rows() {
    let push = r#"{"id":1,"items":[{"sku":"x"},{"sku":"y"}]}"#;
    let outcome = load(
        "reported",
        vec![BatchStream::json("events", &[push])],
        vec![normalized("events")],
    )
    .await;
    succeeded(&outcome);
    assert_eq!(
        outcome.report.streams["events"].rows, 3,
        "one row and two items"
    );
}

#[tokio::test(start_paused = true)]
async fn a_column_set_to_json_in_a_normalized_stream_stays_whole() {
    let plan = normalized("events").column("meta", SchemaSettings::new().nested(Nested::Json));
    let push = r#"{"id":1,"meta":{"a":[1,2]}}"#;
    succeeded(
        &load(
            "whole",
            vec![BatchStream::json("events", &[push])],
            vec![plan],
        )
        .await,
    );
    let rows = published_json("whole", "events");
    let meta: Value = serde_json::from_str(rows[0]["meta"].as_str().expect("JSON text"))
        .expect("the column holds its JSON");
    assert_eq!(meta, json!({"a": [1, 2]}));
    assert!(
        schema("whole", "events__meta__a").is_none(),
        "no child table"
    );
}

#[tokio::test(start_paused = true)]
async fn a_declared_schema_creates_a_normalized_streams_columns_before_any_row() {
    let fields = |fields: Vec<RdltField>| LogicalType::Struct(Fields::new(fields).unwrap());
    let meta = fields(vec![
        RdltField::new("a", LogicalType::Int64, true),
        RdltField::new(
            "b",
            fields(vec![RdltField::new("c", LogicalType::Utf8, true)]),
            true,
        ),
    ]);
    let item = fields(vec![RdltField::new("sku", LogicalType::Utf8, true)]);
    let declared = TableSchema::new(vec![
        RdltField::new("id", LogicalType::Int64, false),
        RdltField::new("meta", meta, true),
        RdltField::new(
            "items",
            LogicalType::List(Box::new(RdltField::new("item", item, true))),
            true,
        ),
    ])
    .unwrap();
    let cases: [(&str, u8, &[&str]); 3] = [
        ("declared_deep", 8, &["id", "meta__a", "meta__b__c"]),
        ("declared_shallow", 1, &["id", "meta__a", "meta__b"]),
        ("declared_whole", 0, &["id", "meta", "items"]),
    ];
    for (store, max_depth, expected) in cases {
        let plan =
            stream("events").schema(SchemaSettings::new().nested(Nested::Normalize { max_depth }));
        let source = BatchStream::json("events", &[]).declared(declared.clone());
        succeeded(&load(store, vec![source], vec![plan]).await);
        let created = schema(store, "events").expect("the table is created");
        let columns: Vec<&str> = created
            .fields()
            .iter()
            .map(RdltField::name)
            .filter(|name| !name.starts_with("_rdlt_"))
            .collect();
        assert_eq!(columns, expected, "{store}");
        assert!(
            schema(store, "events__items").is_none(),
            "arrays wait for rows"
        );
    }
}
