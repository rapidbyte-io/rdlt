//! Normalized streams end to end: child tables, lineage, naming and replace.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{ArrayRef, Int64Array, ListArray, RecordBatch, StringArray, StructArray};
use arrow_schema::Field;
use rdlt_connector::{Destination, Field as RdltField, Fields, LogicalType, TableSchema};
use rdlt_connector_reference::{published, schema};
use rdlt_engine::{
    BatchPolicy, ErrorKind, Nested, ResetScope, RunOutcome, RunStatus, SchemaPolicy,
    SchemaSettings, StreamPlan, WriteMode, codes,
};
use serde_json::{Value, json};

use crate::support::batches::{BatchStream, batches};
use crate::support::destinations::limited;
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
    let idx: Vec<i32> = published("normalized", "events__items__tags")
        .iter()
        .flat_map(|batch| {
            let idx = batch
                .column_by_name("_rdlt_idx")
                .expect("child rows have a position");
            idx.as_primitive::<Int32Type>().values().to_vec()
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
async fn a_merge_roots_id_is_its_keys_in_every_run_and_every_other_rows_its_position() {
    let push = r#"{"id":1,"items":[{"sku":"x"}]}"#;
    let merged = || normalized("events").write(WriteMode::Merge).key(["id"]);
    let pushed = || BatchStream::json("events", &[push]);
    succeeded(&load("stable", vec![pushed()], vec![merged()]).await);
    let first = lineage("stable", "events", "_rdlt_id");
    let first_items = lineage("stable", "events__items", "_rdlt_id");
    succeeded(&load("stable", vec![pushed()], vec![merged()]).await);
    let roots = lineage("stable", "events", "_rdlt_id");
    assert_eq!(
        (first.len(), roots.len()),
        (1, 1),
        "the merge keeps one row"
    );
    assert_eq!(first, roots, "one key, one id, in every run");
    assert_eq!(
        roots[0].len(),
        32,
        "a merge root's id is the 32 bytes of its key's hash"
    );
    let items = lineage("stable", "events__items", "_rdlt_id");
    assert_eq!(items.len(), 1, "the merge replaced its root's children");
    assert_ne!(
        first_items, items,
        "a child's id is its position, of its session"
    );
    let item_roots = lineage("stable", "events__items", "_rdlt_root_id");
    assert_eq!(item_roots, roots);
}

#[tokio::test(start_paused = true)]
async fn a_merge_into_roots_identified_by_position_is_refused_until_the_streams_tables_reset() {
    let store = "identity";
    let push = r#"{"id":1,"items":[{"sku":"x"}]}"#;
    let pushed = || vec![BatchStream::json("events", &[push]).primary_key(&["id"])];
    let merged = || vec![normalized("events").write(WriteMode::Merge)];
    // A load that does not merge takes a merged table, and identifies its rows by position.
    succeeded(&load(store, pushed(), merged()).await);
    succeeded(&load(store, pushed(), vec![normalized("events")]).await);
    let refused = load(store, pushed(), merged()).await;
    assert_eq!(refused.report.status, RunStatus::Failed);
    let error = refused.error.expect("the merge is refused");
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Config, Some(codes::TABLE_IDENTITY_MISMATCH))
    );
    assert_eq!(published_json(store, "events").len(), 2, "nothing changed");
    let source = batches(store, pushed()).await;
    engine(commit_every(1))
        .reset(
            store,
            &["events"],
            ResetScope::Tables,
            source,
            memory(store).await,
        )
        .await
        .expect("the reset commits");
    succeeded(&load(store, pushed(), merged()).await);
    assert_eq!(published_json(store, "events"), [json!({"id": 1})]);
    assert_eq!(
        published_json(store, "events__items"),
        [json!({"sku": "x"})]
    );
}

#[tokio::test(start_paused = true)]
async fn rows_loaded_twice_without_a_merge_are_identified_apart() {
    let push = r#"{"id":1,"items":[{"sku":"x"}]}"#;
    let pushed = || BatchStream::json("events", &[push]).primary_key(&["id"]);
    succeeded(&load("twice", vec![pushed()], vec![normalized("events")]).await);
    succeeded(&load("twice", vec![pushed()], vec![normalized("events")]).await);
    for (table, rows) in [("events", 2), ("events__items", 2)] {
        let ids = lineage("twice", table, "_rdlt_id");
        assert_eq!(ids.len(), rows, "{table}");
        assert_ne!(
            ids[0], ids[1],
            "{table}: each load's rows have ids of their own"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn each_version_of_a_key_appended_names_only_its_own_children() {
    let versions = [
        r#"{"id":7,"v":1,"items":[{"sku":"a"},{"sku":"b"}]}"#,
        r#"{"id":7,"v":2,"items":[{"sku":"c"}]}"#,
    ]
    .join("\n");
    let pushed = BatchStream::json("events", &[&versions]).primary_key(&["id"]);
    succeeded(&load("versions", vec![pushed], vec![normalized("events")]).await);
    let roots: Vec<(Vec<u8>, i64)> = published("versions", "events")
        .iter()
        .flat_map(|batch| {
            let ids = batch.column_by_name("_rdlt_id").expect("roots have ids");
            let ids = ids.as_binary::<i32>();
            let v = batch.column_by_name("v").expect("a version");
            let v = v.as_primitive::<Int64Type>();
            (0..batch.num_rows())
                .map(|row| (ids.value(row).to_vec(), v.value(row)))
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(roots.len(), 2);
    assert_ne!(roots[0].0, roots[1].0, "two versions of one key, two ids");
    let parents = lineage("versions", "events__items", "_rdlt_parent_id");
    let skus = published_json("versions", "events__items");
    let joined = |version: i64| -> Vec<Value> {
        let (root, _) = roots
            .iter()
            .find(|(_, v)| *v == version)
            .expect("the version");
        parents
            .iter()
            .zip(&skus)
            .filter(|(parent, _)| *parent == root)
            .map(|(_, sku)| sku.clone())
            .collect()
    };
    assert_eq!(joined(1), [json!({"sku": "a"}), json!({"sku": "b"})]);
    assert_eq!(joined(2), [json!({"sku": "c"})]);
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
    let skus = Arc::new(StringArray::from(vec!["x", "y"])) as ArrayRef;
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

fn dropping(name: &str) -> StreamPlan {
    stream(name).schema(
        SchemaSettings::new()
            .nested(Nested::normalize())
            .policy(SchemaPolicy::DiscardRow),
    )
}

#[tokio::test(start_paused = true)]
async fn a_dropped_row_takes_its_children_with_it() {
    let first = r#"{"id":1,"items":[{"sku":"a","tags":["t"]}]}"#;
    let events = || vec![dropping("events")];
    succeeded(
        &load(
            "dropped",
            vec![BatchStream::json("events", &[first])],
            events(),
        )
        .await,
    );
    let second = [
        r#"{"id":2,"extra":1,"items":[{"sku":"b","tags":["u"]}]}"#,
        r#"{"id":3,"items":[{"sku":"c","n":1,"tags":["v"]},{"sku":"d"}]}"#,
    ]
    .join("\n");
    let outcome = load(
        "dropped",
        vec![BatchStream::json("events", &[&second])],
        events(),
    )
    .await;
    succeeded(&outcome);
    let ids: Vec<Value> = published_json("dropped", "events")
        .iter()
        .map(|row| row["id"].clone())
        .collect();
    assert_eq!(ids, [json!(1), json!(3)]);
    let skus: Vec<Value> = published_json("dropped", "events__items")
        .iter()
        .map(|row| row["sku"].clone())
        .collect();
    assert_eq!(skus, [json!("a"), json!("d")], "c carried a new column");
    assert_eq!(published_json("dropped", "events__items__tags").len(), 1);
    assert_eq!(
        outcome.report.streams["events"].discarded_rows, 5,
        "row 2, its item and tag, item c and its tag"
    );
}

#[tokio::test(start_paused = true)]
async fn a_new_array_drops_the_rows_holding_it() {
    let events = || vec![dropping("events")];
    let first = r#"{"id":1,"items":[{"sku":"a"}]}"#;
    succeeded(
        &load(
            "new_array_rows",
            vec![BatchStream::json("events", &[first])],
            events(),
        )
        .await,
    );
    let second = [
        r#"{"id":2,"items":[{"sku":"b","tags":["t"]}]}"#,
        r#"{"id":3,"items":[{"sku":"c"}]}"#,
    ]
    .join("\n");
    let outcome = load(
        "new_array_rows",
        vec![BatchStream::json("events", &[&second])],
        events(),
    )
    .await;
    succeeded(&outcome);
    assert!(schema("new_array_rows", "events__items__tags").is_none());
    let skus: Vec<Value> = published_json("new_array_rows", "events__items")
        .iter()
        .map(|row| row["sku"].clone())
        .collect();
    assert_eq!(skus, [json!("a"), json!("c")], "item b held the new array");
    assert_eq!(published_json("new_array_rows", "events").len(), 3);
    assert_eq!(outcome.report.streams["events"].discarded_rows, 1);
}

#[tokio::test(start_paused = true)]
async fn a_new_array_follows_the_policy_once_the_streams_table_exists_in_the_load() {
    let frozen = stream("events").schema(
        SchemaSettings::new()
            .nested(Nested::normalize())
            .policy(SchemaPolicy::Freeze),
    );
    let pushes = [r#"{"id":1}"#, r#"{"id":2,"items":[{"sku":"x"}]}"#];
    let outcome = load(
        "created_in_load",
        vec![BatchStream::json("events", &pushes)],
        vec![frozen],
    )
    .await;
    let error = outcome
        .error
        .expect("the table the first push created is frozen");
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Schema, Some(codes::SCHEMA_FROZEN))
    );
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

/// A table's data columns by name.
type Columns = &'static [&'static str];

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
    let cases: [(&str, u8, Columns, Option<Columns>); 3] = [
        (
            "declared_deep",
            8,
            &["id", "meta__a", "meta__b__c"],
            Some(&["sku"]),
        ),
        (
            "declared_shallow",
            1,
            &["id", "meta__a", "meta__b"],
            Some(&["value"]),
        ),
        ("declared_whole", 0, &["id", "meta", "items"], None),
    ];
    let data = |schema: TableSchema| -> Vec<String> {
        schema
            .fields()
            .iter()
            .map(|field| field.name().to_owned())
            .filter(|name| !name.starts_with("_rdlt_"))
            .collect()
    };
    for (store, max_depth, expected, items) in cases {
        let plan =
            stream("events").schema(SchemaSettings::new().nested(Nested::Normalize { max_depth }));
        let source = BatchStream::json("events", &[]).declared(declared.clone());
        succeeded(&load(store, vec![source], vec![plan]).await);
        let created = schema(store, "events").expect("the table is created");
        assert_eq!(data(created), expected, "{store}");
        let child = schema(store, "events__items").map(data);
        let items = items.map(|items| items.iter().map(|item| (*item).to_owned()).collect());
        assert_eq!(child, items, "{store}: an array within depth has its table");
    }
}

#[tokio::test(start_paused = true)]
async fn settings_on_a_streams_columns_stay_on_its_own_table() {
    let plan = normalized("events").hint("sku", LogicalType::Int64);
    let push = r#"{"sku":1,"items":[{"sku":"abc"}]}"#;
    succeeded(
        &load(
            "scoped",
            vec![BatchStream::json("events", &[push])],
            vec![plan],
        )
        .await,
    );
    assert_eq!(published_json("scoped", "events"), [json!({"sku": 1})]);
    assert_eq!(
        published_json("scoped", "events__items"),
        [json!({"sku": "abc"})],
        "the hint on events.sku leaves events.items.sku alone"
    );
}

#[tokio::test(start_paused = true)]
async fn a_new_array_follows_the_streams_policy_once_its_table_exists() {
    let with_policy = |policy| {
        stream("events").schema(
            SchemaSettings::new()
                .nested(Nested::normalize())
                .policy(policy),
        )
    };
    let second = r#"{"id":2,"items":[{"sku":"x"},{"sku":"y"}]}"#;
    for policy in [SchemaPolicy::Freeze, SchemaPolicy::DiscardValue] {
        let store = format!("new_array_{policy:?}").to_lowercase();
        let first = BatchStream::json("events", &[r#"{"id":1}"#]);
        succeeded(&load(&store, vec![first], vec![with_policy(policy)]).await);
        let outcome = load(
            &store,
            vec![BatchStream::json("events", &[second])],
            vec![with_policy(policy)],
        )
        .await;
        assert!(schema(&store, "events__items").is_none(), "{policy:?}");
        if policy == SchemaPolicy::Freeze {
            let error = outcome.error.expect("a frozen table refuses a new array");
            assert_eq!(
                (error.kind(), error.code()),
                (ErrorKind::Schema, Some(codes::SCHEMA_FROZEN))
            );
        } else {
            succeeded(&outcome);
            assert_eq!(published_json(&store, "events").len(), 2);
            assert_eq!(outcome.report.streams["events"].discarded_values, 2);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_stream_that_evolves_may_normalize_in_a_pipeline_that_drops_rows() {
    let source = batches(
        "overridden",
        vec![BatchStream::json("events", &[r#"{"id":1}"#])],
    )
    .await;
    let plan = pipeline(
        "overridden",
        [stream("events").schema(
            SchemaSettings::new()
                .nested(Nested::normalize())
                .policy(SchemaPolicy::Evolve),
        )],
    )
    .schema(SchemaSettings::new().policy(SchemaPolicy::DiscardRow));
    let outcome = engine(commit_every(1))
        .run(plan, source, memory("overridden").await)
        .await;
    succeeded(&outcome);
}

#[tokio::test(start_paused = true)]
async fn a_frozen_stream_keeps_loading_the_arrays_it_has() {
    let frozen = || {
        stream("events").schema(
            SchemaSettings::new()
                .nested(Nested::normalize())
                .policy(SchemaPolicy::Freeze),
        )
    };
    for id in [1, 2] {
        let push = format!(r#"{{"id":{id},"items":[{{"sku":"x"}}]}}"#);
        let outcome = load(
            "frozen_arrays",
            vec![BatchStream::json("events", &[&push])],
            vec![frozen()],
        )
        .await;
        succeeded(&outcome);
    }
    assert_eq!(published_json("frozen_arrays", "events__items").len(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_policy_on_a_streams_column_stays_off_its_child_tables() {
    let plan = || {
        normalized("events").column(
            "sku",
            SchemaSettings::new().policy(SchemaPolicy::DiscardValue),
        )
    };
    let first = r#"{"id":1,"sku":1,"items":[{"sku":1}]}"#;
    succeeded(
        &load(
            "column_policy",
            vec![BatchStream::json("events", &[first])],
            vec![plan()],
        )
        .await,
    );
    let second = r#"{"id":2,"items":[{"sku":"x"}]}"#;
    let outcome = load(
        "column_policy",
        vec![BatchStream::json("events", &[second])],
        vec![plan()],
    )
    .await;
    succeeded(&outcome);
    assert_eq!(
        outcome.report.streams["events"].discarded_values, 0,
        "the child table's sku evolves"
    );
    let items = published_json("column_policy", "events__items");
    assert!(
        items.iter().any(|row| row.to_string().contains('x')),
        "{items:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_policy_on_a_normalized_column_governs_the_columns_and_child_tables_it_becomes() {
    let plan = || {
        normalized("events").column(
            "meta",
            SchemaSettings::new().policy(SchemaPolicy::DiscardValue),
        )
    };
    let first = BatchStream::json("events", &[r#"{"id":1}"#]);
    succeeded(&load("owned", vec![first], vec![plan()]).await);
    let second = r#"{"id":2,"meta":{"a":1,"items":[{"sku":"x"}]}}"#;
    let outcome = load(
        "owned",
        vec![BatchStream::json("events", &[second])],
        vec![plan()],
    )
    .await;
    succeeded(&outcome);
    assert_eq!(
        outcome.report.streams["events"].discarded_values, 2,
        "meta.a and the item of meta.items"
    );
    let events = schema("owned", "events").expect("the table");
    assert!(
        events
            .fields()
            .iter()
            .all(|field| !field.name().starts_with("meta")),
        "{events:?}"
    );
    assert!(schema("owned", "events__meta__items").is_none());
}

#[tokio::test(start_paused = true)]
async fn a_frozen_array_column_freezes_its_child_tables_columns() {
    let plan =
        || normalized("events").column("items", SchemaSettings::new().policy(SchemaPolicy::Freeze));
    let first = r#"{"id":1,"items":[{"sku":"x"}]}"#;
    succeeded(
        &load(
            "frozen_items",
            vec![BatchStream::json("events", &[first])],
            vec![plan()],
        )
        .await,
    );
    let second = r#"{"id":2,"items":[{"sku":"y","qty":2}]}"#;
    let outcome = load(
        "frozen_items",
        vec![BatchStream::json("events", &[second])],
        vec![plan()],
    )
    .await;
    let error = outcome.error.expect("the frozen items refuse a new field");
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Schema, Some(codes::SCHEMA_FROZEN))
    );
}

#[tokio::test(start_paused = true)]
async fn a_hinted_column_of_a_normalized_stream_is_stored_whole_as_its_hint() {
    let plan = normalized("events").hint("meta", LogicalType::Json);
    let push = r#"{"id":1,"meta":{"a":[1,2]}}"#;
    succeeded(
        &load(
            "hinted_whole",
            vec![BatchStream::json("events", &[push])],
            vec![plan],
        )
        .await,
    );
    let rows = published_json("hinted_whole", "events");
    let meta: Value = serde_json::from_str(rows[0]["meta"].as_str().expect("JSON text"))
        .expect("the column holds its JSON");
    assert_eq!(meta, json!({"a": [1, 2]}));
    assert!(
        schema("hinted_whole", "events__meta__a").is_none(),
        "no child table"
    );
}

#[tokio::test(start_paused = true)]
async fn the_items_of_a_discarded_array_count_only_in_rows_that_load() {
    let plan = || {
        normalized("events")
            .column(
                "flag",
                SchemaSettings::new().policy(SchemaPolicy::DiscardRow),
            )
            .column(
                "tags",
                SchemaSettings::new().policy(SchemaPolicy::DiscardValue),
            )
    };
    let first = BatchStream::json("events", &[r#"{"id":1}"#]);
    succeeded(&load("dropped_items", vec![first], vec![plan()]).await);
    let second = [
        r#"{"id":2,"flag":1,"tags":[1,2]}"#,
        r#"{"id":3,"tags":[3]}"#,
    ]
    .join("\n");
    let outcome = load(
        "dropped_items",
        vec![BatchStream::json("events", &[&second])],
        vec![plan()],
    )
    .await;
    succeeded(&outcome);
    let report = &outcome.report.streams["events"];
    assert_eq!(
        (report.discarded_rows, report.discarded_values),
        (1, 1),
        "row 2 goes whole; row 3 loads without its one tag"
    );
}

#[tokio::test(start_paused = true)]
async fn arrays_a_source_declares_are_no_schema_change() {
    let fields = |fields: Vec<RdltField>| LogicalType::Struct(Fields::new(fields).unwrap());
    let list = |item: LogicalType| LogicalType::List(Box::new(RdltField::new("item", item, true)));
    let item = fields(vec![
        RdltField::new("sku", LogicalType::Utf8, true),
        RdltField::new("tags", list(LogicalType::Utf8), true),
    ]);
    let declared = TableSchema::new(vec![
        RdltField::new("id", LogicalType::Int64, false),
        RdltField::new("items", list(item), true),
    ])
    .unwrap();
    let push = r#"{"id":1,"items":[{"sku":"a","tags":["t"]}]}"#;
    for policy in [
        SchemaPolicy::Freeze,
        SchemaPolicy::DiscardRow,
        SchemaPolicy::DiscardValue,
    ] {
        let store = format!("declared_arrays_{policy:?}").to_lowercase();
        let plan = stream("events").schema(
            SchemaSettings::new()
                .nested(Nested::normalize())
                .policy(policy),
        );
        let source = BatchStream::json("events", &[push]).declared(declared.clone());
        let outcome = load(&store, vec![source], vec![plan]).await;
        succeeded(&outcome);
        let report = &outcome.report.streams["events"];
        assert_eq!(
            (report.discarded_rows, report.discarded_values),
            (0, 0),
            "{policy:?}"
        );
        for table in ["events", "events__items", "events__items__tags"] {
            assert_eq!(
                published_json(&store, table).len(),
                1,
                "{policy:?}: {table}"
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn the_arrays_within_a_discarded_array_go_with_it() {
    for policy in [SchemaPolicy::DiscardRow, SchemaPolicy::DiscardValue] {
        let store = format!("within_discarded_{policy:?}").to_lowercase();
        let plan = || normalized("events").column("items", SchemaSettings::new().policy(policy));
        let first = BatchStream::json("events", &[r#"{"id":1}"#]);
        succeeded(&load(&store, vec![first], vec![plan()]).await);
        let second = r#"{"id":2,"items":[{"sku":"a","tags":["t"]}]}"#;
        let outcome = load(
            &store,
            vec![BatchStream::json("events", &[second])],
            vec![plan()],
        )
        .await;
        succeeded(&outcome);
        for table in ["events__items", "events__items__tags"] {
            assert!(schema(&store, table).is_none(), "{policy:?}: {table}");
        }
        let kept = if policy == SchemaPolicy::DiscardRow {
            1
        } else {
            2
        };
        assert_eq!(published_json(&store, "events").len(), kept, "{policy:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn the_children_of_a_dropped_row_change_no_schema() {
    let plan = || {
        stream("events")
            .schema(
                SchemaSettings::new()
                    .nested(Nested::normalize())
                    .policy(SchemaPolicy::Freeze),
            )
            .column(
                "extra",
                SchemaSettings::new().policy(SchemaPolicy::DiscardRow),
            )
    };
    let first = r#"{"id":1,"items":[{"sku":"a"}]}"#;
    succeeded(
        &load(
            "unchanged",
            vec![BatchStream::json("events", &[first])],
            vec![plan()],
        )
        .await,
    );
    let second = [
        r#"{"id":2,"extra":1,"items":[{"sku":"b","n":1,"tags":["t"]}]}"#,
        r#"{"id":3,"items":[{"sku":"c"}]}"#,
    ]
    .join("\n");
    let outcome = load(
        "unchanged",
        vec![BatchStream::json("events", &[&second])],
        vec![plan()],
    )
    .await;
    succeeded(&outcome);
    let items = schema("unchanged", "events__items").expect("the items table");
    assert!(
        items.fields().iter().all(|field| field.name() != "n"),
        "{items:?}"
    );
    assert!(schema("unchanged", "events__items__tags").is_none());
    assert_eq!(published_json("unchanged", "events__items").len(), 2);
    assert_eq!(
        outcome.report.streams["events"].discarded_rows, 3,
        "row 2, its item and the item's tag"
    );
}

/// A batch of `ids`' rows, each holding one item whose `z` is `z`'s value, and `extra` where
/// given.
fn items_of(ids: Vec<i64>, extra: Option<Vec<Option<i64>>>, z: ArrayRef) -> RecordBatch {
    let rows = ids.len();
    let items = StructArray::try_from(vec![("z", z)]).expect("a struct of z");
    let item = Arc::new(Field::new("item", items.data_type().clone(), true));
    let offsets = arrow_buffer::OffsetBuffer::from_lengths(vec![1; rows]);
    let list = ListArray::new(item, offsets, Arc::new(items), None);
    let mut columns = vec![("id", Arc::new(Int64Array::from(ids)) as ArrayRef)];
    if let Some(extra) = extra {
        columns.push(("extra", Arc::new(Int64Array::from(extra)) as ArrayRef));
    }
    columns.push(("items", Arc::new(list) as ArrayRef));
    RecordBatch::try_from_iter(columns).expect("a batch of items")
}

/// What loading `groups` of `store`'s stream, a load each, as `plan` leaves: whether every load
/// succeeded, and the schemas of the stream's table and of `child`.
async fn grouped(
    store: &str,
    groups: Vec<RecordBatch>,
    plan: fn() -> StreamPlan,
    child: &str,
) -> (bool, Option<TableSchema>, Option<TableSchema>) {
    let mut succeeded = true;
    for group in groups.into_iter().take(4) {
        let streams = vec![BatchStream::new("events", vec![group])];
        let outcome = load(store, streams, vec![plan()]).await;
        succeeded &= outcome.report.status == RunStatus::Succeeded;
    }
    (succeeded, schema(store, "events"), schema(store, child))
}

#[tokio::test(start_paused = true)]
async fn a_child_column_no_kept_row_holds_a_value_in_makes_no_column_however_rows_are_grouped() {
    fn plan() -> StreamPlan {
        stream("events")
            .schema(
                SchemaSettings::new()
                    .nested(Nested::normalize())
                    .policy(SchemaPolicy::Freeze),
            )
            .column(
                "extra",
                SchemaSettings::new().policy(SchemaPolicy::DiscardRow),
            )
    }
    let first = || items_of(vec![1], None, Arc::new(Int64Array::from(vec![5])));
    let texts = |texts: Vec<Option<&str>>| Arc::new(StringArray::from(texts)) as ArrayRef;
    // Row 2 goes for its extra value, and with it the only text in `z`.
    let together = vec![
        first(),
        items_of(
            vec![2, 3],
            Some(vec![Some(1), None]),
            texts(vec![Some("t"), None]),
        ),
    ];
    let apart = vec![
        first(),
        items_of(vec![2], Some(vec![Some(1)]), texts(vec![Some("t")])),
        items_of(vec![3], Some(vec![None]), texts(vec![None])),
    ];
    let together = grouped("valued_together", together, plan, "events__items").await;
    let apart = grouped("valued_apart", apart, plan, "events__items").await;
    assert!(together.0 && apart.0, "{together:?} {apart:?}");
    assert_eq!(together, apart);
    let items = together.2.expect("the items table");
    let z = items.fields().iter().find(|field| field.name() == "z");
    assert_eq!(z.map(RdltField::logical_type), Some(&LogicalType::Int64));
}

#[tokio::test(start_paused = true)]
async fn dropping_rows_for_a_new_array_removes_no_column_of_the_streams_table() {
    fn plan() -> StreamPlan {
        normalized("events").column(
            "tags",
            SchemaSettings::new().policy(SchemaPolicy::DiscardRow),
        )
    }
    let tagged = |ids: Vec<i64>, texts: Vec<Option<&str>>, tags: &[Option<&str>]| {
        let mut list =
            arrow_array::builder::ListBuilder::new(arrow_array::builder::StringBuilder::new());
        for tag in tags.iter().take(4) {
            if let Some(tag) = tag {
                list.values().append_value(tag);
            }
            list.append(tag.is_some());
        }
        RecordBatch::try_from_iter([
            ("id", Arc::new(Int64Array::from(ids)) as ArrayRef),
            ("text", Arc::new(StringArray::from(texts)) as ArrayRef),
            ("tags", Arc::new(list.finish()) as ArrayRef),
        ])
        .expect("a batch of tagged rows")
    };
    let first = || {
        RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
            .expect("a batch of ids")
    };
    // Row 2 goes for its tags, a new array, and holds the only text.
    let together = vec![
        first(),
        tagged(vec![2, 3], vec![Some("t"), None], &[Some("x"), None]),
    ];
    let apart = vec![
        first(),
        tagged(vec![2], vec![Some("t")], &[Some("x")]),
        tagged(vec![3], vec![None], &[None]),
    ];
    let together = grouped("rooted_together", together, plan, "events__tags").await;
    let apart = grouped("rooted_apart", apart, plan, "events__tags").await;
    assert!(together.0 && apart.0, "{together:?} {apart:?}");
    assert_eq!(together, apart);
    let events = together.1.expect("the stream's table");
    assert!(
        events.fields().iter().any(|field| field.name() == "text"),
        "{events:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_json_column_only_rows_dropped_for_a_new_array_held_makes_no_column_however_grouped() {
    fn plan() -> StreamPlan {
        stream("events")
            .schema(
                SchemaSettings::new()
                    .nested(Nested::normalize())
                    .policy(SchemaPolicy::Freeze),
            )
            .column(
                "tags",
                SchemaSettings::new().policy(SchemaPolicy::DiscardRow),
            )
    }
    // Row 2 goes for its tags, a new array, and holds the only `a`.
    let (first, second, third) = (
        r#"{"id":1}"#,
        r#"{"id":2,"a":"v","tags":["t"]}"#,
        r#"{"id":3}"#,
    );
    let mut outcomes = Vec::new();
    for (store, loads) in [
        (
            "json_together",
            vec![first.to_owned(), [second, third].join("\n")],
        ),
        (
            "json_apart",
            vec![first.to_owned(), second.to_owned(), third.to_owned()],
        ),
    ] {
        let mut succeeded = true;
        for push in loads.iter().take(4) {
            let streams = vec![BatchStream::json("events", &[push.as_str()])];
            let outcome = load(store, streams, vec![plan()]).await;
            succeeded &= outcome.report.status == RunStatus::Succeeded;
        }
        outcomes.push((succeeded, schema(store, "events")));
    }
    assert!(
        outcomes.iter().all(|(succeeded, _)| *succeeded),
        "{outcomes:?}"
    );
    assert_eq!(outcomes[0], outcomes[1]);
    let events = outcomes[0].1.clone().expect("the stream's table");
    assert!(
        events.fields().iter().all(|field| field.name() != "a"),
        "{events:?}"
    );
}

/// Loads `push`, cut into chunks of 16 bytes, into a new table of a stream normalized under
/// `policy`, at `destination`.
async fn in_chunks(
    store: &str,
    push: &str,
    policy: SchemaPolicy,
    destination: Arc<dyn Destination>,
) -> RunOutcome {
    let batching =
        BatchPolicy::new(8 << 20, 1 << 20, Duration::from_secs(1), 16).expect("a valid policy");
    let plan = stream("events").schema(
        SchemaSettings::new()
            .nested(Nested::normalize())
            .policy(policy),
    );
    let source = batches(store, vec![BatchStream::json("events", &[push])]).await;
    engine(commit_every(1000).batch(batching))
        .run(pipeline(store, [plan]), source, destination)
        .await
}

/// Loads `push`, cut into chunks of 16 bytes, into a new table of a frozen normalized stream.
async fn frozen_in_chunks(store: &str, push: &str) -> RunOutcome {
    in_chunks(store, push, SchemaPolicy::Freeze, memory(store).await).await
}

#[tokio::test(start_paused = true)]
async fn the_flush_that_creates_a_table_takes_a_column_only_a_later_chunk_holds_values_in() {
    // The first record fills a chunk, so the second, the only one holding `a`, is cut apart.
    let outcome = frozen_in_chunks(
        "creating_chunks_column",
        "{\"id\":10000000001}\n{\"id\":2,\"a\":\"v\"}",
    )
    .await;
    succeeded(&outcome);
    let events = schema("creating_chunks_column", "events").expect("the stream's table");
    assert!(
        events.fields().iter().any(|field| field.name() == "a"),
        "{events:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn the_flush_that_creates_a_table_takes_an_array_only_a_later_chunk_holds_items_in() {
    let outcome = frozen_in_chunks(
        "creating_chunks_array",
        "{\"id\":10000000001}\n{\"id\":2,\"tags\":[\"t\"]}",
    )
    .await;
    succeeded(&outcome);
    assert!(schema("creating_chunks_array", "events__tags").is_some());
}

#[tokio::test(start_paused = true)]
async fn a_destination_that_cannot_add_columns_refuses_one_only_a_later_chunk_of_its_first_flush_holds()
 {
    // The first chunk creates the table without `a`, which no row of it holds: the second
    // chunk's `a` is then a column to add, which the destination cannot.
    let store = "creating_chunks_fixed";
    let fixed = limited(memory(store).await, |capabilities| {
        capabilities.schema_changes.add_column = false;
    });
    let outcome = in_chunks(
        store,
        "{\"id\":10000000001}\n{\"id\":2,\"a\":\"v\"}",
        SchemaPolicy::Evolve,
        fixed,
    )
    .await;
    assert_eq!(outcome.report.status, RunStatus::Failed);
    let error = outcome.error.expect("the run failed");
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Schema, Some(codes::SCHEMA_CHANGE_UNSUPPORTED)),
        "{error:?}"
    );
    let events = schema(store, "events").expect("the first chunk created the table");
    assert!(
        events.fields().iter().all(|field| field.name() != "a"),
        "{events:?}"
    );
}

/// Loads two hundred thousand rows of one Arrow batch into a new table of a frozen normalized
/// stream at `destination`, under a budget of 40 MiB, which lowers the flush in several pieces:
/// only the last row's item holds `z`.
async fn in_pieces(store: &str, destination: Arc<dyn Destination>) -> RunOutcome {
    const ROWS: usize = 200_000;
    let z: Vec<Option<&str>> = (0..ROWS)
        .map(|row| (row == ROWS - 1).then_some("v"))
        .take(ROWS)
        .collect();
    let ids = (0..ROWS).map(|row| i64::try_from(row).expect("a row fits"));
    let batch = items_of(
        ids.take(ROWS).collect(),
        None,
        Arc::new(StringArray::from(z)),
    );
    let plan = stream("events").schema(
        SchemaSettings::new()
            .nested(Nested::normalize())
            .policy(SchemaPolicy::Freeze),
    );
    let source = batches(store, vec![BatchStream::new("events", vec![batch])]).await;
    let config = commit_every(1_000_000_000).memory(40 << 20).lanes(1);
    engine(config)
        .run(pipeline(store, [plan]), source, destination)
        .await
}

#[tokio::test(start_paused = true)]
async fn the_flush_that_creates_a_table_takes_a_child_column_only_a_later_piece_holds_values_in() {
    let store = "creating_pieces";
    succeeded(&in_pieces(store, memory(store).await).await);
    let items = schema(store, "events__items").expect("the items table");
    assert!(
        items.fields().iter().any(|field| field.name() == "z"),
        "{items:?}"
    );
    // A destination that cannot add columns refuses `z`, so the piece holding its value did not
    // create the table: the flush was lowered in more than one piece.
    let store = "creating_pieces_fixed";
    let fixed = limited(memory(store).await, |capabilities| {
        capabilities.schema_changes.add_column = false;
    });
    let error = in_pieces(store, fixed)
        .await
        .error
        .expect("a later piece's column is refused");
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Schema, Some(codes::SCHEMA_CHANGE_UNSUPPORTED)),
        "{error:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_tables_columns_follow_the_order_in_which_they_are_first_valued() {
    let push = "{\"id\":10000000001}\n{\"id\":2,\"a\":\"v\"}";
    let names = |store: &str| -> Vec<String> {
        let events = schema(store, "events").expect("the stream's table");
        events
            .fields()
            .iter()
            .map(|field| field.name().to_owned())
            .collect()
    };
    // Cut between the rows, `a` is first valued after the first chunk created the table, so it
    // follows every column the table was created with, its metadata columns included.
    let store = "ordered_chunks";
    succeeded(&in_chunks(store, push, SchemaPolicy::Evolve, memory(store).await).await);
    assert_eq!(
        names(store),
        ["id", "_rdlt_load_id", "_rdlt_loaded_at", "_rdlt_id", "a"]
    );
    // Whole, `a` is valued as the table is created, among its columns.
    let store = "ordered_whole";
    let source = batches(store, vec![BatchStream::json("events", &[push])]).await;
    let outcome = engine(commit_every(1000))
        .run(
            pipeline(store, [normalized("events")]),
            source,
            memory(store).await,
        )
        .await;
    succeeded(&outcome);
    assert_eq!(
        names(store),
        ["id", "a", "_rdlt_load_id", "_rdlt_loaded_at", "_rdlt_id"]
    );
}

/// The compute jobs a run of `push` as `plan` takes.
async fn pool_jobs(store: &str, push: &str, plan: StreamPlan) -> usize {
    let source = batches(store, vec![BatchStream::json("events", &[push])]).await;
    let (engine, jobs) = crate::support::counting_engine(commit_every(1));
    let outcome = engine
        .run(pipeline(store, [plan]), source, memory(store).await)
        .await;
    succeeded(&outcome);
    jobs.load(std::sync::atomic::Ordering::SeqCst)
}

#[tokio::test(start_paused = true)]
async fn a_normalized_unit_that_drops_nothing_takes_no_pool_trips_per_part() {
    let push = r#"{"id":1,"items":[{"sku":"a","tags":["t"]}],"more":[1,2]}"#;
    let plain = pool_jobs("pool_plain", push, stream("events")).await;
    let normalized = pool_jobs("pool_normalized", push, normalized("events")).await;
    assert_eq!(
        normalized,
        plain + 2,
        "normalizing takes one job, and measuring its parts for their tables one more; none of \
         its four parts takes a trip of its own"
    );
}

#[tokio::test(start_paused = true)]
async fn an_array_under_a_key_no_table_can_be_named_after_follows_the_streams_policy() {
    let with_policy = |policy| {
        stream("events").schema(
            SchemaSettings::new()
                .nested(Nested::normalize())
                .policy(policy),
        )
    };
    let long = "k".repeat(300);
    let documents = [
        r#"{"id":1,"":[{"a":1}]}"#.to_owned(),
        "{\"id\":1,\"a\\u0001b\":[{\"a\":1}]}".to_owned(),
        format!("{{\"id\":1,\"{long}\":[{{\"a\":1}}]}}"),
        r#"{"id":1,"o":{"":[1,2]}}"#.to_owned(),
    ];
    for (index, document) in documents.iter().enumerate() {
        // On a stream's first load, and with its default policy, the array is refused, typed.
        let store = format!("unnamable_{index}");
        let outcome = load(
            &store,
            vec![BatchStream::json("events", &[document])],
            vec![normalized("events")],
        )
        .await;
        let error = outcome.error.expect("the array has no table");
        assert_eq!(
            (error.kind(), error.code()),
            (ErrorKind::Schema, Some(codes::TABLE_PATH_INVALID)),
            "{document}"
        );
        // A stream that discards the values or rows of schema changes loads without it.
        for policy in [SchemaPolicy::DiscardValue, SchemaPolicy::DiscardRow] {
            let store = format!("unnamable_{index}_{policy:?}").to_lowercase();
            let outcome = load(
                &store,
                vec![BatchStream::json("events", &[document])],
                vec![with_policy(policy)],
            )
            .await;
            succeeded(&outcome);
            let report = &outcome.report.streams["events"];
            let rows = published_json(&store, "events").len();
            if policy == SchemaPolicy::DiscardValue {
                assert_eq!((rows, report.discarded_values > 0), (1, true), "{document}");
            } else {
                assert_eq!((rows, report.discarded_rows), (0, 1), "{document}");
            }
        }
        let store = format!("unnamable_{index}_frozen");
        let outcome = load(
            &store,
            vec![BatchStream::json("events", &[document])],
            vec![with_policy(SchemaPolicy::Freeze)],
        )
        .await;
        let error = outcome.error.expect("a frozen stream adds no table");
        assert_eq!(error.code(), Some(codes::SCHEMA_FROZEN), "{document}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_declared_schema_gaining_an_array_follows_the_policy_once_its_table_exists() {
    let list = |item: LogicalType| LogicalType::List(Box::new(RdltField::new("item", item, true)));
    let plan = |policy| {
        stream("events").schema(
            SchemaSettings::new()
                .nested(Nested::normalize())
                .policy(policy),
        )
    };
    let declared = |items: bool| {
        let mut fields = vec![RdltField::new("id", LogicalType::Int64, false)];
        if items {
            fields.push(RdltField::new("items", list(LogicalType::Int64), true));
        }
        TableSchema::new(fields).unwrap()
    };
    for (store, policy) in [
        ("declared_frozen", SchemaPolicy::Freeze),
        ("declared_discarded", SchemaPolicy::DiscardValue),
        ("declared_evolving", SchemaPolicy::Evolve),
    ] {
        let first = BatchStream::json("events", &[r#"{"id":1}"#]).declared(declared(false));
        succeeded(&load(store, vec![first], vec![plan(policy)]).await);
        let second =
            BatchStream::json("events", &[r#"{"id":2,"items":[7,8]}"#]).declared(declared(true));
        let outcome = load(store, vec![second], vec![plan(policy)]).await;
        let child = schema(store, "events__items").is_some();
        match policy {
            SchemaPolicy::Freeze => {
                let error = outcome.error.expect("a frozen stream gains no table");
                assert_eq!(
                    (error.kind(), error.code()),
                    (ErrorKind::Schema, Some(codes::SCHEMA_FROZEN))
                );
                assert!(!child);
            }
            SchemaPolicy::DiscardValue => {
                succeeded(&outcome);
                assert!(!child);
                assert_eq!(outcome.report.streams["events"].discarded_values, 2);
            }
            _ => {
                succeeded(&outcome);
                assert!(child);
            }
        }
    }
    // A table the declared array had from the start keeps its child table under any policy.
    let store = "declared_from_the_start";
    let first = BatchStream::json("events", &[r#"{"id":1,"items":[1]}"#]).declared(declared(true));
    succeeded(&load(store, vec![first], vec![plan(SchemaPolicy::Freeze)]).await);
    let second = BatchStream::json("events", &[r#"{"id":2,"items":[2]}"#]).declared(declared(true));
    succeeded(&load(store, vec![second], vec![plan(SchemaPolicy::Freeze)]).await);
    assert_eq!(published_json(store, "events__items").len(), 2);
}

#[tokio::test(start_paused = true)]
async fn a_declared_array_has_its_table_from_the_start_though_no_row_holds_an_item() {
    let list = |item: LogicalType| LogicalType::List(Box::new(RdltField::new("item", item, true)));
    let declared = TableSchema::new(vec![
        RdltField::new("id", LogicalType::Int64, false),
        RdltField::new("items", list(LogicalType::Int64), true),
    ])
    .unwrap();
    let plan = |policy| {
        stream("events").schema(
            SchemaSettings::new()
                .nested(Nested::normalize())
                .policy(policy),
        )
    };
    for (store, policy) in [
        ("empty_frozen", SchemaPolicy::Freeze),
        ("empty_discarded", SchemaPolicy::DiscardValue),
        ("empty_dropped", SchemaPolicy::DiscardRow),
        ("empty_evolving", SchemaPolicy::Evolve),
    ] {
        let first = BatchStream::json("events", &[r#"{"id":1}"#, r#"{"id":2,"items":[]}"#])
            .declared(declared.clone());
        succeeded(&load(store, vec![first], vec![plan(policy)]).await);
        assert!(schema(store, "events__items").is_some(), "{store}");
        let second =
            BatchStream::json("events", &[r#"{"id":3,"items":[7,8]}"#]).declared(declared.clone());
        let outcome = load(store, vec![second], vec![plan(policy)]).await;
        succeeded(&outcome);
        assert_eq!(published_json(store, "events__items").len(), 2, "{store}");
        assert_eq!(
            outcome.report.streams["events"].discarded_values, 0,
            "{store}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn a_declared_array_deeper_than_the_stream_normalizes_is_no_table_of_its_own() {
    let tags = LogicalType::List(Box::new(RdltField::new("item", LogicalType::Int64, true)));
    let meta = LogicalType::Struct(Fields::new(vec![RdltField::new("tags", tags, true)]).unwrap());
    let declared = TableSchema::new(vec![
        RdltField::new("id", LogicalType::Int64, false),
        RdltField::new("meta", meta, true),
    ])
    .unwrap();
    let plan = stream("events").schema(
        SchemaSettings::new()
            .nested(Nested::Normalize { max_depth: 1 })
            .policy(SchemaPolicy::Freeze),
    );
    let store = "declared_deep_frozen";
    for row in [
        r#"{"id":1,"meta":{"tags":[1]}}"#,
        r#"{"id":2,"meta":{"tags":[2,3]}}"#,
    ] {
        let rows = BatchStream::json("events", &[row]).declared(declared.clone());
        succeeded(&load(store, vec![rows], vec![plan.clone()]).await);
    }
    assert!(schema(store, "events__meta__tags").is_none());
    assert_eq!(published_json(store, "events").len(), 2);
}
