//! A column of JSON whose table holds the column as another type: each value its own column
//! holds goes there, and only the others take a variant or the schema policy.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use rdlt_engine::{ErrorKind, Nested, RunOutcome, RunStatus, SchemaPolicy, SchemaSettings};
use serde_json::{Value, json};

use crate::support::making::{Step, Steps, making};
use crate::support::{commit_every, engine, memory, pipeline, published_json, stream};

/// What a load sends: an Arrow batch or a JSON push.
#[derive(Clone)]
enum Sent {
    Batch(RecordBatch),
    Json(Bytes),
}

impl Sent {
    fn step(&self) -> Step {
        match self {
            Self::Batch(batch) => Step::Batch(batch.clone()),
            Self::Json(text) => Step::Json(text.clone()),
        }
    }
}

/// Loads `first`, then, past a checkpoint, `second` into `store`'s stream `events`, as
/// `settings` say.
async fn load(store: &str, settings: SchemaSettings, first: Sent, second: Sent) -> RunOutcome {
    let steps: Steps = Arc::new(move |step| match step {
        0 => Some(first.step()),
        1 => Some(Step::Checkpoint(8)),
        2 => Some(second.step()),
        _ => None,
    });
    let plan = pipeline(store, [stream("events").schema(settings)]);
    engine(commit_every(100_000))
        .run(plan, making(store, steps).await, memory(store).await)
        .await
}

/// Loads, as `policy` says, into a table whose `amount` holds integers a push of a thousand
/// integer amounts and one string: how many values `amount` and its JSON variant hold, and the
/// values and rows discarded.
async fn one_hostile_value(policy: SchemaPolicy) -> (usize, usize, u64, u64) {
    let store = format!("hostile_{policy:?}").to_lowercase();
    let pushed: Vec<String> = (1..=1000)
        .map(|id| format!(r#"{{"id":{id},"amount":{}}}"#, id * 10))
        .chain([r#"{"id":5000,"amount":"x"}"#.to_owned()])
        .collect();
    let first = Sent::Json(Bytes::from_static(br#"{"id":0,"amount":7}"#));
    let second = Sent::Json(Bytes::from(pushed.join("\n")));
    let settings = SchemaSettings::default().policy(policy);
    let outcome = load(&store, settings, first, second).await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let rows = published_json(&store, "events");
    let held = |column: &str| rows.iter().filter(|row| !row[column].is_null()).count();
    let report = &outcome.report.streams["events"];
    (
        held("amount"),
        held("amount__json"),
        report.discarded_values,
        report.discarded_rows,
    )
}

#[tokio::test(start_paused = true)]
async fn one_value_of_another_kind_takes_its_policy_alone_and_the_others_keep_their_column() {
    // The first row's amount and the thousand integer ones in `amount`; the string in the
    // variant, nulled or dropped with its row.
    assert_eq!(
        one_hostile_value(SchemaPolicy::Evolve).await,
        (1001, 1, 0, 0)
    );
    assert_eq!(
        one_hostile_value(SchemaPolicy::DiscardValue).await,
        (1001, 0, 1, 0)
    );
    assert_eq!(
        one_hostile_value(SchemaPolicy::DiscardRow).await,
        (1001, 0, 0, 1)
    );
}

#[tokio::test(start_paused = true)]
async fn a_frozen_table_still_refuses_a_value_its_column_does_not_hold() {
    let first = Sent::Json(Bytes::from_static(br#"{"id":0,"amount":7}"#));
    let second = Sent::Json(Bytes::from_static(
        b"{\"id\":1,\"amount\":8}\n{\"id\":2,\"amount\":\"x\"}",
    ));
    let settings = SchemaSettings::default().policy(SchemaPolicy::Freeze);
    let outcome = load("split_frozen", settings, first, second).await;
    let error = outcome.error.expect("a frozen table refuses the string");
    assert_eq!(error.code(), Some("schema_frozen"));
}

/// The rows of `store`'s `events`, each's `id` and the `column` and `variant` it holds, by id.
fn placed(store: &str, column: &str, variant: &str) -> Vec<(i64, Value, Value)> {
    let mut rows: Vec<(i64, Value, Value)> = published_json(store, "events")
        .into_iter()
        .map(|row| {
            let id = row["id"].as_i64().expect("every row has an id");
            (id, row[column].clone(), row[variant].clone())
        })
        .collect();
    rows.sort_by_key(|(id, _, _)| *id);
    rows
}

#[tokio::test(start_paused = true)]
async fn objects_a_struct_holds_keep_it_and_one_that_would_widen_it_takes_the_variant() {
    let first = Sent::Json(Bytes::from_static(br#"{"id":0,"meta":{"a":1}}"#));
    let second = Sent::Json(Bytes::from_static(
        b"{\"id\":1,\"meta\":{\"a\":2}}\n{\"id\":2,\"meta\":{\"a\":3,\"b\":\"x\"}}\n\
          {\"id\":3,\"meta\":\"s\"}",
    ));
    let settings = SchemaSettings::default().nested(Nested::Native);
    let outcome = load("split_struct", settings, first, second).await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let store = "split_struct";
    assert_eq!(
        placed(store, "meta", "meta__json"),
        [
            (0, json!({"a": 1}), Value::Null),
            (1, json!({"a": 2}), Value::Null),
            (2, Value::Null, json!(r#"{"a":3,"b":"x"}"#)),
            (3, Value::Null, json!(r#""s""#)),
        ]
    );
}

/// Loads into `store` an Arrow batch whose `amount` holds `first`, integers, then past a
/// checkpoint one whose `amount` is a column of JSON holding `second`, ids counting on.
async fn arrow_amounts(store: &str, first: DataType, second: Vec<Option<&str>>) -> RunOutcome {
    let schema = |amount: Field| {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            amount,
        ]))
    };
    let firsts: ArrayRef =
        arrow_cast::cast(&Int64Array::from(vec![7]), &first).expect("an integer casts");
    let first = RecordBatch::try_new(
        schema(Field::new("amount", first, true)),
        vec![Arc::new(Int64Array::from(vec![0])), firsts],
    )
    .expect("columns match their schema");
    let json = Field::new("amount", DataType::Utf8, true)
        .with_metadata([("ARROW:extension:name".to_owned(), "arrow.json".to_owned())].into());
    let ids = Int64Array::from_iter_values(1..=i64::try_from(second.len()).expect("few rows"));
    let second = RecordBatch::try_new(
        schema(json),
        vec![Arc::new(ids), Arc::new(StringArray::from(second))],
    )
    .expect("columns match their schema");
    load(
        store,
        SchemaSettings::default(),
        Sent::Batch(first),
        Sent::Batch(second),
    )
    .await
}

#[tokio::test(start_paused = true)]
async fn an_arrow_column_of_json_sends_each_value_its_own_column_holds_there() {
    let amounts = vec![Some("10"), Some("2.0e1"), Some("\"x\""), None, Some("[1]")];
    let outcome = arrow_amounts("split_arrow", DataType::Int64, amounts).await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    // `2.0e1` is a float, which a column of integers does not hold, however whole.
    assert_eq!(
        placed("split_arrow", "amount", "amount__json"),
        [
            (0, json!(7), Value::Null),
            (1, json!(10), Value::Null),
            (2, Value::Null, json!("2.0e1")),
            (3, Value::Null, json!(r#""x""#)),
            (4, Value::Null, Value::Null),
            (5, Value::Null, json!("[1]")),
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_number_no_float_holds_stays_text_beside_a_column_of_floats() {
    let amounts = vec![Some("1.5"), Some("1e400")];
    let outcome = arrow_amounts("split_beyond", DataType::Float64, amounts).await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    assert_eq!(
        placed("split_beyond", "amount", "amount__json"),
        [
            (0, json!(7.0), Value::Null),
            (1, json!(1.5), Value::Null),
            (2, Value::Null, json!("1e400")),
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn a_string_no_text_holds_fails_its_write_before_it_is_split() {
    let amounts = vec![Some("1.5"), Some("\"\\ud800\"")];
    let outcome = arrow_amounts("split_surrogate", DataType::Float64, amounts).await;
    let error = outcome.error.expect("a lone surrogate is refused");
    assert_eq!(error.kind(), ErrorKind::Source, "{error:?}");
    assert_eq!(error.code(), Some("json_invalid"));
}

#[tokio::test(start_paused = true)]
async fn a_child_table_s_column_of_json_sends_each_value_its_own_column_holds_there() {
    let first = Sent::Json(Bytes::from_static(br#"{"id":0,"tags":[{"n":1}]}"#));
    let second = Sent::Json(Bytes::from_static(
        br#"{"id":1,"tags":[{"n":2},{"n":"x"},{"n":3}]}"#,
    ));
    let settings = SchemaSettings::default().nested(Nested::normalize());
    let outcome = load("split_child", settings, first, second).await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    let rows = published_json("split_child", "events__tags");
    let mut held: Vec<(Value, Value)> = rows
        .iter()
        .map(|row| (row["n"].clone(), row["n__json"].clone()))
        .collect();
    held.sort_by_key(|(n, text)| (n.as_i64(), text.as_str().map(str::to_owned)));
    assert_eq!(
        held,
        [
            (Value::Null, json!(r#""x""#)),
            (json!(1), Value::Null),
            (json!(2), Value::Null),
            (json!(3), Value::Null),
        ]
    );
}
