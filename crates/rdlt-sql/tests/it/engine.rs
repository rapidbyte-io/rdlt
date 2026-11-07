//! A SQL transform through the engine, on JSON and on encoded Arrow columns, and through its
//! stage on partial updates.

use std::sync::Arc;

use arrow_array::cast::AsArray as _;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::{
    ArrayRef, BinaryArray, DictionaryArray, FixedSizeBinaryArray, Int8Array, Int32Array,
    Int64Array, RecordBatch, RunArray, StringArray,
};
use rdlt_connector::{
    ChangeOp, ConnectContext, OP_COLUMN, PipelineId, ReadMode, SEQ_COLUMN, Source, StreamName,
    UNCHANGED_COLUMN, destination_factory, source_factory,
};
use rdlt_connector_reference::{MemoryDestination, MemorySource, published};
use rdlt_engine::bench::{Transformed, Untransformed};
use rdlt_engine::{
    Cores, Engine, EngineConfig, Env, ErrorKind, PipelinePlan, RunOutcome, RunStatus, StreamPlan,
    SystemEnv, WriteMode, codes,
};
use rdlt_sql::Sql;
use serde_json::json;

use crate::served;

/// A run of the stream `people` from `source` into the memory store `name`, through `sql`.
async fn ran_through(name: &str, source: Arc<dyn Source>, sql: &str) -> RunOutcome {
    let sql = Sql::parse(sql).expect("the query parses");
    let stream = StreamPlan::new(StreamName::new("people").expect("a valid name")).transform(sql);
    ran_plan(name, source, stream).await
}

/// A run of `stream` from `source` into the memory store `name`, which succeeds.
async fn ran_plan(name: &str, source: Arc<dyn Source>, stream: StreamPlan) -> RunOutcome {
    let destination = destination_factory::<MemoryDestination>()
        .connect(json!({ "store": name }), ConnectContext::new())
        .await
        .expect("the memory destination connects");
    let pipeline = PipelineId::parse(name).expect("a valid pipeline id");
    let plan = PipelinePlan::new(pipeline, [stream]).expect("the plan is valid");
    let cores = Cores::try_from_host().expect("the host has cores");
    let env: Arc<dyn Env> = Arc::new(SystemEnv::try_new(cores).expect("the pool starts"));
    let config = EngineConfig::builder()
        .build()
        .expect("the default configuration");
    let outcome = Engine::new(config, env)
        .run(plan, source, Arc::from(destination))
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
    outcome
}

/// Each published row of `people` in the store `name`: its id and its text in `columns`, sorted.
fn landed(name: &str, columns: &[&str]) -> Vec<(i64, Vec<String>)> {
    let mut rows = Vec::new();
    for batch in published(name, "people") {
        let ids = batch.column_by_name("id").expect("a table of ids");
        let ids = ids.as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            let texts = columns.iter().map(|column| {
                let values = batch
                    .column_by_name(column)
                    .expect("the table has the column");
                values.as_string::<i32>().value(row).to_owned()
            });
            rows.push((ids.value(row), texts.collect()));
        }
    }
    rows.sort();
    rows
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sql_transform_reshapes_and_filters_a_stream_through_the_engine() {
    let name = "sql_engine";
    let rows = json!([
        {"id": 1, "email": "A@X", "status": "live"},
        {"id": 2, "email": "b@y", "status": null},
        {"id": 3, "email": "c@z", "status": "test"},
        {"id": 4, "email": "D@Ω", "status": "gone"},
    ]);
    let config = json!({"streams": {"people": rows}, "page_size": 2});
    let source = source_factory::<MemorySource>()
        .connect(config, ConnectContext::new())
        .await
        .unwrap();
    let sql = "SELECT id, lower(email) AS email FROM input WHERE status <> 'test'";
    ran_through(name, Arc::from(source), sql).await;
    // Row 2, whose status is null, is dropped too: a null keeps nothing.
    let expected = [(1, vec!["a@x".to_owned()]), (4, vec!["d@ω".to_owned()])];
    assert_eq!(landed(name, &["email"]), expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_computes_on_dictionary_and_run_end_columns_through_the_engine() {
    let name = "sql_encoded";
    let statuses: DictionaryArray<Int32Type> = vec![Some("live"), Some("test"), Some("live"), None]
        .into_iter()
        .collect();
    let names: DictionaryArray<Int32Type> = vec!["ann", "bob", "ann", "cy"].into_iter().collect();
    let nicks = RunArray::<Int32Type>::try_new(
        &Int32Array::from(vec![2, 4]),
        &StringArray::from(vec!["x", "y"]),
    )
    .unwrap();
    let batch = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
        ),
        ("status", Arc::new(statuses) as ArrayRef),
        ("name", Arc::new(names) as ArrayRef),
        ("nick", Arc::new(nicks) as ArrayRef),
    ])
    .unwrap();
    let source = served::source(name, vec![batch]).await;
    let sql = "SELECT id, upper(name) AS name, upper(nick) AS nick FROM input \
               WHERE status = 'live'";
    ran_through(name, source, sql).await;
    let expected = [
        (1, vec!["ANN".to_owned(), "X".to_owned()]),
        (3, vec!["ANN".to_owned(), "Y".to_owned()]),
    ];
    assert_eq!(landed(name, &["name", "nick"]), expected);
}

/// One update of `people`, `id` 1, flagging `email` unchanged: the source sent no value of it.
fn update_leaving_email_unchanged() -> RecordBatch {
    let flags: &[u8] = &[0b0000_0010];
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        (
            "email",
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
        ),
        (
            "status",
            Arc::new(StringArray::from(vec!["live"])) as ArrayRef,
        ),
        (
            OP_COLUMN,
            Arc::new(Int8Array::from(vec![ChangeOp::Update.code()])) as ArrayRef,
        ),
        (
            SEQ_COLUMN,
            Arc::new(
                FixedSizeBinaryArray::try_from_iter([[0_u8; 16]].into_iter())
                    .expect("a position is 16 bytes"),
            ) as ArrayRef,
        ),
        (
            UNCHANGED_COLUMN,
            Arc::new(BinaryArray::from(vec![Some(flags)])) as ArrayRef,
        ),
    ])
    .expect("the columns are of one length")
}

/// A change stream of `people`, merged by key, transformed by `sql`.
fn merged(sql: Sql) -> StreamPlan {
    StreamPlan::new(StreamName::new("people").expect("a valid name"))
        .read(ReadMode::Cdc)
        .write(WriteMode::Merge)
        .transform(sql)
}

/// The unchanged flags of the first row the stage leaves of `batch`, through `sql`.
fn flags_through(sql: &str, batch: RecordBatch) -> Result<Vec<u8>, Untransformed> {
    let plan = merged(Sql::parse(sql).expect("the query parses"));
    let output = Transformed::try_new(&plan, &["id"], batch)?.batches()?;
    let flags = output[0]
        .column_by_name(UNCHANGED_COLUMN)
        .expect("a change batch keeps its flags")
        .as_binary::<i32>();
    Ok(flags.value(0).to_vec())
}

#[test]
fn an_update_leaving_a_column_unchanged_passes_a_projection_as_it_arrived() {
    let sql = "SELECT id, lower(email) AS email, upper(status) AS status, 'x' AS tag FROM input";
    // `email` reads only the unchanged `email`; `status` reads a changed column and `tag` none.
    let moved = flags_through(sql, update_leaving_email_unchanged()).unwrap();
    assert_eq!(moved, [0b0000_0010]);
}

#[test]
fn a_filter_on_the_key_passes_an_update_leaving_another_column_unchanged() {
    let sql = "SELECT * FROM input WHERE id > 0";
    let moved = flags_through(sql, update_leaving_email_unchanged()).unwrap();
    assert_eq!(moved, [0b0000_0010]);
}

#[test]
fn a_filter_reading_beyond_the_key_refuses_an_update_it_keeps_leaving_a_column_unchanged() {
    // `status` changed, but a filter reading it may drop the row as a delete and later keep it,
    // and the row would come back without the value of `email` it had.
    let sql = "SELECT * FROM input WHERE id > 0 AND status <> 'test'";
    let refused = flags_through(sql, update_leaving_email_unchanged()).unwrap_err();
    let Untransformed::Refused(error) = refused else {
        panic!("the stage refused no batch");
    };
    assert_eq!(
        (error.kind(), error.code()),
        (ErrorKind::Transform, Some(codes::TRANSFORM_PARTIAL))
    );
}

#[test]
fn a_query_reading_a_column_an_update_left_unchanged_is_refused_as_partial() {
    for sql in [
        "SELECT * FROM input WHERE email <> 'x'",
        "SELECT id, concat(email, status) AS both FROM input",
    ] {
        let refused = flags_through(sql, update_leaving_email_unchanged()).unwrap_err();
        let Untransformed::Refused(error) = refused else {
            panic!("{sql}: the stage refused no batch");
        };
        assert_eq!(
            error.code(),
            Some(codes::TRANSFORM_PARTIAL),
            "{sql}: {error}"
        );
    }
}

/// One row of `people` keyed by a text `id`.
fn keyed_people() -> RecordBatch {
    RecordBatch::try_from_iter([
        ("id", Arc::new(StringArray::from(vec!["a"])) as ArrayRef),
        (
            "email",
            Arc::new(StringArray::from(vec!["A@X"])) as ArrayRef,
        ),
    ])
    .expect("the columns are of one length")
}

#[test]
fn a_query_making_a_key_anew_is_refused_and_one_passing_it_is_not() {
    let stage = |sql: &str| {
        let plan = StreamPlan::new(StreamName::new("people").expect("a valid name"))
            .transform(Sql::parse(sql).expect("the query parses"));
        Transformed::try_new(&plan, &["id"], keyed_people()).map(|_| ())
    };
    let Err(Untransformed::Refused(refused)) = stage("SELECT upper(id) AS id, email FROM input")
    else {
        panic!("a query making the key anew was taken");
    };
    assert_eq!(
        (refused.kind(), refused.code()),
        (ErrorKind::Config, Some(codes::TRANSFORM_INVALID))
    );
    assert!(stage("SELECT id, lower(email) AS email FROM input").is_ok());
    assert!(stage("SELECT id AS id, email FROM input AS i").is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_merged_stream_through_sql_keeps_its_keys() {
    let name = "sql_merged";
    let push = |ids: &[&str], emails: &[&str]| {
        RecordBatch::try_from_iter([
            ("id", Arc::new(StringArray::from(ids.to_vec())) as ArrayRef),
            (
                "email",
                Arc::new(StringArray::from(emails.to_vec())) as ArrayRef,
            ),
        ])
        .unwrap()
    };
    let pushes = vec![
        push(&["a", "b"], &["A@X", "B@Y"]),
        push(&["a", "c"], &["A2@X", "C@Z"]),
    ];
    let source = served::source(name, pushes).await;
    let sql = Sql::parse("SELECT id, lower(email) AS email FROM input").unwrap();
    let stream = StreamPlan::new(StreamName::new("people").unwrap())
        .write(WriteMode::Merge)
        .key(["id"])
        .transform(sql);
    ran_plan(name, source, stream).await;
    let mut rows: Vec<(String, String)> = Vec::new();
    for batch in published(name, "people") {
        let ids = batch.column_by_name("id").unwrap().as_string::<i32>();
        let emails = batch.column_by_name("email").unwrap().as_string::<i32>();
        let pairs = (0..batch.num_rows()).map(|row| (ids.value(row), emails.value(row)));
        rows.extend(pairs.map(|(id, email)| (id.to_owned(), email.to_owned())));
    }
    rows.sort();
    let expected = [("a", "a2@x"), ("b", "b@y"), ("c", "c@z")];
    let expected: Vec<(String, String)> = expected
        .iter()
        .map(|(id, email)| ((*id).to_owned(), (*email).to_owned()))
        .collect();
    assert_eq!(rows, expected);
}
