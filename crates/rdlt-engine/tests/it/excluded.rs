//! Columns a stream excludes: never in its table, its name map or its state, whether its source
//! leaves them out or not, a change batch's flags moved past them, and a column the table had
//! kept and left null.

use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, Int8Array, RecordBatch};
use rdlt_connector::{
    ColumnKey, ColumnPath, Destination, Exclusion, Field, OP_COLUMN, OpenContext, Partition,
    PartitionId, PipelineId, PipelineState, Push, ReadMode, ReadRequest, SEQ_COLUMN, SourceEvent,
    StreamName, UNCHANGED_COLUMN, partition_channel,
};
use rdlt_connector_reference::{published, schema};
use rdlt_engine::{ErrorKind, RunOutcome, RunStatus, StreamPlan, WriteMode, codes};

use crate::support::served::{Form, Served, ints, people, people_schema, source, texts};
use crate::support::targets::Target;
use crate::support::{commit_every, engine, memory, pipeline, spawned_generator, stream};

/// The state `destination` holds for pipeline `name`.
async fn state(destination: &dyn Destination, name: &str) -> PipelineState {
    let context = OpenContext {
        pipeline: PipelineId::parse(name).expect("a valid pipeline"),
    };
    let opened = destination.open(&context).await.expect("it opens");
    PipelineState::from_records(&opened.state).expect("the state reads")
}

/// Checks that `column` is in none of `table`'s columns, published rows, name map or recorded
/// schema, in `store` and the state of `pipeline`.
async fn lands_nowhere(store: &str, pipeline: &str, table: &str, column: &str) {
    let columns = schema(store, table).expect("the table exists");
    let names: Vec<&str> = columns.fields().iter().map(Field::name).collect();
    assert!(!names.contains(&column), "{store}: {names:?}");
    for batch in published(store, table) {
        assert!(batch.column_by_name(column).is_none(), "{store}");
    }
    let state = state(memory(store).await.as_ref(), pipeline).await;
    let path = rdlt_connector::TablePath::new([table]).expect("a valid path");
    let table = &state.tables[&path];
    let key = ColumnKey::Source(ColumnPath::from(column));
    assert!(table.names.get(&key).is_none(), "{store}");
    let (_, recorded) = table.schema.as_ref().expect("a schema is recorded");
    let recorded: Vec<&str> = recorded.fields().iter().map(Field::name).collect();
    assert!(!recorded.contains(&column), "{store}: {recorded:?}");
}

fn succeeded(outcome: &RunOutcome) {
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{:?}",
        outcome.error
    );
}

#[tokio::test]
async fn an_excluded_column_lands_nowhere_whether_its_source_leaves_it_out_or_not() {
    for form in [Form::Arrow, Form::Json] {
        for honours in [false, true] {
            for declares in [false, true] {
                let name = format!("excluded_{form:?}_{honours}_{declares}").to_lowercase();
                let mut served = Served::new(form, vec![people(0, 3), people(3, 2)]);
                served.honours = honours;
                served.declared = declares.then(people_schema);
                let source = source(&name, served).await;
                let plan = pipeline(&name, [stream("people").exclude(["secret"])]);
                let outcome = engine(commit_every(4))
                    .run(plan, source, memory(&name).await)
                    .await;
                succeeded(&outcome);
                assert_eq!(outcome.report.rows, 5, "{name}");
                lands_nowhere(&name, &name, "people", "secret").await;
            }
        }
    }
}

#[tokio::test]
async fn a_column_the_table_holds_stays_and_holds_null_once_excluded() {
    let store = "excluded_later";
    let run = |plan: StreamPlan, first: i64| async move {
        let served = Served::new(Form::Arrow, vec![people(first, 2)]);
        let plan = pipeline(store, [plan.write(WriteMode::Append)]);
        let source = source(store, served).await;
        engine(commit_every(4))
            .run(plan, source, memory(store).await)
            .await
    };
    succeeded(&run(stream("people"), 0).await);
    succeeded(&run(stream("people").exclude(["note"]), 10).await);
    let columns = schema(store, "people").expect("the table exists");
    assert!(columns.field("note").is_some());
    let mut notes: Vec<(i64, Option<String>)> = Vec::new();
    for batch in published(store, "people") {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_primitive::<Int64Type>();
        let held = batch.column_by_name("note").unwrap().as_string::<i32>();
        for row in 0..batch.num_rows() {
            notes.push((
                ids.value(row),
                held.is_valid(row).then(|| held.value(row).to_owned()),
            ));
        }
    }
    notes.sort();
    let written = |id: i64| (id, Some(format!("w{id}")));
    assert_eq!(notes, [written(0), written(1), (10, None), (11, None)]);
}

/// A change batch of `people`: each row's id, secret, value, and `n`, an update leaving the fields
/// `unchanged` flags as they were.
fn changes(rows: &[(i64, &str, i64, Option<&[u8]>)], first: u64) -> RecordBatch {
    let ids: Vec<i64> = rows.iter().map(|row| row.0).collect();
    let values: Vec<&str> = rows.iter().map(|row| row.1).collect();
    let ns: Vec<i64> = rows.iter().map(|row| row.2).collect();
    let seqs = FixedSizeBinaryArray::try_from_iter((0..rows.len()).map(|row| {
        let mut seq = [0_u8; 16];
        seq[8..].copy_from_slice(&(first + row as u64).to_be_bytes());
        seq
    }))
    .expect("16 bytes each");
    let flags: BinaryArray = rows.iter().map(|row| row.3).collect();
    RecordBatch::try_from_iter([
        ("id", ints(&ids)),
        ("secret", texts(&values)),
        ("value", texts(&values)),
        ("n", ints(&ns)),
        (
            OP_COLUMN,
            Arc::new(Int8Array::from(vec![1_i8; rows.len()])) as ArrayRef,
        ),
        (SEQ_COLUMN, Arc::new(seqs) as ArrayRef),
        (UNCHANGED_COLUMN, Arc::new(flags) as ArrayRef),
    ])
    .expect("a valid change batch")
}

#[tokio::test]
async fn an_update_s_flags_move_past_an_excluded_column() {
    for honours in [false, true] {
        let store = format!("excluded_changes_{honours}");
        // The second update leaves `n`, field 3 of 7, as it was; once `secret` is excluded, `n`
        // is field 2, and a flag left at 3 would name the op column.
        let pushes = vec![
            changes(&[(1, "a", 1, None), (2, "b", 1, None)], 1),
            changes(&[(1, "c", 9, Some(&[0b1000])), (2, "d", 2, None)], 3),
        ];
        let mut served = Served::new(Form::Changes, pushes);
        served.honours = honours;
        served.key = Some(ColumnPath::from("id"));
        let plan = pipeline(
            &store,
            [stream("people")
                .read(ReadMode::Cdc)
                .write(WriteMode::Merge)
                .exclude(["secret"])],
        );
        let source = source(&store, served).await;
        let outcome = engine(commit_every(1))
            .run(plan, source, memory(&store).await)
            .await;
        succeeded(&outcome);
        let mut rows: Vec<(i64, String, i64)> = Vec::new();
        for batch in published(&store, "people") {
            assert!(batch.column_by_name("secret").is_none());
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_primitive::<Int64Type>();
            let values = batch.column_by_name("value").unwrap().as_string::<i32>();
            let ns = batch
                .column_by_name("n")
                .unwrap()
                .as_primitive::<Int64Type>();
            for row in 0..batch.num_rows() {
                rows.push((ids.value(row), values.value(row).to_owned(), ns.value(row)));
            }
        }
        rows.sort();
        assert_eq!(
            rows,
            [(1, "c".to_owned(), 1), (2, "d".to_owned(), 2)],
            "{store}"
        );
    }
}

#[tokio::test]
async fn a_source_s_key_and_change_time_cannot_be_excluded_nor_the_columns_holding_them() {
    let path = |segments: &[&str]| ColumnPath::new(segments.iter().copied()).expect("a segment");
    let cases = [
        ("key", path(&["id"]), path(&["at"]), "id"),
        ("time", path(&["id"]), path(&["at"]), "at"),
        ("nested_key", path(&["a", "b"]), path(&["at"]), "a"),
        ("nested_time", path(&["id"]), path(&["t", "at"]), "t"),
    ];
    for (case, key, change_time, column) in cases {
        let store = format!("excluded_needed_{case}");
        let mut served = Served::new(Form::Arrow, vec![people(0, 1)]);
        served.key = Some(key);
        served.change_time = Some(change_time);
        let plan = pipeline(&store, [stream("people").exclude([column])]);
        let source = source(&store, served).await;
        let outcome = engine(commit_every(4))
            .run(plan, source, memory(&store).await)
            .await;
        assert_eq!(outcome.report.status, RunStatus::Failed, "{case}");
        let error = outcome.error.expect("the run failed");
        assert_eq!(error.kind(), ErrorKind::Config, "{error}");
        assert_eq!(error.code(), Some(codes::PLAN_EXCLUDE_INVALID), "{error}");
        assert!(schema(&store, "people").is_none(), "nothing is read");
    }
}

#[tokio::test]
async fn a_source_served_in_a_process_of_its_own_sends_no_excluded_column() {
    let source = spawned_generator(&[("events", 40, 1, 8)]).await;
    let (sink, mut feed) = partition_channel(NonZeroUsize::new(64).expect("not zero"));
    let request = ReadRequest::new(
        StreamName::new("events").expect("a valid name"),
        Partition::new(PartitionId::parse("0").expect("a valid id")),
        None,
    )
    .excluding(Exclusion::new(["name", "value"]));
    let received = async {
        let mut columns = Vec::new();
        while let Some(event) = feed.recv().await {
            if let SourceEvent::Push(Push::Arrow(batch)) = event {
                let schema = batch.schema();
                columns.push(
                    schema
                        .fields()
                        .iter()
                        .map(|field| field.name().clone())
                        .collect(),
                );
            }
        }
        columns
    };
    let (read, columns): (_, Vec<Vec<String>>) = tokio::join!(source.read(request, sink), received);
    read.expect("the read ends");
    assert_eq!(columns.len(), 5);
    for pushed in columns {
        assert_eq!(pushed, ["id"]);
    }
}

/// Runs pipeline `store` of `target`, whose stream appends `served` excluding every column.
async fn excluding_every_column(target: Target, store: &str, served: Served) -> RunOutcome {
    let name = target.name(store);
    let every = stream("people")
        .write(WriteMode::Append)
        .exclude(["id", "secret", "note"]);
    let source = source(&name, served).await;
    engine(commit_every(4))
        .run(
            pipeline(&name, [every]),
            source,
            target.destination(store).await,
        )
        .await
}

#[tokio::test]
async fn rows_that_keep_no_column_load_into_a_table_of_the_engine_s_columns() {
    let cases = [(false, false), (false, true), (true, false), (true, true)];
    for target in Target::IN_PROCESS {
        for form in [Form::Arrow, Form::Json] {
            for (declares, honours) in cases {
                let store = format!("excluded_every_{form:?}_{declares}_{honours}").to_lowercase();
                let served = |pushes| Served {
                    honours,
                    declared: declares.then(people_schema),
                    ..Served::new(form, pushes)
                };
                let pushes = vec![people(0, 3), people(3, 2)];
                succeeded(&excluding_every_column(target, &store, served(pushes)).await);
                // The table the first run created takes the next run's rows.
                let pushes = vec![people(5, 1)];
                succeeded(&excluding_every_column(target, &store, served(pushes)).await);
                let published = target.published(&store, "people");
                let rows: usize = published.iter().map(RecordBatch::num_rows).sum();
                assert_eq!(rows, 6, "{target:?} {store}");
                for batch in &published {
                    let schema = batch.schema();
                    let mut names = schema.fields().iter().map(|field| field.name());
                    assert!(
                        names.all(|name| name.starts_with("_rdlt_")),
                        "{target:?} {store}: {schema:?}"
                    );
                }
            }
        }
    }
}
