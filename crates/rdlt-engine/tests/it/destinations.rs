//! The engine's destination-facing behavior against every reference destination: exactly-once
//! loads, resumes, replace generations, merges, lost responses, fencing and schema changes.

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{ArrayRef, Int32Array, StringArray, StructArray};
use arrow_schema::{DataType, Field as ArrowField};
use rdlt_connector::ReadMode;
use rdlt_engine::{CommitPolicy, EngineConfig, ErrorKind, RunStatus, StopMode, WriteMode};
use serde_json::json;

use crate::schema::{batch, ints, text};
use crate::support::batches::{BatchStream, batches};
use crate::support::destinations::{Step, failing};
use crate::support::script::{Script, ScriptStream, id, reconnect};
use crate::support::targets::Target;
use crate::support::{
    commit_every, each, engine, every_id, generator, pipeline, retrying, stream, until,
};

fn ids(partitions: usize, rows: u64) -> Vec<i64> {
    let mut ids: Vec<i64> = (0..partitions)
        .flat_map(|partition| (0..rows).map(move |offset| id(partition, offset)))
        .collect();
    ids.sort_unstable();
    ids
}

#[tokio::test(start_paused = true)]
async fn every_destination_publishes_every_row_once() {
    each(Target::IN_PROCESS, publishes_every_row_once).await;
}

pub(crate) async fn publishes_every_row_once(target: Target) {
    let source = generator(&[("orders", 1000, 4, 37)]).await;
    let outcome = engine(commit_every(250))
        .run(
            pipeline("every-row", [stream("orders")]),
            source,
            target.destination("every_row").await,
        )
        .await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded, "{target:?}");
    assert_eq!(
        target.ids("every_row", "orders"),
        every_id(1000),
        "{target:?}"
    );
    assert_eq!(outcome.report.rows, 1000, "{target:?}");
}

#[tokio::test(start_paused = true)]
async fn every_destination_resumes_an_incremental_read_where_it_committed() {
    each(
        Target::IN_PROCESS,
        resumes_an_incremental_read_where_it_committed,
    )
    .await;
}

pub(crate) async fn resumes_an_incremental_read_where_it_committed(target: Target) {
    let name = target.name("incremental");
    let (script, source) = Script::new(vec![ScriptStream::new("events", 2, 30, 7)])
        .connect(&name)
        .await;
    let plan = pipeline(
        "incremental",
        [stream("events").read(ReadMode::Incremental)],
    );
    let engine = engine(commit_every(10));
    let first = engine
        .run(
            plan.clone(),
            source,
            target.destination("incremental").await,
        )
        .await;
    assert_eq!(first.report.rows, 60, "{target:?}");
    script.streams[0].grow(5);
    let second = engine
        .run(
            plan,
            reconnect(&name).await,
            target.destination("incremental").await,
        )
        .await;
    assert_eq!(second.report.rows, 10, "{target:?}");
    assert_eq!(
        target.ids("incremental", "events"),
        ids(2, 35),
        "{target:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn every_destination_appends_a_fresh_copy_every_full_run_and_replaces_one() {
    each(
        Target::IN_PROCESS,
        appends_a_fresh_copy_every_full_run_and_replaces_one,
    )
    .await;
}

pub(crate) async fn appends_a_fresh_copy_every_full_run_and_replaces_one(target: Target) {
    let engine = engine(commit_every(100));
    for run in 1..=2 {
        let source = generator(&[("orders", 300, 3, 25)]).await;
        let plan = pipeline("full-append", [stream("orders")]);
        let outcome = engine
            .run(plan, source, target.destination("full_append").await)
            .await;
        assert_eq!(outcome.report.rows, 300, "{target:?}");
        assert_eq!(
            target.rows("full_append", "orders"),
            300 * run,
            "{target:?}"
        );
        let source = generator(&[("orders", 400, 2, 30)]).await;
        let plan = pipeline("replace", [stream("orders").write(WriteMode::Replace)]);
        let outcome = engine
            .run(plan, source, target.destination("replace").await)
            .await;
        assert_eq!(
            outcome.report.streams["orders"].generations_swapped, 1,
            "{target:?}"
        );
        assert_eq!(target.ids("replace", "orders"), every_id(400), "{target:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn every_destination_keeps_a_stopped_replace_hidden_until_it_completes() {
    each(
        Target::IN_PROCESS,
        keeps_a_stopped_replace_hidden_until_it_completes,
    )
    .await;
}

pub(crate) async fn keeps_a_stopped_replace_hidden_until_it_completes(target: Target) {
    let mut stream_script = ScriptStream::new("orders", 2, 40, 5);
    stream_script.idle = true;
    let (script, source) = Script::new(vec![stream_script])
        .connect(&target.name("stopped_replace"))
        .await;
    let plan = pipeline(
        "stopped-replace",
        [stream("orders").write(WriteMode::Replace)],
    );
    let engine = engine(commit_every(20));
    let run = engine.run(
        plan.clone(),
        source,
        target.destination("stopped_replace").await,
    );
    let control = run.control();
    let stop = async {
        until(|| script.acks.lock().len() >= 4).await;
        control.stop(StopMode::AfterCommit);
    };
    let (outcome, ()) = tokio::join!(run, stop);
    assert_eq!(outcome.report.status, RunStatus::Stopped, "{target:?}");
    assert_eq!(target.rows("stopped_replace", "orders"), 0, "{target:?}");
    let (_, source) = Script::new(vec![ScriptStream::new("orders", 2, 40, 5)])
        .connect(&target.name("stopped_replace_done"))
        .await;
    let outcome = engine
        .run(plan, source, target.destination("stopped_replace").await)
        .await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded, "{target:?}");
    assert_eq!(
        target.ids("stopped_replace", "orders"),
        ids(2, 40),
        "{target:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn every_destination_merges_the_newest_row_of_each_key_across_runs() {
    each(
        Target::IN_PROCESS,
        merges_the_newest_row_of_each_key_across_runs,
    )
    .await;
}

pub(crate) async fn merges_the_newest_row_of_each_key_across_runs(target: Target) {
    let engine = engine(commit_every(1));
    let runs = [
        vec![
            batch(vec![("id", ints(&[1, 2])), ("v", text(&["a", "b"]))]),
            batch(vec![("id", ints(&[1])), ("v", text(&["c"]))]),
        ],
        vec![batch(vec![
            ("id", ints(&[2, 3, 2])),
            ("v", text(&["x", "y", "z"])),
        ])],
    ];
    for rows in runs {
        let keyed = BatchStream::new("events", rows).primary_key(&["id"]);
        let outcome = engine
            .run(
                pipeline("merge", [stream("events").write(WriteMode::Merge)]),
                batches(&target.name("merge"), vec![keyed]).await,
                target.destination("merge").await,
            )
            .await;
        assert_eq!(outcome.report.status, RunStatus::Succeeded, "{target:?}");
    }
    let expected = [
        json!({"id": 1, "v": "c"}),
        json!({"id": 2, "v": "z"}),
        json!({"id": 3, "v": "y"}),
    ];
    assert_eq!(target.json("merge", "events"), expected, "{target:?}");
}

#[tokio::test(start_paused = true)]
async fn every_destination_publishes_a_commit_whose_response_was_lost_once() {
    each(
        Target::IN_PROCESS,
        publishes_a_commit_whose_response_was_lost_once,
    )
    .await;
}

pub(crate) async fn publishes_a_commit_whose_response_was_lost_once(target: Target) {
    let source = generator(&[("orders", 300, 3, 25)]).await;
    let destination = failing(target.destination("lost").await, Step::LoseResponse);
    let config =
        retrying(3).commit(CommitPolicy::new(None, Some(1_000_000), None).expect("a valid policy"));
    let outcome = engine(config)
        .run(pipeline("lost", [stream("orders")]), source, destination)
        .await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded, "{target:?}");
    assert_eq!(outcome.report.attempts.len(), 2, "{target:?}");
    assert_eq!(target.ids("lost", "orders"), every_id(300), "{target:?}");
    let (_, source) = Script::new(vec![ScriptStream::new("events", 1, 40, 5)])
        .connect(&target.name("lost_then_open"))
        .await;
    let destination = failing(
        target.destination("lost_then_open").await,
        Step::LoseResponseThenOpen,
    );
    let plan = pipeline(
        "lost-then-open",
        [stream("events").read(ReadMode::Incremental)],
    );
    let outcome = engine(retrying(3)).run(plan, source, destination).await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded, "{target:?}");
    assert_eq!(target.rows("lost_then_open", "events"), 40, "{target:?}");
}

#[tokio::test(start_paused = true)]
async fn every_destination_fences_an_older_run() {
    each(Target::IN_PROCESS, fences_an_older_run).await;
}

pub(crate) async fn fences_an_older_run(target: Target) {
    let name = target.name("fenced");
    let mut idle = ScriptStream::new("events", 1, 10, 5);
    idle.idle = true;
    let (script, source) = Script::new(vec![idle]).connect(&name).await;
    let plan = pipeline("fenced", [stream("events").read(ReadMode::Incremental)]);
    let policy =
        CommitPolicy::new(Some(Duration::from_millis(200)), None, None).expect("a valid policy");
    let engine = engine(EngineConfig::builder().commit(policy).lanes(1));
    let older = engine.run(plan.clone(), source, target.destination("fenced").await);
    let newer = async {
        until(|| !script.acks.lock().is_empty()).await;
        let newer = engine.run(
            plan.clone(),
            reconnect(&name).await,
            target.destination("fenced").await,
        );
        let control = newer.control();
        let stop = async {
            tokio::time::sleep(Duration::from_millis(500)).await;
            script.streams[0].grow(5);
            tokio::time::sleep(Duration::from_secs(3)).await;
            control.stop(StopMode::AfterCommit);
        };
        tokio::join!(newer, stop).0
    };
    let (older, newer) = tokio::join!(older, newer);
    assert_eq!(
        older.error.map(|error| error.kind()),
        Some(ErrorKind::Fenced),
        "{target:?}"
    );
    assert_eq!(newer.report.status, RunStatus::Stopped, "{target:?}");
    assert_eq!(target.ids("fenced", "events"), ids(1, 15), "{target:?}");
}

#[tokio::test(start_paused = true)]
async fn every_destination_evolves_a_table_as_its_batches_change() {
    each(Target::IN_PROCESS, evolves_a_table_as_its_batches_change).await;
}

pub(crate) async fn evolves_a_table_as_its_batches_change(target: Target) {
    let small: ArrayRef = Arc::new(Int32Array::from(vec![1]));
    let rows = vec![
        batch(vec![("id", small)]),
        batch(vec![("id", ints(&[1 << 40])), ("note", text(&["n"]))]),
    ];
    let source = batches(
        &target.name("evolve"),
        vec![BatchStream::new("events", rows)],
    )
    .await;
    let outcome = engine(commit_every(10))
        .run(
            pipeline("evolve", [stream("events")]),
            source,
            target.destination("evolve").await,
        )
        .await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded, "{target:?}");
    assert_eq!(
        target.json("evolve", "events"),
        [json!({"id": 1_i64 << 40, "note": "n"}), json!({"id": 1})],
        "{target:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn every_destination_stores_nested_values_natively_or_as_json_text() {
    each(
        Target::IN_PROCESS,
        stores_nested_values_natively_or_as_json_text,
    )
    .await;
}

pub(crate) async fn stores_nested_values_natively_or_as_json_text(target: Target) {
    let inner: ArrayRef = Arc::new(StringArray::from(vec!["a"]));
    let point: ArrayRef = Arc::new(StructArray::from(vec![(
        Arc::new(ArrowField::new("tag", DataType::Utf8, true)),
        inner,
    )]));
    let rows = vec![batch(vec![("id", ints(&[1])), ("point", point)])];
    let source = batches(
        &target.name("nested"),
        vec![BatchStream::new("events", rows)],
    )
    .await;
    let outcome = engine(commit_every(10))
        .run(
            pipeline("nested", [stream("events")]),
            source,
            target.destination("nested").await,
        )
        .await;
    assert_eq!(outcome.report.status, RunStatus::Succeeded, "{target:?}");
    let point = match target.kind() {
        Target::Sqlite => json!("{\"tag\":\"a\"}"),
        _ => json!({"tag": "a"}),
    };
    assert_eq!(
        target.json("nested", "events"),
        [json!({"id": 1, "point": point})],
        "{target:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn every_destination_loads_streams_named_like_its_own_tables() {
    each(Target::IN_PROCESS, loads_streams_named_like_its_own_tables).await;
}

pub(crate) async fn loads_streams_named_like_its_own_tables(target: Target) {
    let source = generator(&[
        ("orders", 20, 1, 10),
        ("sqlite_stat", 20, 1, 10),
        ("_rdlt_staging__orders", 20, 1, 10),
    ])
    .await;
    let plan = pipeline(
        "hostile",
        [
            stream("orders"),
            stream("sqlite_stat"),
            stream("_rdlt_staging__orders"),
        ],
    );
    let outcome = engine(commit_every(10))
        .run(plan, source, target.destination("hostile").await)
        .await;
    assert_eq!(
        outcome.report.status,
        RunStatus::Succeeded,
        "{target:?}: {:?}",
        outcome.error
    );
    let escaped = |name: &str| match target.kind() {
        Target::Sqlite if name != "orders" => format!("_{name}"),
        _ => name.to_owned(),
    };
    for name in ["orders", "sqlite_stat", "_rdlt_staging__orders"] {
        assert_eq!(
            target.ids("hostile", &escaped(name)),
            every_id(20),
            "{target:?} {name}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn the_json_lines_destination_keeps_few_files_however_many_commits_append() {
    keeps_few_files(Target::Jsonl).await;
}

#[tokio::test(start_paused = true)]
async fn the_arrow_destination_keeps_few_files_however_many_commits_append() {
    keeps_few_files(Target::Arrow).await;
}

/// The runs each steady source of [`keeps_few_files`] is read by.
const STEADY_RUNS: u64 = 32;

/// One partition appended by run after run, each a session committing what the source grew by,
/// for sources of each steady shape: the batches the engine writes, each with the dictionary
/// columns of its load, are merged as they are published, so the table's files stay few, no
/// merge writes the table again and again, and every row is there once, in the order it was
/// read.
async fn keeps_few_files(target: Target) {
    // Rows a run, pushed in batches of eight: less than a batch, one batch, a batch and its
    // remainder, and batches and their remainder.
    let mut failures = Vec::new();
    for rows in [3, 8, 9, 20] {
        let store = format!("few_files_{rows}");
        let written = steadily_appended(target, &store, rows).await;
        let files = target.data_files(&store);
        // Files that at least halve in rows number at most the logarithm of the rows, and one.
        let most = usize::try_from((rows * STEADY_RUNS).ilog2()).expect("a logarithm fits") + 1;
        if files.len() > most {
            failures.push(format!("{rows} rows a run: {} files", files.len()));
        }
        // A row is written again only into a file half as large again as the file it was in,
        // so a table is written by merges at most the logarithm of its runs times over, and
        // not once a run, as a rule that merges everything does.
        let kept: u64 = files.iter().map(|(_, bytes)| bytes).sum();
        let often = u64::from(STEADY_RUNS.ilog2()) + 2;
        if written > often * kept {
            failures.push(format!(
                "{rows} rows a run: merges wrote {written} of {kept}"
            ));
        }
        let expected = ids(1, rows * STEADY_RUNS);
        if read_in_order(target, &store) != expected {
            failures.push(format!("{rows} rows a run: other rows than were read"));
        }
    }
    assert!(failures.is_empty(), "{target:?}: {failures:?}");
}

/// Reads a source that grows by `rows` rows a run into `store`, run after run; the bytes of
/// every file a merge wrote.
async fn steadily_appended(target: Target, store: &str, rows: u64) -> u64 {
    let name = target.name(store);
    let (script, source) = Script::new(vec![ScriptStream::new("events", 1, rows, 8)])
        .connect(&name)
        .await;
    let plan = pipeline("few-files", [stream("events").read(ReadMode::Incremental)]);
    let engine = engine(commit_every(1000));
    let mut source = Some(source);
    let (mut seen, mut written) = (Vec::new(), 0);
    for run in 0..STEADY_RUNS {
        let source = match source.take() {
            Some(source) => source,
            None => reconnect(&name).await,
        };
        let outcome = engine
            .run(plan.clone(), source, target.destination(store).await)
            .await;
        assert_eq!(
            outcome.report.status,
            RunStatus::Succeeded,
            "{target:?} {rows} {run}"
        );
        script.streams[0].grow(rows);
        let files = target.data_files(store);
        written += files
            .iter()
            .filter(|(path, _)| {
                path.components()
                    .any(|part| part.as_os_str() == "compacted")
            })
            .filter(|(path, _)| !seen.contains(path))
            .map(|(_, bytes)| bytes)
            .sum::<u64>();
        seen = files.into_iter().map(|(path, _)| path).collect();
    }
    written
}

/// The ids `store` publishes for the table, in the order its files hold them; every other
/// column, the load's dictionary columns among them, holds a value in every row.
fn read_in_order(target: Target, store: &str) -> Vec<i64> {
    target
        .published(store, "events")
        .iter()
        .flat_map(|batch| {
            for column in batch.columns() {
                assert_eq!(column.logical_null_count(), 0, "{target:?} {store}");
            }
            let ids = batch.column_by_name("id").expect("an id column");
            let ids = ids
                .as_any()
                .downcast_ref::<arrow_array::Int64Array>()
                .expect("ids read back as Int64");
            ids.values().to_vec()
        })
        .collect()
}
