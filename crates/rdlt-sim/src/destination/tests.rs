use std::sync::Arc;
use std::time::Duration;

use arrow_array::types::Int8Type;
use arrow_array::{
    ArrayRef, BinaryArray, DictionaryArray, Int8Array, Int64Array, ListArray, RecordBatch,
    StringArray, StructArray,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::{DataType, Field as ArrowField};
use rdlt_connector::limits::ListenLimits;
use rdlt_connector::{
    ChildTable, ColumnKey, ColumnPath, CommitMeta, CommitSeq, ConnectContext, ConnectorErrorKind,
    Cursor, DestinationConnector, Epoch, Field, LoadId, LogicalType, MergeKey, NameMap,
    OpenContext, PartitionId, PartitionState, PipelineId, ReadMode, RootKey, SchemaVersion,
    SegmentId, SegmentSet, Session, StateChange, StateEntry, StreamName, TableChange, TablePath,
    TableRef, TableSchema, TableWriter, source_factory,
};
use rdlt_testkit::canon::Canon;
use serde::Serialize;

use super::cells::{self, Stored};
use super::store::{Staged, Store};
use super::{SimDestination, SimSession};
use crate::changes::{Position, SimChangeSource};
use crate::owners::Live;
use crate::rng::SplitMix64;
use crate::seed::{Seed, run};
use crate::source::{SimCursor, SimSource};
use crate::world::{Registered, World};

fn table() -> TableRef {
    TableRef {
        path: TablePath::new(["t"]).unwrap(),
        name: "t".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    }
}

/// A destination over `world` that holds no session yet.
fn destination(world: Arc<World>) -> SimDestination {
    SimDestination {
        world,
        sessions: Arc::default(),
    }
}

fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    RecordBatch::try_from_iter(columns).unwrap()
}

#[test]
fn a_stored_row_keeps_alive_its_own_values_not_its_batch() {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..65_536));
    let texts: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..65_536).map(|row| format!("value {row}")),
    ));
    let stored = cells::rows(&batch(vec![("id", ids), ("text", texts)])).unwrap();
    let kept = stored[7].row.get_array_memory_size();
    assert!(kept < 1 << 10, "a row kept {kept} bytes alive");
    assert_eq!(stored[7].cells["id"], Canon::Number("7".into()));
    assert_eq!(stored[7].cells["text"], Canon::Text("value 7".into()));
}

#[test]
fn a_stored_row_keeps_nested_and_dictionary_values_apart_too() {
    let items = StringArray::from_iter_values((0..4096 * 2).map(|row| format!("item {row}")));
    let lists: ArrayRef = Arc::new(
        ListArray::try_new(
            Arc::new(ArrowField::new("item", DataType::Utf8, true)),
            OffsetBuffer::from_lengths(vec![2; 4096]),
            Arc::new(items),
            None,
        )
        .unwrap(),
    );
    let one: ArrayRef = Arc::new(StringArray::from(vec!["load"]));
    let load: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0_i8; 4096]), one).unwrap(),
    );
    let stored = cells::rows(&batch(vec![("l", lists), ("load", load)])).unwrap();
    let kept = stored[4000].row.get_array_memory_size();
    assert!(kept < 1 << 10, "a row kept {kept} bytes alive");
    assert_eq!(
        stored[4000].cells["l"],
        Canon::List(vec![
            Canon::Text("item 8000".into()),
            Canon::Text("item 8001".into())
        ])
    );
}

/// The violations and results of writing `batches` to a table created with `id: Int64`, then of
/// adding `extra` at `extra`'s type.
fn session_over(
    name: &str,
    batches: Vec<RecordBatch>,
    extra: LogicalType,
) -> (Vec<String>, Option<ConnectorErrorKind>) {
    let registered = World::register(name, &mut SplitMix64::new(1));
    let world = Arc::clone(registered.world());
    let outcome = run(Seed::new(1), {
        let world = Arc::clone(&world);
        |_env| async move {
            let mut session = SimSession {
                world,
                pipeline: PipelineId::parse("sim").unwrap(),
                epoch: Epoch::default(),
                _held: Live::of(&[]),
            };
            let create = TableChange::Create {
                table: table(),
                schema: TableSchema::new(vec![
                    Field::new("id", LogicalType::Int64, false),
                    Field::new("extra", LogicalType::Utf8, true),
                ])
                .unwrap(),
            };
            session.apply_schema(&create).await.unwrap();
            let mut writer = session.writer(&table()).await.unwrap();
            for batch in batches {
                writer.write(SegmentId(1), batch).await.unwrap();
            }
            let add = TableChange::AddColumn {
                table: table(),
                field: Field::new("extra", extra, true),
            };
            session.apply_schema(&add).await.err().map(|error| {
                assert_eq!(error.code(), Some("schema_conflict"));
                error.kind()
            })
        }
    });
    drop(registered);
    (world.violations(), outcome)
}

#[test]
fn writes_of_the_writers_version_pass() {
    let rows = |ids: Vec<i64>, extras: Vec<Option<&str>>| {
        batch(vec![
            ("id", Arc::new(Int64Array::from(ids)) as _),
            ("extra", Arc::new(StringArray::from(extras)) as _),
        ])
    };
    let fitting = vec![
        rows(vec![1, 2], vec![Some("a"), None]),
        rows(vec![3], vec![None]),
    ];
    assert_eq!(
        session_over("fits", fitting, LogicalType::Utf8),
        (Vec::new(), None)
    );
}

#[test]
fn writes_to_missing_columns_or_at_types_they_cannot_hold_are_violations() {
    let unfit = vec![
        batch(vec![("missing", Arc::new(Int64Array::from(vec![1])) as _)]),
        batch(vec![("id", Arc::new(StringArray::from(vec!["2"])) as _)]),
    ];
    let (violations, conflict) = session_over("unfit", unfit, LogicalType::Int64);
    let found = |text: &str| violations.iter().any(|violation| violation.contains(text));
    assert!(found("wrote column missing"), "{violations:?}");
    assert!(found("to column id"), "{violations:?}");
    assert_eq!(conflict, Some(ConnectorErrorKind::Data));
}

#[test]
fn a_writer_given_batches_of_two_schemas_is_a_violation() {
    let name = "versions";
    let registered = World::register(name, &mut SplitMix64::new(1));
    let world = Arc::clone(registered.world());
    run(Seed::new(1), {
        let world = Arc::clone(&world);
        |_env| async move {
            let mut session = SimSession {
                world,
                pipeline: PipelineId::parse("sim").unwrap(),
                epoch: Epoch::default(),
                _held: Live::of(&[]),
            };
            let create = TableChange::Create {
                table: table(),
                schema: TableSchema::new(vec![Field::new("id", LogicalType::Int64, false)])
                    .unwrap(),
            };
            session.apply_schema(&create).await.unwrap();
            let v2 = TableRef {
                version: SchemaVersion(2),
                ..table()
            };
            let add = TableChange::AddColumn {
                table: v2.clone(),
                field: Field::new("extra", LogicalType::Utf8, true),
            };
            session.apply_schema(&add).await.unwrap();
            let both = || {
                batch(vec![
                    ("id", Arc::new(Int64Array::from(vec![1])) as _),
                    ("extra", Arc::new(StringArray::from(vec!["a"])) as _),
                ])
            };
            let id = || batch(vec![("id", Arc::new(Int64Array::from(vec![1])) as _)]);
            // Version 1's batches through its writer, then a version 2 batch through it too.
            let mut old = session.writer(&table()).await.unwrap();
            old.write(SegmentId(1), id()).await.unwrap();
            old.write(SegmentId(1), id()).await.unwrap();
            old.write(SegmentId(1), both()).await.unwrap();
            let mut new = session.writer(&v2).await.unwrap();
            new.write(SegmentId(1), both()).await.unwrap();
        }
    });
    drop(registered);
    let violations = world.violations();
    assert_eq!(violations.len(), 1, "{violations:?}");
    assert!(
        violations[0].contains("version 1 wrote batches of two schemas"),
        "{violations:?}"
    );
}

#[test]
fn a_column_widened_along_two_branches_holds_their_join() {
    let mut columns = super::columns::Columns::new();
    let widen = |to| TableChange::Widen {
        table: table(),
        column: "n".into(),
        from: LogicalType::Int32,
        to,
    };
    let create = TableChange::Create {
        table: table(),
        schema: TableSchema::new(vec![Field::new("n", LogicalType::Int32, true)]).unwrap(),
    };
    for change in [
        create,
        widen(LogicalType::Int64),
        widen(LogicalType::Float64),
    ] {
        super::columns::apply(&mut columns, &change).unwrap();
    }
    assert_eq!(
        columns.get("n"),
        Some(&LogicalType::Int64.join(&LogicalType::Float64))
    );
}

/// The first commit of a load, of segment 1, by a session at the first epoch, with the child
/// tables `children`.
fn first_commit(children: Vec<ChildTable>) -> CommitMeta {
    let mut segments = SegmentSet::new();
    segments.insert(SegmentId(1));
    CommitMeta {
        load_id: LoadId::from_parts(std::time::UNIX_EPOCH, 1),
        commit_seq: CommitSeq::FIRST,
        epoch: Epoch::default(),
        segments,
        abandoned: SegmentSet::new(),
        state_delta: Vec::new(),
        finish_generations: Vec::new(),
        child_tables: children,
        drop_tables: Vec::new(),
        horizon: None,
    }
}

/// A store whose tables `tables` each hold the columns named, of text.
fn texts_store(tables: &[(&str, &[&str])]) -> Store {
    let mut store = Store::default();
    for (name, columns) in tables {
        let table = store.tables.entry((*name).to_owned()).or_default();
        table.columns = columns
            .iter()
            .map(|column| ((*column).to_owned(), LogicalType::Utf8))
            .collect();
    }
    store
}

/// `rows` staged for the merge table `table`, merged by `key`.
fn staged(table: &str, key: &MergeKey, rows: Vec<Stored>) -> Staged {
    Staged {
        table: table.to_owned(),
        generation: None,
        merge: Some(key.clone()),
        rows,
    }
}

#[test]
fn a_child_table_keeps_only_the_children_of_each_merged_roots_winning_row() {
    let text = |values: &[&str]| Arc::new(StringArray::from(values.to_vec())) as ArrayRef;
    let stored = |columns: Vec<(&str, ArrayRef)>| cells::rows(&batch(columns)).unwrap();
    let roots_key = MergeKey {
        columns: vec!["id".into()],
        seq: "seq".into(),
        root: None,
        changes: None,
        history: None,
    };
    let key = MergeKey {
        columns: vec!["owner".into()],
        root: Some(RootKey {
            table: "roots".into(),
            id: "id".into(),
            seq: "seq".into(),
        }),
        ..roots_key.clone()
    };
    let pipeline = PipelineId::parse("sim").unwrap();
    let mut store = texts_store(&[("roots", &["id", "seq"]), ("kids", &["owner", "seq", "v"])]);
    store.tables.get_mut("kids").unwrap().published = stored(vec![
        ("owner", text(&["r1", "r2", "r3"])),
        ("seq", text(&["1", "1", "1"])),
        ("v", text(&["old", "kept", "gone"])),
    ]);
    let roots = stored(vec![
        ("id", text(&["r1", "r1", "r3"])),
        ("seq", text(&["3", "5", "2"])),
    ]);
    let kids = stored(vec![
        ("owner", text(&["r1", "r1"])),
        ("seq", text(&["5", "3"])),
        ("v", text(&["new", "stale"])),
    ]);
    store.staged.insert(
        (pipeline.clone(), SegmentId(1)),
        vec![
            staged("roots", &roots_key, roots),
            staged("kids", &key, kids),
        ],
    );
    let meta = first_commit(vec![ChildTable {
        table: "kids".into(),
        merge: key,
    }]);
    store.publish(&pipeline, &meta).unwrap();
    let mut values: Vec<&str> = store.tables["kids"]
        .published
        .iter()
        .map(|row| match &row.cells["v"] {
            Canon::Text(text) => text.as_str(),
            other => panic!("{other:?}"),
        })
        .collect();
    values.sort_unstable();
    assert_eq!(values, ["kept", "new"]);
}

#[test]
fn pipelines_sharing_the_destination_neither_fence_nor_discard_each_other() {
    let name = "shared";
    let registered = World::register(name, &mut SplitMix64::new(1));
    let world = Arc::clone(registered.world());
    run(Seed::new(1), {
        let world = Arc::clone(&world);
        |_env| async move {
            let destination = destination(world);
            let open = |pipeline: &str| OpenContext {
                pipeline: PipelineId::parse(pipeline).unwrap(),
                load_id: LoadId::from_parts(std::time::UNIX_EPOCH, 1),
            };
            let mut first = destination.open(&open("first")).await.unwrap();
            let create = TableChange::Create {
                table: table(),
                schema: TableSchema::new(vec![Field::new("id", LogicalType::Int64, false)])
                    .unwrap(),
            };
            first.session.apply_schema(&create).await.unwrap();
            let mut writer = first.session.writer(&table()).await.unwrap();
            let rows = batch(vec![("id", Arc::new(Int64Array::from(vec![1])) as _)]);
            writer.write(SegmentId(1), rows).await.unwrap();
            writer.flush().await.unwrap();
            // The second pipeline's open bumps its own epoch and discards its own staging only.
            let mut second = destination.open(&open("second")).await.unwrap();
            second.session.discard_staged().await.unwrap();
            let mut segments = SegmentSet::new();
            segments.insert(SegmentId(1));
            let meta = CommitMeta {
                load_id: LoadId::from_parts(std::time::UNIX_EPOCH, 1),
                commit_seq: CommitSeq::FIRST,
                epoch: first.epoch,
                segments,
                abandoned: SegmentSet::new(),
                state_delta: Vec::new(),
                finish_generations: Vec::new(),
                child_tables: Vec::new(),
                drop_tables: Vec::new(),
                horizon: None,
            };
            let receipt = first.session.commit(&meta).await.unwrap();
            assert_eq!(receipt.rows, 1, "the first pipeline's staged row publishes");
            // The second pipeline's commit declares a horizon past the first's commit: the
            // first's receipt is its own, and stays.
            let past = rdlt_connector::Horizon {
                load_id: LoadId::from_parts(std::time::UNIX_EPOCH, 9),
                commit_seq: CommitSeq::FIRST,
            };
            let theirs = CommitMeta {
                load_id: LoadId::from_parts(std::time::UNIX_EPOCH, 9),
                epoch: second.epoch,
                segments: SegmentSet::new(),
                horizon: Some(past),
                ..meta.clone()
            };
            second.session.commit(&theirs).await.unwrap();
            let again = first.session.commit(&meta).await.unwrap();
            assert_eq!(
                again, receipt,
                "the first pipeline's receipt answers it again"
            );
        }
    });
    drop(registered);
}

/// Reads back what the simulated destination published, for certification.
struct StoreProbe(Arc<World>);

impl rdlt_connector::testing::Probe for StoreProbe {
    fn published<'a>(
        &'a self,
        table: &'a TableRef,
    ) -> rdlt_connector::BoxFuture<'a, rdlt_connector::Result<Vec<RecordBatch>>> {
        let store = self.0.store.lock();
        let rows = store
            .tables
            .get(table.name.as_ref())
            .map(|table| table.published.iter().map(|row| row.row.clone()).collect())
            .unwrap_or_default();
        Box::pin(async move { Ok(rows) })
    }
}

#[test]
fn the_simulated_destination_keeps_history_as_certification_asks() {
    let registered = World::register_changes(
        "certified-history",
        &mut SplitMix64::new(3),
        &mut SplitMix64::new(4),
    );
    let world = Arc::clone(registered.world());
    let probe = StoreProbe(Arc::clone(&world));
    let report = run(Seed::new(3), |_| async move {
        rdlt_connector::testing::certify_destination::<SimDestination>(
            serde_json::json!({ "world": "certified-history" }),
            &probe,
        )
        .await
    });
    drop(registered);
    assert_eq!(
        report.outcome("D-HIST"),
        Some(&rdlt_connector::testing::Outcome::Passed),
        "{report}"
    );
}

/// The merge table `name`, keyed by `key` and sequenced by `seq`.
fn merged(name: &str, key: &str) -> TableRef {
    TableRef {
        path: TablePath::new([name]).unwrap(),
        name: name.into(),
        version: SchemaVersion(1),
        generation: None,
        merge: Some(MergeKey {
            columns: vec![key.into()],
            seq: "seq".into(),
            root: None,
            changes: None,
            history: None,
        }),
    }
}

#[test]
fn a_commit_whose_merge_is_refused_changes_no_table_and_keeps_what_was_staged() {
    let registered = World::register("refused-merge", &mut SplitMix64::new(1));
    let world = Arc::clone(registered.world());
    let (before, refused) = run(Seed::new(5), {
        let world = Arc::clone(&world);
        |_env| async move {
            let mut session = SimSession {
                world: Arc::clone(&world),
                pipeline: PipelineId::parse("sim").unwrap(),
                epoch: Epoch::default(),
                _held: Live::of(&[]),
            };
            // Table `b` is merged by a key column its rows lack, which the merge refuses.
            for table in [merged("a", "id"), merged("b", "missing")] {
                let create = TableChange::Create {
                    table: table.clone(),
                    schema: TableSchema::new(vec![
                        Field::new("id", LogicalType::Int64, false),
                        Field::new("seq", LogicalType::Binary, false),
                    ])
                    .unwrap(),
                };
                session.apply_schema(&create).await.unwrap();
                let mut writer = session.writer(&table).await.unwrap();
                let rows = batch(vec![
                    ("id", Arc::new(Int64Array::from(vec![1])) as _),
                    (
                        "seq",
                        Arc::new(BinaryArray::from_iter_values([[0_u8; 16]])) as _,
                    ),
                ]);
                writer.write(SegmentId(1), rows).await.unwrap();
                writer.flush().await.unwrap();
            }
            let before = world.store.lock().digest();
            (before, session.commit(&first_commit(Vec::new())).await)
        }
    });
    let refused = refused.expect_err("the merge refuses table b");
    assert_eq!(refused.code(), Some("merge_key_invalid"));
    let store = world.store.lock();
    assert_eq!(store.digest(), before, "no table changed");
    assert!(
        store
            .tables
            .values()
            .all(|table| table.published.is_empty())
    );
    assert_eq!(
        store.staged.len(),
        1,
        "what was staged stays for the commit again"
    );
    drop(store);
    drop(registered);
}

#[test]
fn a_row_its_column_widened_past_names_the_type_it_was_written_as() {
    use arrow_array::Float64Array;
    let key = MergeKey {
        columns: vec!["id".into()],
        seq: "seq".into(),
        root: None,
        changes: None,
        history: None,
    };
    let pipeline = PipelineId::parse("sim").unwrap();
    let mut store = Store::default();
    let table = store.tables.entry("t".to_owned()).or_default();
    table.columns = [
        ("id".to_owned(), LogicalType::Int64),
        ("n".to_owned(), LogicalType::Float64),
        ("seq".to_owned(), LogicalType::Binary),
    ]
    .into();
    let row = |id: i64, n: ArrayRef| {
        let seq = Arc::new(BinaryArray::from_iter_values([[0_u8; 16]])) as ArrayRef;
        cells::rows(&batch(vec![
            ("id", Arc::new(Int64Array::from(vec![id])) as _),
            ("n", n),
            ("seq", seq),
        ]))
        .unwrap()
    };
    let float = Arc::new(Float64Array::from(vec![1.5])) as ArrayRef;
    store.staged.insert(
        (pipeline.clone(), SegmentId(1)),
        vec![staged("t", &key, row(1, float))],
    );
    store.publish(&pipeline, &first_commit(Vec::new())).unwrap();
    // The column widens to `Json`; the next commit merges the stored row into it.
    let table = store.tables.get_mut("t").unwrap();
    table.columns.insert("n".to_owned(), LogicalType::Json);
    let text = Arc::new(StringArray::from(vec!["2"])) as ArrayRef;
    store.staged.insert(
        (pipeline.clone(), SegmentId(1)),
        vec![staged("t", &key, row(2, text))],
    );
    store.publish(&pipeline, &first_commit(Vec::new())).unwrap();
    let first = store.tables["t"]
        .published
        .iter()
        .find(|row| row.cells["id"] == Canon::Number("1".to_owned()))
        .expect("row 1 stays");
    let schema = first.row.schema();
    let field = schema.field_with_name("n").unwrap();
    assert_eq!(Field::extension_type(field), Some(LogicalType::Json));
    let written = field
        .metadata()
        .get(cells::WRITTEN)
        .expect("the type it was written as");
    assert_eq!(
        serde_json::from_str::<LogicalType>(written).unwrap(),
        LogicalType::Float64
    );
}

#[test]
fn a_child_table_no_row_ever_reached_follows_its_roots_by_staying_absent() {
    let text = |values: &[&str]| Arc::new(StringArray::from(values.to_vec())) as ArrayRef;
    let roots_key = MergeKey {
        columns: vec!["id".into()],
        seq: "seq".into(),
        root: None,
        changes: None,
        history: None,
    };
    let key = MergeKey {
        columns: vec!["owner".into()],
        root: Some(RootKey {
            table: "roots".into(),
            id: "id".into(),
            seq: "seq".into(),
        }),
        ..roots_key.clone()
    };
    let pipeline = PipelineId::parse("sim").unwrap();
    let mut store = texts_store(&[("roots", &["id", "seq"])]);
    let roots = cells::rows(&batch(vec![("id", text(&["r1"])), ("seq", text(&["1"]))])).unwrap();
    store.staged.insert(
        (pipeline.clone(), SegmentId(1)),
        vec![staged("roots", &roots_key, roots)],
    );
    let meta = first_commit(vec![ChildTable {
        table: "kids".into(),
        merge: key,
    }]);
    store.publish(&pipeline, &meta).unwrap();
    assert!(!store.tables.contains_key("kids"));
}

/// Bounds every wait of a test: a call that hangs fails its test.
const WAIT: Duration = Duration::from_secs(60);

fn opening() -> OpenContext {
    OpenContext {
        pipeline: PipelineId::parse("held").unwrap(),
        load_id: LoadId::from_parts(std::time::UNIX_EPOCH, 1),
    }
}

#[test]
fn a_destination_holding_more_sessions_than_a_connection_may_is_a_violation() {
    let registered = World::register("held-sessions", &mut SplitMix64::new(1));
    let world = Arc::clone(registered.world());
    let bound = ListenLimits::default().connection_sessions();
    let (within, beyond, after) = run(Seed::new(1), {
        let world = Arc::clone(&world);
        |_env| async move {
            let destination = destination(Arc::clone(&world));
            let mut sessions = Vec::new();
            for _ in 0..bound {
                let opened = tokio::time::timeout(WAIT, destination.open(&opening()))
                    .await
                    .expect("the open ends")
                    .expect("the session opens");
                sessions.push(opened.session);
            }
            let within = world.violations();
            let opened = tokio::time::timeout(WAIT, destination.open(&opening()))
                .await
                .expect("the open ends")
                .expect("the session opens");
            sessions.push(opened.session);
            let beyond = world.owners.held();
            drop(sessions);
            (within, beyond, world.owners.held())
        }
    });
    drop(registered);
    assert!(within.is_empty(), "{within:?}");
    assert_eq!(beyond.sessions, bound + 1);
    assert!(after.is_empty(), "{after}");
    let violations = world.violations();
    let held = format!("held {} sessions at once", bound + 1);
    assert!(
        violations.iter().any(|violation| violation.contains(&held)),
        "{violations:?}"
    );
}

#[test]
fn a_session_and_its_writer_count_until_they_end() {
    let registered = World::register("held-until", &mut SplitMix64::new(1));
    let world = Arc::clone(registered.world());
    let counted = run(Seed::new(1), {
        let world = Arc::clone(&world);
        |_env| async move {
            let destination = destination(Arc::clone(&world));
            let mut opened = tokio::time::timeout(WAIT, destination.open(&opening()))
                .await
                .expect("the open ends")
                .expect("the session opens");
            let create = TableChange::Create {
                table: table(),
                schema: TableSchema::new(vec![Field::new("id", LogicalType::Int64, false)])
                    .unwrap(),
            };
            tokio::time::timeout(WAIT, opened.session.apply_schema(&create))
                .await
                .expect("the schema change ends")
                .expect("the schema change applies");
            let writer = tokio::time::timeout(WAIT, opened.session.writer(&table()))
                .await
                .expect("the writer's open ends")
                .expect("the writer opens");
            let open = world.owners.held();
            drop(writer);
            let written = world.owners.held();
            tokio::time::timeout(WAIT, opened.session.close())
                .await
                .expect("the close ends")
                .expect("the session closes");
            (open, written, world.owners.held())
        }
    });
    drop(registered);
    let (open, written, closed) = counted;
    assert_eq!((open.sessions, open.writers), (1, 1));
    assert_eq!((written.sessions, written.writers), (1, 0));
    assert!(closed.is_empty(), "{closed}");
}

#[test]
fn a_fat_value_is_stored_in_its_short_form_and_other_json_as_it_is() {
    let row = crate::workload::Row {
        id: 13,
        partition: 0,
        offset: 8,
        value: 0,
        key: None,
        tag: None,
        extras: Vec::new(),
        delivered: 0,
    };
    let shape = rdlt_testkit::drawn::Shape {
        logical: LogicalType::Json,
        encoding: rdlt_testkit::drawn::Encoding::Plain,
        children: Vec::new(),
    };
    let values: ArrayRef = Arc::new(StringArray::from(vec![
        Some(crate::fat::value(&row, 1_000)),
        Some("{\"a\":1}".to_owned()),
        None,
    ]));
    let field = rdlt_testkit::drawn::field(crate::fat::COLUMN, &shape, &values, true);
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(vec![field])),
        vec![values],
    )
    .unwrap();
    let stored = cells::rows(&batch).unwrap();
    let json = |text: &str| rdlt_testkit::decode::json_as(text, &LogicalType::Json);
    assert_eq!(
        stored[0].cells[crate::fat::COLUMN],
        json(&crate::fat::expected(&row, 1_000))
    );
    assert_eq!(stored[1].cells[crate::fat::COLUMN], json("{\"a\":1}"));
}

#[test]
fn a_fat_column_that_does_not_read_as_text_is_refused() {
    let field = Arc::new(ArrowField::new("a", DataType::Int64, false));
    let values: ArrayRef = Arc::new(StructArray::from(vec![(
        field,
        Arc::new(Int64Array::from(vec![1])) as ArrayRef,
    )]));
    let refused = cells::rows(&batch(vec![(crate::fat::COLUMN, values)])).unwrap_err();
    assert_eq!(refused.kind(), ConnectorErrorKind::Data, "{refused}");
}

#[test]
fn stored_rows_are_the_worlds_and_count_beyond_no_charge() {
    let value = vec![7_u8; 1 << 20];
    let wide: ArrayRef = Arc::new(BinaryArray::from_vec(vec![value.as_slice(); 4]));
    // Kept to the end, since the rows share its schema.
    let written = batch(vec![("wide", wide)]);
    let (rows, held) = crate::oracle::beyond_charge(|| cells::rows(&written).unwrap());
    // What the rows hold is the world's: only the vector holding them is not.
    let vector = u64::try_from(rows.capacity() * size_of::<Stored>()).unwrap();
    assert!(held < vector + (64 << 10), "{held} beside {vector}");
    // Freed here, they are given back, so they hide nothing held after them but the vector made
    // here and the count each row shares with its copies.
    let counts = rows.len() * (2 * size_of::<usize>() + size_of::<crate::oracle::Kept>());
    let counts = u64::try_from(counts).unwrap();
    let ((), held) = crate::oracle::beyond_charge(|| {
        drop(rows);
        drop(std::hint::black_box(vec![0_u8; 1 << 20]));
    });
    assert_eq!(
        held + vector + counts,
        1 << 20,
        "held {held} beside {vector}"
    );
    drop(written);
}

/// A commit that puts `position` for `partition` of `stream`, or deletes its cursor where none.
fn moving<C: Serialize + Default>(
    stream: &StreamName,
    partition: &PartitionId,
    position: Option<C>,
) -> CommitMeta {
    let put = position.is_some();
    let entry = StateEntry::Partition {
        stream: stream.clone(),
        partition: partition.clone(),
        state: PartitionState::Cursor(Cursor::encode(1, &position.unwrap_or_default()).unwrap()),
        load: LoadId::from_parts(std::time::UNIX_EPOCH, 1),
    };
    let change = if put {
        StateChange::Put(entry.to_record())
    } else {
        StateChange::Delete(entry.key().encode())
    };
    CommitMeta {
        state_delta: vec![change],
        ..first_commit(Vec::new())
    }
}

#[test]
fn a_source_told_late_of_a_cursor_a_later_commit_removed_hears_what_was_committed() {
    let name = "told-late";
    let registered =
        World::register_changes(name, &mut SplitMix64::new(3), &mut SplitMix64::new(4));
    let world = Arc::clone(registered.world());
    let replayable = world
        .changes
        .streams
        .iter()
        .find(|stream| stream.replayable);
    let stream = StreamName::new(&replayable.expect("a replayable stream").name).unwrap();
    let partition = PartitionId::parse("s0").unwrap();
    let (committed, beyond) = (
        Position {
            next: 7,
            done: true,
        },
        Position {
            next: 8,
            done: true,
        },
    );
    {
        let mut store = world.store.lock();
        let pipeline = PipelineId::parse("changes").unwrap();
        store.apply(
            &world,
            &pipeline,
            &moving(&stream, &partition, Some(committed)),
        );
        // The read of the snapshot partition is over: the next commit moves on to the changes.
        store.apply(
            &world,
            &pipeline,
            &moving::<Position>(&stream, &partition, None),
        );
    }
    for position in [committed, beyond] {
        world.reports.note(stream.name(), &partition, &position);
    }
    let heard = run(Seed::new(3), {
        let (world, stream) = (Arc::clone(&world), stream.clone());
        |_| async move {
            let config = serde_json::json!({ "world": name });
            let source = source_factory::<SimChangeSource>()
                .connect(config, ConnectContext::new())
                .await
                .unwrap();
            let mut heard = Vec::new();
            for position in [committed, beyond] {
                let cursor = Cursor::encode(1, &position).unwrap();
                let told = [(partition.clone(), cursor)];
                source.committed(&stream, &told).await.unwrap();
                heard.push(world.violations().len());
            }
            heard
        }
    });
    drop(registered);
    // Told late, it heard a position a commit landed; a position past every commit is a violation.
    assert_eq!(heard, [0, 1], "{:#?}", world.violations());
}

/// A world registered as `name` whose first stream is read incrementally.
fn incremental_world(name: &str) -> Registered {
    for seed in 0.. {
        let registered = World::register(name, &mut SplitMix64::new(seed));
        if registered.world().workload.streams[0].read == ReadMode::Incremental {
            return registered;
        }
    }
    unreachable!("some seed reads its first stream incrementally")
}

/// A commit that moves partition `p0` of stream `s0` to `next`.
fn cursor_commit(next: u64) -> CommitMeta {
    let cursor = SimCursor {
        next,
        pad: String::new(),
    };
    let entry = StateEntry::Partition {
        stream: StreamName::new("s0").unwrap(),
        partition: PartitionId::parse("p0").unwrap(),
        state: PartitionState::Cursor(Cursor::encode(1, &cursor).unwrap()),
        load: LoadId::from_parts(std::time::UNIX_EPOCH, 1),
    };
    CommitMeta {
        state_delta: vec![StateChange::Put(entry.to_record())],
        ..first_commit(Vec::new())
    }
}

#[test]
fn a_row_of_an_incremental_stream_published_below_where_its_partition_was_held_is_a_violation() {
    let registered = incremental_world("below");
    let world = registered.world();
    let pipeline = PipelineId::parse("sim").unwrap();
    let path = TablePath::new(["s0"]).unwrap();
    let mut names = NameMap::default();
    for column in ["partition", "offset"] {
        names
            .insert(ColumnKey::Source(ColumnPath::from(column)), column)
            .unwrap();
    }
    let mapped = StateEntry::Names {
        table: path.clone(),
        physical: "s0".into(),
        names,
    };
    let mut store = Store::default();
    store.names.insert(path, "s0".to_owned());
    let naming = CommitMeta {
        state_delta: vec![StateChange::Put(mapped.to_record())],
        ..first_commit(Vec::new())
    };
    store.apply(world, &pipeline, &naming);
    store.apply(world, &pipeline, &cursor_commit(5));
    let held = store.positions(&pipeline);
    store.apply(world, &pipeline, &cursor_commit(8));
    let rows = |offsets: Vec<i64>| {
        let partitions: ArrayRef = Arc::new(Int64Array::from(vec![0; offsets.len()]));
        let offsets: ArrayRef = Arc::new(Int64Array::from(offsets));
        cells::rows(&batch(vec![("partition", partitions), ("offset", offsets)])).unwrap()
    };
    store.check_cursors(
        world,
        &pipeline,
        &[("s0".to_owned(), rows(vec![5, 7]))],
        &held,
    );
    assert!(world.violations().is_empty(), "{:?}", world.violations());
    store.check_cursors(world, &pipeline, &[("s0".to_owned(), rows(vec![4]))], &held);
    let violations = world.violations();
    assert_eq!(violations.len(), 1, "{violations:?}");
    assert!(violations[0].contains("below"), "{violations:?}");
    drop(registered);
}

#[test]
fn a_source_told_late_of_a_cursor_a_newer_full_read_restarted_hears_what_was_committed() {
    let name = "told-late-rows";
    let registered = World::register(name, &mut SplitMix64::new(1));
    let world = Arc::clone(registered.world());
    let replayable = world
        .workload
        .streams
        .iter()
        .find(|stream| stream.replayable);
    let stream = StreamName::new(&replayable.expect("a replayable stream").name).unwrap();
    let partition = PartitionId::parse("p0").unwrap();
    let cursor = |next| SimCursor {
        next,
        pad: String::new(),
    };
    {
        let mut store = world.store.lock();
        let pipeline = PipelineId::parse("sim").unwrap();
        store.apply(
            &world,
            &pipeline,
            &moving(&stream, &partition, Some(cursor(22))),
        );
        // A newer run reads the stream in full again, from its start.
        store.apply(
            &world,
            &pipeline,
            &moving::<SimCursor>(&stream, &partition, None),
        );
        store.apply(
            &world,
            &pipeline,
            &moving(&stream, &partition, Some(cursor(20))),
        );
    }
    for next in [22, 23] {
        world.reports.note(stream.name(), &partition, &cursor(next));
    }
    let heard = run(Seed::new(1), {
        let (world, stream) = (Arc::clone(&world), stream.clone());
        |_| async move {
            let config = serde_json::json!({ "world": name });
            let source = source_factory::<SimSource>()
                .connect(config, ConnectContext::new())
                .await
                .unwrap();
            let mut heard = Vec::new();
            for next in [22, 23] {
                let told = [(partition.clone(), Cursor::encode(1, &cursor(next)).unwrap())];
                source.committed(&stream, &told).await.unwrap();
                heard.push(world.violations().len());
            }
            heard
        }
    });
    drop(registered);
    // Told late, it heard a cursor a commit landed; a cursor past every commit is a violation.
    assert_eq!(heard, [0, 1], "{:#?}", world.violations());
}
