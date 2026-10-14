use std::sync::Arc;

use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch, StringArray};
use rdlt_connector::{
    ChildTable, CommitMeta, CommitSeq, ConnectorErrorKind, DestinationConnector, Epoch, Field,
    LoadId, LogicalType, MergeKey, OpenContext, PipelineId, RootKey, SchemaVersion, SegmentId,
    SegmentSet, Session, TableChange, TablePath, TableRef, TableSchema, TableWriter,
};
use rdlt_testkit::canon::Canon;

use super::cells::{self, Stored};
use super::store::{Staged, Store};
use super::{SimDestination, SimSession};
use crate::rng::SplitMix64;
use crate::seed::{Seed, run};
use crate::world::World;

fn table() -> TableRef {
    TableRef {
        path: TablePath::new(["t"]).unwrap(),
        name: "t".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    }
}

fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    RecordBatch::try_from_iter(columns).unwrap()
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
            let destination = SimDestination { world };
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
