use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use rdlt_connector::{
    CommitMeta, CommitSeq, ConnectorErrorKind, DestinationConnector, Epoch, Field, LoadId,
    LogicalType, MergeKey, OpenContext, PipelineId, RootKey, SchemaVersion, SegmentId, SegmentSet,
    Session, TableChange, TablePath, TableRef, TableSchema, TableWriter,
};
use rdlt_testkit::canon::Canon;

use super::cells::{Cells, Stored, merge_children};
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
    let world = World::register(name, &mut SplitMix64::new(1));
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
    World::unregister(name);
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
    let world = World::register(name, &mut SplitMix64::new(1));
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
    World::unregister(name);
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

#[test]
fn a_child_table_keeps_only_the_children_of_each_merged_roots_winning_row() {
    let cells = |pairs: &[(&str, &str)]| -> Stored {
        let cells: Cells = pairs
            .iter()
            .map(|(column, value)| ((*column).to_owned(), Canon::Text((*value).to_owned())))
            .collect();
        let empty = RecordBatch::new_empty(Arc::new(arrow_schema::Schema::empty()));
        Stored { cells, row: empty }
    };
    let key = MergeKey {
        columns: vec!["owner".into()],
        seq: "seq".into(),
        root: Some(RootKey {
            table: "roots".into(),
            id: "id".into(),
            seq: "seq".into(),
        }),
    };
    let root = key.root.clone().unwrap();
    let mut published = vec![
        cells(&[("owner", "r1"), ("seq", "1"), ("v", "old")]),
        cells(&[("owner", "r2"), ("seq", "1"), ("v", "kept")]),
        cells(&[("owner", "r3"), ("seq", "1"), ("v", "gone")]),
    ];
    let incoming = vec![
        cells(&[("owner", "r1"), ("seq", "5"), ("v", "new")]),
        cells(&[("owner", "r1"), ("seq", "3"), ("v", "stale")]),
    ];
    let roots = [
        cells(&[("id", "r1"), ("seq", "3")]),
        cells(&[("id", "r1"), ("seq", "5")]),
        cells(&[("id", "r3"), ("seq", "2")]),
    ];
    merge_children(&mut published, incoming, &key, &root, &roots);
    let values: Vec<&str> = published
        .iter()
        .map(|row| match &row.cells["v"] {
            Canon::Text(text) => text.as_str(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(values, ["kept", "new"]);
}

#[test]
fn pipelines_sharing_the_destination_neither_fence_nor_discard_each_other() {
    let name = "shared";
    let world = World::register(name, &mut SplitMix64::new(1));
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
                state_delta: Vec::new(),
                finish_generations: Vec::new(),
                child_tables: Vec::new(),
            };
            let receipt = first.session.commit(&meta).await.unwrap();
            assert_eq!(receipt.rows, 1, "the first pipeline's staged row publishes");
        }
    });
    World::unregister(name);
}
