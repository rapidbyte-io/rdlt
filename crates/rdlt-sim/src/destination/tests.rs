use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use rdlt_connector::{
    ConnectorErrorKind, Epoch, Field, LogicalType, MergeKey, RootKey, SchemaVersion, SegmentId,
    Session, TableChange, TablePath, TableRef, TableSchema, TableWriter,
};
use serde_json::json;

use super::SimSession;
use super::cells::{Cells, merge_children};
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
fn writes_that_fit_the_tables_columns_pass() {
    let fitting = vec![
        batch(vec![("id", Arc::new(Int64Array::from(vec![1])) as _)]),
        batch(vec![("id", Arc::new(Int32Array::from(vec![2])) as _)]),
        batch(vec![
            ("extra", Arc::new(StringArray::from(vec!["a"])) as _),
            ("id", Arc::new(Int64Array::from(vec![3])) as _),
        ]),
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
    assert_eq!(violations.len(), 2, "{violations:?}");
    assert!(violations[0].contains("missing"), "{violations:?}");
    assert!(violations[1].contains("id"), "{violations:?}");
    assert_eq!(conflict, Some(ConnectorErrorKind::Data));
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
    let cells = |pairs: &[(&str, &str)]| -> Cells {
        pairs
            .iter()
            .map(|(column, value)| ((*column).to_owned(), json!(value)))
            .collect()
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
        .map(|row| row["v"].as_str().unwrap())
        .collect();
    assert_eq!(values, ["kept", "new"]);
}
