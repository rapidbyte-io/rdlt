use super::super::super::tests::{apply, columns, create, database, pipeline, segments, table};
use super::super::Staged;
use crate::destination::{ChangeColumns, Deletion, MergeKey};
use crate::error::ConnectorErrorKind;
use crate::id::{Epoch, GenerationId};
use crate::types::LogicalType;

fn key(deletion: Deletion) -> MergeKey {
    MergeKey {
        columns: vec!["id".into()],
        seq: "seq".into(),
        root: None,
        changes: Some(ChangeColumns {
            op: "op".into(),
            unchanged: None,
            deletion,
        }),
        history: None,
    }
}

#[test]
fn a_change_stream_merges_into_its_table_never_a_generation() {
    let (connection, planner) = database();
    let fields = [
        ("id", LogicalType::Int64, false),
        ("seq", LogicalType::Binary, false),
    ];
    apply(&connection, &planner, &create(&table("orders"), &fields)).unwrap();
    let columns = columns(&connection, &planner, "orders");
    let staged = Staged {
        name: "orders".to_owned(),
        generation: Some(GenerationId(3)),
        merge: Some(key(Deletion::Hard)),
    };
    let error = planner
        .publish_as(
            &staged,
            &columns,
            &pipeline("mine"),
            Epoch(1),
            &segments(&[1]),
        )
        .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Internal);
}

#[test]
fn soft_deletes_into_a_table_without_their_column_are_refused() {
    let (connection, planner) = database();
    let fields = [
        ("id", LogicalType::Int64, false),
        ("seq", LogicalType::Binary, false),
    ];
    apply(&connection, &planner, &create(&table("orders"), &fields)).unwrap();
    let columns = columns(&connection, &planner, "orders");
    let staged = Staged {
        name: "orders".to_owned(),
        generation: None,
        merge: Some(key(Deletion::Soft {
            at: "deleted_at".into(),
        })),
    };
    let error = planner
        .publish_as(
            &staged,
            &columns,
            &pipeline("mine"),
            Epoch(1),
            &segments(&[1]),
        )
        .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Data);
}
