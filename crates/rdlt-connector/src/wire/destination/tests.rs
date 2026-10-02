use std::sync::Arc;

use super::super::{Invalid, v1};
use crate::commit::CommitMeta;
use crate::destination::TableRef;

/// A table at `orders` named `name`, at schema `version`, keyed by `column`.
fn table(name: &str, version: u32, column: &str) -> v1::TableRef {
    v1::TableRef {
        path: Some(v1::TablePath {
            segments: vec!["orders".to_owned()],
        }),
        name: name.to_owned(),
        version,
        generation: None,
        merge: Some(v1::MergeKey {
            columns: vec![column.to_owned()],
            seq: "_seq".to_owned(),
            ..v1::MergeKey::default()
        }),
    }
}

#[test]
fn a_table_named_as_no_destination_identifier_is_refused() {
    let table_ref = TableRef::try_from(table("orders", 1, "id")).unwrap();
    assert_eq!(table_ref.name, Arc::from("orders"));
    let long = "x".repeat(usize::from(u16::MAX) + 1);
    let refused = [
        table("", 1, "id"),
        table("orders\u{202e}", 1, "id"),
        table("orders\n", 1, "id"),
        table(&long, 1, "id"),
        table("orders", 0, "id"),
        table("orders", 1, ""),
        table("orders", 1, "id\u{200b}"),
    ];
    for refused in refused {
        let name = refused.name.chars().take(16).collect::<String>();
        assert!(TableRef::try_from(refused).is_err(), "{name:?}");
    }
}

#[test]
fn a_commit_naming_a_table_as_no_destination_identifier_is_refused() {
    let commit = |dropped: &str, child: &str| v1::CommitMeta {
        load_id: vec![0; 16].into(),
        commit_seq: 1,
        drop_tables: vec![v1::DroppedTable {
            path: Some(v1::TablePath {
                segments: vec!["orders".to_owned()],
            }),
            name: dropped.to_owned(),
        }],
        child_tables: vec![v1::ChildTable {
            table: child.to_owned(),
            merge: table("x", 1, "id").merge,
        }],
        ..v1::CommitMeta::default()
    };
    assert!(CommitMeta::try_from(commit("orders", "orders__items")).is_ok());
    for (dropped, child) in [
        ("", "items"),
        ("orders\u{2029}", "items"),
        ("orders", "\u{202e}"),
    ] {
        let refused = CommitMeta::try_from(commit(dropped, child));
        assert!(
            matches!(refused, Err(Invalid::Rejected { .. })),
            "{dropped:?} {child:?}"
        );
    }
}
