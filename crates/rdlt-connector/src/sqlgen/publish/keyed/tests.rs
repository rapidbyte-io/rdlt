use super::super::super::tests::{pipeline, segments};
use super::super::super::{Column, SqlPlanner, Sqlite, Staged};
use super::unused;
use crate::destination::{ChangeColumns, Deletion, HistoryColumns, MergeKey, RootKey};
use crate::error::ConnectorErrorKind;
use crate::id::Epoch;

fn columns(names: &[&str]) -> Vec<Column> {
    names
        .iter()
        .map(|name| Column {
            name: (*name).to_owned(),
            declared: "INTEGER".to_owned(),
        })
        .collect()
}

#[test]
fn a_name_is_its_own_unless_a_column_has_it_in_any_case() {
    assert_eq!(unused("_rdlt_rank", &columns(&["id", "seq"])), "_rdlt_rank");
    assert_eq!(
        unused("_rdlt_rank", &columns(&["_RDLT_Rank", "_rdlt_rank_"])),
        "_rdlt_rank__"
    );
}

fn plain() -> MergeKey {
    MergeKey {
        columns: vec!["id".into()],
        seq: "seq".into(),
        root: None,
        changes: None,
        history: None,
    }
}

fn changes(deletion: Deletion, unchanged: Option<&str>) -> ChangeColumns {
    ChangeColumns {
        op: "op".into(),
        unchanged: unchanged.map(Into::into),
        deletion,
    }
}

/// A key of `orders`' columns of each kind a table merges by: plain, a change stream's with hard
/// and soft deletes, a history table's, plain and of a change stream, and a child table's.
fn keys() -> Vec<(&'static str, MergeKey)> {
    let soft = || Deletion::Soft {
        at: "deleted_at".into(),
    };
    let history = || {
        Some(HistoryColumns {
            valid_from: "valid_from".into(),
            valid_to: "valid_to".into(),
            is_current: "is_current".into(),
            row_hash: "row_hash".into(),
        })
    };
    let root = Some(RootKey {
        table: "roots".into(),
        id: "id".into(),
        seq: "seq".into(),
    });
    let keyed = |changes, history, root| MergeKey {
        root,
        changes,
        history,
        ..plain()
    };
    vec![
        ("plain", plain()),
        (
            "hard changes",
            keyed(Some(changes(Deletion::Hard, Some("unchanged"))), None, None),
        ),
        (
            "soft changes",
            keyed(Some(changes(soft(), Some("unchanged"))), None, None),
        ),
        ("history", keyed(None, history(), None)),
        (
            "changes' history",
            keyed(Some(changes(soft(), None)), history(), None),
        ),
        ("child", keyed(None, None, root)),
    ]
}

/// Every column a table needs to hold any of [`keys`].
const HELD: [&str; 7] = [
    "id",
    "seq",
    "deleted_at",
    "valid_from",
    "valid_to",
    "is_current",
    "row_hash",
];

/// The outcome of publishing `orders`, whose columns are `names`, by `key`.
fn published(key: &MergeKey, names: &[&str]) -> crate::error::Result<()> {
    let planner = SqlPlanner::try_new(Sqlite).unwrap();
    let staged = Staged {
        name: "orders".to_owned(),
        generation: None,
        merge: Some(key.clone()),
    };
    planner
        .publish_as(
            &staged,
            &columns(names),
            &pipeline("mine"),
            Epoch(1),
            &segments(&[1]),
        )
        .map(drop)
}

fn invalid(outcome: crate::error::Result<()>, context: &str) {
    let error = outcome.unwrap_err();
    assert_eq!(
        (error.kind(), error.code()),
        (ConnectorErrorKind::Data, Some("merge_key_invalid")),
        "{context}"
    );
}

#[test]
fn a_merge_key_of_no_column_or_of_columns_the_table_lacks_is_refused() {
    for (kind, key) in keys() {
        published(&key, &HELD).unwrap_or_else(|error| panic!("{kind}: {error}"));
        // A child table is keyed by its first column alone, its root id.
        let lacked: Vec<std::sync::Arc<str>> = match key.root {
            Some(_) => vec!["missing".into()],
            None => vec!["id".into(), "missing".into()],
        };
        let broken = [
            MergeKey {
                columns: Vec::new(),
                ..key.clone()
            },
            MergeKey {
                columns: lacked,
                ..key.clone()
            },
            MergeKey {
                seq: "missing".into(),
                ..key.clone()
            },
        ];
        for key in &broken {
            invalid(published(key, &HELD), &format!("{kind}: {key:?}"));
        }
    }
}

#[test]
fn a_merge_key_s_deletion_time_and_history_columns_are_the_table_s() {
    for (kind, key) in keys() {
        let history = key.history.iter().flat_map(|history| {
            [
                &history.valid_from,
                &history.valid_to,
                &history.is_current,
                &history.row_hash,
            ]
        });
        let at = key
            .changes
            .iter()
            .filter_map(|changes| match &changes.deletion {
                Deletion::Soft { at } => Some(at),
                Deletion::Hard => None,
            });
        let named: Vec<String> = history.chain(at).map(ToString::to_string).collect();
        assert_eq!(
            named.is_empty(),
            matches!(kind, "plain" | "hard changes" | "child")
        );
        for missing in named {
            let lacking: Vec<&str> = HELD
                .into_iter()
                .filter(|column| *column != missing)
                .collect();
            invalid(
                published(&key, &lacking),
                &format!("{kind} without {missing}"),
            );
        }
    }
}

#[test]
fn a_child_table_s_root_takes_no_reserved_name() {
    let planner = SqlPlanner::try_new(Sqlite).unwrap();
    let (_, mut key) = keys().pop().unwrap();
    if let Some(root) = key.root.as_mut() {
        root.table = "_rdlt_state".into();
    }
    let staged = Staged {
        name: "orders".to_owned(),
        generation: None,
        merge: Some(key),
    };
    let error = planner
        .publish_as(
            &staged,
            &columns(&["id", "seq"]),
            &pipeline("mine"),
            Epoch(1),
            &segments(&[1]),
        )
        .unwrap_err();
    assert_eq!(error.code(), Some("table_name_reserved"));
}

#[test]
fn a_writer_s_indexes_need_a_key() {
    let planner = SqlPlanner::try_new(Sqlite).unwrap();
    let mine = pipeline("mine");
    let owned = planner.own(&mine, "orders");
    for (kind, key) in keys() {
        let child = key.root.is_some();
        let keyless = crate::destination::TableRef {
            merge: Some(MergeKey {
                columns: Vec::new(),
                ..key
            }),
            ..super::super::super::tests::table("orders")
        };
        let indexed = if child {
            planner.root_index(&owned, &keyless).map(drop)
        } else {
            planner.key_indexes(&owned, &keyless).map(drop)
        };
        invalid(indexed, kind);
    }
}
