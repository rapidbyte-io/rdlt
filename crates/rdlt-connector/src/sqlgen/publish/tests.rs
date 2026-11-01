use super::super::tests::{pipeline, segments};
use super::super::{Column, SqlPlanner, Sqlite, Staged, Statement};
use crate::commit::ChildTable;
use crate::destination::{ChangeColumns, Deletion, HistoryColumns, MergeKey, RootKey};
use crate::error::{COLUMN_MISSING, ConnectorErrorKind, Result};
use crate::id::{Epoch, GenerationId};

fn columns(names: &[&str]) -> Vec<Column> {
    names
        .iter()
        .map(|name| Column {
            name: (*name).to_owned(),
            declared: "INTEGER".to_owned(),
        })
        .collect()
}

/// How `orders` is written, by each way a table publishes: appended, merged plainly, by a change
/// stream with hard or soft deletes, as a history, plain or of a change stream, and as a child.
fn ways() -> Vec<(&'static str, Option<MergeKey>)> {
    let plain = MergeKey {
        columns: vec!["id".into()],
        seq: "seq".into(),
        root: None,
        changes: None,
        history: None,
    };
    let changes = |deletion, unchanged: Option<&str>| {
        Some(ChangeColumns {
            op: "op".into(),
            unchanged: unchanged.map(Into::into),
            deletion,
        })
    };
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
    let keyed = |changes, history, root| {
        Some(MergeKey {
            root,
            changes,
            history,
            ..plain.clone()
        })
    };
    vec![
        ("append", None),
        ("plain merge", Some(plain.clone())),
        (
            "hard changes",
            keyed(changes(Deletion::Hard, Some("unchanged")), None, None),
        ),
        (
            "soft changes",
            keyed(changes(soft(), Some("unchanged")), None, None),
        ),
        ("history", keyed(None, history(), None)),
        (
            "changes' history",
            keyed(changes(soft(), None), history(), None),
        ),
        ("child", keyed(None, None, root)),
    ]
}

/// Every column `orders` has: those any way of writing it names, and its data.
const HELD: [&str; 9] = [
    "id",
    "seq",
    "deleted_at",
    "valid_from",
    "valid_to",
    "is_current",
    "row_hash",
    "nickname",
    "price",
];

/// The columns `orders`' rows are written with where its columns are `held` and `merge` merges
/// them: `held`, and a change stream's op and unchanged flags.
fn written<'a>(held: &[&'a str], merge: Option<&'a MergeKey>) -> Vec<&'a str> {
    let changes = merge.and_then(|key| key.changes.as_ref());
    let directing = changes
        .into_iter()
        .flat_map(|changes| std::iter::once(&*changes.op).chain(changes.unchanged.as_deref()));
    held.iter().copied().chain(directing).collect()
}

/// The plan publishing the rows of `orders`, or of its `generation`, written by `merge` with the
/// columns `written`, into a target whose columns are `target`.
fn published(
    merge: Option<&MergeKey>,
    generation: Option<GenerationId>,
    target: &[Column],
    written: &[&str],
) -> Result<Vec<Statement>> {
    let planner = SqlPlanner::try_new(Sqlite).unwrap();
    let owned = planner.own(&pipeline("mine"), "orders");
    let orders = Staged {
        name: "orders".to_owned(),
        generation,
        merge: merge.cloned(),
        rows: 1,
    };
    planner.publish(&owned, &orders, target, written, Epoch(1), &segments(&[1]))
}

/// Checks that `outcome` is a refusal as `column_missing` naming `nickname` and `price`.
fn refused(outcome: Result<Vec<Statement>>, context: &str) {
    let error = outcome.unwrap_err();
    assert_eq!(
        (error.kind(), error.code()),
        (ConnectorErrorKind::Data, Some(COLUMN_MISSING)),
        "{context}: {error}"
    );
    assert!(!error.is_retryable(), "{context}");
    for missing in ["nickname", "price"] {
        assert!(error.to_string().contains(missing), "{context}: {error}");
    }
}

#[test]
fn a_publish_into_a_table_lacking_columns_its_rows_were_written_with_is_refused() {
    let lacking: Vec<&str> = HELD[..7].to_vec();
    for (way, merge) in ways() {
        let written = written(&HELD, merge.as_ref());
        // Every column the table holds, and a change stream's flags beside them.
        let whole = published(merge.as_ref(), None, &columns(&HELD), &written);
        whole.unwrap_or_else(|error| panic!("{way}: {error}"));
        refused(
            published(merge.as_ref(), None, &columns(&lacking), &written),
            way,
        );
        // A column of the staging the rows were not written with holds none of their values.
        let fewer = written
            .iter()
            .filter(|name| !["nickname", "price"].contains(name));
        let fewer: Vec<&str> = fewer.copied().collect();
        let held = published(merge.as_ref(), None, &columns(&lacking), &fewer);
        held.unwrap_or_else(|error| panic!("{way}: {error}"));
    }
}

#[test]
fn a_generation_lacking_columns_its_rows_were_written_with_is_refused() {
    let generation = Some(GenerationId(3));
    let target = columns(&["id"]);
    refused(
        published(None, generation, &target, &["id", "nickname", "price"]),
        "generation",
    );
    // The staging it shares with its table may hold other columns, which its rows hold no value
    // in.
    published(None, generation, &target, &["id"]).unwrap();
}

#[test]
fn a_publish_of_staged_rows_told_none_of_their_columns_is_refused() {
    for (way, merge) in ways() {
        let error = published(merge.as_ref(), None, &columns(&HELD), &[]).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::Internal, "{way}: {error}");
    }
}

#[test]
fn a_child_table_that_follows_its_root_publishes_told_no_columns() {
    let planner = SqlPlanner::try_new(Sqlite).unwrap();
    let (_, child) = ways().pop().expect("the last way is a child's");
    let child = child.expect("a child table merges");
    let root = child.root.clone().expect("a child table has a root");
    let roots = Staged {
        name: root.table.to_string(),
        generation: None,
        merge: Some(MergeKey {
            root: None,
            ..child.clone()
        }),
        rows: 1,
    };
    let listed = ChildTable {
        table: "orders".into(),
        merge: child,
    };
    let publishing = planner.publishing(vec![roots], &[listed]);
    let follows = publishing.iter().find(|staged| staged.name == "orders");
    let follows = follows.expect("the child table follows its root");
    let owned = planner.own(&pipeline("mine"), "orders");
    let plan = planner.publish(
        &owned,
        follows,
        &columns(&HELD),
        &[],
        Epoch(1),
        &segments(&[1]),
    );
    plan.unwrap();
}

#[test]
fn written_columns_are_compared_with_the_table_s_as_the_database_compares_names() {
    let target = columns(&["id", "Nickname"]);
    published(None, None, &target, &["ID", "nickname"]).unwrap();
}
