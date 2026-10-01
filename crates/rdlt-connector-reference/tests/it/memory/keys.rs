//! A merge key the memory destination cannot merge by is refused, never a panic.

use rdlt_connector::{
    ChangeColumns, ConnectorErrorKind, Deletion, HistoryColumns, MergeKey, RootKey, TableRef,
};
use rdlt_connector_reference::tables;

use super::owned::{meta, open, refusal, stage, store, table};

fn keyed(key: MergeKey) -> TableRef {
    TableRef {
        merge: Some(key),
        ..table("orders", "orders", None)
    }
}

fn by(columns: &[&str], seq: &str) -> MergeKey {
    MergeKey {
        columns: columns.iter().map(|column| (*column).into()).collect(),
        seq: seq.into(),
        root: None,
        changes: None,
        history: None,
    }
}

#[tokio::test]
async fn a_merge_key_the_table_does_not_hold_is_refused_where_its_writer_opens() {
    let destination = store("keys-writer").await;
    let mut session = open(destination.as_ref(), "p", 1).await;
    stage(&mut session, &keyed(by(&["id"], "seq")), 1, &[1]).await;
    let invalid = (
        ConnectorErrorKind::Data,
        Some("merge_key_invalid".to_owned()),
    );
    for key in [
        by(&[], "seq"),
        by(&["missing"], "seq"),
        by(&["id"], "missing"),
    ] {
        let writer = session.session.writer(&keyed(key.clone())).await.map(drop);
        assert_eq!(refusal(writer), invalid, "{key:?}");
    }
    // The refused writers changed nothing: the table still merges by its key.
    stage(&mut session, &keyed(by(&["id"], "seq")), 2, &[1, 2]).await;
    session
        .session
        .commit(&meta(&session, 1, 1, &[1, 2]))
        .await
        .expect("the commit lands");
    assert_eq!(super::owned::ids("keys-writer", "orders"), [1, 2]);
}

#[tokio::test]
async fn every_column_a_merge_key_names_is_the_table_s() {
    let destination = store("keys-named").await;
    let mut session = open(destination.as_ref(), "p", 1).await;
    stage(&mut session, &keyed(by(&["id"], "seq")), 1, &[1]).await;
    let invalid = (
        ConnectorErrorKind::Data,
        Some("merge_key_invalid".to_owned()),
    );
    // Where deletes are soft their deletion time is a column of the table, and so are a history
    // table's columns; a child table is keyed by its first column alone.
    let soft = MergeKey {
        changes: Some(ChangeColumns {
            op: "op".into(),
            unchanged: None,
            deletion: Deletion::Soft {
                at: "missing".into(),
            },
        }),
        ..by(&["id"], "seq")
    };
    let history = |missing: usize| {
        let mut columns = ["id", "key", "seq", "id"];
        columns[missing] = "missing";
        MergeKey {
            history: Some(HistoryColumns {
                valid_from: columns[0].into(),
                valid_to: columns[1].into(),
                is_current: columns[2].into(),
                row_hash: columns[3].into(),
            }),
            ..by(&["id"], "seq")
        }
    };
    for key in [soft, history(0), history(1), history(2), history(3)] {
        let writer = session.session.writer(&keyed(key.clone())).await.map(drop);
        assert_eq!(refusal(writer), invalid, "{key:?}");
    }
    let child = MergeKey {
        root: Some(RootKey {
            table: "roots".into(),
            id: "id".into(),
            seq: "seq".into(),
        }),
        ..by(&["key", "missing"], "seq")
    };
    let orphan = MergeKey {
        columns: vec!["missing".into(), "key".into()],
        ..child.clone()
    };
    let writer = session.session.writer(&keyed(orphan)).await.map(drop);
    assert_eq!(refusal(writer), invalid);
    session
        .session
        .writer(&keyed(child))
        .await
        .expect("a child table's writer opens");
}

#[tokio::test]
async fn a_key_of_no_column_on_a_table_without_a_schema_fails_no_commit_by_a_panic() {
    let destination = store("keys-schemaless").await;
    let mut session = open(destination.as_ref(), "p", 1).await;
    // No schema was applied, so only the key itself can be judged where the writer opens.
    let writer = session.session.writer(&keyed(by(&[], "seq"))).await;
    assert_eq!(
        refusal(writer.map(drop)),
        (
            ConnectorErrorKind::Data,
            Some("merge_key_invalid".to_owned())
        )
    );
    assert_eq!(tables("keys-schemaless"), Vec::<String>::new());
}
