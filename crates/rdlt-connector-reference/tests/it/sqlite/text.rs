//! No statement is written from what the database or the host says without a check.

use rdlt_connector::{ConnectorErrorKind, MergeKey, TableRef};

use super::kit::{Shared, by_id, generation, refusal, rows, table};

fn data(code: &str) -> (ConnectorErrorKind, Option<String>) {
    (ConnectorErrorKind::Data, Some(code.to_owned()))
}

/// The statement that created the table `name`, or none where it does not exist.
pub(super) fn created(shared: &Shared, name: &str) -> Vec<String> {
    shared.texts(&format!(
        "SELECT sql FROM sqlite_schema WHERE name = '{name}'"
    ))
}

#[tokio::test]
async fn a_declared_type_the_dialect_does_not_render_is_never_copied() {
    let shared = Shared::new().await;
    let mut session = shared.open("p", 1).await;
    shared.execute(
        "CREATE TABLE t (id \"INTEGER, injected TEXT DEFAULT (sqlite_version())\", seq BLOB)",
    );
    // The declared columns hold their types; the table's other column is what staging would copy.
    shared.execute("CREATE TABLE u (id INTEGER, seq BLOB, extra \"TEXT, injected TEXT\")");
    // Both stand as tables of the pipeline's that another program made again.
    shared.execute("INSERT INTO _rdlt_owners (name, pipeline) VALUES ('t', 'p'), ('u', 'p')");
    for name in ["t", "u"] {
        let adopted = table(name, name, true);
        assert_eq!(
            refusal(session.create(&adopted).await),
            data("schema_conflict")
        );
        assert_eq!(created(&shared, &format!("_rdlt_staging__{name}")).len(), 0);
    }
    // A generation table copies its base table's columns: one a schema change left behind since
    // is refused there.
    let orders = table("orders", "orders", false);
    session.load(&orders, 1, &[1]).await;
    shared.execute("ALTER TABLE orders ADD COLUMN extra \"TEXT, injected TEXT\"");
    let filling = generation(&orders, 1);
    let writer = session.session.session.writer(&filling).await.map(drop);
    assert_eq!(refusal(writer), data("schema_conflict"));
    assert!(created(&shared, "_rdlt_generation_1__orders").is_empty());
}

/// `table` merging by `key`.
fn merging(table: &TableRef, key: MergeKey) -> TableRef {
    TableRef {
        merge: Some(key),
        ..table.clone()
    }
}

#[tokio::test]
async fn a_merge_key_the_table_does_not_hold_is_refused_where_its_writer_opens() {
    let shared = Shared::new().await;
    let mut session = shared.open("p", 1).await;
    let orders = table("orders", "orders", true);
    session.load(&orders, 1, &[1, 2]).await;
    let before = shared.objects();
    let broken = [
        MergeKey {
            columns: Vec::new(),
            ..by_id()
        },
        MergeKey {
            columns: vec!["missing".into()],
            ..by_id()
        },
        MergeKey {
            seq: "no_such_seq".into(),
            ..by_id()
        },
    ];
    for key in broken {
        let keyed = merging(&orders, key.clone());
        let writer = session.session.session.writer(&keyed);
        assert_eq!(
            refusal(writer.await.map(drop)),
            data("merge_key_invalid"),
            "{key:?}"
        );
        assert_eq!(shared.objects(), before, "{key:?}");
    }
}

#[tokio::test]
async fn a_recorded_sequence_that_is_no_column_fails_the_commit() {
    let shared = Shared::new().await;
    let mut session = shared.open("p", 1).await;
    let orders = table("orders", "orders", true);
    session.load(&orders, 1, &[1]).await;
    // Three rows of one key, the newest written second.
    let mut writer = session
        .session
        .session
        .writer(&orders)
        .await
        .expect("a writer opens");
    for id in [1, 1, 1] {
        writer
            .write(rdlt_connector::SegmentId(2), rows(&[id]))
            .await
            .expect("the write buffers");
    }
    writer.flush().await.expect("the flush stages");
    shared.execute("UPDATE _rdlt_segments SET merge_seq = 'tampered'");
    let meta = session.meta(&[2]);
    assert_eq!(
        refusal(session.commit(&meta).await),
        data("merge_key_invalid")
    );
    assert_eq!(shared.ids("orders"), [1]);
    assert_eq!(
        shared.count("SELECT count(*) FROM _rdlt_staging__orders"),
        3
    );
}
