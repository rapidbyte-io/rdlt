//! Read-back reads what pipelines published, and changes nothing.

use rdlt_connector_reference::sqlite;

use super::kit::{Shared, config, refusal, table};

#[tokio::test]
async fn reading_back_a_database_that_is_missing_creates_none() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("never-written.db");
    let read = sqlite::published(&path, "orders").expect("nothing to read");
    assert!(read.is_empty());
    let left: Vec<_> = std::fs::read_dir(directory.path())
        .expect("the directory lists")
        .collect();
    assert!(left.is_empty(), "{left:?}");
}

#[tokio::test]
async fn reading_back_reads_a_table_a_pipeline_owns_and_no_other() {
    let shared = Shared::new().await;
    let mut session = shared.open("p", 1).await;
    session
        .load(&table("orders", "orders", true), 1, &[1, 2])
        .await;
    session
        .session
        .session
        .close()
        .await
        .expect("the session closes");
    assert_eq!(shared.ids("orders"), [1, 2]);
    // The catalog, the tables derived from a table, and what SQLite keeps are no table's rows.
    let kept = [
        "_rdlt_state",
        "_rdlt_owners",
        "_rdlt_receipts",
        "_rdlt_staging__orders",
        "_RDLT_STATE",
        "sqlite_schema",
        "sqlite_master",
        "pragma_table_info",
        "Orders",
    ];
    for name in kept {
        let read = sqlite::published(&shared.path, name);
        assert_eq!(refusal(read), config("table_name_reserved"), "{name}");
    }
    // A table another program made is no pipeline's, and a view is no table.
    shared.execute(
        "CREATE TABLE customers (id INTEGER); INSERT INTO customers VALUES (7); \
         CREATE VIEW leaked AS SELECT * FROM _rdlt_state",
    );
    for name in ["customers", "leaked"] {
        let read = sqlite::published(&shared.path, name);
        assert_eq!(refusal(read), config("table_unowned"), "{name}");
    }
    assert!(
        sqlite::published(&shared.path, "missing")
            .expect("no table")
            .is_empty()
    );
}

#[tokio::test]
async fn reading_back_a_database_no_pipeline_opened_reads_nothing_and_adds_no_catalog() {
    let directory = tempfile::tempdir().expect("a temporary directory");
    let path = directory.path().join("plain.db");
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        let created = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path);
        drop(created.expect("a private file"));
        let raw = rusqlite::Connection::open(&path).expect("the database opens");
        raw.execute_batch("CREATE TABLE customers (id INTEGER); INSERT INTO customers VALUES (7)")
            .expect("a table");
    }
    let read = sqlite::published(&path, "customers");
    assert_eq!(refusal(read), config("table_unowned"));
    assert!(
        sqlite::published(&path, "missing")
            .expect("no table")
            .is_empty()
    );
    let raw = rusqlite::Connection::open(&path).expect("the database opens");
    let objects: i64 = raw
        .query_row("SELECT count(*) FROM sqlite_schema", [], |row| row.get(0))
        .expect("the schema counts");
    assert_eq!(objects, 1);
}
