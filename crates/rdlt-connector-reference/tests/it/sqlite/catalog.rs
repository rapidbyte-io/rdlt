//! A database whose catalog was made in a shape this destination no longer writes is refused,
//! not written in a shape it cannot read.

use std::os::unix::fs::PermissionsExt as _;
use std::time::UNIX_EPOCH;

use rdlt_connector::{ConnectorErrorKind, LoadId, OpenContext, PipelineId};

use super::kit::connect;

#[tokio::test]
async fn a_catalog_whose_tables_list_names_no_pipeline_is_refused_and_left_as_it_was() {
    let directory = crate::fixtures::tempdir().expect("a temporary directory");
    let path = directory.path().join("older.db");
    let older = rusqlite::Connection::open(&path).expect("a database");
    older
        .execute_batch(
            "CREATE TABLE _rdlt_tables (path TEXT PRIMARY KEY, name TEXT NOT NULL); \
             INSERT INTO _rdlt_tables VALUES ('[\"orders\"]', 'orders')",
        )
        .expect("a catalog of the earlier shape");
    drop(older);
    let private = std::fs::Permissions::from_mode(0o600);
    std::fs::set_permissions(&path, private).expect("the database is private");
    let destination = connect(&path).await;
    let context = OpenContext {
        pipeline: PipelineId::parse("p").expect("a valid pipeline id"),
        load_id: LoadId::from_parts(UNIX_EPOCH, 1),
    };
    let refusals = [
        destination.check().await.expect_err("the check refuses it"),
        destination
            .open(&context)
            .await
            .expect_err("an open refuses it"),
    ];
    for refused in refusals {
        assert_eq!(refused.kind(), ConnectorErrorKind::Config, "{refused}");
        assert_eq!(refused.code(), Some("catalog_outdated"), "{refused}");
    }
    let kept = rusqlite::Connection::open(&path).expect("the database opens");
    let listed: i64 = kept
        .query_row("SELECT count(*) FROM _rdlt_tables", [], |row| row.get(0))
        .expect("the listing reads");
    assert_eq!(listed, 1);
    let epochs: i64 = kept
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE name = '_rdlt_epochs'",
            [],
            |row| row.get(0),
        )
        .expect("the schema reads");
    assert_eq!(epochs, 0, "nothing of the catalog was written");
}
