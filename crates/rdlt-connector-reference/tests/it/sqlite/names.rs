//! Names SQLite gives a meaning of its own.

use rdlt_connector::ConnectorErrorKind;

use super::kit::{Shared, refusal, table};
use super::text::created;

#[tokio::test]
async fn a_stream_named_like_a_built_in_virtual_table_loads_into_its_own_table() {
    let shared = Shared::new().await;
    let mut session = shared.open("p", 1).await;
    let built_in = [
        "json_each",
        "json_tree",
        "dbstat",
        "fts5vocab",
        "generate_series",
        "bytecode",
    ];
    for (segment, name) in (1..).zip(built_in) {
        let named = table(name, name, true);
        // Created twice, as a retry creates it again, then loaded and merged into.
        session
            .create(&named)
            .await
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        session.load(&named, segment, &[1, 2]).await;
        session.load(&named, segment + 100, &[2, 3]).await;
        assert_eq!(shared.ids(name), [1, 2, 3], "{name}");
        assert_eq!(created(&shared, name).len(), 1, "{name}");
    }
    // The table-valued pragmas keep their names: a table of one would answer for them.
    let reserved = table("pragma_table_info", "pragma_table_info", true);
    let (kind, code) = refusal(session.create(&reserved).await);
    assert_eq!(
        (kind, code.as_deref()),
        (ConnectorErrorKind::Config, Some("table_name_reserved"))
    );
}

#[tokio::test]
async fn a_path_sqlite_would_read_as_a_uri_is_refused_where_the_destination_connects() {
    use rdlt_connector::{ConnectContext, ConnectorErrorKind, destination_factory};
    use rdlt_connector_reference::SqliteDestination;
    for path in [
        "file:orders.db",
        "file:orders.db?nolock=1",
        "FILE:/var/orders.db",
        "",
    ] {
        let refused = destination_factory::<SqliteDestination>()
            .connect(serde_json::json!({ "path": path }), ConnectContext::new())
            .await
            .err()
            .unwrap_or_else(|| panic!("{path:?} is refused"));
        assert_eq!(refused.kind(), ConnectorErrorKind::Config, "{path:?}");
        assert_eq!(refused.code(), Some("database_path_invalid"), "{path:?}");
    }
}
