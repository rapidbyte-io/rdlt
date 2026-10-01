//! An owner record stands for a table the destination created: none is written for a table that
//! was not, and none is taken for a table the database holds under another name or for no one.

use rdlt_connector::{
    ConnectorErrorKind, DroppedTable, LoadId, OpenContext, PipelineId, SegmentId, TableChange,
};
use rdlt_connector::{Field, LogicalType};

use super::kit::{Shared, config, connect, generation, refusal, rows, table};

fn dropped(name: &str) -> DroppedTable {
    DroppedTable {
        path: table(name, name, false).path,
        name: name.into(),
    }
}

/// Each table's owner, as `name=pipeline`.
fn owners(shared: &Shared) -> Vec<String> {
    shared.texts("SELECT name || '=' || pipeline FROM _rdlt_owners ORDER BY name")
}

#[tokio::test]
async fn a_writer_or_a_change_of_a_table_never_created_claims_nothing() {
    let shared = Shared::new().await;
    let mut session = shared.open("p", 1).await;
    let before = shared.objects();
    let ghost = table("ghost", "ghost", false);
    for written in [
        ghost.clone(),
        table("ghost", "ghost", true),
        generation(&ghost, 4),
    ] {
        let writer = session.session.session.writer(&written).await;
        assert_eq!(
            refusal(writer.map(drop)),
            config("table_unowned"),
            "{written:?}"
        );
    }
    let added = TableChange::AddColumn {
        table: ghost.clone(),
        field: Field::new("more", LogicalType::Utf8, true),
    };
    let changed = session.session.session.apply_schema(&added).await;
    assert_eq!(refusal(changed), config("table_unowned"));
    assert_eq!(owners(&shared), Vec::<String>::new());
    assert_eq!(shared.objects(), before);
    // The pipeline's next session opens, discarding what older ones staged, as before.
    let mut next = shared.open("p", 2).await;
    next.load(&ghost, 1, &[1]).await;
    assert_eq!(shared.ids("ghost"), [1]);
}

#[tokio::test]
async fn an_owner_record_without_its_tables_is_released_where_its_pipeline_opens() {
    let shared = Shared::new().await;
    let mut first = shared.open("p", 1).await;
    first.load(&table("orders", "orders", false), 1, &[1]).await;
    shared.execute(
        "INSERT INTO _rdlt_owners (name, pipeline) VALUES ('ghost', 'p'), ('theirs', 'q'); \
         INSERT INTO _rdlt_tables (pipeline, path, name) VALUES ('p', 'ghost', 'ghost')",
    );
    // The open, which discards what older sessions staged, releases the record.
    drop(shared.open("p", 2).await);
    assert_eq!(owners(&shared), ["orders=p", "theirs=q"]);
    assert_eq!(
        shared.count("SELECT count(*) FROM _rdlt_tables WHERE name = 'ghost'"),
        0
    );
    // Any pipeline may take the name now.
    let mut other = shared.open("q", 3).await;
    other.load(&table("ghost", "ghost", false), 1, &[7]).await;
    assert_eq!(shared.ids("ghost"), [7]);
}

#[tokio::test]
async fn a_name_the_database_takes_for_another_table_is_refused_and_that_table_stays() {
    for foreign in ["Customers", "CUSTOMERS", "cuSTomers"] {
        let shared = Shared::new().await;
        let mut session = shared.open("p", 1).await;
        shared.execute(&format!(
            "CREATE TABLE {foreign} (id INTEGER, seq BLOB, extra TEXT); \
             INSERT INTO {foreign} (id) VALUES (7)"
        ));
        let before = shared.objects();
        let customers = table("customers", "customers", false);
        let created = session.create(&customers).await;
        assert_eq!(refusal(created), config("table_unowned"), "{foreign}");
        let writer = session.session.session.writer(&customers).await;
        assert_eq!(
            refusal(writer.map(drop)),
            config("table_unowned"),
            "{foreign}"
        );
        let mut drop = session.meta(&[]);
        drop.drop_tables = vec![dropped("customers")];
        let outcome = session.commit(&drop).await;
        assert_eq!(refusal(outcome), config("table_unowned"), "{foreign}");
        assert_eq!(shared.objects(), before, "{foreign}");
        assert_eq!(owners(&shared), Vec::<String>::new(), "{foreign}");
        let kept = shared.count(&format!("SELECT count(*) FROM {foreign} WHERE id = 7"));
        assert_eq!(kept, 1, "{foreign}");
    }
}

#[tokio::test]
async fn a_table_no_pipeline_owns_is_not_claimed_by_creating_it() {
    let shared = Shared::new().await;
    let mut session = shared.open("p", 1).await;
    shared.execute(
        "CREATE TABLE customers (id INTEGER, seq BLOB); INSERT INTO customers (id) VALUES (7); \
         CREATE VIEW seen AS SELECT 1 AS id; CREATE INDEX listed ON customers (id)",
    );
    let before = shared.objects();
    for name in ["customers", "seen", "listed"] {
        let created = session.create(&table(name, name, false)).await;
        assert_eq!(refusal(created), config("table_unowned"), "{name}");
        let mut drop = session.meta(&[]);
        drop.drop_tables = vec![dropped(name)];
        let outcome = session.commit(&drop).await;
        assert_eq!(refusal(outcome), config("table_unowned"), "{name}");
    }
    assert_eq!(shared.objects(), before);
    assert_eq!(owners(&shared), Vec::<String>::new());
    assert_eq!(shared.count("SELECT count(*) FROM customers"), 1);
}

#[tokio::test]
async fn an_owner_record_whose_name_the_database_takes_for_another_table_drops_nothing() {
    let shared = Shared::new().await;
    let mut session = shared.open("p", 1).await;
    shared.execute(
        "CREATE TABLE Customers (id INTEGER); INSERT INTO Customers VALUES (7); \
         INSERT INTO _rdlt_owners (name, pipeline) VALUES ('customers', 'p')",
    );
    let mut dropping = session.meta(&[]);
    dropping.drop_tables = vec![dropped("customers")];
    let outcome = session.commit(&dropping).await;
    assert_eq!(refusal(outcome), config("table_unowned"));
    // The pipeline's next open leaves the record and the table as they are, and opens.
    drop(shared.open("p", 2).await);
    assert_eq!(owners(&shared), ["customers=p"]);
    assert_eq!(shared.count("SELECT count(*) FROM Customers"), 1);
}

#[tokio::test]
async fn a_derived_table_the_database_holds_in_another_case_is_a_clash() {
    let derived = [
        ("_RDLT_STAGING__orders", false),
        ("_rdlt_Staging__orders", false),
        ("_RDLT_TOMBSTONES__orders", false),
        ("_Rdlt_Generation_3__orders", true),
    ];
    for (foreign, replaced) in derived {
        let shared = Shared::new().await;
        let mut session = shared.open("p", 1).await;
        shared.execute(&format!("CREATE TABLE \"{foreign}\" (id INTEGER)"));
        let before = shared.objects();
        let orders = table("orders", "orders", true);
        let orders = if replaced {
            generation(&orders, 3)
        } else {
            orders
        };
        let created = session.create(&orders).await;
        assert_eq!(refusal(created), config("table_name_clash"), "{foreign}");
        assert_eq!(shared.objects(), before, "{foreign}");
        assert_eq!(owners(&shared), Vec::<String>::new(), "{foreign}");
    }
}

#[tokio::test]
async fn a_generation_table_planted_in_another_case_after_its_table_is_a_clash() {
    let shared = Shared::new().await;
    let mut session = shared.open("p", 1).await;
    let orders = table("orders", "orders", false);
    session.load(&orders, 1, &[1]).await;
    shared.execute("CREATE TABLE \"_RDLT_GENERATION_9__orders\" (id INTEGER)");
    let writer = session
        .session
        .session
        .writer(&generation(&orders, 9))
        .await;
    assert_eq!(refusal(writer.map(drop)), config("table_name_clash"));
    assert_eq!(shared.ids("orders"), [1]);
}

#[tokio::test]
async fn a_catalog_table_the_database_holds_in_another_case_is_a_clash() {
    for catalog in [
        "_RDLT_OWNERS",
        "_Rdlt_State",
        "_rdlt_EPOCHS",
        "_RDLT_tables",
    ] {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let path = directory.path().join("planted.db");
        let destination = connect(&path).await;
        // The file is made as the destination makes it, private, and its catalog table replaced.
        destination.check().await.expect("the database is created");
        let lower = catalog.to_ascii_lowercase();
        rusqlite::Connection::open(&path)
            .expect("the database opens")
            .execute_batch(&format!(
                "DROP TABLE \"{lower}\"; CREATE TABLE \"{catalog}\" (x INTEGER)"
            ))
            .expect("the table is planted");
        let context = OpenContext {
            pipeline: PipelineId::parse("p").expect("a valid pipeline id"),
            load_id: LoadId::from_parts(std::time::UNIX_EPOCH, 1),
        };
        let opened = destination.open(&context).await.map(drop);
        let (kind, code) = refusal(opened);
        assert_eq!(kind, ConnectorErrorKind::Config, "{catalog}");
        assert_eq!(code.as_deref(), Some("table_name_clash"), "{catalog}");
    }
}

#[tokio::test]
async fn a_table_created_again_by_its_pipeline_keeps_its_rows_and_its_record() {
    let shared = Shared::new().await;
    let mut session = shared.open("p", 1).await;
    let orders = table("orders", "orders", false);
    session.load(&orders, 1, &[1, 2]).await;
    session.create(&orders).await.expect("created again");
    let mut writer = session
        .session
        .session
        .writer(&orders)
        .await
        .expect("a writer");
    writer
        .write(SegmentId(2), rows(&[3]))
        .await
        .expect("buffers");
    writer.flush().await.expect("stages");
    let meta = session.meta(&[2]);
    session.commit(&meta).await.expect("the commit lands");
    assert_eq!(shared.ids("orders"), [1, 2, 3]);
    assert_eq!(owners(&shared), ["orders=p"]);
}
