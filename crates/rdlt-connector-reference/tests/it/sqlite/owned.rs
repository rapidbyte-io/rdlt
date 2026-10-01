//! A session touches only tables its pipeline owns, and never the catalog's.

use rdlt_connector::{ChildTable, DroppedTable, GenerationId, MergeKey, RootKey};

use super::kit::{Shared, by_id, config, generation, refusal, table};

/// Names no destination table may take: the catalog's and every derived table's prefix in any
/// case, SQLite's own, its table-valued pragmas', and a name SQLite would match another case of.
const RESERVED: [&str; 9] = [
    "_rdlt_state",
    "_rdlt_epochs",
    "_rdlt_owners",
    "_rdlt_staging__orders",
    "_RDLT_state",
    "sqlite_sequence",
    "SQLite_x",
    "pragma_table_info",
    "Orders",
];

fn dropped(name: &str) -> DroppedTable {
    DroppedTable {
        path: table(name, name, false).path,
        name: name.into(),
    }
}

/// `child` listed as a child table of `root`, which it follows by its `id`.
fn child_of(child: &str, root: &str) -> ChildTable {
    ChildTable {
        table: child.into(),
        merge: MergeKey {
            root: Some(RootKey {
                table: root.into(),
                id: "id".into(),
                seq: "seq".into(),
            }),
            ..by_id()
        },
    }
}

#[tokio::test]
async fn a_catalog_or_reserved_name_is_never_a_table() {
    let shared = Shared::new().await;
    let mut victim = shared.open("victim", 1).await;
    victim
        .load(&table("orders", "orders", true), 1, &[1, 2])
        .await;
    let before = shared.objects();
    let mut intruder = shared.open("intruder", 2).await;
    intruder.stage(&table("r", "r", true), 1, &[2]).await;
    let staged = shared.objects();
    for name in RESERVED {
        let named = table(name, name, true);
        assert_eq!(
            refusal(intruder.create(&named).await),
            config("table_name_reserved"),
            "creating {name}"
        );
        let writer = intruder.session.session.writer(&named).await.map(drop);
        assert_eq!(
            refusal(writer),
            config("table_name_reserved"),
            "writing {name}"
        );
        let mut drop = intruder.meta(&[]);
        drop.drop_tables = vec![dropped(name)];
        assert_eq!(
            refusal(intruder.commit(&drop).await),
            config("table_name_reserved"),
            "dropping {name}"
        );
        let mut listing = intruder.meta(&[1]);
        listing.child_tables = vec![child_of(name, "r")];
        assert_eq!(
            refusal(intruder.commit(&listing).await),
            config("table_name_reserved"),
            "listing {name}"
        );
        assert_eq!(shared.objects(), staged, "after {name}");
    }
    assert!(before.iter().all(|object| staged.contains(object)));
    assert_eq!(
        shared.texts("SELECT name FROM _rdlt_owners ORDER BY name"),
        ["orders", "r"]
    );
    assert_eq!(shared.ids("orders"), [1, 2]);
}

#[tokio::test]
async fn a_table_without_an_owner_record_is_never_dropped() {
    let shared = Shared::new().await;
    let mut session = shared.open("pipeline", 1).await;
    shared.execute("CREATE TABLE customers (id INTEGER); INSERT INTO customers VALUES (7)");
    let mut drop = session.meta(&[]);
    drop.drop_tables = vec![dropped("customers")];
    assert_eq!(
        refusal(session.commit(&drop).await),
        config("table_unowned")
    );
    assert_eq!(shared.ids("customers"), [7]);
    // A table that is not there, dropped by an earlier try of the commit, is no drop at all.
    let mut again = session.meta(&[]);
    again.drop_tables = vec![dropped("gone")];
    session.commit(&again).await.expect("the commit lands");
}

#[tokio::test]
async fn a_listed_child_table_of_another_pipeline_is_refused() {
    let shared = Shared::new().await;
    let mut victim = shared.open("victim", 1).await;
    victim
        .load(&table("orders", "orders", true), 1, &[1, 2])
        .await;
    let mut intruder = shared.open("intruder", 2).await;
    intruder.stage(&table("r", "r", true), 1, &[2]).await;
    let mut listing = intruder.meta(&[1]);
    listing.child_tables = vec![child_of("orders", "r")];
    assert_eq!(
        refusal(intruder.commit(&listing).await),
        config("table_owned")
    );
    assert_eq!(shared.ids("orders"), [1, 2]);
    // A child table no pipeline owns is refused too: its rows are no one's to replace.
    shared.execute("CREATE TABLE loose (id INTEGER, seq BLOB); INSERT INTO loose (id) VALUES (2)");
    let mut loose = intruder.meta(&[1]);
    loose.child_tables = vec![child_of("loose", "r")];
    assert_eq!(
        refusal(intruder.commit(&loose).await),
        config("table_unowned")
    );
    assert_eq!(shared.ids("loose"), [2]);
}

/// Pipeline `a` replaces its table while pipeline `b` creates, and where `dropping` then drops,
/// a table of the same path under another name; returns what `a`'s table holds once its
/// generation is finished.
async fn replaced_beside_a_namesake(dropping: bool) -> (Vec<i64>, Vec<String>) {
    let shared = Shared::new().await;
    let orders = table("orders", "orders", false);
    let mut a = shared.open("a", 1).await;
    a.load(&orders, 1, &[1]).await;
    a.load(&generation(&orders, 1), 2, &[2]).await;
    let namesake = table("orders_abc234", "orders", false);
    let mut b = shared.open("b", 2).await;
    b.load(&namesake, 1, &[99]).await;
    if dropping {
        let mut drop = b.meta(&[]);
        drop.drop_tables = vec![dropped("orders_abc234")];
        drop.drop_tables[0].path = namesake.path.clone();
        b.commit(&drop).await.expect("the drop lands");
    }
    let mut finish = a.meta(&[]);
    finish.finish_generations = vec![(orders.path.clone(), GenerationId(1))];
    a.commit(&finish).await.expect("the finishing commit lands");
    (shared.ids("orders"), shared.objects())
}

#[tokio::test]
async fn a_path_another_pipeline_registered_swaps_the_pipeline_s_own_table() {
    for dropping in [false, true] {
        let (ids, objects) = replaced_beside_a_namesake(dropping).await;
        assert_eq!(ids, [2], "dropping: {dropping}");
        assert!(
            !objects
                .iter()
                .any(|name| name.starts_with("_rdlt_generation_")),
            "dropping: {dropping}: {objects:?}"
        );
    }
}

#[tokio::test]
async fn an_open_discards_the_staging_of_every_table_the_pipeline_owns() {
    let shared = Shared::new().await;
    let mut a = shared.open("a", 1).await;
    a.stage(&table("orders", "orders", true), 1, &[1]).await;
    drop(a);
    let mut b = shared.open("b", 2).await;
    b.create(&table("orders_abcdef", "orders", true))
        .await
        .expect("the namesake is created");
    let staged = || shared.count("SELECT count(*) FROM \"_rdlt_staging__orders\"");
    assert_eq!(staged(), 1);
    let _again = shared.open("a", 3).await;
    assert_eq!(staged(), 0);
}

#[tokio::test]
async fn a_generation_finished_for_a_table_dropped_since_creates_nothing() {
    let shared = Shared::new().await;
    let orders = table("orders", "orders", false);
    let mut session = shared.open("a", 1).await;
    session.load(&orders, 1, &[1]).await;
    session.load(&generation(&orders, 1), 2, &[2]).await;
    let mut drop = session.meta(&[]);
    drop.drop_tables = vec![dropped("orders")];
    session.commit(&drop).await.expect("the drop lands");
    let mut finish = session.meta(&[]);
    finish.finish_generations = vec![(orders.path.clone(), GenerationId(1))];
    session.commit(&finish).await.expect("the commit lands");
    let left: Vec<String> = shared
        .objects()
        .into_iter()
        .filter(|name| name.contains("orders"))
        .collect();
    assert_eq!(left, Vec::<String>::new());
    assert_eq!(shared.count("SELECT count(*) FROM _rdlt_tables"), 0);
    assert_eq!(shared.count("SELECT count(*) FROM _rdlt_owners"), 0);
}
