use rusqlite::Connection;

use super::super::tests::{Widening, database, pipeline, run_all, value};
use super::super::{SqlPlanner, SqlValue, Sqlite, Statement};
use super::{Owned, Standing};
use crate::error::ConnectorErrorKind;

type Refusal = (ConnectorErrorKind, Option<String>);

fn config(code: &str) -> Refusal {
    (ConnectorErrorKind::Config, Some(code.to_owned()))
}

fn refusal<T: std::fmt::Debug>(outcome: crate::error::Result<T>) -> Refusal {
    let error = outcome.unwrap_err();
    (error.kind(), error.code().map(str::to_owned))
}

fn names(names: &[&str]) -> Vec<Vec<SqlValue>> {
    names
        .iter()
        .map(|name| vec![SqlValue::Text((*name).to_owned())])
        .collect()
}

/// How `name` stands where `owner` owns it and the database takes its name for `found`.
fn standing(name: &str, owner: Option<&str>, found: &[&str]) -> Standing<'static> {
    let (_, planner) = database();
    let owner = names(&owner.into_iter().collect::<Vec<_>>());
    let check = planner.check(name).unwrap();
    check.answered(&(), &owner, &names(found)).unwrap()
}

/// What `statement` answers on `connection`, as the rows a connector hands back.
fn rows(connection: &Connection, statement: &Statement) -> Vec<Vec<SqlValue>> {
    let mut prepared = connection.prepare(&statement.sql).unwrap();
    let params = rusqlite::params_from_iter(statement.params.iter().map(value));
    prepared
        .query_map(params, |row| Ok(vec![SqlValue::Text(row.get(0)?)]))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

#[test]
fn a_table_is_created_by_its_owner_or_where_nothing_holds_its_name() {
    let mine = pipeline("mine");
    let claimed = standing("orders", None, &[]).created(&mine).unwrap();
    assert_eq!((claimed.name(), claimed.pipeline()), ("orders", &mine));
    assert!(claimed.claims());
    for found in [&[][..], &["orders"]] {
        let again = standing("orders", Some("mine"), found).created(&mine);
        assert!(!again.unwrap().claims(), "{found:?}");
    }
    let refused: [(Option<&str>, &[&str], &str); 7] = [
        (Some("theirs"), &[], "table_owned"),
        (Some("theirs"), &["orders"], "table_owned"),
        (Some("theirs"), &["Orders"], "table_owned"),
        // What the database already holds under the name is no pipeline's to take.
        (None, &["orders"], "table_unowned"),
        (None, &["Orders"], "table_unowned"),
        (Some("mine"), &["ORDERS"], "table_unowned"),
        (Some("mine"), &["orders", "Orders"], "table_unowned"),
    ];
    for (owner, found, code) in refused {
        let created = standing("orders", owner, found).created(&mine);
        assert_eq!(refusal(created), config(code), "{owner:?} {found:?}");
    }
}

#[test]
fn a_table_is_changed_only_by_the_pipeline_its_owner_record_names() {
    let mine = pipeline("mine");
    for found in [&[][..], &["orders"]] {
        let owned = standing("orders", Some("mine"), found)
            .owned(&mine)
            .unwrap();
        assert_eq!((owned.name(), owned.pipeline()), ("orders", &mine));
        assert!(!owned.claims());
    }
    let refused: [(Option<&str>, &[&str], &str); 6] = [
        (None, &[], "table_unowned"),
        (None, &["orders"], "table_unowned"),
        (None, &["Orders"], "table_unowned"),
        (Some("theirs"), &["orders"], "table_owned"),
        (Some("theirs"), &[], "table_owned"),
        (Some("mine"), &["Orders"], "table_unowned"),
    ];
    for (owner, found, code) in refused {
        let owned = standing("orders", owner, found).owned(&mine);
        assert_eq!(refusal(owned), config(code), "{owner:?} {found:?}");
    }
}

#[test]
fn a_dropped_table_is_the_one_its_owner_record_names_or_was_dropped_before() {
    let mine = pipeline("mine");
    assert_eq!(standing("gone", None, &[]).dropped(&mine).unwrap(), None);
    // An owner record without its table still drops: the record and the derived tables go.
    for found in [&[][..], &["orders"]] {
        let dropped = standing("orders", Some("mine"), found).dropped(&mine);
        let name = dropped.unwrap().map(|owned| owned.name().to_owned());
        assert_eq!(name.as_deref(), Some("orders"), "{found:?}");
    }
    let refused: [(Option<&str>, &[&str], &str); 5] = [
        (None, &["customers"], "table_unowned"),
        (None, &["Customers"], "table_unowned"),
        (Some("mine"), &["Customers"], "table_unowned"),
        (Some("theirs"), &["customers"], "table_owned"),
        (Some("theirs"), &[], "table_owned"),
    ];
    for (owner, found, code) in refused {
        let dropped = standing("customers", owner, found).dropped(&mine);
        assert_eq!(refusal(dropped), config(code), "{owner:?} {found:?}");
    }
}

#[test]
fn a_name_the_destination_or_the_database_keeps_is_never_checked_as_a_table() {
    let (_, planner) = database();
    let reserved = [
        "",
        "_rdlt_state",
        "_rdlt_",
        "_RDLT_state",
        "_Rdlt_staging__orders",
        "sqlite_master",
        "SQLITE_x",
        "pragma_table_info",
        "Pragma_x",
        "Orders",
        "ordeRs",
    ];
    for name in reserved {
        let reserved = config("table_name_reserved");
        assert_eq!(refusal(planner.check(name)), reserved, "{name}");
        assert_eq!(refusal(planner.named(name)), reserved, "{name}");
    }
    // Names that only resemble the reserved ones, and names beyond ASCII, are tables' to take.
    for name in [
        "_rdl",
        "_rdlt",
        "rdlt_x",
        "sqlite",
        "pragma",
        "ünïcode",
        "_rdlté",
    ] {
        planner.check(name).unwrap();
    }
    // A dialect that keeps no names leaves the planner's own.
    let plain = SqlPlanner::try_new(Widening).unwrap();
    plain.check("sqlite_master").unwrap();
    plain.check("Orders").unwrap();
    assert!(plain.check("_rdlt_state").is_err());
}

#[test]
fn an_answer_that_is_no_name_is_refused() {
    let (_, planner) = database();
    let odd = [vec![SqlValue::Integer(1)]];
    let wide = [vec![SqlValue::Text("a".into()), SqlValue::Text("b".into())]];
    for rows in [&odd[..], &wide[..]] {
        let owner = planner.check("orders").unwrap().answered(&(), rows, &[]);
        assert_eq!(owner.unwrap_err().kind(), ConnectorErrorKind::Internal);
        let found = planner.check("orders").unwrap().answered(&(), &[], rows);
        assert_eq!(found.unwrap_err().kind(), ConnectorErrorKind::Internal);
    }
}

#[test]
fn sqlite_answers_with_every_table_view_or_index_it_takes_a_name_for() {
    let (connection, planner) = database();
    connection
        .execute_batch(
            "CREATE TABLE Customers (id INTEGER); CREATE VIEW Seen AS SELECT 1; \
             CREATE INDEX Listed ON Customers (id); CREATE TABLE plain (id INTEGER); \
             CREATE TRIGGER fired AFTER INSERT ON plain BEGIN SELECT 1; END",
        )
        .unwrap();
    let found = |name: &str| rows(&connection, &planner.resolves(name));
    assert_eq!(found("customers"), names(&["Customers"]));
    assert_eq!(found("CUSTOMERS"), names(&["Customers"]));
    assert_eq!(found("seen"), names(&["Seen"]));
    assert_eq!(found("listed"), names(&["Listed"]));
    assert_eq!(found("plain"), names(&["plain"]));
    // A trigger takes no table's name, and letters beyond ASCII are matched as they are.
    assert_eq!(found("fired"), names(&[]));
    assert_eq!(found("missing"), names(&[]));
    connection
        .execute_batch("CREATE TABLE \"é\" (id INTEGER)")
        .unwrap();
    assert_eq!(found("É"), names(&[]));
    // The check's own queries are the owner's and this one.
    run_all(&connection, &planner.claim(&pipeline("mine"), "plain"));
    let check = planner.check("plain").unwrap();
    let owner = rows(&connection, check.owner());
    let resolved = rows(&connection, check.resolved());
    assert_eq!((&owner, &resolved), (&names(&["mine"]), &names(&["plain"])));
    let witness: Owned<'_> = check
        .answered(&connection, &owner, &resolved)
        .unwrap()
        .owned(&pipeline("mine"))
        .unwrap();
    assert_eq!(witness.name(), "plain");
}

#[test]
fn a_kept_name_the_database_takes_for_another_table_is_a_clash() {
    let (_, planner) = database();
    planner.exact("_rdlt_staging__orders", &[]).unwrap();
    planner
        .exact("_rdlt_staging__orders", &names(&["_rdlt_staging__orders"]))
        .unwrap();
    let clashing = [
        names(&["_RDLT_STAGING__orders"]),
        names(&["_rdlt_staging__orders", "_rdlt_Staging__orders"]),
        vec![vec![SqlValue::Integer(1)]],
    ];
    for found in clashing {
        let clash = planner.exact("_rdlt_staging__orders", &found);
        assert_eq!(refusal(clash), config("table_name_clash"), "{found:?}");
    }
    assert_eq!(planner.catalog().len(), 7);
    for name in planner.catalog() {
        assert!(planner.named(name).is_err(), "{name}");
    }
}

#[test]
fn the_tables_derived_from_a_table_are_its_staging_tombstones_and_generation() {
    use super::super::tests::table;
    let planner = SqlPlanner::try_new(Sqlite).unwrap();
    let orders = table("orders");
    let derived = planner.derived(&orders);
    assert_eq!(
        derived,
        ["_rdlt_staging__orders", "_rdlt_tombstones__orders"]
    );
    let generation = crate::destination::TableRef {
        generation: Some(crate::id::GenerationId(3)),
        ..orders
    };
    let derived = planner.derived(&generation);
    assert_eq!(derived.len(), 3);
    assert_eq!(derived[2], "_rdlt_generation_3__orders");
}

#[test]
fn creating_a_table_writes_its_owner_record_its_tables_and_its_registration_as_one_plan() {
    use super::super::tests::{create, query, table};
    use crate::types::LogicalType;
    use rusqlite::types::Value;
    let (connection, planner) = database();
    let mine = pipeline("mine");
    let orders = table("orders");
    let change = create(&orders, &[("id", LogicalType::Int64, false)]);
    let count = |sql: &str| -> i64 { connection.query_row(sql, [], |row| row.get(0)).unwrap() };
    let claiming = planner.claiming(&mine, "orders");
    // Until the table is created, its witness plans nothing else.
    let early = planner.drop_table(&claiming, &[]).unwrap_err();
    assert_eq!(early.kind(), ConnectorErrorKind::Internal);
    let early = planner.key_indexes(&claiming, &orders).unwrap_err();
    assert_eq!(early.kind(), ConnectorErrorKind::Internal);
    let added = crate::destination::TableChange::AddColumn {
        table: orders.clone(),
        field: crate::types::Field::new("more", LogicalType::Utf8, true),
    };
    let early = planner
        .change(&claiming, &added, [&[], &[], &[]])
        .unwrap_err();
    assert_eq!(early.kind(), ConnectorErrorKind::Internal);
    assert_eq!(count("SELECT count(*) FROM _rdlt_owners"), 0);
    let plan = planner.change(&claiming, &change, [&[], &[], &[]]).unwrap();
    run_all(&connection, &plan);
    assert_eq!(
        query(&connection, &planner.owner("orders")),
        [[Value::Text("mine".into())]]
    );
    assert_eq!(
        count("SELECT count(*) FROM _rdlt_tables WHERE name = 'orders'"),
        1
    );
    for created in ["orders", "_rdlt_staging__orders"] {
        let found = rows(&connection, &planner.resolves(created));
        assert_eq!(found, names(&[created]));
    }
    // A table its pipeline creates again claims nothing anew, and keeps its record.
    let owned = planner.own(&mine, "orders");
    let again = planner.change(&owned, &change, [&[], &[], &[]]).unwrap();
    assert_eq!(
        again.len(),
        plan.len() - planner.claim(&mine, "orders").len()
    );
    run_all(&connection, &again);
    assert_eq!(count("SELECT count(*) FROM _rdlt_owners"), 1);
    // Releasing forgets the record and the registration, and drops nothing.
    run_all(&connection, &planner.release(&owned));
    assert_eq!(count("SELECT count(*) FROM _rdlt_owners"), 0);
    assert_eq!(count("SELECT count(*) FROM _rdlt_tables"), 0);
    assert_eq!(
        rows(&connection, &planner.resolves("orders")),
        names(&["orders"])
    );
}

#[test]
fn a_standing_says_whether_any_pipeline_owns_its_table() {
    assert!(standing("orders", None, &["orders"]).unowned());
    assert!(!standing("orders", Some("theirs"), &[]).unowned());
}

#[test]
fn a_name_holding_a_nul_is_no_table_s() {
    let (_, planner) = database();
    let plain = SqlPlanner::try_new(Widening).unwrap();
    for name in ["a\0b", "\0", "orders\0", "\0orders"] {
        let reserved = config("table_name_reserved");
        assert_eq!(refusal(planner.check(name)), reserved, "{name:?}");
        assert_eq!(refusal(plain.named(name)), reserved, "{name:?}");
    }
}

#[test]
fn a_witness_of_a_table_yet_to_be_created_plans_no_swap_and_forgets_no_tombstones() {
    use crate::id::GenerationId;
    let mine = pipeline("mine");
    let (_, planner) = database();
    let claimed = standing("orders", None, &[]).created(&mine).unwrap();
    let generations = [("_rdlt_gen".to_owned(), GenerationId(1))];
    for listed in [&generations[..], &[]] {
        let swap = planner.swap(&claimed, true, GenerationId(1), listed);
        assert_eq!(refusal(swap).0, ConnectorErrorKind::Internal, "{listed:?}");
    }
    let forget = planner.forget_tombstones(&claimed);
    assert_eq!(refusal(forget).0, ConnectorErrorKind::Internal);
    // The witness of a table its pipeline owns plans both.
    let owned = standing("orders", Some("mine"), &["orders"])
        .owned(&mine)
        .unwrap();
    assert!(
        !planner
            .swap(&owned, true, GenerationId(1), &[])
            .unwrap()
            .is_empty()
    );
    let forgotten = planner.forget_tombstones(&owned).unwrap();
    assert!(
        forgotten.sql.starts_with("DELETE FROM"),
        "{}",
        forgotten.sql
    );
}
