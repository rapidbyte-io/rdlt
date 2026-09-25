use std::time::SystemTime;

use bytes::Bytes;
use rusqlite::Connection;
use rusqlite::types::Value;

use super::{
    CATALOG_TABLES, Column, SqlDialect, SqlPlanner, SqlValue, Sqlite, Staged, Statement,
    generation_table, micros, receipt, staging_table,
};
use crate::commit::SegmentSet;
use crate::destination::{MergeKey, RootKey, TableChange, TableRef};
use crate::error::ConnectorErrorKind;
use crate::id::{
    CommitSeq, Epoch, GenerationId, LoadId, PipelineId, SchemaVersion, SegmentId, TablePath,
};
use crate::schema::TableSchema;
use crate::state::{StateChange, StateRecord};
use crate::types::{Field, LogicalType};

/// SQLite with declared types that name each integer width, and a statement that redeclares a
/// column, so widening in place can be planned.
#[derive(Debug)]
struct Widening;

impl SqlDialect for Widening {
    fn placeholder(&self, index: usize) -> String {
        Sqlite.placeholder(index)
    }

    fn column_type(&self, logical: &LogicalType) -> Option<String> {
        match logical {
            LogicalType::Int32 => Some("INT32".to_owned()),
            other => Sqlite.column_type(other),
        }
    }

    fn widen(&self, table: &str, column: &str, declared: &str) -> Option<String> {
        Some(format!(
            "ALTER TABLE {table} ALTER COLUMN {column} TYPE {declared}"
        ))
    }

    fn columns(&self, table: &str) -> Statement {
        Sqlite.columns(table)
    }
}

/// SQLite without bytes.
#[derive(Debug)]
struct Bytesless;

impl SqlDialect for Bytesless {
    fn placeholder(&self, index: usize) -> String {
        Sqlite.placeholder(index)
    }

    fn column_type(&self, logical: &LogicalType) -> Option<String> {
        match logical {
            LogicalType::Binary => None,
            other => Sqlite.column_type(other),
        }
    }

    fn columns(&self, table: &str) -> Statement {
        Sqlite.columns(table)
    }
}

fn database() -> (Connection, SqlPlanner<Sqlite>) {
    let connection = Connection::open_in_memory().unwrap();
    let planner = SqlPlanner::try_new(Sqlite).unwrap();
    for statement in planner.bootstrap() {
        run(&connection, &statement);
    }
    (connection, planner)
}

fn value(value: &SqlValue) -> Value {
    match value {
        SqlValue::Null => Value::Null,
        SqlValue::Integer(integer) => Value::Integer(*integer),
        SqlValue::Text(text) => Value::Text(text.clone()),
        SqlValue::Blob(blob) => Value::Blob(blob.clone()),
    }
}

/// Runs `statement`; returns the rows it changed.
fn run(connection: &Connection, statement: &Statement) -> usize {
    let params = rusqlite::params_from_iter(statement.params.iter().map(value));
    connection
        .execute(&statement.sql, params)
        .unwrap_or_else(|error| panic!("{}: {error}", statement.sql))
}

fn run_all(connection: &Connection, statements: &[Statement]) {
    for statement in statements {
        run(connection, statement);
    }
}

fn query(connection: &Connection, statement: &Statement) -> Vec<Vec<Value>> {
    let mut prepared = connection
        .prepare(&statement.sql)
        .unwrap_or_else(|error| panic!("{}: {error}", statement.sql));
    let width = prepared.column_count();
    let params = rusqlite::params_from_iter(statement.params.iter().map(value));
    prepared
        .query_map(params, |row| {
            (0..width).map(|index| row.get(index)).collect()
        })
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn columns(connection: &Connection, planner: &SqlPlanner<Sqlite>, table: &str) -> Vec<Column> {
    query(connection, &planner.dialect().columns(table))
        .into_iter()
        .map(|row| match &row[..] {
            [Value::Text(name), Value::Text(declared)] => Column {
                name: name.clone(),
                declared: declared.clone(),
            },
            other => panic!("{other:?}"),
        })
        .collect()
}

fn text(value: &str) -> Value {
    Value::Text(value.to_owned())
}

fn pipeline(name: &str) -> PipelineId {
    PipelineId::parse(name).unwrap()
}

fn table(name: &str) -> TableRef {
    TableRef {
        path: TablePath::new([name]).unwrap(),
        name: name.into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    }
}

fn keyed(name: &str) -> TableRef {
    TableRef {
        merge: Some(MergeKey {
            columns: vec!["id".into()],
            seq: "seq".into(),
            root: None,
        }),
        ..table(name)
    }
}

fn schema(fields: &[(&str, LogicalType, bool)]) -> TableSchema {
    TableSchema::new(
        fields
            .iter()
            .map(|(name, logical, nullable)| Field::new(*name, logical.clone(), *nullable))
            .collect(),
    )
    .unwrap()
}

fn create(table: &TableRef, fields: &[(&str, LogicalType, bool)]) -> TableChange {
    TableChange::Create {
        table: table.clone(),
        schema: schema(fields),
    }
}

/// Applies `change` to the database's current tables, as a destination does.
fn apply(
    connection: &Connection,
    planner: &SqlPlanner<Sqlite>,
    change: &TableChange,
) -> crate::error::Result<Vec<Statement>> {
    let target = columns(connection, planner, &planner.target(change.table()));
    let staging = columns(connection, planner, &staging_table(&change.table().name));
    let plan = planner.change(change, &target, &staging)?;
    run_all(connection, &plan);
    Ok(plan)
}

fn segments(ids: &[u64]) -> SegmentSet {
    ids.iter().copied().map(SegmentId).collect()
}

/// Stages rows of `(id, name)` for `table` as `pipeline` at `epoch` in `segment`.
fn stage(
    connection: &Connection,
    planner: &SqlPlanner<Sqlite>,
    table: &TableRef,
    (pipeline, epoch, segment): (&PipelineId, u64, u64),
    rows: &[(i64, &str)],
) {
    let statement = planner.stage(
        table,
        pipeline,
        Epoch(epoch),
        SegmentId(segment),
        &["id", "name"],
    );
    for (id, name) in rows {
        let mut values: Vec<Value> = statement.params.iter().map(value).collect();
        values.extend([Value::Integer(*id), text(name)]);
        connection
            .execute(&statement.sql, rusqlite::params_from_iter(values))
            .unwrap();
    }
    let record = planner.record_segment(
        table,
        pipeline,
        Epoch(epoch),
        SegmentId(segment),
        [rows.len() as u64, 10 * rows.len() as u64],
    );
    run(connection, &record);
}

fn rows_of(connection: &Connection, table: &str) -> Vec<(i64, String)> {
    let statement = Statement {
        sql: format!("SELECT id, name FROM \"{table}\" ORDER BY id, name"),
        params: Vec::new(),
    };
    query(connection, &statement)
        .into_iter()
        .map(|row| match &row[..] {
            [Value::Integer(id), Value::Text(name)] => (*id, name.clone()),
            other => panic!("{other:?}"),
        })
        .collect()
}

#[test]
fn a_planner_needs_a_dialect_that_stores_text_integers_and_bytes() {
    let error = SqlPlanner::try_new(Bytesless).unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    assert!(SqlPlanner::try_new(Widening).is_ok());
}

#[test]
fn the_catalog_bootstraps_again_without_change() {
    let (connection, planner) = database();
    run_all(&connection, &planner.bootstrap());
    for table in CATALOG_TABLES {
        assert!(!columns(&connection, &planner, table).is_empty(), "{table}");
    }
}

#[test]
fn each_open_increments_the_epoch_and_only_the_latest_epoch_fences_through() {
    let (connection, planner) = database();
    let orders = pipeline("orders");
    for expected in 1..=2 {
        run_all(&connection, &planner.open(&orders));
        let epoch = query(&connection, &planner.epoch(&orders));
        assert_eq!(epoch, [[Value::Integer(expected)]]);
    }
    assert_eq!(run(&connection, &planner.fence(&orders, Epoch(1))), 0);
    assert_eq!(run(&connection, &planner.fence(&orders, Epoch(2))), 1);
    assert_eq!(
        run(&connection, &planner.fence(&pipeline("other"), Epoch(2))),
        0
    );
    run_all(&connection, &planner.open(&pipeline("other")));
    let epoch = query(&connection, &planner.epoch(&pipeline("other")));
    assert_eq!(
        epoch,
        [[Value::Integer(1)]],
        "each pipeline counts its own epochs"
    );
}

#[test]
fn state_changes_put_replace_and_delete_one_pipelines_records() {
    let (connection, planner) = database();
    let put = |key: &str, value: &'static [u8]| {
        StateChange::Put(StateRecord {
            key: key.to_owned(),
            value: Bytes::from_static(value),
        })
    };
    let (orders, other) = (pipeline("orders"), pipeline("other"));
    run_all(
        &connection,
        &planner.state_changes(&orders, &[put("a", b"1"), put("b", b"2"), put("a", b"3")]),
    );
    run_all(
        &connection,
        &planner.state_changes(&other, &[put("a", b"9")]),
    );
    run_all(
        &connection,
        &planner.state_changes(&orders, &[StateChange::Delete("b".to_owned())]),
    );
    let state = query(&connection, &planner.state(&orders));
    assert_eq!(state, [[text("a"), Value::Blob(b"3".to_vec())]]);
}

#[test]
fn a_stored_receipt_is_found_by_its_load_and_commit_and_reads_back_equal() {
    let (connection, planner) = database();
    let orders = pipeline("orders");
    let load = LoadId::from_parts(SystemTime::now(), 7);
    let micros = i64::try_from(micros(SystemTime::now())).unwrap();
    let stored = receipt(load, CommitSeq::FIRST, micros, 3, 40);
    run(&connection, &planner.record_receipt(&orders, &stored));
    let rows = query(
        &connection,
        &planner.receipt(&orders, load, CommitSeq::FIRST),
    );
    let [row] = &rows[..] else { panic!("{rows:?}") };
    let [
        Value::Integer(at),
        Value::Integer(count),
        Value::Integer(bytes),
    ] = &row[..]
    else {
        panic!("{row:?}")
    };
    assert_eq!(receipt(load, CommitSeq::FIRST, *at, *count, *bytes), stored);
    let missing = [
        planner.receipt(&orders, load, CommitSeq::FIRST.next()),
        planner.receipt(&pipeline("other"), load, CommitSeq::FIRST),
        planner.receipt(
            &orders,
            LoadId::from_parts(SystemTime::now(), 8),
            CommitSeq::FIRST,
        ),
    ];
    for statement in &missing {
        assert!(query(&connection, statement).is_empty());
    }
}

#[test]
fn a_create_makes_the_table_and_its_staging_table_and_applying_it_again_plans_nothing() {
    let (connection, planner) = database();
    let orders = table("orders");
    let change = create(
        &orders,
        &[
            ("id", LogicalType::Int64, false),
            ("name", LogicalType::Utf8, true),
        ],
    );
    apply(&connection, &planner, &change).unwrap();
    let expected = vec![
        Column {
            name: "id".to_owned(),
            declared: "INTEGER".to_owned(),
        },
        Column {
            name: "name".to_owned(),
            declared: "TEXT".to_owned(),
        },
    ];
    assert_eq!(columns(&connection, &planner, "orders"), expected);
    let staging: Vec<String> = columns(&connection, &planner, &staging_table("orders"))
        .into_iter()
        .map(|column| column.name)
        .collect();
    assert_eq!(
        staging,
        [
            "_rdlt_pipeline",
            "_rdlt_epoch",
            "_rdlt_segment",
            "_rdlt_generation",
            "id",
            "name"
        ]
    );
    assert!(apply(&connection, &planner, &change).unwrap().is_empty());
    let null_id = connection.execute("INSERT INTO orders (id, name) VALUES (NULL, 'x')", []);
    assert!(null_id.is_err(), "a column declared non-null refuses nulls");
}

#[test]
fn a_create_on_existing_tables_adds_only_the_columns_they_lack() {
    let (connection, planner) = database();
    let orders = table("orders");
    apply(
        &connection,
        &planner,
        &create(&orders, &[("id", LogicalType::Int64, false)]),
    )
    .unwrap();
    let wider = create(
        &orders,
        &[
            ("id", LogicalType::Int32, false),
            ("name", LogicalType::Utf8, false),
        ],
    );
    let plan = apply(&connection, &planner, &wider).unwrap();
    assert_eq!(plan.len(), 2, "one column added to each table: {plan:?}");
    let names = |table: &str| -> Vec<String> {
        columns(&connection, &planner, table)
            .into_iter()
            .map(|c| c.name)
            .collect()
    };
    assert_eq!(names("orders"), ["id", "name"]);
    assert!(names(&staging_table("orders")).ends_with(&["id".to_owned(), "name".to_owned()]));
    connection
        .execute("INSERT INTO orders (id) VALUES (1)", [])
        .expect("added columns are nullable");
}

#[test]
fn a_staging_table_lost_beside_its_table_is_created_again_with_every_column() {
    let (connection, planner) = database();
    let orders = table("orders");
    apply(
        &connection,
        &planner,
        &create(&orders, &[("id", LogicalType::Int64, false)]),
    )
    .unwrap();
    connection
        .execute(&format!("DROP TABLE \"{}\"", staging_table("orders")), [])
        .unwrap();
    let add = TableChange::AddColumn {
        table: orders,
        field: Field::new("name", LogicalType::Utf8, true),
    };
    apply(&connection, &planner, &add).unwrap();
    let staging: Vec<String> = columns(&connection, &planner, &staging_table("orders"))
        .into_iter()
        .map(|column| column.name)
        .skip(4)
        .collect();
    assert_eq!(staging, ["id", "name"]);
}

#[test]
fn columns_declared_at_types_they_do_not_hold_conflict_and_plan_nothing() {
    let (connection, planner) = database();
    let orders = table("orders");
    apply(
        &connection,
        &planner,
        &create(
            &orders,
            &[
                ("id", LogicalType::Int64, false),
                ("name", LogicalType::Utf8, true),
            ],
        ),
    )
    .unwrap();
    connection
        .execute(
            &format!(
                "ALTER TABLE \"{}\" ADD COLUMN extra TEXT",
                staging_table("orders")
            ),
            [],
        )
        .unwrap();
    let conflicts = [
        create(&orders, &[("id", LogicalType::Utf8, false)]),
        TableChange::AddColumn {
            table: orders.clone(),
            field: Field::new("name", LogicalType::Bool, true),
        },
        TableChange::AddColumn {
            table: orders.clone(),
            field: Field::new("extra", LogicalType::Int64, true),
        },
        TableChange::Widen {
            table: orders.clone(),
            column: "id".into(),
            from: LogicalType::Int64,
            to: LogicalType::Float64,
        },
    ];
    for change in &conflicts {
        let error = apply(&connection, &planner, change).unwrap_err();
        assert_eq!(
            (error.kind(), error.code()),
            (ConnectorErrorKind::Data, Some("schema_conflict")),
            "{change:?}"
        );
    }
    let names: Vec<String> = columns(&connection, &planner, "orders")
        .into_iter()
        .map(|c| c.name)
        .collect();
    assert_eq!(names, ["id", "name"], "a conflict changes nothing");
}

#[test]
fn a_widen_the_column_holds_plans_nothing_and_a_dialect_that_can_redeclares_it() {
    let (connection, planner) = database();
    let orders = table("orders");
    apply(
        &connection,
        &planner,
        &create(&orders, &[("n", LogicalType::Int32, true)]),
    )
    .unwrap();
    let widen = TableChange::Widen {
        table: orders,
        column: "n".into(),
        from: LogicalType::Int32,
        to: LogicalType::Int64,
    };
    assert!(apply(&connection, &planner, &widen).unwrap().is_empty());
    let widening = SqlPlanner::try_new(Widening).unwrap();
    let existing = [Column {
        name: "n".to_owned(),
        declared: "INT32".to_owned(),
    }];
    let plan = widening.change(&widen, &existing, &existing).unwrap();
    let sql: Vec<&str> = plan
        .iter()
        .map(|statement| statement.sql.as_str())
        .collect();
    assert_eq!(
        sql,
        [
            "ALTER TABLE orders ALTER COLUMN n TYPE INTEGER",
            "ALTER TABLE _rdlt_staging__orders ALTER COLUMN n TYPE INTEGER",
        ]
    );
    let lacking = widening.change(&widen, &existing, &[]).unwrap();
    assert_eq!(
        lacking.len(),
        1,
        "a staging table without the column is left alone"
    );
}

#[test]
fn changes_to_missing_tables_or_columns_are_data_errors() {
    let (connection, planner) = database();
    let orders = table("orders");
    let add = TableChange::AddColumn {
        table: orders.clone(),
        field: Field::new("extra", LogicalType::Int64, true),
    };
    assert_eq!(
        apply(&connection, &planner, &add).unwrap_err().kind(),
        ConnectorErrorKind::Data
    );
    apply(
        &connection,
        &planner,
        &create(&orders, &[("id", LogicalType::Int64, false)]),
    )
    .unwrap();
    let widen = TableChange::Widen {
        table: orders,
        column: "missing".into(),
        from: LogicalType::Int32,
        to: LogicalType::Int64,
    };
    assert_eq!(
        apply(&connection, &planner, &widen).unwrap_err().kind(),
        ConnectorErrorKind::Data
    );
}

#[test]
fn types_the_database_does_not_store_are_unsupported() {
    let (connection, planner) = database();
    let orders = table("orders");
    let decimal = create(&orders, &[("price", LogicalType::Json, true)]);
    let error = apply(&connection, &planner, &decimal).unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    apply(
        &connection,
        &planner,
        &create(&orders, &[("id", LogicalType::Int64, false)]),
    )
    .unwrap();
    let widen = TableChange::Widen {
        table: orders,
        column: "id".into(),
        from: LogicalType::Int64,
        to: LogicalType::Json,
    };
    let error = apply(&connection, &planner, &widen).unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
}

#[test]
fn a_merge_replaces_every_published_row_of_its_keys_whatever_the_table_held() {
    let (connection, planner) = database();
    let fields = [
        ("id", LogicalType::Int64, false),
        ("name", LogicalType::Utf8, true),
        ("seq", LogicalType::Binary, true),
    ];
    apply(&connection, &planner, &create(&table("orders"), &fields)).unwrap();
    connection
        .execute(
            "INSERT INTO orders VALUES (1, 'a', NULL), (1, 'b', NULL), (2, 'kept', NULL)",
            [],
        )
        .unwrap();
    let orders = keyed("orders");
    let mine = pipeline("mine");
    let statement = planner.stage(
        &orders,
        &mine,
        Epoch(1),
        SegmentId(1),
        &["id", "name", "seq"],
    );
    let mut values: Vec<Value> = statement.params.iter().map(value).collect();
    values.extend([Value::Integer(1), text("c"), Value::Blob(vec![0; 16])]);
    connection
        .execute(&statement.sql, rusqlite::params_from_iter(values))
        .unwrap();
    run(
        &connection,
        &planner.record_segment(&orders, &mine, Epoch(1), SegmentId(1), [1, 10]),
    );
    let rows = query(
        &connection,
        &planner.staged(&mine, Epoch(1), &segments(&[1])),
    );
    let [row] = &rows[..] else { panic!("{rows:?}") };
    let [_, _, Value::Text(key), Value::Text(seq), ..] = &row[..] else {
        panic!("{row:?}")
    };
    let merge = super::merge_key(key, seq).unwrap();
    assert_eq!(
        Some(&merge),
        orders.merge.as_ref(),
        "the writer's key is recorded"
    );
    let columns = columns(&connection, &planner, "orders");
    let plan = planner
        .publish(
            &staged("orders", None, Some(merge)),
            &columns,
            &mine,
            Epoch(1),
            &segments(&[1]),
        )
        .unwrap();
    run_all(&connection, &plan);
    assert_eq!(
        rows_of(&connection, "orders"),
        [(1, "c".to_owned()), (2, "kept".to_owned())]
    );
}

#[test]
fn a_recorded_merge_key_that_is_not_json_is_a_bug() {
    let error = super::merge_key("not json", "seq").unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Internal);
}

fn staged(name: &str, generation: Option<GenerationId>, merge: Option<MergeKey>) -> Staged {
    Staged {
        name: name.to_owned(),
        generation,
        merge,
    }
}

/// Stages rows of `orders` as pipeline `mine` at epoch 2 in segments 1, 2, 4 and 5, one in its
/// generation 3, and rows no commit of `mine` at epoch 2 may publish: `mine`'s at epoch 1 and
/// `theirs`.
fn staged_orders(connection: &Connection, planner: &SqlPlanner<Sqlite>) -> PipelineId {
    let orders = table("orders");
    let fields = [
        ("id", LogicalType::Int64, false),
        ("name", LogicalType::Utf8, true),
    ];
    apply(connection, planner, &create(&orders, &fields)).unwrap();
    let (mine, theirs) = (pipeline("mine"), pipeline("theirs"));
    for segment in [1, 2, 4, 5] {
        let id = i64::try_from(segment).unwrap();
        stage(
            connection,
            planner,
            &orders,
            (&mine, 2, segment),
            &[(id, "mine")],
        );
    }
    stage(
        connection,
        planner,
        &orders,
        (&mine, 1, 1),
        &[(10, "stale")],
    );
    stage(
        connection,
        planner,
        &orders,
        (&theirs, 2, 1),
        &[(20, "theirs")],
    );
    let generation = TableRef {
        generation: Some(GenerationId(3)),
        ..orders
    };
    apply(connection, planner, &create(&generation, &fields)).unwrap();
    stage(
        connection,
        planner,
        &generation,
        (&mine, 2, 1),
        &[(30, "hidden")],
    );
    mine
}

#[test]
fn staged_rows_are_counted_by_table_and_generation_until_forgotten() {
    let (connection, planner) = database();
    let mine = staged_orders(&connection, &planner);
    let committed = segments(&[1, 2, 5]);
    let rows = query(&connection, &planner.staged(&mine, Epoch(2), &committed));
    let count = |generation, rows, bytes| {
        vec![
            text("orders"),
            generation,
            Value::Null,
            Value::Null,
            Value::Integer(rows),
            Value::Integer(bytes),
        ]
    };
    assert_eq!(
        rows,
        [count(Value::Null, 3, 30), count(Value::Integer(3), 1, 10)]
    );
    run(&connection, &planner.forget(&mine, Epoch(2), &committed));
    let all = segments(&[1, 2, 4, 5]);
    let rows = query(&connection, &planner.staged(&mine, Epoch(2), &all));
    assert_eq!(
        rows,
        [count(Value::Null, 1, 10)],
        "segment 4 is still recorded"
    );
    let none = planner.staged(&mine, Epoch(2), &SegmentSet::new());
    assert!(query(&connection, &none).is_empty());
}

#[test]
fn a_commit_publishes_exactly_the_rows_its_pipeline_staged_at_its_epoch_in_its_segments() {
    let (connection, planner) = database();
    let mine = staged_orders(&connection, &planner);
    let columns = columns(&connection, &planner, "orders");
    let orders = staged("orders", None, None);
    let plan = planner
        .publish(&orders, &columns, &mine, Epoch(2), &segments(&[1, 2, 5]))
        .unwrap();
    run_all(&connection, &plan);
    let published: Vec<i64> = rows_of(&connection, "orders")
        .iter()
        .map(|row| row.0)
        .collect();
    assert_eq!(published, [1, 2, 5]);
    let left: Vec<i64> = rows_of(&connection, &staging_table("orders"))
        .iter()
        .map(|row| row.0)
        .collect();
    assert_eq!(
        left,
        [4, 10, 20, 30],
        "only the published rows leave staging"
    );
}

#[test]
fn a_generation_publishes_into_its_own_table() {
    let (connection, planner) = database();
    let orders = table("orders");
    let generation = TableRef {
        generation: Some(GenerationId(3)),
        ..orders.clone()
    };
    let fields = [
        ("id", LogicalType::Int64, false),
        ("name", LogicalType::Utf8, true),
    ];
    apply(&connection, &planner, &create(&generation, &fields)).unwrap();
    let mine = pipeline("mine");
    stage(
        &connection,
        &planner,
        &generation,
        (&mine, 1, 1),
        &[(1, "new")],
    );
    let target = generation_table("orders", GenerationId(3));
    assert_eq!(planner.target(&generation), target);
    let columns = columns(&connection, &planner, &target);
    let plan = planner.publish(
        &staged("orders", Some(GenerationId(3)), None),
        &columns,
        &mine,
        Epoch(1),
        &segments(&[1]),
    );
    run_all(&connection, &plan.unwrap());
    assert_eq!(rows_of(&connection, &target), [(1, "new".to_owned())]);
    assert!(
        columns_of_missing(&connection, &planner, "orders"),
        "the base table is untouched"
    );
}

fn columns_of_missing(connection: &Connection, planner: &SqlPlanner<Sqlite>, table: &str) -> bool {
    columns(connection, planner, table).is_empty()
}

#[test]
fn a_merge_keeps_the_greatest_sequence_of_each_key_and_replaces_published_rows() {
    let (connection, planner) = database();
    let orders = keyed("orders");
    let fields = [
        ("id", LogicalType::Int64, false),
        ("name", LogicalType::Utf8, true),
        ("seq", LogicalType::Binary, false),
    ];
    apply(&connection, &planner, &create(&orders, &fields)).unwrap();
    let mine = pipeline("mine");
    let stage_keyed = |segment: u64, rows: &[(i64, &str, u8)]| {
        let statement = planner.stage(
            &orders,
            &mine,
            Epoch(1),
            SegmentId(segment),
            &["id", "name", "seq"],
        );
        for (id, name, seq) in rows {
            let mut values: Vec<Value> = statement.params.iter().map(value).collect();
            let mut bytes = vec![0; 16];
            bytes[15] = *seq;
            values.extend([Value::Integer(*id), text(name), Value::Blob(bytes)]);
            connection
                .execute(&statement.sql, rusqlite::params_from_iter(values))
                .unwrap();
        }
    };
    stage_keyed(1, &[(1, "a", 1), (2, "b", 2)]);
    stage_keyed(2, &[(2, "late", 9), (3, "c", 3)]);
    stage_keyed(3, &[(2, "early", 4)]);
    let columns = columns(&connection, &planner, "orders");
    let merge = staged("orders", None, orders.merge.clone());
    for committed in [segments(&[1]), segments(&[2, 3])] {
        run_all(
            &connection,
            &planner
                .publish(&merge, &columns, &mine, Epoch(1), &committed)
                .unwrap(),
        );
    }
    let expected = [
        (1, "a".to_owned()),
        (2, "late".to_owned()),
        (3, "c".to_owned()),
    ];
    assert_eq!(rows_of(&connection, "orders"), expected);
}

/// The table `roots`, merging by `id`, and its child table `items`, whose rows name their root
/// in `root`.
fn roots_and_items() -> (TableRef, TableRef) {
    let items = TableRef {
        merge: Some(MergeKey {
            columns: vec!["root".into()],
            seq: "seq".into(),
            root: Some(RootKey {
                table: "roots".into(),
                id: "id".into(),
                seq: "seq".into(),
            }),
        }),
        ..table("items")
    };
    (keyed("roots"), items)
}

/// A 16-byte sequence whose last byte is `byte`.
fn seq(byte: u8) -> Value {
    let mut bytes = vec![0; 16];
    bytes[15] = byte;
    Value::Blob(bytes)
}

/// Stages `rows` of `columns` for `table` as pipeline `mine` at epoch 1 in segment 1.
fn stage_values(
    connection: &Connection,
    planner: &SqlPlanner<Sqlite>,
    table: &TableRef,
    columns: &[&str],
    rows: Vec<Vec<Value>>,
) {
    let statement = planner.stage(table, &pipeline("mine"), Epoch(1), SegmentId(1), columns);
    for row in rows {
        let mut values: Vec<Value> = statement.params.iter().map(value).collect();
        values.extend(row);
        connection
            .execute(&statement.sql, rusqlite::params_from_iter(values))
            .unwrap();
    }
}

#[test]
fn a_child_table_keeps_only_the_children_of_its_staged_roots_winning_rows() {
    let (connection, planner) = database();
    let (roots, items) = roots_and_items();
    let root_fields = [
        ("id", LogicalType::Int64, false),
        ("seq", LogicalType::Binary, false),
    ];
    let item_fields = [
        ("root", LogicalType::Int64, false),
        ("name", LogicalType::Utf8, false),
        ("seq", LogicalType::Binary, false),
    ];
    apply(&connection, &planner, &create(&roots, &root_fields)).unwrap();
    apply(&connection, &planner, &create(&items, &item_fields)).unwrap();
    connection
        .execute(
            "INSERT INTO items VALUES (1, 'old', x''), (2, 'kept', x'')",
            [],
        )
        .unwrap();
    let root = |id: i64, byte: u8| vec![Value::Integer(id), seq(byte)];
    stage_values(
        &connection,
        &planner,
        &roots,
        &["id", "seq"],
        vec![root(1, 3), root(1, 5)],
    );
    let item = |id: i64, name: &str, byte: u8| vec![Value::Integer(id), text(name), seq(byte)];
    let staged_items = vec![item(1, "stale", 3), item(1, "new", 5), item(3, "orphan", 7)];
    stage_values(
        &connection,
        &planner,
        &items,
        &["root", "name", "seq"],
        staged_items,
    );
    let columns = columns(&connection, &planner, "items");
    let merge = staged("items", None, items.merge.clone());
    let plan = planner
        .publish(
            &merge,
            &columns,
            &pipeline("mine"),
            Epoch(1),
            &segments(&[1]),
        )
        .unwrap();
    run_all(&connection, &plan);
    let statement = Statement {
        sql: "SELECT root, name FROM items ORDER BY root, name".to_owned(),
        params: Vec::new(),
    };
    assert_eq!(
        query(&connection, &statement),
        [
            vec![Value::Integer(1), text("new")],
            vec![Value::Integer(2), text("kept")],
        ],
        "root 1's old and stale children go, and a child of no staged root waits"
    );
}

#[test]
fn a_child_tables_root_columns_are_read_from_the_root_staging_only() {
    let (connection, planner) = database();
    let (roots, mut items) = roots_and_items();
    // The root id column names a column only the child table has.
    if let Some(key) = items.merge.as_mut().and_then(|key| key.root.as_mut()) {
        key.id = "root".into();
    }
    let root_fields = [
        ("id", LogicalType::Int64, false),
        ("seq", LogicalType::Binary, false),
    ];
    let item_fields = [
        ("root", LogicalType::Int64, false),
        ("name", LogicalType::Utf8, false),
        ("seq", LogicalType::Binary, false),
    ];
    apply(&connection, &planner, &create(&roots, &root_fields)).unwrap();
    apply(&connection, &planner, &create(&items, &item_fields)).unwrap();
    let item = |id: i64, name: &str, byte: u8| vec![Value::Integer(id), text(name), seq(byte)];
    stage_values(
        &connection,
        &planner,
        &items,
        &["root", "name", "seq"],
        vec![item(1, "a", 1)],
    );
    let columns = columns(&connection, &planner, "items");
    let merge = staged("items", None, items.merge.clone());
    let plan = planner
        .publish(
            &merge,
            &columns,
            &pipeline("mine"),
            Epoch(1),
            &segments(&[1]),
        )
        .unwrap();
    let failed = plan.iter().any(|statement| {
        connection
            .execute(
                &statement.sql,
                rusqlite::params_from_iter(statement.params.iter().map(value)),
            )
            .is_err()
    });
    assert!(
        failed,
        "a root column the root staging lacks is an error, not the child's column"
    );
}

#[test]
fn a_merge_of_a_table_of_only_key_columns_keeps_each_key_once() {
    let (connection, planner) = database();
    let keys = TableRef {
        merge: Some(MergeKey {
            columns: vec!["id".into(), "seq".into()],
            seq: "seq".into(),
            root: None,
        }),
        ..table("keys")
    };
    let fields = [
        ("id", LogicalType::Int64, false),
        ("seq", LogicalType::Int64, false),
    ];
    apply(&connection, &planner, &create(&keys, &fields)).unwrap();
    let mine = pipeline("mine");
    let statement = planner.stage(&keys, &mine, Epoch(1), SegmentId(1), &["id", "seq"]);
    for _ in 0..2 {
        let mut values: Vec<Value> = statement.params.iter().map(value).collect();
        values.extend([Value::Integer(1), Value::Integer(1)]);
        connection
            .execute(&statement.sql, rusqlite::params_from_iter(values))
            .unwrap();
    }
    let columns = columns(&connection, &planner, "keys");
    let merge = staged("keys", None, keys.merge.clone());
    for _ in 0..2 {
        run_all(
            &connection,
            &planner
                .publish(&merge, &columns, &mine, Epoch(1), &segments(&[1]))
                .unwrap(),
        );
    }
    let count = query(
        &connection,
        &Statement {
            sql: "SELECT COUNT(*) FROM keys".to_owned(),
            params: Vec::new(),
        },
    );
    assert_eq!(count, [[Value::Integer(1)]]);
}

#[test]
fn registered_tables_are_found_by_path_with_their_generations() {
    let (connection, planner) = database();
    let orders = keyed("orders");
    run_all(&connection, &planner.register(&orders));
    run_all(&connection, &planner.register(&orders));
    let generation = TableRef {
        generation: Some(GenerationId(4)),
        ..table("events")
    };
    run_all(&connection, &planner.register(&generation));
    assert_eq!(
        query(&connection, &planner.tables()),
        [[text("events")], [text("orders")]]
    );
    let found = query(
        &connection,
        &planner.table_name(&TablePath::new(["orders"]).unwrap()),
    );
    assert_eq!(found, [[text("orders")]]);
    let generations = query(&connection, &planner.generations("events"));
    assert_eq!(
        generations,
        [[
            text(&generation_table("events", GenerationId(4))),
            Value::Integer(4)
        ]]
    );
}

#[test]
fn a_swap_replaces_the_table_with_its_generation_and_drops_the_others() {
    let (connection, planner) = database();
    let fields = [
        ("id", LogicalType::Int64, false),
        ("name", LogicalType::Utf8, true),
    ];
    let base = table("orders");
    apply(&connection, &planner, &create(&base, &fields)).unwrap();
    connection
        .execute("INSERT INTO orders VALUES (1, 'old')", [])
        .unwrap();
    for generation in [1, 2] {
        let generation = TableRef {
            generation: Some(GenerationId(generation)),
            ..base.clone()
        };
        apply(&connection, &planner, &create(&generation, &fields)).unwrap();
        run_all(&connection, &planner.register(&generation));
    }
    connection
        .execute(
            &format!(
                "INSERT INTO \"{}\" VALUES (2, 'new')",
                generation_table("orders", GenerationId(2))
            ),
            [],
        )
        .unwrap();
    let generations = generations_of(&connection, &planner, "orders");
    run_all(
        &connection,
        &planner.swap("orders", true, GenerationId(2), &generations),
    );
    assert_eq!(rows_of(&connection, "orders"), [(2, "new".to_owned())]);
    for generation in [1, 2] {
        let name = generation_table("orders", GenerationId(generation));
        assert!(columns_of_missing(&connection, &planner, &name), "{name}");
    }
    assert!(query(&connection, &planner.generations("orders")).is_empty());
}

fn generations_of(
    connection: &Connection,
    planner: &SqlPlanner<Sqlite>,
    base: &str,
) -> Vec<(String, GenerationId)> {
    query(connection, &planner.generations(base))
        .into_iter()
        .map(|row| match &row[..] {
            [Value::Text(name), Value::Integer(generation)] => {
                (name.clone(), GenerationId(super::unsigned(*generation)))
            }
            other => panic!("{other:?}"),
        })
        .collect()
}

#[test]
fn a_swap_of_a_generation_without_a_table_empties_the_table() {
    let (connection, planner) = database();
    let base = table("orders");
    let fields = [
        ("id", LogicalType::Int64, false),
        ("name", LogicalType::Utf8, true),
    ];
    apply(&connection, &planner, &create(&base, &fields)).unwrap();
    connection
        .execute("INSERT INTO orders VALUES (1, 'old')", [])
        .unwrap();
    run_all(
        &connection,
        &planner.swap("orders", true, GenerationId(7), &[]),
    );
    assert!(rows_of(&connection, "orders").is_empty());
    let nothing = planner.swap("missing", false, GenerationId(7), &[]);
    assert_eq!(
        nothing.len(),
        1,
        "only the generation catalog is touched: {nothing:?}"
    );
    run_all(&connection, &nothing);
}

#[test]
fn discarding_removes_only_what_older_sessions_of_the_pipeline_staged() {
    let (connection, planner) = database();
    let fields = [
        ("id", LogicalType::Int64, false),
        ("name", LogicalType::Utf8, true),
    ];
    let (mine, theirs) = (pipeline("mine"), pipeline("theirs"));
    for name in ["orders", "users"] {
        let table = table(name);
        apply(&connection, &planner, &create(&table, &fields)).unwrap();
        stage(
            &connection,
            &planner,
            &table,
            (&mine, 1, 1),
            &[(1, "older")],
        );
        stage(
            &connection,
            &planner,
            &table,
            (&mine, 3, 1),
            &[(3, "newer")],
        );
        stage(
            &connection,
            &planner,
            &table,
            (&theirs, 1, 1),
            &[(2, "theirs")],
        );
    }
    let names = ["orders".to_owned(), "users".to_owned()];
    run_all(&connection, &planner.discard(&mine, Epoch(2), &names));
    for name in ["orders", "users"] {
        let left: Vec<i64> = rows_of(&connection, &staging_table(name))
            .iter()
            .map(|row| row.0)
            .collect();
        assert_eq!(left, [2, 3], "{name}");
    }
    let staged = |pipeline, epoch| {
        query(
            &connection,
            &planner.staged(pipeline, Epoch(epoch), &segments(&[1])),
        )
    };
    assert!(staged(&mine, 1).is_empty());
    assert_eq!(
        staged(&mine, 3).len(),
        2,
        "a newer session's segments stay recorded"
    );
    assert_eq!(staged(&theirs, 1).len(), 2);
}

#[test]
fn identifiers_are_quoted_whatever_they_hold() {
    let (connection, planner) = database();
    let odd = table("a \"quoted\" name");
    let fields = [
        ("id", LogicalType::Int64, false),
        ("select", LogicalType::Utf8, true),
    ];
    apply(&connection, &planner, &create(&odd, &fields)).unwrap();
    let names: Vec<String> = columns(&connection, &planner, "a \"quoted\" name")
        .into_iter()
        .map(|c| c.name)
        .collect();
    assert_eq!(names, ["id", "select"]);
    assert_eq!(Sqlite.quote("a\"b"), "\"a\"\"b\"");
}

#[test]
fn a_generation_never_created_starts_with_its_base_tables_columns() {
    let (connection, planner) = database();
    let base = table("orders");
    let fields = [
        ("id", LogicalType::Int64, false),
        ("name", LogicalType::Utf8, true),
    ];
    apply(&connection, &planner, &create(&base, &fields)).unwrap();
    let generation = TableRef {
        generation: Some(GenerationId(5)),
        ..base.clone()
    };
    let columns_of_base = columns(&connection, &planner, "orders");
    run_all(
        &connection,
        &planner.generation(&generation, &columns_of_base),
    );
    let name = generation_table("orders", GenerationId(5));
    let names: Vec<String> = columns(&connection, &planner, &name)
        .into_iter()
        .map(|column| column.name)
        .collect();
    assert_eq!(names, ["id", "name"]);
    assert_eq!(
        generations_of(&connection, &planner, "orders"),
        [(name, GenerationId(5))]
    );
    assert!(
        planner.generation(&generation, &[]).is_empty(),
        "no base, nothing to copy"
    );
    assert!(
        planner.generation(&base, &columns_of_base).is_empty(),
        "not a generation"
    );
}

#[test]
fn commit_times_are_kept_to_the_microsecond() {
    use std::time::{Duration, UNIX_EPOCH};
    let at = UNIX_EPOCH + Duration::from_nanos(1_234_567_891);
    assert_eq!(micros(at), 1_234_567);
    let load = LoadId::from_parts(at, 1);
    let read = receipt(load, CommitSeq::FIRST, 1_234_567, 2, 3);
    assert_eq!(
        read.committed_at,
        UNIX_EPOCH + Duration::from_micros(1_234_567)
    );
    assert_eq!((read.rows, read.bytes), (2, 3));
    assert_eq!(
        receipt(load, CommitSeq::FIRST, -1, -1, 0).rows,
        0,
        "negative counts read as none"
    );
}

#[test]
fn ids_beyond_the_signed_range_keep_their_value() {
    let (connection, planner) = database();
    let large = GenerationId(u64::MAX - 5);
    let generation = TableRef {
        generation: Some(large),
        ..table("orders")
    };
    let fields = [
        ("id", LogicalType::Int64, false),
        ("name", LogicalType::Utf8, true),
    ];
    apply(&connection, &planner, &create(&generation, &fields)).unwrap();
    run_all(&connection, &planner.register(&generation));
    let mine = pipeline("mine");
    stage(
        &connection,
        &planner,
        &generation,
        (&mine, 1, 1),
        &[(1, "big")],
    );
    let rows = query(
        &connection,
        &planner.staged(&mine, Epoch(1), &segments(&[1])),
    );
    let [row] = &rows[..] else { panic!("{rows:?}") };
    let Value::Integer(stored) = row[1] else {
        panic!("{row:?}")
    };
    assert_eq!(super::unsigned(stored), large.0);
    assert_eq!(
        generations_of(&connection, &planner, "orders"),
        [(generation_table("orders", large), large)]
    );
}

#[test]
fn publishing_into_a_table_that_does_not_exist_is_a_data_error() {
    let (_, planner) = database();
    let error = planner
        .publish(
            &staged("missing", None, None),
            &[],
            &pipeline("mine"),
            Epoch(1),
            &segments(&[1]),
        )
        .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Data);
}
