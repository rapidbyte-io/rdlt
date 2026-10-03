use std::time::SystemTime;

use bytes::Bytes;
use rusqlite::Connection;
use rusqlite::types::Value;

use super::{
    Column, Owned, SqlDialect, SqlPlanner, SqlValue, Sqlite, Staged, Statement, micros, receipt,
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
pub(super) struct Widening;

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

    fn resolves(&self, name: &str) -> Statement {
        Sqlite.resolves(name)
    }

    fn transactional_ddl(&self) -> bool {
        Sqlite.transactional_ddl()
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

    fn resolves(&self, name: &str) -> Statement {
        Sqlite.resolves(name)
    }

    fn transactional_ddl(&self) -> bool {
        Sqlite.transactional_ddl()
    }
}

/// SQLite whose identifiers are at most `MAX` bytes.
#[derive(Debug)]
struct Short<const MAX: usize>;

impl<const MAX: usize> SqlDialect for Short<MAX> {
    fn placeholder(&self, index: usize) -> String {
        Sqlite.placeholder(index)
    }

    fn column_type(&self, logical: &LogicalType) -> Option<String> {
        Sqlite.column_type(logical)
    }

    fn columns(&self, table: &str) -> Statement {
        Sqlite.columns(table)
    }

    fn resolves(&self, name: &str) -> Statement {
        Sqlite.resolves(name)
    }

    fn transactional_ddl(&self) -> bool {
        Sqlite.transactional_ddl()
    }

    fn max_identifier(&self) -> Option<usize> {
        Some(MAX)
    }
}

#[test]
fn tables_whose_derived_tables_would_share_a_name_are_refused() {
    let planner = SqlPlanner::try_new(Short::<63>).unwrap();
    let clash = |outcome: crate::error::Result<()>| {
        let error = outcome.unwrap_err();
        assert_eq!(
            (error.kind(), error.code()),
            (ConnectorErrorKind::Config, Some("table_name_clash"))
        );
    };
    // The index of this table's staging is the index a table named as that staging would take.
    let staging = planner.staging_table("y");
    clash(planner.distinct(&table("y"), std::slice::from_ref(&staging), &[]));
    // A generation table of another base under the name of this table's staging, tombstones or
    // generation table is in its way too.
    let generation = TableRef {
        generation: Some(GenerationId(7)),
        ..table("y")
    };
    let taken = [
        planner.staging_table("y"),
        planner.tombstone_table("y"),
        planner.generation_table("y", GenerationId(7)),
    ];
    for name in taken {
        let filling = [(name, "other".to_owned())];
        clash(planner.distinct(&generation, &[], &filling));
    }
    // The table's own generation is no clash, and neither is the generation of a table that
    // fills none itself.
    let own = [(
        planner.generation_table("y", GenerationId(7)),
        "y".to_owned(),
    )];
    planner.distinct(&generation, &[], &own).unwrap();
    let other = [(
        planner.generation_table("y", GenerationId(7)),
        "other".to_owned(),
    )];
    planner.distinct(&table("y"), &[], &other).unwrap();
    // Names whose derived tables differ, and a table registered again, are not refused.
    let distinct = ["orders_by_region_north", "orders_by_region_south"];
    planner
        .distinct(&table(distinct[0]), &[distinct[1].to_owned()], &[])
        .unwrap();
    planner
        .distinct(&table("y"), &["y".to_owned()], &[])
        .unwrap();
}

#[test]
fn every_name_derived_from_a_table_is_compared() {
    let planner = SqlPlanner::try_new(Short::<63>).unwrap();
    let generation = TableRef {
        generation: Some(GenerationId(7)),
        ..table("y")
    };
    let data = [
        planner.staging_table("y"),
        planner.tombstone_table("y"),
        planner.generation_table("y", GenerationId(7)),
    ];
    let mut derived = vec![planner.key_index_name("y"), planner.root_index_name("y")];
    for table in data {
        derived.extend([
            planner.key_index_name(&table),
            planner.root_index_name(&table),
        ]);
        derived.push(table);
    }
    assert_eq!(derived.len(), 11);
    for name in derived {
        // An index of another base's generation table would take the name too.
        let filling = [(name.clone(), "other".to_owned())];
        let error = planner.distinct(&generation, &[], &filling).unwrap_err();
        assert_eq!(error.code(), Some("table_name_clash"), "{name}");
    }
    // The generation table is the generation's own: the table itself does not take its name.
    let named = planner.generation_table("y", GenerationId(7));
    let filling = [(named, "other".to_owned())];
    planner.distinct(&table("y"), &[], &filling).unwrap();
}

#[test]
fn a_commit_publishes_child_tables_first_and_those_listed_that_follow_a_staged_root() {
    let (_, planner) = database();
    let child = |root: &str| MergeKey {
        columns: vec!["root".into()],
        seq: "seq".into(),
        root: Some(RootKey {
            table: root.into(),
            id: "id".into(),
            seq: "seq".into(),
        }),
        changes: None,
        history: None,
    };
    let listed = |name: &str, root: &str| crate::commit::ChildTable {
        table: name.into(),
        merge: child(root),
    };
    let roots = staged("roots", None, keyed("roots").merge);
    let items = staged("items", None, Some(child("roots")));
    let plain = staged("plain", None, None);
    let published = planner.publishing(
        vec![roots.clone(), plain.clone(), items.clone()],
        &[
            listed("items", "roots"),
            listed("tags", "roots"),
            listed("strays", "absent"),
        ],
    );
    let names: Vec<&str> = published
        .iter()
        .map(|staged| staged.name.as_str())
        .collect();
    assert_eq!(names, ["items", "tags", "roots", "plain"]);
    assert_eq!(published[1], staged("tags", None, Some(child("roots"))));
    // Nothing staged, nothing published, whatever the commit lists.
    assert!(
        planner
            .publishing(Vec::new(), &[listed("tags", "roots")])
            .is_empty()
    );
}

/// The table `orders`, a generation of it and it as a change stream's, each of which copies the
/// types of its columns into a table derived from it.
fn copying() -> [TableRef; 3] {
    let orders = keyed("orders");
    let generation = TableRef {
        generation: Some(GenerationId(3)),
        ..orders.clone()
    };
    let changes = TableRef {
        merge: orders.merge.clone().map(|key| MergeKey {
            changes: Some(crate::destination::ChangeColumns {
                op: "op".into(),
                unchanged: None,
                deletion: crate::destination::Deletion::Hard,
            }),
            ..key
        }),
        ..orders.clone()
    };
    [orders, generation, changes]
}

/// What the planner plans to derive a staging table, a generation table and the tombstones from
/// a table of `target`'s columns.
fn derived_from(target: &[Column]) -> [crate::error::Result<Vec<Statement>>; 3] {
    let (_, planner) = database();
    let [orders, generation, changes] = copying();
    let change = create(&orders, &[("v", LogicalType::Utf8, true)]);
    let owned = planner.own(&pipeline("mine"), "orders");
    [
        planner.change_of(&change, [target, &[], &[]]),
        planner.generation(&owned, &generation, target),
        planner.change_tables_of(&changes, [target, target, &[]]),
    ]
}

#[test]
fn a_declared_type_the_dialect_does_not_render_is_never_copied() {
    let column = |name: &str, declared: &str| Column {
        name: name.to_owned(),
        declared: declared.to_owned(),
    };
    let hostile = [
        "INTEGER, injected TEXT DEFAULT (sqlite_version())",
        "INTEGER, CHECK (0)) --",
        "INTEGER NOT NULL",
        "INT",
        "",
    ];
    for declared in hostile {
        let target = [column("id", declared), column("seq", "BLOB")];
        for derived in derived_from(&target) {
            let error = derived.unwrap_err();
            assert_eq!(
                (error.kind(), error.code()),
                (ConnectorErrorKind::Data, Some("schema_conflict")),
                "{declared}"
            );
        }
    }
    // A type the dialect renders is written as the dialect renders it, whatever its case.
    let target = [column("id", "integer"), column("seq", "Blob")];
    for plan in derived_from(&target) {
        let plan = plan.unwrap();
        let created: Vec<&str> = plan
            .iter()
            .map(|statement| statement.sql.as_str())
            .filter(|sql| sql.starts_with("CREATE TABLE"))
            .collect();
        assert_eq!(created.len(), 1, "{plan:?}");
        assert!(created[0].contains("\"id\" INTEGER"), "{}", created[0]);
        assert!(created[0].contains("\"seq\" BLOB"), "{}", created[0]);
    }
}

#[test]
fn no_statement_is_planned_for_a_table_from_the_owner_check_of_another() {
    let (_, planner) = database();
    let mine = pipeline("mine");
    let orders = planner.own(&mine, "orders");
    let (users, columns) = (keyed("users"), [][..].as_ref());
    let staged = staged("users", None, None);
    let change = create(&users, &[("id", LogicalType::Int64, false)]);
    let (epoch, segment, set) = (Epoch(1), SegmentId(1), segments(&[1]));
    let refused = [
        planner.register(&orders, &users).map(drop),
        planner.generation(&orders, &users, columns).map(drop),
        planner.change(&orders, &change, [columns; 3]).map(drop),
        planner
            .change_tables(&orders, &users, [columns; 3])
            .map(drop),
        planner.key_indexes(&orders, &users).map(drop),
        planner.root_index(&orders, &users).map(drop),
        planner
            .stage(&orders, &users, epoch, segment, &["id"])
            .map(drop),
        planner
            .record_segment(&orders, &users, epoch, segment, [1, 1])
            .map(drop),
        planner
            .publish(&orders, &staged, columns, epoch, &set)
            .map(drop),
    ];
    for (index, outcome) in refused.into_iter().enumerate() {
        let error = outcome.unwrap_err();
        assert_eq!(
            error.kind(),
            ConnectorErrorKind::Internal,
            "statement {index}"
        );
    }
    // A discard removes only the staging of the tables its pipeline's owner checks name.
    let theirs = planner.own(&pipeline("theirs"), "users");
    let plan = planner.discard(&mine, epoch, &[orders, theirs]);
    let touched: Vec<&str> = plan
        .iter()
        .map(|statement| statement.sql.as_str())
        .filter(|sql| sql.contains("_rdlt_staging__"))
        .collect();
    assert_eq!(touched.len(), 1, "{touched:?}");
    assert!(touched[0].contains("_rdlt_staging__orders"), "{touched:?}");
}

#[test]
fn a_dialect_whose_schema_changes_do_not_commit_with_it_swaps_no_generation_table() {
    let planner = SqlPlanner::try_new(Autocommitting).unwrap();
    assert!(!planner.swaps_atomically());
    assert!(database().1.swaps_atomically());
    let generation = planner.generation_table("orders", GenerationId(1));
    for generations in [
        vec![(generation.clone(), GenerationId(1))],
        vec![(generation, GenerationId(2))],
    ] {
        let error = planner
            .swap_of("orders", true, GenerationId(1), &generations)
            .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    }
    // A generation without a table swaps in by emptying the table, which changes no schema.
    let (connection, sqlite) = database();
    let fields = [("id", LogicalType::Int64, false)];
    apply(&connection, &sqlite, &create(&table("orders"), &fields)).unwrap();
    connection
        .execute("INSERT INTO orders VALUES (1)", [])
        .unwrap();
    run_all(
        &connection,
        &planner
            .swap_of("orders", true, GenerationId(1), &[])
            .unwrap(),
    );
    let count = Statement {
        sql: "SELECT count(*) FROM orders".into(),
        params: Vec::new(),
    };
    assert_eq!(query(&connection, &count), [[Value::Integer(0)]]);
}

/// SQLite, as if its schema changes committed on their own.
#[derive(Debug)]
struct Autocommitting;

impl SqlDialect for Autocommitting {
    fn placeholder(&self, index: usize) -> String {
        Sqlite.placeholder(index)
    }

    fn column_type(&self, logical: &LogicalType) -> Option<String> {
        Sqlite.column_type(logical)
    }

    fn columns(&self, table: &str) -> Statement {
        Sqlite.columns(table)
    }

    fn resolves(&self, name: &str) -> Statement {
        Sqlite.resolves(name)
    }

    fn transactional_ddl(&self) -> bool {
        false
    }
}

#[test]
fn a_dialect_whose_identifiers_cannot_hold_a_derived_name_is_refused() {
    let refused = SqlPlanner::try_new(Short::<62>).unwrap_err();
    assert_eq!(refused.kind(), ConnectorErrorKind::Unsupported);
    assert!(SqlPlanner::try_new(Short::<63>).is_ok());
}

#[test]
fn a_cut_name_is_a_hash_no_uncut_name_can_take() {
    let planner = SqlPlanner::try_new(Short::<63>).unwrap();
    let long = [
        "orders_by_region_and_by_customer_segment_north_of_the_river",
        "orders_by_region_and_by_customer_segment_south_of_the_river",
    ];
    let generation = GenerationId(u64::MAX);
    let cut: Vec<String> = long
        .iter()
        .flat_map(|name| {
            [
                planner.staging_table(name),
                planner.tombstone_table(name),
                planner.generation_table(name, generation),
                planner.key_index_name(name),
                planner.root_index_name(name),
                planner.key_index_name(&planner.staging_table(name)),
            ]
        })
        .collect();
    for name in &cut {
        // The prefix, then the 256 bits of the name's SHA-256 in base 32.
        assert_eq!(name.len(), 62, "{name}");
        let hash = name.strip_prefix("_rdlt_fit_").expect(name);
        assert!(
            hash.bytes()
                .all(|byte| matches!(byte, b'a'..=b'z' | b'2'..=b'7')),
            "{name}"
        );
        planner.named(name).unwrap_err();
    }
    let distinct: std::collections::BTreeSet<&String> = cut.iter().collect();
    assert_eq!(distinct.len(), cut.len(), "{cut:?}");
    // A name derives alike in every build: SHA-256 of the whole derived name.
    assert_eq!(
        planner.staging_table(long[0]),
        planner.fitted(format!("_rdlt_staging__{}", long[0]))
    );
    assert_eq!(
        planner.fitted("x".repeat(64)),
        "_rdlt_fit_ptqqbfy7mttqahup4wsrs47m37q45vbl57t65dk72yqzkbvvhe6a"
    );
    // A name of the longest length fits as it is, and so does every name without a limit.
    assert_eq!(planner.fitted("x".repeat(63)), "x".repeat(63));
    assert_eq!(planner.staging_table("t"), "_rdlt_staging__t");
    let unlimited = SqlPlanner::try_new(Sqlite).unwrap();
    assert_eq!(
        unlimited.staging_table(long[0]),
        format!("_rdlt_staging__{}", long[0])
    );
    // No name derived uncut begins as a cut name does.
    let uncut = [
        unlimited.staging_table("fit_x"),
        unlimited.tombstone_table("fit_x"),
        unlimited.generation_table("fit_x", GenerationId(1)),
        unlimited.key_index_name("fit_x"),
        unlimited.root_index_name("fit_x"),
    ];
    for name in uncut {
        assert!(!name.starts_with("_rdlt_fit_"), "{name}");
    }
}

/// The planner's statements as a session plans them once its pipeline owns the table: the tests
/// of what a statement does name the pipeline where they stage and publish, and are `mine`
/// otherwise.
impl<D: SqlDialect> SqlPlanner<D> {
    pub(in crate::sqlgen) fn own(&self, pipeline: &PipelineId, name: &str) -> Owned<'static> {
        let owner = [vec![SqlValue::Text(pipeline.to_string())]];
        let found = [vec![SqlValue::Text(name.to_owned())]];
        let standing = self.check(name).unwrap().answered(&(), &owner, &found);
        standing.unwrap().owned(pipeline).unwrap()
    }

    /// The table `name` as `pipeline` creates it where no pipeline owns it and nothing holds
    /// its name.
    pub(in crate::sqlgen) fn claiming(&self, pipeline: &PipelineId, name: &str) -> Owned<'static> {
        let standing = self.check(name).unwrap().answered(&(), &[], &[]);
        standing.unwrap().created(pipeline).unwrap()
    }

    pub(in crate::sqlgen) fn publish_as(
        &self,
        staged: &Staged,
        columns: &[Column],
        pipeline: &PipelineId,
        epoch: Epoch,
        segments: &SegmentSet,
    ) -> crate::error::Result<Vec<Statement>> {
        let owned = self.own(pipeline, &staged.name);
        self.publish(&owned, staged, columns, epoch, segments)
    }

    pub(in crate::sqlgen) fn stage_as(
        &self,
        table: &TableRef,
        pipeline: &PipelineId,
        epoch: Epoch,
        segment: SegmentId,
        columns: &[&str],
    ) -> Statement {
        let owned = self.own(pipeline, &table.name);
        self.stage(&owned, table, epoch, segment, columns).unwrap()
    }

    pub(in crate::sqlgen) fn record_as(
        &self,
        table: &TableRef,
        pipeline: &PipelineId,
        epoch: Epoch,
        segment: SegmentId,
        counts: [u64; 2],
    ) -> Statement {
        let owned = self.own(pipeline, &table.name);
        self.record_segment(&owned, table, epoch, segment, counts)
            .unwrap()
    }

    pub(in crate::sqlgen) fn register_of(&self, table: &TableRef) -> Vec<Statement> {
        let owned = self.own(&pipeline("mine"), &table.name);
        self.register(&owned, table).unwrap()
    }

    pub(in crate::sqlgen) fn swap_of(
        &self,
        base: &str,
        base_exists: bool,
        generation: GenerationId,
        generations: &[(String, GenerationId)],
    ) -> crate::error::Result<Vec<Statement>> {
        let owned = self.own(&pipeline("mine"), base);
        self.swap(&owned, base_exists, generation, generations)
    }

    pub(in crate::sqlgen) fn key_indexes_of(&self, table: &TableRef) -> Vec<Statement> {
        let owned = self.own(&pipeline("mine"), &table.name);
        self.key_indexes(&owned, table).unwrap()
    }

    pub(in crate::sqlgen) fn root_index_of(
        &self,
        table: &TableRef,
    ) -> crate::error::Result<Option<Statement>> {
        self.root_index(&self.own(&pipeline("mine"), &table.name), table)
    }

    pub(in crate::sqlgen) fn change_tables_of(
        &self,
        table: &TableRef,
        tables: [&[Column]; 3],
    ) -> crate::error::Result<Vec<Statement>> {
        self.change_tables(&self.own(&pipeline("mine"), &table.name), table, tables)
    }

    pub(in crate::sqlgen) fn generation_of(
        &self,
        table: &TableRef,
        base: &[Column],
    ) -> Vec<Statement> {
        let owned = self.own(&pipeline("mine"), &table.name);
        self.generation(&owned, table, base).unwrap()
    }

    pub(in crate::sqlgen) fn change_of(
        &self,
        change: &TableChange,
        tables: [&[Column]; 3],
    ) -> crate::error::Result<Vec<Statement>> {
        let owned = self.own(&pipeline("mine"), &change.table().name);
        self.change(&owned, change, tables)
    }

    pub(in crate::sqlgen) fn discard_of(
        &self,
        pipeline: &PipelineId,
        epoch: Epoch,
        names: &[String],
    ) -> Vec<Statement> {
        let tables: Vec<Owned<'_>> = names.iter().map(|name| self.own(pipeline, name)).collect();
        self.discard(pipeline, epoch, &tables)
    }
}

pub(super) fn database() -> (Connection, SqlPlanner<Sqlite>) {
    let connection = Connection::open_in_memory().unwrap();
    let planner = SqlPlanner::try_new(Sqlite).unwrap();
    for statement in planner.bootstrap() {
        run(&connection, &statement);
    }
    (connection, planner)
}

pub(super) fn value(value: &SqlValue) -> Value {
    match value {
        SqlValue::Null => Value::Null,
        SqlValue::Integer(integer) => Value::Integer(*integer),
        SqlValue::Text(text) => Value::Text(text.clone()),
        SqlValue::Blob(blob) => Value::Blob(blob.clone()),
    }
}

/// Runs `statement`; returns the rows it changed.
pub(super) fn run(connection: &Connection, statement: &Statement) -> usize {
    let params = rusqlite::params_from_iter(statement.params.iter().map(value));
    connection
        .execute(&statement.sql, params)
        .unwrap_or_else(|error| panic!("{}: {error}", statement.sql))
}

pub(super) fn run_all(connection: &Connection, statements: &[Statement]) {
    for statement in statements {
        run(connection, statement);
    }
}

pub(super) fn query(connection: &Connection, statement: &Statement) -> Vec<Vec<Value>> {
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

pub(super) fn columns(
    connection: &Connection,
    planner: &SqlPlanner<Sqlite>,
    table: &str,
) -> Vec<Column> {
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

pub(super) fn pipeline(name: &str) -> PipelineId {
    PipelineId::parse(name).unwrap()
}

pub(super) fn table(name: &str) -> TableRef {
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
            changes: None,
            history: None,
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

pub(super) fn create(table: &TableRef, fields: &[(&str, LogicalType, bool)]) -> TableChange {
    TableChange::Create {
        table: table.clone(),
        schema: schema(fields),
    }
}

/// Applies `change` to the database's current tables, as a destination does.
pub(super) fn apply(
    connection: &Connection,
    planner: &SqlPlanner<Sqlite>,
    change: &TableChange,
) -> crate::error::Result<Vec<Statement>> {
    let target = columns(connection, planner, &planner.target(change.table()));
    let staging = columns(
        connection,
        planner,
        &planner.staging_table(&change.table().name),
    );
    let tombstones = columns(
        connection,
        planner,
        &planner.tombstone_table(&change.table().name),
    );
    let plan = planner.change_of(change, [&target, &staging, &tombstones])?;
    run_all(connection, &plan);
    Ok(plan)
}

pub(super) fn segments(ids: &[u64]) -> SegmentSet {
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
    let statement = planner.stage_as(
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
    let record = planner.record_as(
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
    let catalog = [
        "_rdlt_epochs",
        "_rdlt_state",
        "_rdlt_receipts",
        "_rdlt_tables",
        "_rdlt_owners",
        "_rdlt_generations",
        "_rdlt_segments",
    ];
    for table in catalog {
        assert!(!columns(&connection, &planner, table).is_empty(), "{table}");
        // No destination table takes a catalog table's name.
        assert_eq!(
            planner.named(table).unwrap_err().code(),
            Some("table_name_reserved")
        );
        // Every catalog table the planner makes is of its current shape.
        let names: Vec<String> = columns(&connection, &planner, table)
            .into_iter()
            .map(|column| column.name)
            .collect();
        planner.catalog_current(table, &names).unwrap();
    }
    // A table of registered paths made before they were each a pipeline's is refused, and so is
    // one lacking any column it has now; one that is missing is the bootstrap's to make.
    let shapes: [&[&str]; 4] = [
        &["path", "name"],
        &["pipeline", "path"],
        &["pipeline", "name"],
        &["name"],
    ];
    for shape in shapes {
        let held: Vec<String> = shape.iter().map(|name| (*name).to_owned()).collect();
        let refused = planner.catalog_current("_rdlt_tables", &held).unwrap_err();
        assert_eq!(refused.kind(), ConnectorErrorKind::Config);
        assert_eq!(refused.code(), Some(super::CATALOG_OUTDATED), "{shape:?}");
    }
    planner.catalog_current("_rdlt_tables", &[]).unwrap();
    let older = ["path".to_owned(), "name".to_owned()];
    planner.catalog_current("_rdlt_owners", &older).unwrap();
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
fn a_create_makes_the_table_and_its_staging_table_and_applying_it_again_changes_neither() {
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
    let staging: Vec<String> = columns(&connection, &planner, &planner.staging_table("orders"))
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
    // Applied again it changes no table: what is left of its plan registers the table again.
    let again = apply(&connection, &planner, &change).unwrap();
    assert_eq!(again, planner.register_of(change.table()));
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
    let added = plan.len() - planner.register_of(&orders).len();
    assert_eq!(added, 2, "one column added to each table: {plan:?}");
    let names = |table: &str| -> Vec<String> {
        columns(&connection, &planner, table)
            .into_iter()
            .map(|c| c.name)
            .collect()
    };
    assert_eq!(names("orders"), ["id", "name"]);
    assert!(
        names(&planner.staging_table("orders")).ends_with(&["id".to_owned(), "name".to_owned()])
    );
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
        .execute(
            &format!("DROP TABLE \"{}\"", planner.staging_table("orders")),
            [],
        )
        .unwrap();
    let add = TableChange::AddColumn {
        table: orders,
        field: Field::new("name", LogicalType::Utf8, true),
    };
    apply(&connection, &planner, &add).unwrap();
    let staging: Vec<String> = columns(&connection, &planner, &planner.staging_table("orders"))
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
                planner.staging_table("orders")
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
    // A change stream's tombstones hold the key, so they widen with it.
    let plan = widening
        .change_of(&widen, [&existing, &existing, &existing])
        .unwrap();
    let sql: Vec<&str> = plan
        .iter()
        .map(|statement| statement.sql.as_str())
        .collect();
    assert_eq!(
        sql,
        [
            "ALTER TABLE \"orders\" ALTER COLUMN \"n\" TYPE INTEGER",
            "ALTER TABLE \"_rdlt_staging__orders\" ALTER COLUMN \"n\" TYPE INTEGER",
            "ALTER TABLE \"_rdlt_tombstones__orders\" ALTER COLUMN \"n\" TYPE INTEGER",
        ]
    );
    let lacking = widening.change_of(&widen, [&existing, &[], &[]]).unwrap();
    assert_eq!(lacking.len(), 1, "tables without the column are left alone");
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
    let statement = planner.stage_as(
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
        &planner.record_as(&orders, &mine, Epoch(1), SegmentId(1), [1, 10]),
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
        .publish_as(
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
fn a_recorded_merge_key_keeps_a_change_stream_s_columns() {
    let (connection, planner) = database();
    let mine = pipeline("mine");
    let changes = crate::destination::ChangeColumns {
        op: "op".into(),
        unchanged: None,
        deletion: crate::destination::Deletion::Soft { at: "at".into() },
    };
    let mut orders = keyed("orders");
    if let Some(key) = orders.merge.as_mut() {
        key.changes = Some(changes);
    }
    run(
        &connection,
        &planner.record_as(&orders, &mine, Epoch(1), SegmentId(1), [1, 10]),
    );
    let rows = query(
        &connection,
        &planner.staged(&mine, Epoch(1), &segments(&[1])),
    );
    let [row] = &rows[..] else { panic!("{rows:?}") };
    let [_, _, Value::Text(key), Value::Text(seq), ..] = &row[..] else {
        panic!("{row:?}")
    };
    assert_eq!(super::merge_key(key, seq).ok(), orders.merge);
}

#[test]
fn a_recorded_merge_key_keeps_a_history_table_s_columns() {
    let (connection, planner) = database();
    let mine = pipeline("mine");
    let history = crate::destination::HistoryColumns {
        valid_from: "from".into(),
        valid_to: "to".into(),
        is_current: "current".into(),
        row_hash: "hash".into(),
    };
    for changes in [
        None,
        Some(crate::destination::ChangeColumns {
            op: "op".into(),
            unchanged: None,
            deletion: crate::destination::Deletion::Hard,
        }),
    ] {
        let mut orders = keyed("orders");
        if let Some(key) = orders.merge.as_mut() {
            key.changes = changes;
            key.history = Some(history.clone());
        }
        let segment = if orders
            .merge
            .as_ref()
            .is_some_and(|key| key.changes.is_some())
        {
            2
        } else {
            1
        };
        run(
            &connection,
            &planner.record_as(&orders, &mine, Epoch(1), SegmentId(segment), [1, 10]),
        );
        let rows = query(
            &connection,
            &planner.staged(&mine, Epoch(1), &segments(&[segment])),
        );
        let [row] = &rows[..] else { panic!("{rows:?}") };
        let [_, _, Value::Text(key), Value::Text(seq), ..] = &row[..] else {
            panic!("{row:?}")
        };
        assert_eq!(super::merge_key(key, seq).ok(), orders.merge);
    }
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
        .publish_as(&orders, &columns, &mine, Epoch(2), &segments(&[1, 2, 5]))
        .unwrap();
    run_all(&connection, &plan);
    let published: Vec<i64> = rows_of(&connection, "orders")
        .iter()
        .map(|row| row.0)
        .collect();
    assert_eq!(published, [1, 2, 5]);
    let left: Vec<i64> = rows_of(&connection, &planner.staging_table("orders"))
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
    let target = planner.generation_table("orders", GenerationId(3));
    assert_eq!(planner.target(&generation), target);
    let columns = columns(&connection, &planner, &target);
    let plan = planner.publish_as(
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
        let statement = planner.stage_as(
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
                .publish_as(&merge, &columns, &mine, Epoch(1), &committed)
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
            changes: None,
            history: None,
        }),
        ..table("items")
    };
    (keyed("roots"), items)
}

/// A 16-byte sequence whose last byte is `byte`.
pub(super) fn seq(byte: u8) -> Value {
    let mut bytes = vec![0; 16];
    bytes[15] = byte;
    Value::Blob(bytes)
}

/// Stages `rows` of `columns` for `table` as pipeline `mine` at epoch 1 in segment 1.
pub(super) fn stage_values(
    connection: &Connection,
    planner: &SqlPlanner<Sqlite>,
    table: &TableRef,
    columns: &[&str],
    rows: Vec<Vec<Value>>,
) {
    let statement = planner.stage_as(table, &pipeline("mine"), Epoch(1), SegmentId(1), columns);
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
        .publish_as(
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
        .publish_as(
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
fn a_child_table_is_indexed_by_its_root_where_its_rows_are_staged_however_it_was_created() {
    let (connection, planner) = database();
    let (roots, items) = roots_and_items();
    let root_fields = [
        ("id", LogicalType::Int64, false),
        ("seq", LogicalType::Binary, false),
    ];
    let item_fields = [
        ("root", LogicalType::Int64, false),
        ("seq", LogicalType::Binary, false),
    ];
    // Created while its stream appended, so without a merge key.
    apply(&connection, &planner, &create(&roots, &root_fields)).unwrap();
    apply(
        &connection,
        &planner,
        &create(&table("items"), &item_fields),
    )
    .unwrap();
    let indexes = || {
        let statement = Statement {
            sql: "SELECT sql FROM sqlite_master WHERE type = 'index' AND tbl_name = 'items'"
                .to_owned(),
            params: Vec::new(),
        };
        query(&connection, &statement)
    };
    assert!(indexes().is_empty());
    // Indexed where its rows are staged, however often; a table merging by no root never is.
    assert_eq!(planner.root_index_of(&table("items")).unwrap(), None);
    for _ in 0..2 {
        let index = planner
            .root_index_of(&items)
            .unwrap()
            .expect("a child table's index");
        run_all(&connection, &[index]);
        let indexes = indexes();
        assert_eq!(indexes.len(), 1, "{indexes:?}");
        let rendered = format!("{:?}", indexes[0]);
        assert!(rendered.contains("(\\\"root\\\")"), "{indexes:?}");
        assert!(rendered.contains("_rdlt_root__items"), "{indexes:?}");
    }
    // A commit changes no table's indexes.
    let columns = columns(&connection, &planner, "items");
    let merge = staged("items", None, items.merge.clone());
    let plan = planner
        .publish_as(
            &merge,
            &columns,
            &pipeline("mine"),
            Epoch(1),
            &segments(&[1]),
        )
        .unwrap();
    assert!(
        plan.iter()
            .all(|statement| !statement.sql.starts_with("CREATE"))
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
            changes: None,
            history: None,
        }),
        ..table("keys")
    };
    let fields = [
        ("id", LogicalType::Int64, false),
        ("seq", LogicalType::Int64, false),
    ];
    apply(&connection, &planner, &create(&keys, &fields)).unwrap();
    let mine = pipeline("mine");
    let statement = planner.stage_as(&keys, &mine, Epoch(1), SegmentId(1), &["id", "seq"]);
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
                .publish_as(&merge, &columns, &mine, Epoch(1), &segments(&[1]))
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
fn a_merge_ranks_rows_under_a_name_no_column_of_the_table_has() {
    let (connection, planner) = database();
    // The table's own columns take the names a merge could rank its rows under.
    let ranked = TableRef {
        merge: Some(MergeKey {
            columns: vec!["id".into()],
            seq: "seq".into(),
            root: None,
            changes: None,
            history: None,
        }),
        ..table("ranked")
    };
    let fields = [
        ("id", LogicalType::Int64, false),
        ("seq", LogicalType::Int64, false),
        ("_rdlt_rank", LogicalType::Int64, true),
        ("_RDLT_RANK_", LogicalType::Int64, true),
    ];
    apply(&connection, &planner, &create(&ranked, &fields)).unwrap();
    let mine = pipeline("mine");
    let names = ["id", "seq", "_rdlt_rank", "_RDLT_RANK_"];
    let statement = planner.stage_as(&ranked, &mine, Epoch(1), SegmentId(1), &names);
    for (id, rank) in [(1, 7), (2, 1), (3, 5)] {
        let mut values: Vec<Value> = statement.params.iter().map(value).collect();
        values.extend([id, 1, rank, rank].map(Value::Integer));
        connection
            .execute(&statement.sql, rusqlite::params_from_iter(values))
            .unwrap();
    }
    let columns = columns(&connection, &planner, "ranked");
    let merge = staged("ranked", None, ranked.merge.clone());
    run_all(
        &connection,
        &planner
            .publish_as(&merge, &columns, &mine, Epoch(1), &segments(&[1]))
            .unwrap(),
    );
    let ids = query(
        &connection,
        &Statement {
            sql: "SELECT id FROM ranked ORDER BY id".to_owned(),
            params: Vec::new(),
        },
    );
    assert_eq!(ids, [1, 2, 3].map(|id| [Value::Integer(id)]));
}

#[test]
fn registered_tables_are_found_by_path_with_their_generations() {
    let (connection, planner) = database();
    let orders = keyed("orders");
    run_all(&connection, &planner.register_of(&orders));
    run_all(&connection, &planner.register_of(&orders));
    let generation = TableRef {
        generation: Some(GenerationId(4)),
        ..table("events")
    };
    run_all(&connection, &planner.register_of(&generation));
    let path = TablePath::new(["orders"]).unwrap();
    let found = query(&connection, &planner.table_name(&pipeline("mine"), &path));
    assert_eq!(found, [[text("orders")]]);
    // Another pipeline's table of the same path, under another name, answers only for it.
    let theirs = pipeline("theirs");
    let namesake = TableRef {
        name: "orders_abc234".into(),
        ..keyed("orders")
    };
    let owned = planner.own(&theirs, "orders_abc234");
    run_all(&connection, &planner.register(&owned, &namesake).unwrap());
    let found = query(&connection, &planner.table_name(&pipeline("mine"), &path));
    assert_eq!(found, [[text("orders")]]);
    let found = query(&connection, &planner.table_name(&theirs, &path));
    assert_eq!(found, [[text("orders_abc234")]]);
    assert_eq!(
        query(&connection, &planner.generation_tables()),
        [[
            text(&planner.generation_table("events", GenerationId(4))),
            text("events")
        ]]
    );
    let generations = query(&connection, &planner.generations("events"));
    assert_eq!(
        generations,
        [[
            text(&planner.generation_table("events", GenerationId(4))),
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
        run_all(&connection, &planner.register_of(&generation));
    }
    connection
        .execute(
            &format!(
                "INSERT INTO \"{}\" VALUES (2, 'new')",
                planner.generation_table("orders", GenerationId(2))
            ),
            [],
        )
        .unwrap();
    let generations = generations_of(&connection, &planner, "orders");
    run_all(
        &connection,
        &planner
            .swap_of("orders", true, GenerationId(2), &generations)
            .unwrap(),
    );
    assert_eq!(rows_of(&connection, "orders"), [(2, "new".to_owned())]);
    for generation in [1, 2] {
        let name = planner.generation_table("orders", GenerationId(generation));
        assert!(columns_of_missing(&connection, &planner, &name), "{name}");
    }
    assert!(query(&connection, &planner.generations("orders")).is_empty());
}

#[test]
fn a_swapped_in_generation_keeps_no_index_named_after_it() {
    let (connection, planner) = database();
    let fields = [
        ("id", LogicalType::Int64, false),
        ("seq", LogicalType::Binary, true),
    ];
    let base = keyed("orders");
    apply(&connection, &planner, &create(&base, &fields)).unwrap();
    let generation = TableRef {
        generation: Some(GenerationId(2)),
        ..base.clone()
    };
    apply(&connection, &planner, &create(&generation, &fields)).unwrap();
    run_all(&connection, &planner.register_of(&generation));
    run_all(&connection, &planner.key_indexes_of(&generation));
    let name = planner.generation_table("orders", GenerationId(2));
    run(
        &connection,
        &Statement {
            sql: format!(
                "CREATE INDEX \"{}\" ON \"{name}\" (id)",
                planner.root_index_name(&name)
            ),
            params: Vec::new(),
        },
    );
    let generations = generations_of(&connection, &planner, "orders");
    run_all(
        &connection,
        &planner
            .swap_of("orders", true, GenerationId(2), &generations)
            .unwrap(),
    );
    // The next writer indexes the table under its own name; the generation's leave with it.
    run_all(&connection, &planner.key_indexes_of(&base));
    let listing = Statement {
        sql: "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'orders'".into(),
        params: Vec::new(),
    };
    assert_eq!(
        query(&connection, &listing),
        [[text(&planner.key_index_name("orders"))]]
    );
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
        &planner
            .swap_of("orders", true, GenerationId(7), &[])
            .unwrap(),
    );
    assert!(rows_of(&connection, "orders").is_empty());
    let nothing = planner
        .swap_of("missing", false, GenerationId(7), &[])
        .unwrap();
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
    run_all(&connection, &planner.discard_of(&mine, Epoch(2), &names));
    for name in ["orders", "users"] {
        let left: Vec<i64> = rows_of(&connection, &planner.staging_table(name))
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
        &planner.generation_of(&generation, &columns_of_base),
    );
    let name = planner.generation_table("orders", GenerationId(5));
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
        planner.generation_of(&generation, &[]).is_empty(),
        "no base, nothing to copy"
    );
    assert!(
        planner.generation_of(&base, &columns_of_base).is_empty(),
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
    run_all(&connection, &planner.register_of(&generation));
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
        [(planner.generation_table("orders", large), large)]
    );
}

#[test]
fn publishing_into_a_table_that_does_not_exist_is_a_data_error() {
    let (_, planner) = database();
    let error = planner
        .publish_as(
            &staged("missing", None, None),
            &[],
            &pipeline("mine"),
            Epoch(1),
            &segments(&[1]),
        )
        .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Data);
}

#[test]
fn a_merge_finds_the_rows_its_keys_replace_through_the_key_s_indexes() {
    let (connection, planner) = database();
    let orders = TableRef {
        merge: Some(MergeKey {
            columns: vec!["id".into(), "region".into()],
            seq: "seq".into(),
            root: None,
            changes: None,
            history: None,
        }),
        ..table("orders")
    };
    let fields = [
        ("id", LogicalType::Int64, false),
        ("region", LogicalType::Int64, false),
        ("seq", LogicalType::Binary, false),
    ];
    apply(&connection, &planner, &create(&orders, &fields)).unwrap();
    run_all(&connection, &planner.key_indexes_of(&orders));
    let columns = columns(&connection, &planner, "orders");
    let plan = planner
        .publish_as(
            &staged("orders", None, orders.merge.clone()),
            &columns,
            &pipeline("mine"),
            Epoch(1),
            &segments(&[1]),
        )
        .unwrap();
    // Deleting the rows the staged keys replace reads neither the table whole nor, for each of
    // its rows, the staging table: a commit costs what it stages.
    let explain = Statement {
        sql: format!("EXPLAIN QUERY PLAN {}", plan[0].sql),
        params: plan[0].params.clone(),
    };
    let steps: Vec<String> = query(&connection, &explain)
        .into_iter()
        .map(|row| match &row[3] {
            Value::Text(step) => step.clone(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert!(!steps.iter().any(|step| step == "SCAN orders"), "{steps:?}");
    for (index, step) in steps.iter().enumerate() {
        if step.starts_with("CORRELATED") {
            let inner = steps.get(index + 1).map_or("", String::as_str);
            assert!(inner.starts_with("SEARCH"), "{steps:?}");
        }
    }
}

/// The steps SQLite's virtual machine takes to run `plan` on `connection`, rolled back so the
/// next run finds what this one did: what a plan costs, whatever else the machine is doing.
pub(super) fn planned(connection: &Connection, plan: &[Statement]) -> u64 {
    connection.execute_batch("BEGIN").unwrap();
    let steps = plan
        .iter()
        .map(|statement| {
            let mut prepared = connection.prepare(&statement.sql).unwrap();
            let params = rusqlite::params_from_iter(statement.params.iter().map(value));
            prepared.execute(params).unwrap();
            let steps = prepared.get_status(rusqlite::StatementStatus::VmStep);
            u64::try_from(steps).expect("a count of steps")
        })
        .sum();
    connection.execute_batch("ROLLBACK").unwrap();
    steps
}

/// A sequence as `sqlgen`'s tests fill tables in SQL: sixteen digits, which order as numbers do.
pub(super) fn digits(number: &str) -> String {
    format!("CAST(printf('%016d', {number}) AS BLOB)")
}

/// The numbers from 1 to `count`, as a table `_n` of one column `i`.
pub(super) fn counting(count: u32) -> String {
    format!("WITH RECURSIVE _n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM _n WHERE i < {count})")
}
