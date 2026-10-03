use rusqlite::types::Value;
use rusqlite::{Connection, StatementStatus};

use super::super::super::tests::{
    apply, columns, counting, create, database, digits, pipeline, planned, query, run_all,
};
use super::super::super::tests::{segments, seq, stage_values, table, value};
use super::super::super::{SqlPlanner, Sqlite, Statement};
use super::super::Staged;
use crate::destination::{ChangeColumns, Deletion, HistoryColumns, MergeKey, TableRef};
use crate::error::ConnectorErrorKind;
use crate::id::{Epoch, GenerationId};
use crate::types::LogicalType;

/// What a written row does.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Op {
    Upsert,
    Delete,
    Truncate,
}

/// A written row: its op, key, name, sequence and when it takes effect.
#[derive(Clone, Copy)]
struct Row {
    op: Op,
    id: Option<i64>,
    name: Option<&'static str>,
    seq: u8,
    at: i64,
}

fn upsert(id: i64, name: &'static str, seq: u8, at: i64) -> Row {
    Row {
        op: Op::Upsert,
        id: Some(id),
        name: Some(name),
        seq,
        at,
    }
}

fn delete(id: i64, seq: u8, at: i64) -> Row {
    Row {
        op: Op::Delete,
        name: None,
        ..upsert(id, "", seq, at)
    }
}

fn truncate(seq: u8, at: i64) -> Row {
    Row {
        op: Op::Truncate,
        id: None,
        ..delete(0, seq, at)
    }
}

/// How a history table is written: its rows all upserts, or a change stream's with hard or soft
/// deletes.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Plain,
    Hard,
    Soft,
}

/// A published version: key, name, deletion time, sequence byte, valid from, valid to, current.
type Version = (i64, Option<String>, Option<i64>, u8, i64, Option<i64>, bool);

fn current(id: i64, name: &str, seq: u8, from: i64) -> Version {
    (id, Some(name.to_owned()), None, seq, from, None, true)
}

fn closed(id: i64, name: &str, seq: u8, from: i64, to: i64) -> Version {
    (id, Some(name.to_owned()), None, seq, from, Some(to), false)
}

/// A version of `id` keeping `name`, deleted at `from`, current or closed at `to`.
fn deleted(id: i64, name: &str, seq: u8, from: i64, to: Option<i64>) -> Version {
    let at = Some(from);
    (id, Some(name.to_owned()), at, seq, from, to, to.is_none())
}

/// The hash a writer gives `name`: its bytes, zero-padded to 16.
fn hash(name: &str) -> Vec<u8> {
    let mut hash = vec![0_u8; 16];
    for (byte, source) in hash.iter_mut().zip(name.bytes()) {
        *byte = source;
    }
    hash
}

fn history_key(kind: Kind) -> MergeKey {
    let deletion = match kind {
        Kind::Plain => None,
        Kind::Hard => Some(Deletion::Hard),
        Kind::Soft => Some(Deletion::Soft {
            at: "deleted_at".into(),
        }),
    };
    MergeKey {
        columns: vec!["id".into()],
        seq: "seq".into(),
        root: None,
        changes: deletion.map(|deletion| ChangeColumns {
            op: "op".into(),
            unchanged: None,
            deletion,
        }),
        history: Some(HistoryColumns {
            valid_from: "valid_from".into(),
            valid_to: "valid_to".into(),
            is_current: "is_current".into(),
            row_hash: "row_hash".into(),
        }),
    }
}

/// A history table of `kind` in its own database, its change tables readied and indexed as a
/// writer readies them.
struct History {
    connection: Connection,
    planner: SqlPlanner<Sqlite>,
    table: TableRef,
    kind: Kind,
}

impl History {
    fn new(kind: Kind) -> Self {
        Self::with_columns(kind, &[])
    }

    /// A history table with the data columns `extra` besides its name.
    fn with_columns(kind: Kind, extra: &[&str]) -> Self {
        let (connection, planner) = database();
        let table = TableRef {
            merge: Some(history_key(kind)),
            ..table("orders")
        };
        let mut fields = vec![
            ("id", LogicalType::Int64, false),
            ("name", LogicalType::Utf8, true),
            ("seq", LogicalType::Binary, false),
        ];
        fields.extend(extra.iter().map(|name| (*name, LogicalType::Int64, true)));
        if kind == Kind::Soft {
            fields.push(("deleted_at", LogicalType::Int64, true));
        }
        fields.extend([
            ("valid_from", LogicalType::Int64, false),
            ("valid_to", LogicalType::Int64, true),
            ("is_current", LogicalType::Bool, false),
            ("row_hash", LogicalType::Binary, true),
        ]);
        apply(&connection, &planner, &create(&table, &fields)).unwrap();
        let names = [
            "orders".to_owned(),
            planner.staging_table("orders"),
            planner.tombstone_table("orders"),
        ];
        let tables = names.map(|name| columns(&connection, &planner, &name));
        let ready = planner
            .change_tables_of(&table, [&tables[0], &tables[1], &tables[2]])
            .unwrap();
        run_all(&connection, &ready);
        run_all(&connection, &planner.key_indexes_of(&table));
        Self {
            connection,
            planner,
            table,
            kind,
        }
    }

    fn staged(&self, generation: Option<GenerationId>) -> Staged {
        Staged {
            name: "orders".to_owned(),
            generation,
            merge: self.table.merge.clone(),
        }
    }

    /// The statements publishing what the commit staged.
    fn plan(&self) -> crate::error::Result<Vec<Statement>> {
        let columns = columns(&self.connection, &self.planner, "orders");
        self.planner.publish_as(
            &self.staged(None),
            &columns,
            &pipeline("mine"),
            Epoch(1),
            &segments(&[1]),
        )
    }

    /// Stages `rows` and publishes them in one commit; returns the versions then published.
    fn commit(&self, rows: &[Row]) -> Vec<Version> {
        self.stage(rows, &[]);
        run_all(&self.connection, &self.plan().unwrap());
        self.versions()
    }

    /// Stages `rows`, each with the values `extra` of the table's extra columns.
    fn stage(&self, rows: &[Row], extra: &[&str]) {
        let mut names = vec!["id", "name", "seq"];
        names.extend(extra);
        if self.kind == Kind::Soft {
            names.push("deleted_at");
        }
        names.extend(["valid_from", "valid_to", "is_current", "row_hash"]);
        if self.kind != Kind::Plain {
            names.push("op");
        }
        let values = rows
            .iter()
            .map(|row| self.values(row, extra.len()))
            .collect();
        stage_values(&self.connection, &self.planner, &self.table, &names, values);
    }

    /// `row` as a writer stages it, with `extra` extra columns holding its sequence.
    fn values(&self, row: &Row, extra: usize) -> Vec<Value> {
        let or_null = |value: Option<Value>| value.unwrap_or(Value::Null);
        let mut values = vec![
            or_null(row.id.map(Value::Integer)),
            or_null(row.name.map(|name| Value::Text(name.to_owned()))),
            seq(row.seq),
        ];
        values.extend((0..extra).map(|_| Value::Integer(row.seq.into())));
        if self.kind == Kind::Soft {
            values.push(or_null(
                (row.op != Op::Upsert).then_some(Value::Integer(row.at)),
            ));
        }
        values.extend([
            Value::Integer(row.at),
            Value::Null,
            Value::Integer(1),
            or_null(row.name.map(|name| Value::Blob(hash(name)))),
        ]);
        let op = match row.op {
            Op::Upsert => 1,
            Op::Delete => 2,
            Op::Truncate => 3,
        };
        if self.kind != Kind::Plain {
            values.push(Value::Integer(op));
        }
        values
    }

    /// The versions the table publishes, sorted.
    fn versions(&self) -> Vec<Version> {
        let at = if self.kind == Kind::Soft {
            "deleted_at"
        } else {
            "NULL"
        };
        let sql = format!(
            "SELECT id, name, {at}, seq, valid_from, valid_to, is_current FROM orders ORDER BY id, \
             valid_from, is_current"
        );
        let mut versions: Vec<Version> = self
            .select(&sql)
            .into_iter()
            .map(|row| match &row[..] {
                [
                    Value::Integer(id),
                    name,
                    at,
                    Value::Blob(seq),
                    Value::Integer(from),
                    to,
                    Value::Integer(current),
                ] => (
                    *id,
                    text(name),
                    integer(at),
                    seq[15],
                    *from,
                    integer(to),
                    *current == 1,
                ),
                other => panic!("{other:?}"),
            })
            .collect();
        versions.sort_unstable();
        versions
    }

    /// The table's tombstones: each key, or none for the bound, with its sequence byte.
    fn tombstones(&self) -> Vec<(Option<i64>, u8)> {
        if self.kind == Kind::Plain {
            return Vec::new();
        }
        let sql = "SELECT id, seq FROM _rdlt_tombstones__orders ORDER BY id, seq";
        self.select(sql)
            .into_iter()
            .map(|row| match &row[..] {
                [id, Value::Blob(seq)] => (integer(id), seq[15]),
                other => panic!("{other:?}"),
            })
            .collect()
    }

    /// The rows staging holds.
    fn staging(&self) -> Vec<Vec<Value>> {
        self.select("SELECT * FROM _rdlt_staging__orders")
    }

    fn select(&self, sql: &str) -> Vec<Vec<Value>> {
        let statement = Statement {
            sql: sql.to_owned(),
            params: Vec::new(),
        };
        query(&self.connection, &statement)
    }
}

fn text(value: &Value) -> Option<String> {
    match value {
        Value::Text(text) => Some(text.clone()),
        Value::Null => None,
        other => panic!("{other:?}"),
    }
}

fn integer(value: &Value) -> Option<i64> {
    match value {
        Value::Integer(integer) => Some(*integer),
        Value::Null => None,
        other => panic!("{other:?}"),
    }
}

fn sorted(mut versions: Vec<Version>) -> Vec<Version> {
    versions.sort_unstable();
    versions
}

#[test]
fn an_upsert_equal_to_its_key_s_current_version_changes_nothing() {
    for kind in [Kind::Plain, Kind::Hard, Kind::Soft] {
        let history = History::new(kind);
        history.commit(&[upsert(1, "a", 1, 10)]);
        let published = history.commit(&[upsert(1, "a", 2, 20)]);
        assert_eq!(published, [current(1, "a", 1, 10)]);
    }
}

#[test]
fn a_changed_upsert_closes_the_current_version_where_its_own_begins() {
    for kind in [Kind::Plain, Kind::Hard, Kind::Soft] {
        let history = History::new(kind);
        history.commit(&[upsert(1, "a", 1, 10), upsert(2, "b", 2, 10)]);
        let published = history.commit(&[upsert(1, "a2", 3, 20)]);
        let expected = [
            closed(1, "a", 1, 10, 20),
            current(1, "a2", 3, 20),
            current(2, "b", 2, 10),
        ];
        assert_eq!(published, sorted(expected.to_vec()));
    }
}

#[test]
fn versions_of_one_key_in_one_commit_chain_in_sequence_order() {
    for kind in [Kind::Plain, Kind::Hard, Kind::Soft] {
        let history = History::new(kind);
        // Written out of order; an upsert equal to the version before it is skipped, one equal
        // to an older version is not.
        let published = history.commit(&[
            upsert(1, "c", 4, 40),
            upsert(1, "a", 1, 10),
            upsert(1, "a", 5, 50),
            upsert(1, "b", 2, 20),
            upsert(1, "b", 3, 30),
        ]);
        let expected = [
            closed(1, "a", 1, 10, 20),
            closed(1, "b", 2, 20, 40),
            closed(1, "c", 4, 40, 50),
            current(1, "a", 5, 50),
        ];
        assert_eq!(published, sorted(expected.to_vec()));
    }
}

#[test]
fn a_plain_table_applies_later_commits_after_earlier_ones_whatever_their_sequences() {
    let history = History::new(Kind::Plain);
    history.commit(&[upsert(1, "a", 5, 10), upsert(2, "b", 6, 10)]);
    // Sequences order rows within their commit only: these are older than the versions.
    let published = history.commit(&[
        upsert(1, "a2", 1, 20),
        upsert(1, "a3", 2, 30),
        upsert(2, "b", 1, 20),
    ]);
    let expected = [
        closed(1, "a", 5, 10, 20),
        closed(1, "a2", 1, 20, 30),
        current(1, "a3", 2, 30),
        current(2, "b", 6, 10),
    ];
    assert_eq!(published, sorted(expected.to_vec()));
}

#[test]
fn a_change_applies_only_past_its_key_s_newest_version_its_tombstone_and_the_bound() {
    let history = History::new(Kind::Hard);
    history.commit(&[
        upsert(1, "a", 2, 10),
        upsert(1, "a2", 4, 20),
        upsert(2, "b", 5, 10),
        delete(2, 6, 30),
        delete(3, 7, 30),
    ]);
    let before = history.versions();
    // Each is at or before its key's newest version, which is not its current one for key 1,
    // or its tombstone, whether or not the key had a version.
    let replayed = history.commit(&[
        upsert(1, "old", 3, 40),
        upsert(2, "old", 6, 40),
        upsert(3, "old", 7, 40),
    ]);
    assert_eq!(replayed, before);
    history.commit(&[truncate(10, 50)]);
    let truncated = history.versions();
    let bounded = history.commit(&[upsert(4, "old", 9, 60), truncate(8, 60)]);
    assert_eq!(bounded, truncated);
    let later = history.commit(&[upsert(1, "new", 11, 70), upsert(3, "new", 12, 70)]);
    let expected = [
        closed(1, "a", 2, 10, 20),
        closed(1, "a2", 4, 20, 50),
        current(1, "new", 11, 70),
        closed(2, "b", 5, 10, 30),
        current(3, "new", 12, 70),
    ];
    assert_eq!(later, sorted(expected.to_vec()));
}

#[test]
fn a_hard_delete_closes_its_key_s_version_and_a_later_upsert_opens_another() {
    let history = History::new(Kind::Hard);
    history.commit(&[upsert(1, "a", 1, 10)]);
    let published = history.commit(&[
        delete(1, 2, 20),
        upsert(1, "a", 3, 30),
        upsert(2, "b", 4, 40),
        delete(2, 5, 50),
    ]);
    let expected = [
        closed(1, "a", 1, 10, 20),
        current(1, "a", 3, 30),
        closed(2, "b", 4, 40, 50),
    ];
    assert_eq!(published, sorted(expected.to_vec()));
    // Key 2's last change is a delete: its tombstone keeps the change sent again out.
    assert_eq!(history.tombstones(), [(Some(2), 5)]);
    let replayed = history.commit(&[upsert(2, "b", 4, 40)]);
    assert_eq!(replayed, sorted(expected.to_vec()));
    // A key reopened past its tombstone needs it no longer.
    history.commit(&[upsert(2, "c", 6, 60)]);
    assert_eq!(history.tombstones(), []);
}

#[test]
fn a_soft_delete_opens_a_deleted_version_that_an_equal_upsert_closes() {
    let history = History::new(Kind::Soft);
    history.commit(&[upsert(1, "a", 1, 10), upsert(2, "b", 2, 10)]);
    let published = history.commit(&[delete(1, 3, 30)]);
    let expected = [
        closed(1, "a", 1, 10, 30),
        deleted(1, "a", 3, 30, None),
        current(2, "b", 2, 10),
    ];
    assert_eq!(published, sorted(expected.to_vec()));
    let published = history.commit(&[upsert(1, "a", 4, 40)]);
    let expected = [
        closed(1, "a", 1, 10, 30),
        deleted(1, "a", 3, 30, Some(40)),
        current(1, "a", 4, 40),
        current(2, "b", 2, 10),
    ];
    assert_eq!(published, sorted(expected.to_vec()));
    // In one commit alike, where the delete keeps the data of the version it closes.
    let published = history.commit(&[
        upsert(2, "b2", 5, 50),
        delete(2, 6, 60),
        upsert(2, "b2", 7, 70),
    ]);
    let mut expected = expected[..3].to_vec();
    expected.extend([
        closed(2, "b", 2, 10, 50),
        closed(2, "b2", 5, 50, 60),
        deleted(2, "b2", 6, 60, Some(70)),
        current(2, "b2", 7, 70),
    ]);
    assert_eq!(published, sorted(expected));
    assert_eq!(history.tombstones(), []);
}

#[test]
fn a_soft_delete_keeps_every_column_of_the_version_it_closes() {
    let history = History::with_columns(Kind::Soft, &["origin"]);
    history.stage(&[upsert(1, "a", 1, 10)], &["origin"]);
    run_all(&history.connection, &history.plan().unwrap());
    // The upsert equal to the version is skipped, so the delete keeps the version's origin.
    history.stage(&[upsert(1, "a", 2, 20), delete(1, 3, 30)], &["origin"]);
    run_all(&history.connection, &history.plan().unwrap());
    let origins =
        history.select("SELECT origin, deleted_at FROM orders ORDER BY valid_from, is_current");
    let [first, kept] = &origins[..] else {
        panic!("{origins:?}")
    };
    assert_eq!(first, &[Value::Integer(1), Value::Null]);
    assert_eq!(kept, &[Value::Integer(1), Value::Integer(30)]);
}

#[test]
fn a_delete_of_a_deleted_or_missing_version_changes_nothing() {
    let history = History::new(Kind::Soft);
    history.commit(&[upsert(1, "a", 1, 10), delete(1, 2, 20)]);
    let before = history.versions();
    let published = history.commit(&[delete(1, 3, 30), delete(1, 4, 40), delete(2, 5, 50)]);
    assert_eq!(published, before);
    let hard = History::new(Kind::Hard);
    hard.commit(&[upsert(1, "a", 1, 10), delete(1, 2, 20)]);
    let before = hard.versions();
    let published = hard.commit(&[delete(1, 3, 30), delete(2, 4, 40)]);
    assert_eq!(published, before);
    // A hard delete keeps its key's tombstone even so.
    assert_eq!(hard.tombstones(), [(Some(1), 3), (Some(2), 4)]);
}

#[test]
fn a_hard_truncate_closes_every_version_sequenced_before_it_and_raises_the_bound() {
    let history = History::new(Kind::Hard);
    history.commit(&[
        upsert(1, "a", 1, 10),
        upsert(2, "b", 20, 20),
        delete(3, 2, 20),
    ]);
    let published = history.commit(&[
        upsert(4, "d", 3, 30),
        truncate(5, 50),
        upsert(5, "e", 6, 60),
        upsert(1, "a", 7, 70),
    ]);
    // Key 2's version is sequenced past the truncate; key 1's opens again equal to the closed.
    let expected = [
        closed(1, "a", 1, 10, 50),
        current(1, "a", 7, 70),
        current(2, "b", 20, 20),
        closed(4, "d", 3, 30, 50),
        current(5, "e", 6, 60),
    ];
    assert_eq!(published, sorted(expected.to_vec()));
    assert_eq!(history.tombstones(), [(None, 5)]);
    let replayed = history.commit(&[truncate(5, 80), upsert(4, "d", 4, 80)]);
    assert_eq!(replayed, sorted(expected.to_vec()));
    // The bound raised again replaces the old one.
    assert_eq!(history.tombstones(), [(None, 5)]);
}

#[test]
fn a_soft_truncate_marks_every_version_sequenced_before_it_deleted() {
    let history = History::new(Kind::Soft);
    history.commit(&[
        upsert(1, "a", 1, 10),
        upsert(2, "b", 2, 10),
        delete(2, 3, 30),
        upsert(3, "c", 20, 20),
    ]);
    let published = history.commit(&[
        upsert(4, "d", 4, 40),
        truncate(5, 50),
        upsert(5, "e", 6, 60),
    ]);
    let expected = [
        closed(1, "a", 1, 10, 50),
        deleted(1, "a", 5, 50, None),
        closed(2, "b", 2, 10, 30),
        deleted(2, "b", 3, 30, None),
        current(3, "c", 20, 20),
        closed(4, "d", 4, 40, 50),
        deleted(4, "d", 5, 50, None),
        current(5, "e", 6, 60),
    ];
    assert_eq!(published, sorted(expected.to_vec()));
    assert_eq!(history.tombstones(), []);
    // Sent again, it finds nothing sequenced before it that is not deleted.
    let replayed = history.commit(&[truncate(5, 90)]);
    assert_eq!(replayed, sorted(expected.to_vec()));
}

#[test]
fn a_history_table_never_publishes_into_a_generation() {
    let history = History::new(Kind::Plain);
    let columns = columns(&history.connection, &history.planner, "orders");
    for kind in [Kind::Plain, Kind::Hard] {
        let staged = Staged {
            merge: Some(history_key(kind)),
            ..history.staged(Some(GenerationId(3)))
        };
        let error = history
            .planner
            .publish_as(
                &staged,
                &columns,
                &pipeline("mine"),
                Epoch(1),
                &segments(&[1]),
            )
            .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::Internal);
    }
}

#[test]
fn a_history_of_partial_updates_is_refused() {
    let history = History::new(Kind::Hard);
    let columns = columns(&history.connection, &history.planner, "orders");
    let mut key = history_key(Kind::Hard);
    if let Some(changes) = key.changes.as_mut() {
        changes.unchanged = Some("unchanged".into());
    }
    let staged = Staged {
        merge: Some(key),
        ..history.staged(None)
    };
    let error = history
        .planner
        .publish_as(
            &staged,
            &columns,
            &pipeline("mine"),
            Epoch(1),
            &segments(&[1]),
        )
        .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Internal);
}

#[test]
fn a_commit_leaves_nothing_staged_and_publishing_nothing_changes_nothing() {
    for kind in [Kind::Plain, Kind::Hard, Kind::Soft] {
        let history = History::new(kind);
        let mut rows = vec![upsert(1, "a", 1, 10), upsert(1, "b", 2, 20)];
        if kind != Kind::Plain {
            rows.extend([delete(1, 3, 30), delete(2, 4, 40), truncate(5, 50)]);
        }
        let published = history.commit(&rows);
        assert_eq!(history.staging(), Vec::<Vec<Value>>::new());
        let tombstones = history.tombstones();
        assert_eq!(history.commit(&[]), published);
        assert_eq!(history.tombstones(), tombstones);
    }
}

#[test]
fn a_history_computes_under_names_no_column_of_the_table_has() {
    let taken = [
        "_rdlt_kind",
        "_rdlt_pos",
        "_rdlt_last",
        "_rdlt_gone",
        "_rdlt_acts",
        "_rdlt_next",
        "_rdlt_from",
        "_rdlt_q",
        "_rdlt_rank",
    ];
    for kind in [Kind::Plain, Kind::Hard, Kind::Soft] {
        let history = History::with_columns(kind, &taken);
        history.stage(&[upsert(1, "a", 1, 10), upsert(1, "b", 2, 20)], &taken);
        run_all(&history.connection, &history.plan().unwrap());
        let mut expected = vec![closed(1, "a", 1, 10, 20), closed(1, "b", 2, 20, 30)];
        let last = match kind {
            Kind::Plain => {
                expected.push(current(1, "c", 3, 30));
                upsert(1, "c", 3, 30)
            }
            Kind::Hard => delete(1, 3, 30),
            Kind::Soft => {
                expected.push(deleted(1, "b", 3, 30, None));
                delete(1, 3, 30)
            }
        };
        let mut rows = vec![last];
        if kind != Kind::Plain {
            rows.push(truncate(4, 40));
        }
        history.stage(&rows, &taken);
        run_all(&history.connection, &history.plan().unwrap());
        assert_eq!(history.versions(), sorted(expected));
    }
}

#[test]
fn a_commit_reads_only_the_versions_and_tombstones_of_its_keys() {
    let history = History::new(Kind::Hard);
    let first: Vec<Row> = (1..=100).map(|id| upsert(id, "a", 1, 10)).collect();
    let second: Vec<Row> = (1..=100).map(|id| upsert(id, "b", 2, 20)).collect();
    let buried: Vec<Row> = (101..=200).map(|id| delete(id, 3, 30)).collect();
    for rows in [first, second, buried] {
        history.commit(&rows);
    }
    history.stage(
        &[upsert(1, "c", 4, 40), delete(2, 5, 50), delete(150, 6, 60)],
        &[],
    );
    // No statement steps through the table's 200 versions or 100 tombstones: a commit costs
    // what it changes.
    for statement in history.plan().unwrap() {
        let mut prepared = history.connection.prepare(&statement.sql).unwrap();
        let params = statement.params.iter().map(value);
        prepared
            .execute(rusqlite::params_from_iter(params))
            .unwrap();
        let steps = prepared.get_status(StatementStatus::FullscanStep);
        assert!(steps < 50, "{steps} steps: {}", statement.sql);
    }
    let published = history.versions();
    assert_eq!(published.len(), 201);
    assert!(published.contains(&current(1, "c", 4, 40)));
    assert!(published.contains(&closed(2, "b", 2, 20, 50)));
    assert_eq!(history.tombstones().len(), 101);
}

#[test]
fn truncates_cost_their_count_and_the_keys_not_their_product() {
    const KEYS: u32 = 3_000;
    const TRUNCATES: u32 = 300;
    for kind in [Kind::Hard, Kind::Soft] {
        let history = History::new(kind);
        let (soft, at) = match kind {
            Kind::Soft => ("deleted_at, ", "NULL, "),
            _ => ("", ""),
        };
        let filled = format!(
            "{} INSERT INTO orders (id, name, seq, {soft}valid_from, valid_to, is_current, \
             row_hash) SELECT i, 'a', {}, {at}10, NULL, 1, x'00' FROM _n",
            counting(KEYS),
            digits("1000000 + i")
        );
        history.connection.execute_batch(&filled).unwrap();
        let staging = history.planner.staging_table("orders");
        // One truncate in segments 1 and 3, many in 2 and 4; the first two before every version,
        // the last two past each.
        for (segment, count, first) in [
            (1, 1, 0),
            (2, TRUNCATES, 0),
            (3, 1, 2_000_000),
            (4, TRUNCATES, 2_000_000),
        ] {
            let deleted = if kind == Kind::Soft { "i, " } else { "" };
            let staged = format!(
                "{} INSERT INTO \"{staging}\" (_rdlt_pipeline, _rdlt_epoch, _rdlt_segment, seq, \
                 {soft}valid_from, is_current, op) SELECT 'mine', 1, {segment}, {}, {deleted}50, \
                 1, 3 FROM _n",
                counting(count),
                digits(&format!("{first} + i"))
            );
            history.connection.execute_batch(&staged).unwrap();
        }
        let columns = columns(&history.connection, &history.planner, "orders");
        let committing = |segment: u64| {
            let plan = history
                .planner
                .publish_as(
                    &history.staged(None),
                    &columns,
                    &pipeline("mine"),
                    Epoch(1),
                    &segments(&[segment]),
                )
                .unwrap();
            planned(&history.connection, &plan)
        };
        for (one, many) in [(1, 2), (3, 4)] {
            let (one, many) = (committing(one), committing(many));
            assert!(
                many < one * 6,
                "one truncate took {one} steps, {TRUNCATES} took {many}"
            );
        }
    }
}

#[test]
fn a_change_sent_as_of_before_its_key_s_latest_instant_begins_at_that_instant() {
    let history = History::new(Kind::Plain);
    history.commit(&[upsert(1, "a", 1, 30)]);
    // Sent as of before the version it replaces began, in a later commit and within one.
    let published = history.commit(&[
        upsert(1, "b", 2, 10),
        upsert(2, "c", 3, 50),
        upsert(2, "d", 4, 5),
        upsert(2, "c", 5, 5),
    ]);
    let expected = [
        closed(1, "a", 1, 30, 30),
        current(1, "b", 2, 30),
        closed(2, "c", 3, 50, 50),
        closed(2, "d", 4, 50, 50),
        current(2, "c", 5, 50),
    ];
    assert_eq!(published, sorted(expected.to_vec()));
}

#[test]
fn a_key_opened_again_after_a_delete_begins_no_earlier_than_its_deletion() {
    for kind in [Kind::Hard, Kind::Soft] {
        let history = History::new(kind);
        history.commit(&[upsert(1, "a", 1, 10)]);
        history.commit(&[delete(1, 2, 20)]);
        let published = history.commit(&[upsert(1, "b", 3, 15)]);
        for (_, _, _, _, from, to, _) in &published {
            assert!(to.is_none_or(|to| to >= *from), "{published:?}");
        }
        let (.., from, _, current) = published.last().unwrap();
        assert_eq!((*from, *current), (20, true), "{published:?}");
    }
}
