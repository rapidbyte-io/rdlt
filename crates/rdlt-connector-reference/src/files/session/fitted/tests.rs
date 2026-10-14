use std::sync::Arc;
use std::sync::mpsc::channel;
use std::time::Duration;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, TimestampSecondArray};
use rdlt_connector::{
    Field, LogicalType, MergeKey, SchemaVersion, Session as _, TableChange, TablePath, TableRef,
    TableSchema, TimeUnit,
};

use super::{Checked, TABLE_CHANGED, fits, gains_json};
use crate::files::session::FilesSession;
use crate::files::session::tests::Sessions;
use crate::files::{FileFormat, tables};

fn table() -> TableRef {
    TableRef {
        path: TablePath::new(["rows"]).unwrap(),
        name: "rows".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: Some(MergeKey {
            columns: vec!["id".into()],
            seq: "seq".into(),
            root: None,
            changes: None,
            history: None,
        }),
    }
}

fn seconds() -> LogicalType {
    LogicalType::Timestamp(TimeUnit::Second, None)
}

fn schema(held: LogicalType) -> TableSchema {
    TableSchema::new(vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("held", held, true),
        Field::new("seq", LogicalType::Int64, false),
    ])
    .unwrap()
}

/// One row whose `held` is `seconds`.
fn row(id: i64, seconds: i64) -> RecordBatch {
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![id])) as ArrayRef),
        (
            "held",
            Arc::new(TimestampSecondArray::from(vec![seconds])) as ArrayRef,
        ),
        ("seq", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
    ])
    .unwrap()
}

fn widen() -> TableChange {
    TableChange::Widen {
        table: table(),
        column: "held".into(),
        from: seconds(),
        to: LogicalType::Timestamp(TimeUnit::Nanosecond, None),
    }
}

/// Sessions over a table that publishes one row whose `held` is `seconds`.
fn publishing(seconds_held: i64) -> Sessions {
    let sessions = Sessions::new(FileFormat::Arrow);
    sessions.create(&table(), &schema(seconds()));
    sessions.stage(&table(), 1, row(1, seconds_held));
    sessions.commit(&sessions.meta(1, 1, &[1])).unwrap();
    sessions
}

/// The last day of the year 9999 in seconds, which nanoseconds do not reach.
const BEYOND: i64 = 253_402_214_400;

#[tokio::test(flavor = "multi_thread")]
async fn a_table_s_rows_are_read_before_its_lock_is_taken_and_a_change_is_taken_under_it() {
    for (held, code) in [(BEYOND, "schema_conflict"), (5, "lock_timeout")] {
        let mut sessions = publishing(held);
        sessions.location.lock_wait = Duration::from_millis(50);
        let (release, released) = channel::<()>();
        let (taken, took) = channel::<()>();
        let rdlt = Arc::clone(&sessions.location.rdlt);
        let holder = std::thread::spawn(move || {
            tables::locked(&rdlt, "rows", Duration::from_secs(5), || {
                taken.send(()).unwrap();
                released.recv().unwrap();
                Ok(())
            })
            .unwrap();
        });
        took.recv().unwrap();
        // Another holds the table's lock: the rows are checked without it, so a change they do
        // not fit is refused, and one they fit waits for the lock to be taken.
        let mut session = FilesSession::new(sessions.location.clone());
        let refused = session.apply_schema(&widen()).await.unwrap_err();
        assert_eq!(refused.code(), Some(code), "{refused}");
        release.send(()).unwrap();
        holder.join().unwrap();
        let after = session.apply_schema(&widen()).await;
        assert_eq!(after.is_ok(), held != BEYOND);
    }
}

#[test]
fn a_change_checked_against_a_table_that_changed_since_is_not_taken() {
    let sessions = publishing(5);
    let (current, next) = (schema(seconds()), schema(LogicalType::Int64));
    let stands = |checked: &Checked| {
        let asked = ("rows", &current, &next);
        checked.stands(&sessions.location, &sessions.shared, asked)
    };
    let checked = fits(&sessions.location, &sessions.shared, &widen()).unwrap();
    stands(&checked).unwrap();
    // A change of no column's type needs no check.
    let unchecked = Checked::default();
    let same = ("rows", &current, &current);
    unchecked
        .stands(&sessions.location, &sessions.shared, same)
        .unwrap();
    assert_eq!(stands(&unchecked).unwrap_err().code(), Some(TABLE_CHANGED));
    // A file staged since, and a manifest published since.
    sessions.stage(&table(), 2, row(2, 5));
    assert_eq!(stands(&checked).unwrap_err().code(), Some(TABLE_CHANGED));
    let checked = fits(&sessions.location, &sessions.shared, &widen()).unwrap();
    stands(&checked).unwrap();
    sessions.commit(&sessions.meta(1, 2, &[2])).unwrap();
    let changed = stands(&checked).unwrap_err();
    assert_eq!(changed.code(), Some(TABLE_CHANGED));
    assert_eq!(
        changed.kind(),
        rdlt_connector::ConnectorErrorKind::Transient
    );
    // The schema itself.
    let checked = fits(&sessions.location, &sessions.shared, &widen()).unwrap();
    let other = ("rows", &next, &current);
    let moved = checked.stands(&sessions.location, &sessions.shared, other);
    assert_eq!(moved.unwrap_err().code(), Some(TABLE_CHANGED));
}

#[test]
fn a_type_gains_json_where_it_or_a_field_or_item_within_it_becomes_json() {
    use LogicalType as T;
    let one = |name: &str, logical: T| {
        T::Struct(vec![Field::new(name, logical, true)].try_into().unwrap())
    };
    let list = |logical: T| T::List(Box::new(Field::new("item", logical, true)));
    assert!(gains_json(&T::Bool, &T::Json));
    assert!(!gains_json(&T::Json, &T::Json));
    assert!(!gains_json(&T::Int32, &T::Int64));
    assert!(gains_json(&one("y", T::Bool), &one("y", T::Json)));
    assert!(!gains_json(&one("y", T::Json), &one("y", T::Json)));
    assert!(!gains_json(&one("y", T::Bool), &one("z", T::Json)));
    assert!(gains_json(&list(T::Int64), &list(T::Json)));
    assert!(!gains_json(&list(T::Int32), &list(T::Int64)));
}

#[test]
fn a_widen_to_json_of_a_table_of_lines_checked_empty_is_not_taken_once_it_holds_rows() {
    let sessions = Sessions::new(FileFormat::Jsonl);
    sessions.create(&table(), &schema(LogicalType::Utf8));
    let into_json = TableChange::Widen {
        table: table(),
        column: "held".into(),
        from: LogicalType::Utf8,
        to: LogicalType::Json,
    };
    let checked = fits(&sessions.location, &sessions.shared, &into_json).unwrap();
    let (current, next) = (schema(LogicalType::Utf8), schema(LogicalType::Json));
    let asked = ("rows", &current, &next);
    checked
        .stands(&sessions.location, &sessions.shared, asked)
        .unwrap();
    // Rows published between the check and the lock: lines hold no type, so they are read again.
    let text: ArrayRef = Arc::new(arrow_array::StringArray::from(vec!["x"]));
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let rows = RecordBatch::try_from_iter([("id", Arc::clone(&ids)), ("held", text), ("seq", ids)])
        .unwrap();
    sessions.stage(&table(), 1, rows);
    sessions.commit(&sessions.meta(1, 1, &[1])).unwrap();
    let moved = checked.stands(&sessions.location, &sessions.shared, asked);
    assert_eq!(moved.unwrap_err().code(), Some(TABLE_CHANGED));
}
