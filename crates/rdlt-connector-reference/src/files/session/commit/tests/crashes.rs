//! A process that dies at each step of what a session does, in turn: what the pipeline's next
//! session finds is what was there before or what the step was part of, whole, and the work
//! repeated lands once.

use std::sync::Arc;

use arrow_array::cast::AsArray as _;
use arrow_array::{ArrayRef, BinaryArray, Int8Array, Int64Array, RecordBatch, StringArray};
use rdlt_connector::{
    ChangeColumns, ChangeOp, ChildTable, CommitMeta, Deletion, Field, GenerationId, LogicalType,
    MergeKey, Result, RootKey, SchemaVersion, TablePath, TableRef, TableSchema,
};

use crate::files::FileFormat;
use crate::files::session::tests::Sessions;
use crate::files::{destination, manifest, tables};
use crate::rooted::trace;

fn table(name: &str, merge: Option<MergeKey>) -> TableRef {
    TableRef {
        path: TablePath::new([name]).unwrap(),
        name: name.into(),
        version: SchemaVersion(1),
        generation: None,
        merge,
    }
}

fn keyed(columns: &str, root: Option<RootKey>, changes: Option<ChangeColumns>) -> MergeKey {
    MergeKey {
        columns: vec![columns.into()],
        seq: "seq".into(),
        root,
        changes,
        history: None,
    }
}

/// Sixteen bytes that order as `byte` does.
fn seq(byte: u8) -> Vec<u8> {
    let mut bytes = vec![0; 16];
    bytes[15] = byte;
    bytes
}

fn seqs(bytes: impl Iterator<Item = u8>) -> ArrayRef {
    Arc::new(BinaryArray::from_iter_values(bytes.map(seq)))
}

fn ids(values: &[i64]) -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
    RecordBatch::try_from_iter([("id", ids)]).unwrap()
}

/// What the latest manifest lists, sorted.
fn listed(sessions: &Sessions) -> Vec<String> {
    let manifest = manifest::latest(&sessions.location.dir).unwrap().unwrap();
    let mut listed: Vec<String> = manifest.files().map(|file| file.path.clone()).collect();
    listed.sort();
    listed
}

/// The text of `column` in every row the latest manifest publishes for `table`, sorted.
fn texts(sessions: &Sessions, table: &str, column: &str) -> Vec<String> {
    let schema = tables::read(&sessions.location.rdlt, table)
        .unwrap()
        .unwrap();
    let schema = Arc::new(schema.to_arrow());
    let latest = manifest::latest(&sessions.location.dir).unwrap().unwrap();
    let mut found = Vec::new();
    for file in latest
        .tables
        .get(table)
        .map_or(&[][..], |table| &table.files)
    {
        for batch in manifest::read(&sessions.location.dir, &file.path, &schema).unwrap() {
            let texts = batch.column_by_name(column).unwrap().as_string::<i32>();
            found.extend(texts.iter().flatten().map(str::to_owned));
        }
    }
    found.sort();
    found
}

/// The ids the latest manifest publishes for the table `rows`, as text.
fn rows_published(sessions: &Sessions) -> Vec<String> {
    let ids = sessions.ids("rows");
    ids.iter().map(ToString::to_string).collect()
}

/// One commit to die in: the sessions it is made in, what it stages, and what it publishes.
struct Dying<'a> {
    /// Sessions whose earlier commits landed.
    prepared: &'a dyn Fn(FileFormat) -> Sessions,
    /// Stages the commit's segments.
    stage: &'a dyn Fn(&Sessions),
    /// The commit, for the session as it stands.
    meta: &'a dyn Fn(&Sessions) -> CommitMeta,
    /// What the pipeline publishes, to compare.
    published: &'a dyn Fn(&Sessions) -> Vec<String>,
}

/// Dies at each step of the commit in turn, in each format; returns the steps died at.
///
/// After each death the pipeline's next session finds what was published before the commit or
/// what the commit publishes, and the commit staged and made again lands, with nothing on disk
/// but what is listed.
fn dies_at_every_step(dying: &Dying<'_>, before: &[&str], after: &[&str]) -> usize {
    trace::without_syncs();
    let mut died = 0;
    for format in [FileFormat::Jsonl, FileFormat::Arrow] {
        for step in 0.. {
            let mut sessions = (dying.prepared)(format);
            (dying.stage)(&sessions);
            trace::crash_at(step);
            drop(sessions.commit(&(dying.meta)(&sessions)));
            let crashed = trace::refused();
            trace::clear();
            if !crashed {
                assert_eq!((dying.published)(&sessions), after, "{format:?}");
                break;
            }
            died += 1;
            sessions.open(2);
            let found = (dying.published)(&sessions);
            assert!(
                found == before || found == after,
                "{format:?} step {step}: {found:?}"
            );
            (dying.stage)(&sessions);
            sessions.commit(&(dying.meta)(&sessions)).unwrap();
            let found = (dying.published)(&sessions);
            assert_eq!(found, after, "{format:?} step {step}");
            assert_eq!(sessions.data_files(), listed(&sessions), "step {step}");
        }
    }
    died
}

#[test]
fn a_generation_s_finish_that_dies_at_any_step_replaces_the_table_whole_or_not_at_all() {
    let rows = table("rows", None);
    let filling = TableRef {
        generation: Some(GenerationId(1)),
        ..rows.clone()
    };
    let schema = TableSchema::new(vec![Field::new("id", LogicalType::Int64, false)]).unwrap();
    let died = dies_at_every_step(
        &Dying {
            prepared: &|format| {
                let sessions = Sessions::new(format);
                sessions.create(&rows, &schema);
                sessions.stage(&rows, 1, ids(&[1, 2]));
                sessions.commit(&sessions.meta(1, 1, &[1])).unwrap();
                sessions
            },
            stage: &|sessions| {
                sessions.stage(&filling, 2, ids(&[3, 4]));
                sessions.stage(&filling, 3, ids(&[5]));
            },
            meta: &|sessions| {
                let mut meta = sessions.meta(1, 2, &[2, 3]);
                meta.finish_generations = vec![(rows.path.clone(), GenerationId(1))];
                meta
            },
            published: &rows_published,
        },
        &["1", "2"],
        &["3", "4", "5"],
    );
    assert!(died > 12, "only {died} steps");
}

/// A root table merging by `id` and its child table `items`.
fn family() -> (TableRef, TableRef) {
    let root = RootKey {
        table: "roots".into(),
        id: "rid".into(),
        seq: "seq".into(),
    };
    (
        table("roots", Some(keyed("id", None, None))),
        table("items", Some(keyed("root", Some(root), None))),
    )
}

/// Root rows of `(id, seq byte)`, each with the root id its child rows name.
fn roots(rows: &[(u8, u8)]) -> RecordBatch {
    let ids = Int64Array::from_iter_values(rows.iter().map(|row| i64::from(row.0)));
    RecordBatch::try_from_iter([
        ("id", Arc::new(ids) as ArrayRef),
        ("rid", seqs(rows.iter().map(|row| row.0))),
        ("seq", seqs(rows.iter().map(|row| row.1))),
    ])
    .unwrap()
}

/// Child rows of `(value, root id, seq byte)`.
fn items(rows: &[(&str, u8, u8)]) -> RecordBatch {
    let values = StringArray::from_iter_values(rows.iter().map(|row| row.0));
    RecordBatch::try_from_iter([
        ("value", Arc::new(values) as ArrayRef),
        ("root", seqs(rows.iter().map(|row| row.1))),
        ("seq", seqs(rows.iter().map(|row| row.2))),
    ])
    .unwrap()
}

#[test]
fn a_child_table_s_rewrite_that_dies_at_any_step_lands_with_its_root_s_rows_or_not_at_all() {
    let (root_table, item_table) = family();
    let create = |sessions: &Sessions, table: &TableRef, batch: &RecordBatch| {
        sessions.create(table, &TableSchema::from_arrow(&batch.schema()).unwrap());
    };
    let meta = |sessions: &Sessions, seq| {
        let mut meta = sessions.meta(1, seq, &[seq]);
        meta.child_tables = vec![ChildTable {
            table: Arc::clone(&item_table.name),
            merge: item_table.merge.clone().unwrap(),
        }];
        meta
    };
    // A root's rows and its child rows are published by one manifest: both tables as they
    // were, or both as the commit leaves them.
    let published = |sessions: &Sessions| {
        let mut found = texts(sessions, "items", "value");
        let mut ids = sessions.ids("roots");
        ids.sort_unstable();
        found.extend(ids.iter().map(ToString::to_string));
        found
    };
    let died = dies_at_every_step(
        &Dying {
            prepared: &|format| {
                let sessions = Sessions::new(format);
                create(&sessions, &root_table, &roots(&[]));
                create(&sessions, &item_table, &items(&[]));
                sessions.stage(&root_table, 1, roots(&[(1, 1), (2, 2)]));
                sessions.stage(&item_table, 1, items(&[("a", 1, 1), ("b", 2, 2)]));
                sessions.commit(&meta(&sessions, 1)).unwrap();
                sessions
            },
            stage: &|sessions| {
                sessions.stage(&root_table, 2, roots(&[(1, 3), (3, 3)]));
                sessions.stage(&item_table, 2, items(&[("c", 1, 3), ("d", 3, 3)]));
            },
            meta: &|sessions| meta(sessions, 2),
            published: &published,
        },
        &["a", "b", "1", "2"],
        &["b", "c", "d", "1", "2", "3"],
    );
    assert!(died > 12, "only {died} steps");
}

/// Changes of `(id, seq byte, op)`, as a change stream writes them.
fn changes(rows: &[(i64, u8, ChangeOp)]) -> RecordBatch {
    let ids = Int64Array::from_iter_values(rows.iter().map(|row| row.0));
    let ops = Int8Array::from_iter_values(rows.iter().map(|row| row.2.code()));
    RecordBatch::try_from_iter([
        ("id", Arc::new(ids) as ArrayRef),
        ("seq", seqs(rows.iter().map(|row| row.1))),
        ("op", Arc::new(ops) as ArrayRef),
    ])
    .unwrap()
}

#[test]
fn a_delete_that_dies_at_any_step_lands_with_its_tombstone_or_not_at_all() {
    use ChangeOp::{Delete, Insert};
    let columns = ChangeColumns {
        op: "op".into(),
        unchanged: None,
        deletion: Deletion::Hard,
    };
    let rows = table("rows", Some(keyed("id", None, Some(columns))));
    // The table holds the rows' columns; what they do is written beside them.
    let schema = TableSchema::new(vec![
        Field::new("id", LogicalType::Int64, true),
        Field::new("seq", LogicalType::Binary, false),
    ])
    .unwrap();
    let prepared = |format| {
        let sessions = Sessions::new(format);
        sessions.create(&rows, &schema);
        sessions.stage(&rows, 1, changes(&[(1, 1, Insert), (2, 2, Insert)]));
        sessions.commit(&sessions.meta(1, 1, &[1])).unwrap();
        sessions
    };
    let died = dies_at_every_step(
        &Dying {
            prepared: &prepared,
            stage: &|sessions| {
                sessions.stage(&rows, 2, changes(&[(1, 5, Delete), (3, 6, Insert)]));
            },
            meta: &|sessions| sessions.meta(1, 2, &[2]),
            published: &rows_published,
        },
        &["1", "2"],
        &["2", "3"],
    );
    assert!(died > 12, "only {died} steps");
    // The tombstone the commit left holds: the deleted row's insert, sent again, changes nothing.
    let sessions = prepared(FileFormat::Jsonl);
    sessions.stage(&rows, 2, changes(&[(1, 5, Delete), (3, 6, Insert)]));
    sessions.commit(&sessions.meta(1, 2, &[2])).unwrap();
    sessions.stage(&rows, 3, changes(&[(1, 1, Insert)]));
    sessions.commit(&sessions.meta(1, 3, &[3])).unwrap();
    assert_eq!(rows_published(&sessions), ["2", "3"]);
}

/// Dies at each step of `work` in turn, against sessions `prepared` anew each time; returns
/// the steps died at.
///
/// After each death `survived` checks what the pipeline's next session finds, and the work made
/// again succeeds.
fn work_dies_at_every_step(
    prepared: &dyn Fn() -> Sessions,
    work: &dyn Fn(&Sessions) -> Result<()>,
    survived: &dyn Fn(&mut Sessions, usize),
) -> usize {
    trace::without_syncs();
    for step in 0.. {
        let mut sessions = prepared();
        trace::crash_at(step);
        drop(work(&sessions));
        let crashed = trace::refused();
        trace::clear();
        if !crashed {
            return step;
        }
        survived(&mut sessions, step);
        work(&sessions).expect("the work made again succeeds");
    }
    unreachable!("the work takes a bounded number of steps")
}

/// Sessions whose first commit published ids 1 and 2, and whose second, of id 3, was staged by
/// a session that then went away without committing it.
fn abandoned() -> Sessions {
    let sessions = Sessions::new(FileFormat::Jsonl);
    let rows = table("rows", None);
    let schema = TableSchema::new(vec![Field::new("id", LogicalType::Int64, false)]).unwrap();
    sessions.create(&rows, &schema);
    sessions.stage(&rows, 1, ids(&[1, 2]));
    sessions.commit(&sessions.meta(1, 1, &[1])).unwrap();
    sessions.stage(&rows, 2, ids(&[3]));
    sessions
}

#[test]
fn an_open_that_dies_at_any_step_leaves_every_published_row_and_the_next_open_cleans_up() {
    let open = |sessions: &Sessions| {
        let (dir, rdlt) = (&sessions.location.dir, &sessions.location.rdlt);
        let wait = sessions.location.lock_wait;
        let opened = destination::next_epoch(dir, rdlt, &sessions.location.pipeline, wait)?;
        destination::discard(dir, opened.epoch)
    };
    let died = work_dies_at_every_step(&abandoned, &open, &|sessions, step| {
        assert_eq!(sessions.ids("rows"), [1, 2], "step {step}");
        // The next open, whole, discards what the abandoned session staged.
        sessions.open(2);
        assert_eq!(sessions.ids("rows"), [1, 2], "step {step}");
        assert_eq!(sessions.data_files(), listed(sessions), "step {step}");
    });
    assert!(died > 3, "only {died} steps");
}

#[test]
fn a_schema_change_that_dies_at_any_step_leaves_the_columns_as_they_were_or_as_changed() {
    let narrow = TableSchema::new(vec![Field::new("id", LogicalType::Int64, false)]).unwrap();
    let wide = TableSchema::new(vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("more", LogicalType::Int64, true),
    ])
    .unwrap();
    let change = |name: &'static str, schema: &TableSchema| {
        let schema = schema.clone();
        move |sessions: &Sessions| {
            let (rdlt, wait) = (&sessions.location.rdlt, sessions.location.lock_wait);
            tables::locked(rdlt, name, wait, || {
                tables::update(rdlt, name, |_| Ok(Some(schema.clone())))
            })
        }
    };
    // A column added to a table that holds rows.
    let died = work_dies_at_every_step(&abandoned, &change("rows", &wide), &|sessions, step| {
        let found = tables::read(&sessions.location.rdlt, "rows").unwrap();
        assert!(
            found.as_ref() == Some(&narrow) || found.as_ref() == Some(&wide),
            "step {step}: {found:?}"
        );
        assert_eq!(sessions.ids("rows"), [1, 2], "step {step}");
    });
    assert!(died > 3, "only {died} steps");
    // A table created.
    let died = work_dies_at_every_step(&abandoned, &change("other", &narrow), &|sessions, step| {
        let found = tables::read(&sessions.location.rdlt, "other").unwrap();
        assert!(
            found.is_none() || found.as_ref() == Some(&narrow),
            "step {step}"
        );
        assert_eq!(sessions.ids("rows"), [1, 2], "step {step}");
    });
    assert!(died > 3, "only {died} steps");
}
