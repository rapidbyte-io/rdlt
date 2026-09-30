//! The clauses for change streams' tables: deletes, partial updates, truncates, and changes that
//! apply only past the row their key holds, sent again or not.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, BinaryArray, Int8Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::DataType;

use super::evolving::GENERATION;
use super::{Bench, commit, meta};
use crate::change::{ChangeOp, OP_COLUMN, SEQ_COLUMN, UNCHANGED_COLUMN};
use crate::commit::CommitMeta;
use crate::destination::{ChangeColumns, Deletion, MergeKey, TableChange, TableRef};
use crate::id::SegmentId;
use crate::meta::DELETED_AT_COLUMN;
use crate::schema::TableSchema;
use crate::testing::{Violation, bounded_call};
use crate::types::{Field, LogicalType};

/// One change a stream writes: its op, key, name, sequence, deletion time, and whether it leaves
/// the name unchanged.
#[derive(Clone, Copy)]
struct Change {
    op: ChangeOp,
    id: Option<i64>,
    name: Option<&'static str>,
    seq: u8,
    at: Option<i64>,
    partial: bool,
}

fn upsert(id: i64, name: &'static str, seq: u8) -> Change {
    Change {
        op: ChangeOp::Update,
        id: Some(id),
        name: Some(name),
        seq,
        at: None,
        partial: false,
    }
}

/// An update of `id` leaving its name as it is.
fn partial(id: i64, seq: u8) -> Change {
    Change {
        name: None,
        partial: true,
        ..upsert(id, "", seq)
    }
}

fn delete(id: i64, seq: u8, at: i64) -> Change {
    Change {
        op: ChangeOp::Delete,
        at: Some(at),
        ..upsert(id, "", seq)
    }
}

fn truncate(seq: u8, at: i64) -> Change {
    Change {
        op: ChangeOp::Truncate,
        id: None,
        ..delete(0, seq, at)
    }
}

/// A published row of a change stream's table: its id, name, and deletion time where deletes
/// are soft and it was deleted.
type Marked = (i64, Option<String>, Option<i64>);

fn live(id: i64, name: &str) -> Marked {
    (id, Some(name.to_owned()), None)
}

/// `changes` as a change stream writes them: the key, the name, the sequence, where deletes are
/// `soft` the deletion time, then the op and the unchanged flags, bit 1 the name.
fn written(changes: &[Change], soft: bool) -> RecordBatch {
    let mut columns: Vec<(&str, ArrayRef)> = vec![
        (
            "id",
            Arc::new(changes.iter().map(|c| c.id).collect::<Int64Array>()),
        ),
        (
            "name",
            Arc::new(changes.iter().map(|c| c.name).collect::<StringArray>()),
        ),
        (
            SEQ_COLUMN,
            Arc::new(BinaryArray::from_iter_values(changes.iter().map(
                |change| {
                    let mut seq = [0_u8; 16];
                    seq[15] = change.seq;
                    seq
                },
            ))),
        ),
    ];
    if soft {
        let at: Int64Array = changes.iter().map(|change| change.at).collect();
        columns.push((DELETED_AT_COLUMN, Arc::new(at)));
    }
    let ops = Int8Array::from_iter_values(changes.iter().map(|change| change.op.code()));
    columns.push((OP_COLUMN, Arc::new(ops)));
    let flags: BinaryArray = changes
        .iter()
        .map(|change| change.partial.then_some(&[0b10_u8][..]))
        .collect();
    columns.push((UNCHANGED_COLUMN, Arc::new(flags)));
    RecordBatch::try_from_iter(columns).expect("the certification batch is valid")
}

/// The stored columns of a change stream's table: its key, name and sequence, and where deletes
/// are `soft`, the deletion time.
fn stored(soft: bool) -> TableSchema {
    let mut fields = vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("name", LogicalType::Utf8, true),
        Field::new(SEQ_COLUMN, LogicalType::Binary, false),
    ];
    if soft {
        fields.push(Field::new(DELETED_AT_COLUMN, LogicalType::Int64, true));
    }
    TableSchema::new(fields).expect("the certification schema is valid")
}

impl Bench<'_> {
    /// A table of this clause, called `suffix` after its own, merging a change stream whose
    /// deletes and truncates are `soft` or hard.
    fn changed_table(&self, suffix: &str, soft: bool) -> TableRef {
        let deletion = if soft {
            Deletion::Soft {
                at: DELETED_AT_COLUMN.into(),
            }
        } else {
            Deletion::Hard
        };
        TableRef {
            merge: Some(MergeKey {
                columns: vec!["id".into()],
                seq: SEQ_COLUMN.into(),
                root: None,
                changes: Some(ChangeColumns {
                    op: OP_COLUMN.into(),
                    unchanged: Some(UNCHANGED_COLUMN.into()),
                    deletion,
                }),
                history: None,
            }),
            ..self.other_table(suffix)
        }
    }

    /// Commits each of `commits` in turn to `table`, each in a session of its own, as a load
    /// started again after each commit is, and returns what the table publishes after each; the
    /// sessions' loads are numbered from `load` times sixteen.
    async fn changed(
        &self,
        (table, load): (&TableRef, u8),
        soft: bool,
        commits: &[&[Change]],
    ) -> Result<Vec<Vec<Marked>>, Violation> {
        let mut published = Vec::with_capacity(commits.len());
        for (index, changes) in commits.iter().enumerate() {
            let load = load * 16 + u8::try_from(index).unwrap_or(0);
            let mut opened = self.open(self.destination, load).await?;
            let create = TableChange::Create {
                table: table.clone(),
                schema: stored(soft),
            };
            bounded_call("apply_schema", opened.session.apply_schema(&create)).await?;
            let mut writer = bounded_call("writer", opened.session.writer(table)).await?;
            bounded_call("write", writer.write(SegmentId(1), written(changes, soft))).await?;
            bounded_call("flush", writer.flush()).await?;
            let committing = meta(self.load_id(load), opened.epoch, &[1], Vec::new());
            commit(&mut opened.session, &committing).await?;
            drop(writer);
            bounded_call("close", opened.session.close()).await?;
            published.push(self.marked(table).await?);
        }
        Ok(published)
    }

    /// Replaces `table`'s rows whole with a generation holding `rows`, in a session of load
    /// `load`.
    async fn replaced(&self, table: &TableRef, load: u8, rows: &[Change]) -> Result<(), Violation> {
        let mut opened = self.open(self.destination, load).await?;
        let generation = TableRef {
            generation: Some(GENERATION),
            merge: None,
            ..table.clone()
        };
        let batch = written(rows, false);
        let batch = batch
            .project(&[0, 1, 2])
            .expect("the stored columns come first");
        let mut writer = bounded_call("writer", opened.session.writer(&generation)).await?;
        bounded_call("write", writer.write(SegmentId(1), batch)).await?;
        bounded_call("flush", writer.flush()).await?;
        let finishing = CommitMeta {
            finish_generations: vec![(table.path.clone(), GENERATION)],
            ..meta(self.load_id(load), opened.epoch, &[1], Vec::new())
        };
        commit(&mut opened.session, &finishing).await?;
        drop(writer);
        bounded_call("close", opened.session.close()).await?;
        Ok(())
    }

    /// The rows `table` publishes, in order, with their deletion times.
    async fn marked(&self, table: &TableRef) -> Result<Vec<Marked>, Violation> {
        let batches = bounded_call("probe", self.probe.published(table)).await?;
        let mut rows = Vec::new();
        for batch in &batches {
            let column = |name: &str, logical: &DataType| {
                batch
                    .column_by_name(name)
                    .map(|column| arrow_cast::cast(column, logical))
                    .transpose()
                    .map_err(|error| Violation::from(format!("the {name}s read back as {error}")))
            };
            let (Some(ids), Some(names)) = (
                column("id", &DataType::Int64)?,
                column("name", &DataType::Utf8)?,
            ) else {
                return Err("a published batch lacks its id or name column".into());
            };
            let at = column(DELETED_AT_COLUMN, &DataType::Int64)?;
            let (ids, names) = (ids.as_primitive::<Int64Type>(), names.as_string::<i32>());
            let at = at.as_ref().map(AsArray::as_primitive::<Int64Type>);
            for row in 0..batch.num_rows() {
                let name = names.is_valid(row).then(|| names.value(row).to_owned());
                let deleted = at.and_then(|at| at.is_valid(row).then(|| at.value(row)));
                rows.push((ids.value(row), name, deleted));
            }
        }
        rows.sort_unstable();
        Ok(rows)
    }

    /// `D-DELETE`: a hard delete removes its key's row, and a soft one marks it deleted when the
    /// delete says, keeping its values; a change sent again from before the delete brings back no
    /// row and clears no mark, while one after it does.
    pub(super) async fn deletes_remove_rows(&self) -> Result<(), Violation> {
        let modes = self.destination.capabilities().delete_modes;
        if modes.hard {
            let table = self.changed_table("hard", false);
            let commits: [&[Change]; 4] = [
                &[upsert(1, "a", 1), upsert(2, "b", 2), upsert(3, "c", 3)],
                // A key no row holds is deleted too: its insert may still arrive, sent again.
                &[delete(2, 4, 40), delete(9, 5, 50), upsert(3, "d", 6)],
                &[upsert(2, "b", 2), upsert(9, "z", 3), upsert(3, "c", 3)],
                &[upsert(2, "again", 7)],
            ];
            let published = self.changed((&table, 2), false, &commits).await?;
            expect(&published, 2, &[live(1, "a"), live(3, "d")], "hard deletes")?;
            let again = [live(1, "a"), live(2, "again"), live(3, "d")];
            expect(&published, 3, &again, "hard deletes")?;
            if self.destination.capabilities().write_modes.replace {
                self.replacing_forgets_tombstones().await?;
            }
        }
        if modes.soft {
            let table = self.changed_table("soft", true);
            let commits: [&[Change]; 4] = [
                &[upsert(1, "a", 1), upsert(2, "b", 2)],
                &[delete(2, 4, 40)],
                // A later delete keeps when the row was deleted.
                &[upsert(2, "b", 2), delete(2, 5, 50)],
                &[upsert(2, "back", 6)],
            ];
            let published = self.changed((&table, 3), true, &commits).await?;
            let deleted = [live(1, "a"), (2, Some("b".to_owned()), Some(40))];
            expect(&published, 2, &deleted, "soft deletes")?;
            expect(
                &published,
                3,
                &[live(1, "a"), live(2, "back")],
                "soft deletes",
            )?;
        }
        Ok(())
    }

    /// A table a generation replaces whole forgets the tombstones of the rows it held: a key a
    /// hard delete removed takes a change sequenced before the delete again.
    async fn replacing_forgets_tombstones(&self) -> Result<(), Violation> {
        let table = self.changed_table("replaced", false);
        let deleted: [&[Change]; 1] = [&[upsert(1, "a", 1), delete(1, 5, 50)]];
        self.changed((&table, 4), false, &deleted).await?;
        self.replaced(&table, 80, &[upsert(9, "i", 4)]).await?;
        let again: [&[Change]; 1] = [&[upsert(1, "again", 2)]];
        let published = self.changed((&table, 6), false, &again).await?;
        let expected = [live(1, "again"), live(9, "i")];
        expect(&published, 0, &expected, "a table replaced whole")
    }

    /// `D-PARTIAL`: an update flagging a column unchanged keeps its published value, or leaves it
    /// null where no row held the key.
    pub(super) async fn partial_updates_keep_columns(&self) -> Result<(), Violation> {
        // Every destination merging changes takes a table whose deletes remove rows, where none
        // do: a stream ignoring its deletes and truncates writes one.
        let table = self.changed_table("partial", false);
        let commits: [&[Change]; 2] = [&[upsert(1, "a", 1)], &[partial(1, 2), partial(5, 3)]];
        let published = self.changed((&table, 2), false, &commits).await?;
        expect(
            &published,
            1,
            &[live(1, "a"), (5, None, None)],
            "partial updates",
        )
    }

    /// `D-TRUNCATE`: a truncate removes, or marks deleted, every row sequenced before it, those
    /// its own commit applied too, and none after, those an earlier commit published too; a change
    /// sent again from before it brings back no row.
    pub(super) async fn truncates_remove_earlier_rows(&self) -> Result<(), Violation> {
        let modes = self.destination.capabilities().delete_modes;
        if modes.hard {
            let table = self.changed_table("hard", false);
            let commits: [&[Change]; 4] = [
                &[upsert(1, "a", 1), upsert(2, "b", 2)],
                &[upsert(6, "f", 6)],
                &[upsert(3, "c", 3), truncate(4, 40), upsert(5, "e", 5)],
                // Changes from before the truncate stay out; one after it lands.
                &[upsert(1, "a", 1), upsert(3, "c", 3), upsert(7, "g", 7)],
            ];
            let published = self.changed((&table, 2), false, &commits).await?;
            expect(
                &published,
                2,
                &[live(5, "e"), live(6, "f")],
                "hard truncates",
            )?;
            let later = [live(5, "e"), live(6, "f"), live(7, "g")];
            expect(&published, 3, &later, "hard truncates")?;
        }
        if modes.soft {
            let table = self.changed_table("soft", true);
            let commits: [&[Change]; 4] = [
                &[upsert(1, "a", 1), upsert(2, "b", 2)],
                &[upsert(5, "e", 6)],
                &[upsert(3, "c", 3), truncate(4, 40), upsert(4, "d", 5)],
                &[upsert(1, "a", 1)],
            ];
            let published = self.changed((&table, 3), true, &commits).await?;
            let marked = [
                (1, Some("a".to_owned()), Some(40)),
                (2, Some("b".to_owned()), Some(40)),
                (3, Some("c".to_owned()), Some(40)),
                live(4, "d"),
                live(5, "e"),
            ];
            expect(&published, 2, &marked, "soft truncates")?;
            expect(&published, 3, &marked, "soft truncates")?;
        }
        Ok(())
    }

    /// `D-MERGE`: a merge keeps the newest row of each key, and where the destination merges
    /// change streams, a change applies only past the row its key holds.
    pub(super) async fn merges_keep_newest_rows(&self) -> Result<(), Violation> {
        self.merges_keep_the_newest_row().await?;
        if self.destination.capabilities().merge_changes {
            self.changes_apply_past_their_row().await?;
        }
        Ok(())
    }

    /// `D-MERGE`, for a change stream's table: a change applies only past the sequence of the row
    /// its key holds, across commits, and one sent twice in a commit lands once.
    pub(super) async fn changes_apply_past_their_row(&self) -> Result<(), Violation> {
        let table = self.changed_table("changes", false);
        let commits: [&[Change]; 2] = [
            &[upsert(1, "new", 5)],
            &[upsert(1, "old", 3), upsert(2, "b", 4), upsert(2, "b", 4)],
        ];
        let published = self.changed((&table, 2), false, &commits).await?;
        expect(&published, 1, &[live(1, "new"), live(2, "b")], "changes")
    }
}

/// Whether the table held `expected` after commit `index` of `published`.
fn expect(
    published: &[Vec<Marked>],
    index: usize,
    expected: &[Marked],
    what: &str,
) -> Result<(), Violation> {
    let actual = published.get(index).map_or(&[][..], Vec::as_slice);
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "{what}: after commit {} the table holds {actual:?}, expected {expected:?}",
            index + 1
        )
        .into())
    }
}
