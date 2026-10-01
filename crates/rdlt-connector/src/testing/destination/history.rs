//! `D-HIST`: history tables keep every version of each key, closing a version when a change
//! replaces or removes it and skipping a change that leaves its data as it is.

mod rows;

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_schema::DataType;

use super::{Bench, commit, meta};
use crate::change::{OP_COLUMN, SEQ_COLUMN};
use crate::destination::{
    ChangeColumns, Deletion, HistoryColumns, MergeKey, TableChange, TableRef,
};
use crate::id::SegmentId;
use crate::meta::{
    DELETED_AT_COLUMN, IS_CURRENT_COLUMN, ROW_HASH_COLUMN, VALID_FROM_COLUMN, VALID_TO_COLUMN,
};
use crate::testing::reason::Listed;
use crate::testing::{Violation, bounded_call};
use rows::{
    Kind, Row, Version, changed_commits, closed, current, delete, deleted, hash, stored, truncate,
    upsert, written,
};

impl Bench<'_> {
    /// A history table of this clause, called `suffix` after its own, written as `kind` says.
    fn history_table(&self, suffix: &str, kind: Kind) -> TableRef {
        let changes = match kind {
            Kind::Plain => None,
            Kind::Changes { soft } => Some(ChangeColumns {
                op: OP_COLUMN.into(),
                unchanged: None,
                deletion: if soft {
                    Deletion::Soft {
                        at: DELETED_AT_COLUMN.into(),
                    }
                } else {
                    Deletion::Hard
                },
            }),
        };
        TableRef {
            merge: Some(MergeKey {
                columns: vec!["id".into()],
                seq: SEQ_COLUMN.into(),
                root: None,
                changes,
                history: Some(HistoryColumns {
                    valid_from: VALID_FROM_COLUMN.into(),
                    valid_to: VALID_TO_COLUMN.into(),
                    is_current: IS_CURRENT_COLUMN.into(),
                    row_hash: ROW_HASH_COLUMN.into(),
                }),
            }),
            ..self.other_table(suffix)
        }
    }

    /// Commits each of `commits` in turn to a history table of `kind`, each in a session of its
    /// own, loads numbered from `load` times sixteen, and returns the versions it publishes after
    /// each.
    async fn versioned(
        &self,
        (suffix, load): (&str, u8),
        kind: Kind,
        commits: &[&[Row]],
    ) -> Result<Vec<Vec<Version>>, Violation> {
        let table = self.history_table(suffix, kind);
        let mut published = Vec::with_capacity(commits.len());
        for (index, rows) in commits.iter().enumerate() {
            let load = load * 16 + u8::try_from(index).unwrap_or(0);
            let mut opened = self.open(self.destination, load).await?;
            let create = TableChange::Create {
                table: table.clone(),
                schema: stored(kind),
            };
            bounded_call("apply_schema", opened.session.apply_schema(&create)).await?;
            let mut writer = bounded_call("writer", opened.session.writer(&table)).await?;
            bounded_call("write", writer.write(SegmentId(1), written(rows, kind))).await?;
            bounded_call("flush", writer.flush()).await?;
            let committing = meta(self.load_id(load), opened.epoch, &[1], Vec::new());
            commit(&mut opened.session, &committing).await?;
            drop(writer);
            bounded_call("close", opened.session.close()).await?;
            published.push(self.versions(&table).await?);
        }
        Ok(published)
    }

    /// The versions `table` publishes, sorted.
    async fn versions(&self, table: &TableRef) -> Result<Vec<Version>, Violation> {
        let published = self.read(table).await?;
        let mut versions = Vec::with_capacity(published.rows());
        for batch in published.batches() {
            // A history's key, start, current flag and sequence hold no null.
            let ids = batch.required("id", &DataType::Int64)?;
            let names = batch.nullable("name", &DataType::Utf8)?;
            let from = batch.required(VALID_FROM_COLUMN, &DataType::Int64)?;
            let to = batch.nullable(VALID_TO_COLUMN, &DataType::Int64)?;
            let current = batch.required(IS_CURRENT_COLUMN, &DataType::Boolean)?;
            let seqs = batch.required(SEQ_COLUMN, &DataType::Binary)?;
            let hashes = batch.nullable(ROW_HASH_COLUMN, &DataType::Binary)?;
            let (seqs, hashes) = (seqs.as_binary::<i32>(), hashes.as_binary::<i32>());
            let at = batch.optional(DELETED_AT_COLUMN, &DataType::Int64)?;
            let at = at.as_ref().map(AsArray::as_primitive::<Int64Type>);
            let (ids, names) = (ids.as_primitive::<Int64Type>(), names.as_string::<i32>());
            let (from, to) = (
                from.as_primitive::<Int64Type>(),
                to.as_primitive::<Int64Type>(),
            );
            let current = current.as_boolean();
            for row in 0..batch.rows() {
                let valid = |array: &dyn Array| array.is_valid(row);
                let name = valid(names).then(|| names.value(row).to_owned());
                let written = name.as_deref().map(hash);
                versions.push(Version {
                    id: ids.value(row),
                    from: from.value(row),
                    deleted: at.and_then(|at| valid(at).then(|| at.value(row))),
                    to: valid(to).then(|| to.value(row)),
                    current: current.value(row),
                    seq: seqs.value(row).last().copied().unwrap_or_default(),
                    hashed: valid(hashes) && written.is_some_and(|hash| hashes.value(row) == hash),
                    name,
                });
            }
        }
        versions.sort_unstable();
        Ok(versions)
    }

    /// `D-HIST`: a history table keeps each key's versions as the changes it is sent make them.
    pub(super) async fn histories_chain_versions(&self) -> Result<(), Violation> {
        self.plain_histories_chain().await?;
        let capabilities = self.destination.capabilities();
        if capabilities.merge_changes {
            if capabilities.delete_modes.hard {
                self.changed_histories_chain().await?;
            }
            if capabilities.delete_modes.soft {
                self.soft_histories_chain().await?;
            }
        }
        Ok(())
    }

    /// Rows that are all upserts version their keys in sequence order within a commit, and in
    /// commit order across commits; a row equal to its key's current version changes nothing.
    async fn plain_histories_chain(&self) -> Result<(), Violation> {
        let commits: [&[Row]; 2] = [
            &[
                upsert(1, "y", 2, 11),
                upsert(2, "b", 3, 12),
                upsert(1, "x", 1, 10),
            ],
            &[
                upsert(1, "y", 1, 20),
                upsert(2, "c", 2, 21),
                upsert(3, "d", 3, 22),
            ],
        ];
        let published = self.versioned(("plain", 2), Kind::Plain, &commits).await?;
        let (x, y) = (closed(1, "x", 10, 11, 1), current(1, "y", 11, 2));
        expect(
            &published,
            0,
            &[x.clone(), y.clone(), current(2, "b", 12, 3)],
            "a history of upserts",
        )?;
        let second = [
            x,
            y,
            closed(2, "b", 12, 21, 3),
            current(2, "c", 21, 2),
            current(3, "d", 22, 3),
        ];
        expect(&published, 1, &second, "a history of upserts")
    }

    /// A change stream's history: equal changes change nothing, deletes close their key's version
    /// and a later insert opens another, a change applies only past its key's newest version, its
    /// tombstone and the bound, and a truncate closes every version sequenced before it, those its
    /// own commit opened too.
    async fn changed_histories_chain(&self) -> Result<(), Violation> {
        let commits = changed_commits();
        let commits: Vec<&[Row]> = commits.iter().map(Vec::as_slice).collect();
        let published = self
            .versioned(("changes", 3), Kind::Changes { soft: false }, &commits)
            .await?;
        let what = "a change stream's history";
        let a = closed(1, "a", 10, 40, 1);
        expect(
            &published,
            0,
            &[a.clone(), current(1, "a2", 40, 4), current(2, "b", 20, 2)],
            what,
        )?;
        let (b, c) = (closed(2, "b", 20, 60, 2), closed(3, "c", 70, 80, 7));
        let second = [
            a.clone(),
            current(1, "a2", 40, 4),
            b.clone(),
            c.clone(),
            current(3, "c", 90, 9),
        ];
        expect(&published, 1, &second, what)?;
        let mut third = second.to_vec();
        third.push(current(2, "back", 100, 11));
        expect(&published, 2, &third, what)?;
        let fourth = [
            a,
            closed(1, "a2", 40, 110, 4),
            b,
            closed(2, "back", 100, 110, 11),
            c,
            closed(3, "c", 90, 110, 9),
            current(4, "d", 120, 14),
            closed(5, "e", 105, 110, 12),
        ];
        expect(&published, 3, &fourth, what)?;
        let mut fifth = fourth.to_vec();
        fifth.push(current(1, "a3", 130, 15));
        expect(&published, 4, &fifth, what)
    }

    /// A change stream's history with soft deletes: a delete closes its key's version and opens a
    /// deleted one keeping its data, at the delete's sequence, so no change sent again from before
    /// the delete applies; an equal insert closes it again; a delete of a deleted or missing key
    /// changes nothing, and a truncate deletes every version sequenced before it.
    async fn soft_histories_chain(&self) -> Result<(), Violation> {
        let commits: [&[Row]; 5] = [
            &[upsert(1, "a", 1, 10), upsert(2, "b", 2, 20)],
            &[delete(1, 3, 30), delete(1, 4, 40), delete(9, 5, 50)],
            &[upsert(1, "mid", 2, 25)],
            &[upsert(1, "a", 6, 60)],
            &[truncate(7, 70), upsert(3, "c", 8, 80)],
        ];
        let published = self
            .versioned(("soft", 4), Kind::Changes { soft: true }, &commits)
            .await?;
        let what = "a history with soft deletes";
        let a = closed(1, "a", 10, 30, 1);
        let second = [
            a.clone(),
            deleted(1, "a", 30, None, 3),
            current(2, "b", 20, 2),
        ];
        expect(&published, 1, &second, what)?;
        expect(&published, 2, &second, what)?;
        let fourth = [
            a.clone(),
            current(1, "a", 60, 6),
            deleted(1, "a", 30, Some(60), 3),
            current(2, "b", 20, 2),
        ];
        expect(&published, 3, &fourth, what)?;
        let fifth = [
            a,
            closed(1, "a", 60, 70, 6),
            deleted(1, "a", 30, Some(60), 3),
            deleted(1, "a", 70, None, 7),
            closed(2, "b", 20, 70, 2),
            deleted(2, "b", 70, None, 7),
            current(3, "c", 80, 8),
        ];
        expect(&published, 4, &fifth, what)
    }
}

/// Whether the table held `expected` after commit `index` of `published`.
fn expect(
    published: &[Vec<Version>],
    index: usize,
    expected: &[Version],
    what: &str,
) -> Result<(), Violation> {
    let actual = published.get(index).map_or(&[][..], Vec::as_slice);
    let mut sorted = expected.to_vec();
    sorted.sort_unstable();
    if actual == sorted {
        Ok(())
    } else {
        Err(Violation::from(format_args!(
            "{what}: after commit {} the table holds {}, expected {sorted:?}",
            index + 1,
            Listed(actual)
        )))
    }
}
