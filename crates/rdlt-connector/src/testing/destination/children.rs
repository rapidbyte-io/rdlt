//! `D-CHILDREN`: a child table of a merge table follows its root's merges.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{BinaryArray, Int64Array, RecordBatch, StringArray};

use super::identifiers::Identifiers;
use super::sequences::sequence;
use super::{Bench, commit, meta};
use crate::change::SEQ_COLUMN;
use crate::commit::{ChildTable, CommitMeta};
use crate::destination::{
    DestinationSession, DestinationWriter, MergeKey, RootKey, TableChange, TableRef,
};
use crate::id::SegmentId;
use crate::meta::{ID_COLUMN, ROOT_ID_COLUMN};
use crate::schema::TableSchema;
use crate::testing::reason::Listed;
use crate::testing::{Violation, bounded_call};
use crate::types::{Field, LogicalType};

/// A root row: its key, its id's byte and its sequence's ordinal.
type Root = (i64, u8, u8);

/// A child row: its value, its root's id byte and the sequence's ordinal of the root row it came
/// from.
type Child = (&'static str, u8, u8);

impl Bench<'_> {
    /// Merges roots and their children over two commits, and checks the child tables hold the
    /// children of each root's winning row only.
    ///
    /// The second commit gives root 1 fewer items, root 2 none, and root 3 two rows, the later
    /// without items; root 4 keeps what the first commit gave it. It stages nothing for the
    /// second child table, which it lists, and which loses the tags of root 1 all the same.
    pub(super) async fn children_follow_their_roots(&self) -> Result<(), Violation> {
        let [roots, items, tags] = self.family();
        let mut opened = self.open(self.destination).await?;
        create(self.names, &mut opened.session, &roots, &[&items, &tags]).await?;
        let mut writers = Vec::new();
        for table in [&roots, &items, &tags] {
            writers.push(bounded_call("table writer", opened.session.writer(table)).await?);
        }
        stage(self.names, &mut writers).await?;
        let child_tables: Vec<ChildTable> = [&items, &tags].into_iter().map(child_table).collect();
        for segment in [1, 2] {
            let commit_meta = CommitMeta {
                child_tables: child_tables.clone(),
                ..meta(&[segment], Vec::new())
            };
            commit(&mut opened, commit_meta).await?;
        }
        let roots = self.read(&roots).await?.rows();
        if roots != 4 {
            return Err(
                format!("the root table holds {roots} rows, expected one per key: 4").into(),
            );
        }
        let (mut items, mut tags) = (
            self.child_values(&items).await?,
            self.child_values(&tags).await?,
        );
        items.sort();
        tags.sort();
        if items == ["e", "g"] && tags == ["y"] {
            Ok(())
        } else {
            Err(Violation::from(format_args!(
                "the child tables hold {} and {}, expected [e, g] and [y]",
                Listed(&items),
                Listed(&tags)
            )))
        }
    }

    /// The clause's root table, merging by `id`, and two child tables following it.
    fn family(&self) -> [TableRef; 3] {
        let column = |name| self.names.column(name);
        let roots = TableRef {
            merge: Some(MergeKey {
                columns: vec![column("id")],
                seq: column(SEQ_COLUMN),
                root: None,
                changes: None,
                history: None,
            }),
            ..self.other_table("roots")
        };
        let child = |suffix: &str| TableRef {
            merge: Some(MergeKey {
                columns: vec![column(ROOT_ID_COLUMN)],
                seq: column(SEQ_COLUMN),
                root: Some(RootKey {
                    table: Arc::clone(&roots.name),
                    id: column(ID_COLUMN),
                    seq: column(SEQ_COLUMN),
                }),
                changes: None,
                history: None,
            }),
            ..self.other_table(suffix)
        };
        let (items, tags) = (child("roots__items"), child("roots__tags"));
        [roots, items, tags]
    }

    /// The published `value`s of `table`.
    async fn child_values(&self, table: &TableRef) -> Result<Vec<String>, Violation> {
        let published = self.read(table).await?;
        let mut values = Vec::with_capacity(published.rows());
        for batch in published.batches() {
            // A child's value holds no null: a row without one is a row too many.
            let value = self.names.column("value");
            let column = batch.required(&value, &arrow_schema::DataType::Utf8)?;
            let column = column.as_string::<i32>();
            values.extend((0..batch.rows()).map(|row| column.value(row).to_owned()));
        }
        Ok(values)
    }
}

/// Stages, through the table writers of the roots and their two child tables, the clause's first
/// segment and its second, which gives the second child table nothing, its columns as `names`
/// name them.
async fn stage(
    names: &Identifiers,
    writers: &mut [Box<dyn DestinationWriter>],
) -> Result<(), Violation> {
    let first_roots = [(1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4)];
    let first_items = [
        ("a", 1, 1),
        ("b", 1, 1),
        ("c", 2, 2),
        ("d", 3, 3),
        ("g", 4, 4),
    ];
    let first_tags = [("x", 1, 1), ("y", 4, 4)];
    let second_roots = [(1, 1, 10), (2, 2, 11), (3, 3, 12), (3, 3, 13)];
    let second_items = [("e", 1, 10), ("f", 3, 12)];
    let segments = [
        (
            1,
            vec![
                root_batch(names, &first_roots),
                child_batch(names, &first_items),
                child_batch(names, &first_tags),
            ],
        ),
        (
            2,
            vec![
                root_batch(names, &second_roots),
                child_batch(names, &second_items),
            ],
        ),
    ];
    for (segment, batches) in segments {
        for (writer, batch) in writers.iter_mut().zip(batches) {
            bounded_call("write", writer.write(SegmentId(segment), batch)).await?;
            bounded_call("flush", writer.flush()).await?;
        }
    }
    Ok(())
}

/// Creates the root table `roots` and its child tables `children`, their columns as `names`
/// name them.
async fn create(
    names: &Identifiers,
    session: &mut Box<dyn DestinationSession>,
    roots: &TableRef,
    children: &[&TableRef],
) -> Result<(), Violation> {
    let column = |name| names.column(name);
    let root_schema = schema(vec![
        Field::new(column("id"), LogicalType::Int64, false),
        Field::new(column(ID_COLUMN), LogicalType::Binary, false),
        Field::new(column(SEQ_COLUMN), LogicalType::Binary, false),
    ]);
    let child_schema = schema(vec![
        Field::new(column("value"), LogicalType::Utf8, false),
        Field::new(column(ROOT_ID_COLUMN), LogicalType::Binary, false),
        Field::new(column(SEQ_COLUMN), LogicalType::Binary, false),
    ]);
    let tables = std::iter::once((roots, root_schema))
        .chain(children.iter().map(|child| (*child, child_schema.clone())));
    for (table, schema) in tables {
        let create = TableChange::Create {
            table: table.clone(),
            schema,
        };
        bounded_call("apply_schema", session.apply_schema(&create)).await?;
    }
    Ok(())
}

/// `table`, a child table, as a commit lists it.
fn child_table(table: &TableRef) -> ChildTable {
    ChildTable {
        table: Arc::clone(&table.name),
        merge: table
            .merge
            .clone()
            .expect("the clause's child tables merge"),
    }
}

fn schema(fields: Vec<Field>) -> TableSchema {
    TableSchema::new(fields).expect("the certification schema is valid")
}

/// 16 bytes whose last one is `byte`.
fn bytes(byte: u8) -> Vec<u8> {
    let mut bytes = vec![0; 16];
    bytes[15] = byte;
    bytes
}

fn root_batch(names: &Identifiers, rows: &[Root]) -> RecordBatch {
    let keys: Int64Array = rows.iter().map(|row| row.0).collect();
    let ids = BinaryArray::from_iter_values(rows.iter().map(|row| bytes(row.1)));
    let seqs = BinaryArray::from_iter_values(rows.iter().map(|row| sequence(row.2)));
    RecordBatch::try_from_iter([
        (names.column("id"), Arc::new(keys) as _),
        (names.column(ID_COLUMN), Arc::new(ids) as _),
        (names.column(SEQ_COLUMN), Arc::new(seqs) as _),
    ])
    .expect("the certification batch is valid")
}

fn child_batch(names: &Identifiers, rows: &[Child]) -> RecordBatch {
    let values: StringArray = rows.iter().map(|row| Some(row.0)).collect();
    let roots = BinaryArray::from_iter_values(rows.iter().map(|row| bytes(row.1)));
    let seqs = BinaryArray::from_iter_values(rows.iter().map(|row| sequence(row.2)));
    RecordBatch::try_from_iter([
        (names.column("value"), Arc::new(values) as _),
        (names.column(ROOT_ID_COLUMN), Arc::new(roots) as _),
        (names.column(SEQ_COLUMN), Arc::new(seqs) as _),
    ])
    .expect("the certification batch is valid")
}
