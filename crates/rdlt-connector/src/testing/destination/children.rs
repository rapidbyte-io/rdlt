//! `D-CHILDREN`: a child table of a merge table follows its root's merges.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{BinaryArray, Int64Array, RecordBatch, StringArray};

use super::{Bench, commit, meta};
use crate::commit::{ChildTable, CommitMeta};
use crate::destination::{
    DestinationSession, DestinationWriter, MergeKey, RootKey, TableChange, TableRef,
};
use crate::id::{CommitSeq, SegmentId};
use crate::schema::TableSchema;
use crate::testing::{Violation, bounded_call};
use crate::types::{Field, LogicalType};

const ID: &str = "_rdlt_id";
const ROOT_ID: &str = "_rdlt_root_id";
const SEQ: &str = "_rdlt_seq";

/// A root row: its key, its id's byte and its sequence's last byte.
type Root = (i64, u8, u8);

/// A child row: its value, its root's id byte and the sequence of the root row it came from.
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
        let mut opened = self.open(self.destination, 1).await?;
        create(&mut opened.session, &roots, &[&items, &tags]).await?;
        let mut writers = Vec::new();
        for table in [&roots, &items, &tags] {
            writers.push(bounded_call("writer", opened.session.writer(table)).await?);
        }
        stage(&mut writers).await?;
        let child_tables: Vec<ChildTable> = [&items, &tags].into_iter().map(child_table).collect();
        for (seq, segment) in [(CommitSeq::FIRST, 1), (CommitSeq::FIRST.next(), 2)] {
            let commit_meta = CommitMeta {
                commit_seq: seq,
                child_tables: child_tables.clone(),
                ..meta(self.load_id(1), opened.epoch, &[segment], Vec::new())
            };
            commit(&mut opened.session, &commit_meta).await?;
        }
        let roots = bounded_call("probe", self.probe.published(&roots)).await?;
        let roots: usize = roots.iter().map(RecordBatch::num_rows).sum();
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
            Err(
                format!("the child tables hold {items:?} and {tags:?}, expected [e, g] and [y]")
                    .into(),
            )
        }
    }

    /// The clause's root table, merging by `id`, and two child tables following it.
    fn family(&self) -> [TableRef; 3] {
        let roots = TableRef {
            merge: Some(MergeKey {
                columns: vec!["id".into()],
                seq: SEQ.into(),
                root: None,
            }),
            ..self.other_table("roots")
        };
        let child = |suffix: &str| TableRef {
            merge: Some(MergeKey {
                columns: vec![ROOT_ID.into()],
                seq: SEQ.into(),
                root: Some(RootKey {
                    table: Arc::clone(&roots.name),
                    id: ID.into(),
                    seq: SEQ.into(),
                }),
            }),
            ..self.other_table(suffix)
        };
        let (items, tags) = (child("roots__items"), child("roots__tags"));
        [roots, items, tags]
    }

    /// The published `value`s of `table`.
    async fn child_values(&self, table: &TableRef) -> Result<Vec<String>, Violation> {
        let batches = bounded_call("probe", self.probe.published(table)).await?;
        let mut values = Vec::new();
        for batch in &batches {
            let column = batch
                .column_by_name("value")
                .ok_or_else(|| Violation::from("the child table has no value column"))?;
            let column = arrow_cast::cast(column, &arrow_schema::DataType::Utf8)
                .map_err(|error| Violation::from(format!("reading value: {error}")))?;
            values.extend(
                column
                    .as_string::<i32>()
                    .iter()
                    .flatten()
                    .map(str::to_owned),
            );
        }
        Ok(values)
    }
}

/// Stages, through the writers of the roots and their two child tables, the clause's first
/// segment and its second, which gives the second child table nothing.
async fn stage(writers: &mut [Box<dyn DestinationWriter>]) -> Result<(), Violation> {
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
                root_batch(&first_roots),
                child_batch(&first_items),
                child_batch(&first_tags),
            ],
        ),
        (
            2,
            vec![root_batch(&second_roots), child_batch(&second_items)],
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

/// Creates the root table `roots` and its child tables `children`.
async fn create(
    session: &mut Box<dyn DestinationSession>,
    roots: &TableRef,
    children: &[&TableRef],
) -> Result<(), Violation> {
    let root_schema = schema(vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new(ID, LogicalType::Binary, false),
        Field::new(SEQ, LogicalType::Binary, false),
    ]);
    let child_schema = schema(vec![
        Field::new("value", LogicalType::Utf8, false),
        Field::new(ROOT_ID, LogicalType::Binary, false),
        Field::new(SEQ, LogicalType::Binary, false),
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

fn root_batch(rows: &[Root]) -> RecordBatch {
    let keys: Int64Array = rows.iter().map(|row| row.0).collect();
    let ids = BinaryArray::from_iter_values(rows.iter().map(|row| bytes(row.1)));
    let seqs = BinaryArray::from_iter_values(rows.iter().map(|row| bytes(row.2)));
    RecordBatch::try_from_iter([
        ("id", Arc::new(keys) as _),
        (ID, Arc::new(ids) as _),
        (SEQ, Arc::new(seqs) as _),
    ])
    .expect("the certification batch is valid")
}

fn child_batch(rows: &[Child]) -> RecordBatch {
    let values: StringArray = rows.iter().map(|row| Some(row.0)).collect();
    let roots = BinaryArray::from_iter_values(rows.iter().map(|row| bytes(row.1)));
    let seqs = BinaryArray::from_iter_values(rows.iter().map(|row| bytes(row.2)));
    RecordBatch::try_from_iter([
        ("value", Arc::new(values) as _),
        (ROOT_ID, Arc::new(roots) as _),
        (SEQ, Arc::new(seqs) as _),
    ])
    .expect("the certification batch is valid")
}
