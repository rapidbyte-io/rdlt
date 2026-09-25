//! The clauses for tables that change: replace generations, schema changes and merges.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, ArrayRef, BinaryArray, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::DataType;

use super::{Bench, commit, expect_rows, meta, rows};
use crate::OpenedSession;
use crate::commit::CommitMeta;
use crate::destination::Destination;
use crate::destination::{DestinationSession, DestinationWriter, MergeKey, TableChange, TableRef};
use crate::error::ConnectorErrorKind;
use crate::id::{CommitSeq, GenerationId, SchemaVersion, SegmentId, TablePath};
use crate::schema::TableSchema;
use crate::testing::{Violation, bounded, bounded_call};
use crate::types::{Field, LogicalType, TypeKind};

impl Bench<'_> {
    /// Another table of this clause, called `suffix` after the clause's own.
    pub(super) fn other_table(&self, suffix: &str) -> TableRef {
        let name = format!("{}_{suffix}", self.name());
        TableRef {
            path: TablePath::new([name.as_str()]).expect("table paths are valid"),
            name: name.into(),
            version: SchemaVersion(1),
            generation: None,
            merge: None,
        }
    }

    /// Stages a generation over two commits: rows stay hidden until the commit that finishes the
    /// generation replaces the table's rows with them.
    pub(super) async fn generations_swap_in_atomically(&self) -> Result<(), Violation> {
        let base = self.table();
        let generation = TableRef {
            generation: Some(GENERATION),
            ..base.clone()
        };
        let mut opened = self.staged(self.destination, 1, &[1]).await?;
        let mut writer = bounded_call("writer", opened.session.writer(&generation)).await?;
        write(&mut writer, 2, rows()).await?;
        write(&mut writer, 3, rows()).await?;
        commit(
            &mut opened.session,
            &meta(self.load_id(1), opened.epoch, &[1, 2], Vec::new()),
        )
        .await?;
        expect_rows(self.published_rows().await?, 3)?;
        let finish = CommitMeta {
            commit_seq: CommitSeq::FIRST.next(),
            finish_generations: vec![(base.path.clone(), GENERATION)],
            ..meta(self.load_id(1), opened.epoch, &[3], Vec::new())
        };
        commit(&mut opened.session, &finish).await?;
        expect_rows(self.published_rows().await?, 6)
    }

    /// Adds a column and widens one, each applied twice, with rows committed before and after.
    pub(super) async fn schema_changes_apply(&self) -> Result<(), Violation> {
        let changes = self.destination.capabilities().schema_changes.clone();
        let mut opened = self.staged(self.destination, 1, &[1]).await?;
        commit(
            &mut opened.session,
            &meta(self.load_id(1), opened.epoch, &[1], Vec::new()),
        )
        .await?;
        let mut seq = CommitSeq::FIRST;
        if changes.add_column {
            seq = seq.next();
            self.add_column(&mut opened, seq).await?;
        }
        if changes.widens(TypeKind::Int32, TypeKind::Int64) {
            self.widen_column(&mut opened, seq.next()).await?;
        }
        Ok(())
    }

    /// Adds a column to the clause's table and commits rows that fill it as commit `seq`.
    async fn add_column(
        &self,
        opened: &mut OpenedSession,
        seq: CommitSeq,
    ) -> Result<(), Violation> {
        let table = self.table();
        let add = TableChange::AddColumn {
            table: table.clone(),
            field: Field::new("extra", LogicalType::Int64, true),
        };
        apply_twice(&mut opened.session, &add).await?;
        let mut writer = bounded_call("writer", opened.session.writer(&table)).await?;
        write(&mut writer, 2, with_extra(&rows())).await?;
        let second = CommitMeta {
            commit_seq: seq,
            ..meta(self.load_id(1), opened.epoch, &[2], Vec::new())
        };
        commit(&mut opened.session, &second).await?;
        let values = self.values(&table, "extra").await?;
        if values.iter().flatten().count() != 3 || values.len() != 6 {
            return Err(format!("after adding a column, published {values:?}").into());
        }
        let create = TableChange::Create {
            table: table.clone(),
            schema: TableSchema::new(vec![
                Field::new("id", LogicalType::Int64, false),
                Field::new("name", LogicalType::Utf8, true),
                Field::new("extra", LogicalType::Int64, true),
            ])
            .expect("the certification schema is valid"),
        };
        bounded_call("apply_schema again", opened.session.apply_schema(&create)).await?;
        let conflicting = TableChange::AddColumn {
            table,
            field: Field::new("extra", LogicalType::Utf8, true),
        };
        match bounded("apply_schema", opened.session.apply_schema(&conflicting)).await? {
            Err(error)
                if error.kind() == ConnectorErrorKind::Data
                    && error.code() == Some("schema_conflict") =>
            {
                Ok(())
            }
            Err(error) => Err(format!(
                "a conflicting change failed with {:?} {:?}, not Data schema_conflict: {error}",
                error.kind(),
                error.code()
            )
            .into()),
            Ok(()) => Err("adding a column of another type over an existing one succeeded".into()),
        }
    }

    /// Widens an Int32 column of another table to Int64 between two writes, committed as `seq`.
    async fn widen_column(
        &self,
        opened: &mut OpenedSession,
        seq: CommitSeq,
    ) -> Result<(), Violation> {
        let table = self.other_table("widen");
        let create = TableChange::Create {
            table: table.clone(),
            schema: TableSchema::new(vec![Field::new("small", LogicalType::Int32, true)])
                .expect("the certification schema is valid"),
        };
        bounded_call("apply_schema", opened.session.apply_schema(&create)).await?;
        let mut writer = bounded_call("writer", opened.session.writer(&table)).await?;
        let small =
            RecordBatch::try_from_iter([("small", Arc::new(Int32Array::from(vec![7])) as _)])
                .expect("the certification batch is valid");
        write(&mut writer, 3, small).await?;
        let widen = TableChange::Widen {
            table: table.clone(),
            column: "small".into(),
            from: LogicalType::Int32,
            to: LogicalType::Int64,
        };
        apply_twice(&mut opened.session, &widen).await?;
        let narrower = TableChange::Widen {
            table: table.clone(),
            column: "small".into(),
            from: LogicalType::Int16,
            to: LogicalType::Int32,
        };
        for held in [&create, &narrower] {
            bounded_call(
                "apply_schema of a type the column holds",
                opened.session.apply_schema(held),
            )
            .await?;
        }
        let wide = RecordBatch::try_from_iter([(
            "small",
            Arc::new(Int64Array::from(vec![1_i64 << 40])) as _,
        )])
        .expect("the certification batch is valid");
        write(&mut writer, 4, wide).await?;
        let narrow =
            RecordBatch::try_from_iter([("small", Arc::new(Int32Array::from(vec![3])) as _)])
                .expect("the certification batch is valid");
        write(&mut writer, 4, narrow).await?;
        let widened = CommitMeta {
            commit_seq: seq,
            ..meta(self.load_id(1), opened.epoch, &[3, 4], Vec::new())
        };
        commit(&mut opened.session, &widened).await?;
        let mut values = self.values(&table, "small").await?;
        values.sort_unstable();
        if values == [Some(3), Some(7), Some(1 << 40)] {
            Ok(())
        } else {
            Err(format!("after widening a column, published {values:?}").into())
        }
    }

    /// Commits keyed rows twice: the second commit replaces the first's row of a shared key, and
    /// of two rows of one key within a commit the greater sequence wins.
    pub(super) async fn merges_keep_the_newest_row(&self) -> Result<(), Violation> {
        let table = TableRef {
            merge: Some(MergeKey {
                columns: vec!["id".into()],
                seq: SEQ.into(),
                root: None,
            }),
            ..self.table()
        };
        let mut opened = self.open(self.destination, 1).await?;
        let create = TableChange::Create {
            table: table.clone(),
            schema: TableSchema::new(vec![
                Field::new("id", LogicalType::Int64, false),
                Field::new("name", LogicalType::Utf8, true),
                Field::new(SEQ, LogicalType::Binary, false),
            ])
            .expect("the certification schema is valid"),
        };
        bounded_call("apply_schema", opened.session.apply_schema(&create)).await?;
        let mut writer = bounded_call("writer", opened.session.writer(&table)).await?;
        write(&mut writer, 1, keyed(&[(1, "a", 1), (2, "b", 2)])).await?;
        write(&mut writer, 2, keyed(&[(2, "late", 9), (3, "c", 3)])).await?;
        write(&mut writer, 3, keyed(&[(2, "early", 4)])).await?;
        commit(
            &mut opened.session,
            &meta(self.load_id(1), opened.epoch, &[1], Vec::new()),
        )
        .await?;
        let second = CommitMeta {
            commit_seq: CommitSeq::FIRST.next(),
            ..meta(self.load_id(1), opened.epoch, &[2, 3], Vec::new())
        };
        commit(&mut opened.session, &second).await?;
        let mut published = self.keyed_rows(&table).await?;
        published.sort_unstable();
        let expected = [
            (1, Some("a".to_owned())),
            (2, Some("late".to_owned())),
            (3, Some("c".to_owned())),
        ];
        if published == expected {
            Ok(())
        } else {
            Err(format!("the merge table holds {published:?}, expected {expected:?}").into())
        }
    }

    /// The published values of the Int64 or Int32 `column` of `table`, null where a batch lacks it.
    async fn values(&self, table: &TableRef, column: &str) -> Result<Vec<Option<i64>>, Violation> {
        let batches = bounded_call("probe", self.probe.published(table)).await?;
        let mut values = Vec::new();
        for batch in &batches {
            match batch.column_by_name(column) {
                Some(array) => {
                    let array = arrow_cast::cast(array, &DataType::Int64)
                        .map_err(|error| Violation::from(format!("reading {column}: {error}")))?;
                    values.extend(array.as_primitive::<Int64Type>().iter());
                }
                None => values.extend(std::iter::repeat_n(None, batch.num_rows())),
            }
        }
        Ok(values)
    }

    /// The published `(id, name)` rows of `table`.
    async fn keyed_rows(&self, table: &TableRef) -> Result<Vec<(i64, Option<String>)>, Violation> {
        let batches = bounded_call("probe", self.probe.published(table)).await?;
        let mut rows = Vec::new();
        for batch in &batches {
            let (Some(ids), Some(names)) =
                (batch.column_by_name("id"), batch.column_by_name("name"))
            else {
                return Err("a published batch lacks id or name".into());
            };
            let ids = ids.as_primitive::<Int64Type>();
            let names = names.as_string::<i32>();
            for row in 0..batch.num_rows() {
                rows.push((
                    ids.value(row),
                    names.is_valid(row).then(|| names.value(row).to_owned()),
                ));
            }
        }
        Ok(rows)
    }
}

/// Why the clause `id` does not apply to `destination`, if it does not.
pub(super) fn skipped(destination: &dyn Destination, id: &str) -> Option<&'static str> {
    let capabilities = destination.capabilities();
    match id {
        "D-REPLACE" if !capabilities.write_modes.replace => Some("the destination cannot replace"),
        "D-MERGE" | "D-CHILDREN" if !capabilities.write_modes.merge => {
            Some("the destination cannot merge")
        }
        "D-SCHEMA"
            if !capabilities.schema_changes.add_column
                && !capabilities
                    .schema_changes
                    .widens(TypeKind::Int32, TypeKind::Int64) =>
        {
            Some("the destination declares no schema change the clause checks")
        }
        _ => None,
    }
}

/// The generation the replace clause fills: beyond the signed range, as the engine's often are,
/// so a destination that narrows ids is caught.
const GENERATION: GenerationId = GenerationId(18_000_000_000_000_000_000);

/// The sequence column of the merge clause's table.
const SEQ: &str = "_rdlt_seq";

async fn write(
    writer: &mut Box<dyn DestinationWriter>,
    segment: u64,
    batch: RecordBatch,
) -> Result<(), Violation> {
    bounded_call("write", writer.write(SegmentId(segment), batch)).await?;
    bounded_call("flush", writer.flush()).await?;
    Ok(())
}

async fn apply_twice(
    session: &mut Box<dyn DestinationSession>,
    change: &TableChange,
) -> Result<(), Violation> {
    bounded_call("apply_schema", session.apply_schema(change)).await?;
    bounded_call("apply_schema again", session.apply_schema(change)).await
}

/// `batch` with an Int64 column `extra` holding 1, 2, 3...
fn with_extra(batch: &RecordBatch) -> RecordBatch {
    let extra: Int64Array = (1..).take(batch.num_rows()).collect();
    let schema = batch.schema();
    let mut columns: Vec<(&str, ArrayRef)> = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .zip(batch.columns().iter().cloned())
        .collect();
    columns.push(("extra", Arc::new(extra)));
    RecordBatch::try_from_iter(columns).expect("the certification batch is valid")
}

/// Merge rows of `(id, name, sequence)`.
fn keyed(rows: &[(i64, &str, u8)]) -> RecordBatch {
    let ids: Arc<Int64Array> = Arc::new(rows.iter().map(|row| row.0).collect());
    let names: Arc<StringArray> = Arc::new(rows.iter().map(|row| Some(row.1)).collect());
    let seqs: BinaryArray = rows
        .iter()
        .map(|row| {
            let mut seq = [0_u8; 16];
            seq[15] = row.2;
            Some(seq.to_vec())
        })
        .collect();
    RecordBatch::try_from_iter([
        ("id", ids as _),
        ("name", names as _),
        (SEQ, Arc::new(seqs) as _),
    ])
    .expect("the certification batch is valid")
}
