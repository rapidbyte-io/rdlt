//! `D-TABLES`: one segment holds rows for several tables, and its commit publishes each table's
//! rows whole, or nothing where a table lost a column outside the pipeline.

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, RecordBatch};

use super::evolving::{GENERATION, keyed, merged};
use super::rows::{Row, schema};
use super::{Bench, Probe, commit, expect_rows, meta, rows, try_commit};
use crate::commit::CommitMeta;
use crate::destination::{TableChange, TableRef};
use crate::error::{COLUMN_MISSING, ConnectorErrorKind};
use crate::id::{GenerationId, SchemaVersion, SegmentId, TablePath};
use crate::testing::reason::Listed;
use crate::testing::{Outcome, Reason, Violation, bounded, bounded_call};

/// The column a probe drops outside the pipeline.
const DROPPED: &str = "name";

/// What the clause `id` passed on, where its outcome is `passed` and `probe` left part of it
/// unchecked.
pub(super) fn noted(probe: &dyn Probe, id: &str, outcome: &Outcome) -> Option<Reason> {
    let unchecked = id == "D-TABLES" && *outcome == Outcome::Passed && !probe.drops_columns();
    unchecked
        .then(|| Reason::new("no column was dropped outside the pipeline: the probe drops none"))
}

/// One table a write mode's commit stages rows for.
struct Written {
    /// The change creating the table.
    create: TableChange,
    /// What the rows are written to: the table, or a generation of it.
    writer: TableRef,
    /// The rows, each with an `id` and a `name`.
    batch: RecordBatch,
}

/// A write mode's commit of rows into tables, one of which loses a column outside the pipeline
/// between the write and the commit.
struct Dropping {
    mode: &'static str,
    written: Vec<Written>,
    /// The table the probe drops [`DROPPED`] from.
    dropped: TableRef,
    /// The generations the commit finishes.
    finishing: Vec<(TablePath, GenerationId)>,
    /// Each table read back, with the batch whose rows it publishes when the commit lands.
    read: Vec<(TableRef, RecordBatch)>,
}

impl Bench<'_> {
    /// Stages three rows in one segment through writers of two tables, as a stream and its child
    /// table share their partition's segments, commits the segment, and reads both tables back;
    /// then, where the probe drops columns, has it drop one between a write and its commit, for
    /// each write mode the destination declares.
    pub(super) async fn segments_span_tables(&self) -> Result<(), Violation> {
        let mut opened = self.open(self.destination).await?;
        let child = child_table(&self.table());
        let mut parent_writer = self.writer(&mut opened.session).await?;
        let mut child_writer = self.writer_of(&mut opened.session, &child).await?;
        for writer in [&mut parent_writer, &mut child_writer] {
            writer
                .write(SegmentId(1), rows(1))
                .await
                .map_err(|error| Violation::from(format!("write: {error}")))?;
            writer
                .flush()
                .await
                .map_err(|error| Violation::from(format!("flush: {error}")))?;
        }
        commit(&mut opened, meta(&[1], Vec::new())).await?;
        for table in [self.table(), child] {
            expect_rows(&self.rows_of(&table).await?, &[1])
                .map_err(|violation| violation.of(format_args!("table {}", table.name)))?;
        }
        if !self.probe.drops_columns() {
            return Ok(());
        }
        for dropping in self.droppings() {
            let mode = dropping.mode;
            self.dropped_column(dropping)
                .await
                .map_err(|violation| violation.of(format_args!("{mode} with a column dropped")))?;
        }
        Ok(())
    }

    /// A commit for each write mode the destination declares: an append of a table and its child,
    /// which loses the column and is published second; a merge; a history; and a generation that
    /// replaces a table that loses the column.
    fn droppings(&self) -> Vec<Dropping> {
        let modes = self.destination.capabilities().write_modes;
        let create = |table: &TableRef| TableChange::Create {
            table: table.clone(),
            schema: schema(),
        };
        let mut droppings = Vec::new();
        if modes.append {
            let parent = self.other_table("dropped_append");
            let child = child_table(&parent);
            let written = [&parent, &child].map(|table| Written {
                create: create(table),
                writer: table.clone(),
                batch: rows(1),
            });
            droppings.push(Dropping {
                mode: "an append",
                written: written.into(),
                dropped: child.clone(),
                finishing: Vec::new(),
                read: vec![(parent, rows(1)), (child, rows(1))],
            });
        }
        if modes.merge {
            let (table, create) = merged(self.other_table("dropped_merge"));
            let batch = keyed(&[(1, "a", 1), (2, "b", 2)]);
            droppings.push(dropping_one("a merge", table, create, batch));
        }
        if modes.history {
            let (table, create, batch) = self.history_opened("dropped_history");
            droppings.push(dropping_one("a history", table, create, batch));
        }
        if modes.replace {
            let table = self.other_table("dropped_replace");
            let generation = TableRef {
                generation: Some(GENERATION),
                ..table.clone()
            };
            droppings.push(Dropping {
                mode: "a replace",
                written: vec![Written {
                    create: create(&table),
                    writer: generation,
                    batch: rows(1),
                }],
                dropped: table.clone(),
                finishing: vec![(table.path.clone(), GENERATION)],
                read: vec![(table, rows(1))],
            });
        }
        droppings
    }

    /// Stages `dropping`'s rows in a session of its own, has the probe drop the column, and
    /// commits: the commit publishes every column it staged, or fails as `column_missing` and
    /// publishes nothing in any table.
    async fn dropped_column(&self, dropping: Dropping) -> Result<(), Violation> {
        let mut opened = self.open(self.destination).await?;
        for written in &dropping.written {
            bounded_call("apply_schema", opened.session.apply_schema(&written.create)).await?;
            let mut writer = bounded_call("writer", opened.session.writer(&written.writer)).await?;
            bounded_call("write", writer.write(SegmentId(1), written.batch.clone())).await?;
            bounded_call("flush", writer.flush()).await?;
        }
        let dropped = self.probe.drop_column(&dropping.dropped, DROPPED);
        bounded("drop_column", dropped).await?.map_err(|error| {
            Violation::unobserved(format_args!("the probe dropped no column: {error}"))
        })?;
        let committing = CommitMeta {
            finish_generations: dropping.finishing,
            ..meta(&[1], Vec::new())
        };
        match try_commit(&mut opened, committing).await? {
            Ok(_) => {
                for (table, batch) in &dropping.read {
                    let published = self.rows_of(table).await?;
                    let staged = named(batch);
                    if published != staged {
                        return Err(Violation::from(format_args!(
                            "table {} publishes {}, not the rows staged, {}",
                            table.name,
                            Listed(&published),
                            Listed(&staged)
                        )));
                    }
                }
                Ok(())
            }
            Err(error)
                if error.kind() == ConnectorErrorKind::Data
                    && error.code() == Some(COLUMN_MISSING) =>
            {
                for (table, _) in &dropping.read {
                    let rows = self.read(table).await?.rows();
                    if rows > 0 {
                        return Err(Violation::from(format_args!(
                            "the refused commit published {rows} rows into table {}",
                            table.name
                        )));
                    }
                }
                Ok(())
            }
            Err(error) => Err(Violation::from(format_args!(
                "the commit failed with {:?} {:?}, not Data column_missing: {error}",
                error.kind(),
                error.code()
            ))),
        }
    }
}

/// A child of `parent`, as normalized nested data creates one.
fn child_table(parent: &TableRef) -> TableRef {
    let parent = &*parent.name;
    TableRef {
        path: TablePath::new([parent, "items"]).expect("table paths are valid"),
        name: format!("{parent}__items").into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    }
}

/// The commit of `batch` into `table`, which `create` creates and which loses the column.
fn dropping_one(
    mode: &'static str,
    table: TableRef,
    create: TableChange,
    batch: RecordBatch,
) -> Dropping {
    Dropping {
        mode,
        written: vec![Written {
            create,
            writer: table.clone(),
            batch: batch.clone(),
        }],
        dropped: table.clone(),
        finishing: Vec::new(),
        read: vec![(table, batch)],
    }
}

/// The `(id, name)` rows of `batch`, in order.
fn named(batch: &RecordBatch) -> Vec<Row> {
    let column = |name: &str| batch.column_by_name(name).expect("staged rows are named");
    let ids = column("id").as_primitive::<Int64Type>();
    let names = column(DROPPED).as_string::<i32>();
    let mut rows: Vec<Row> = (0..batch.num_rows())
        .map(|row| {
            let name = names.is_valid(row).then(|| names.value(row).to_owned());
            (ids.value(row), name)
        })
        .collect();
    rows.sort_unstable();
    rows
}
