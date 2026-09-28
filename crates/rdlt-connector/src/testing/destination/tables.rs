//! `D-TABLES`: one segment holds rows for several tables.

use super::{Bench, commit, expect_ids, meta, rows};
use crate::destination::TableRef;
use crate::id::{SchemaVersion, SegmentId, TablePath};
use crate::testing::Violation;

impl Bench<'_> {
    /// Stages three rows in one segment through writers of two tables, as a stream and its child
    /// table share their partition's segments, commits the segment, and reads both tables back.
    pub(super) async fn segments_span_tables(&self) -> Result<(), Violation> {
        let mut opened = self.open(self.destination, 1).await?;
        let child = self.child_table();
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
        commit(
            &mut opened.session,
            &meta(self.load_id(1), opened.epoch, &[1], Vec::new()),
        )
        .await?;
        for table in [self.table(), child] {
            expect_ids(&self.ids_of(&table).await?, &[1]).map_err(|Violation(reason)| {
                Violation(format!("table {}: {reason}", table.name))
            })?;
        }
        Ok(())
    }

    /// A child of the clause's table, as normalized nested data creates one.
    fn child_table(&self) -> TableRef {
        let parent = self.name();
        TableRef {
            path: TablePath::new([parent.as_str(), "items"]).expect("table paths are valid"),
            name: format!("{parent}__items").into(),
            version: SchemaVersion(1),
            generation: None,
            merge: None,
        }
    }
}
