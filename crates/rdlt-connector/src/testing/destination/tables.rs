//! `D-TABLES`: one segment holds rows for several tables.

use super::{Bench, commit, meta, rows};
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
                .write(SegmentId(1), rows())
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
            let published: usize = self
                .probe
                .published(&table)
                .await
                .map_err(|error| Violation::from(format!("probe: {error}")))?
                .iter()
                .map(arrow_array::RecordBatch::num_rows)
                .sum();
            if published != 3 {
                return Err(
                    format!("table {} published {published} rows, not 3", table.name).into(),
                );
            }
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
