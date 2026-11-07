//! `D-WRITERS`: table writers of one table staging at once.

use tokio::task::JoinSet;

use super::{Bench, commit, expect_rows, meta, rows};
use crate::id::SegmentId;
use crate::testing::Violation;

/// The most table writers the clause runs at once.
const WRITERS: u16 = 4;

impl Bench<'_> {
    /// Opens a session, and stages three rows through each of as many table writers of one table
    /// as the destination runs writers at once, up to four, all at the same time, each in a segment
    /// of its own; commits every segment, and reads every row back.
    pub(super) async fn writers_stage_at_once(&self) -> Result<(), Violation> {
        let parallel = self
            .destination
            .capabilities()
            .max_parallel_writers
            .get()
            .min(WRITERS);
        let mut opened = self.open(self.destination).await?;
        let mut writers = vec![self.writer(&mut opened.session).await?];
        for _ in 1..parallel {
            let writer = opened
                .session
                .writer(&self.table())
                .await
                .map_err(|error| Violation::from(format!("writer: {error}")))?;
            writers.push(writer);
        }
        let mut staging = JoinSet::new();
        for (segment, mut writer) in (1..).zip(writers) {
            staging.spawn(async move {
                writer
                    .write(SegmentId(segment), rows(segment))
                    .await
                    .map_err(|error| format!("write: {error}"))?;
                writer
                    .flush()
                    .await
                    .map(drop)
                    .map_err(|error| format!("flush: {error}"))
            });
        }
        while let Some(staged) = staging.join_next().await {
            staged
                .map_err(|error| Violation::from(format!("a table writer panicked: {error}")))?
                .map_err(Violation::from)?;
        }
        let segments: Vec<u64> = (1..=u64::from(parallel)).collect();
        commit(&mut opened, meta(&segments, Vec::new())).await?;
        expect_rows(&self.published_rows().await?, &segments)
    }
}
