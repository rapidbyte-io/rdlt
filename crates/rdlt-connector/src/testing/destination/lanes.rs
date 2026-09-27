//! `D-LANES`: writers of one table staging at once.

use tokio::task::JoinSet;

use super::{Bench, commit, expect_rows, meta, rows};
use crate::id::SegmentId;
use crate::testing::Violation;

/// The most writers the clause runs at once.
const LANES: u16 = 4;

impl Bench<'_> {
    /// Opens a session, and stages three rows through each of as many writers of one table as
    /// the destination runs at once, up to four, all at the same time, each in a segment of its
    /// own; commits every segment, and reads every row back.
    pub(super) async fn writers_stage_at_once(&self) -> Result<(), Violation> {
        let lanes = self
            .destination
            .capabilities()
            .max_parallel_writers
            .get()
            .min(LANES);
        let mut opened = self.open(self.destination, 1).await?;
        let mut writers = vec![self.writer(&mut opened.session).await?];
        for _ in 1..lanes {
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
                    .write(SegmentId(segment), rows())
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
                .map_err(|error| Violation::from(format!("a writer panicked: {error}")))?
                .map_err(Violation::from)?;
        }
        let segments: Vec<u64> = (1..=u64::from(lanes)).collect();
        commit(
            &mut opened.session,
            &meta(self.load_id(1), opened.epoch, &segments, Vec::new()),
        )
        .await?;
        expect_rows(self.published_rows().await?, 3 * usize::from(lanes))
    }
}
