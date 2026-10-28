//! `D-DISCARD`: what no commit of the latest session publishes is never published.

use super::{Bench, STALE, commit, expect_rows, meta, rows, try_commit};
use crate::commit::CommitMeta;
use crate::id::SegmentId;
use crate::testing::Violation;

impl Bench<'_> {
    /// Neither an abandoned session's staging nor a fenced worker's late write reaches the
    /// latest session's commit.
    pub(super) async fn earlier_staging_is_discarded(&self) -> Result<(), Violation> {
        let abandoned = self.staged(self.destination, &[2]).await?;
        drop(abandoned);
        let mut stale = self.open(self.destination).await?;
        let mut stale_writer = self.writer(&mut stale.session).await?;
        let mut latest = self.open(self.peer).await?;
        // A fenced worker may still be running; whether its write fails or is ignored is the
        // destination's choice, but it must never be published.
        // Other ids than the latest session's, so the stale rows are told from them.
        drop(stale_writer.write(SegmentId(1), rows(STALE)).await);
        drop(stale_writer.flush().await);
        let mut writer = self.writer(&mut latest.session).await?;
        writer
            .write(SegmentId(1), rows(1))
            .await
            .map_err(|error| Violation::from(format!("write: {error}")))?;
        writer
            .flush()
            .await
            .map_err(|error| Violation::from(format!("flush: {error}")))?;
        commit(&mut latest, meta(&[1], Vec::new())).await?;
        // Committing the abandoned session's segment may fail or succeed, but publishes nothing.
        drop(try_commit(&mut latest, meta(&[2], Vec::new())).await?);
        expect_rows(&self.published_rows().await?, &[1])?;
        self.abandoned_staging_is_discarded().await
    }

    /// What a session staged in a segment its commit abandons is never published, even by a
    /// commit that lists the segment after, and what it staged for a later commit is.
    async fn abandoned_staging_is_discarded(&self) -> Result<(), Violation> {
        let mut opened = self.open(self.destination).await?;
        let mut writer = self.writer(&mut opened.session).await?;
        for segment in [5, 6, 7] {
            writer
                .write(SegmentId(segment), rows(segment))
                .await
                .map_err(|error| Violation::from(format!("write: {error}")))?;
        }
        writer
            .flush()
            .await
            .map_err(|error| Violation::from(format!("flush: {error}")))?;
        let abandoning = CommitMeta {
            abandoned: [SegmentId(5)].into_iter().collect(),
            ..meta(&[6], Vec::new())
        };
        commit(&mut opened, abandoning).await?;
        // A segment staged for a later commit is no abandoned one: that commit publishes it.
        commit(&mut opened, meta(&[7], Vec::new())).await?;
        // Listing the abandoned segment later may fail or succeed, but publishes nothing.
        drop(try_commit(&mut opened, meta(&[5], Vec::new())).await?);
        expect_rows(&self.published_rows().await?, &[1, 6, 7])
    }
}
