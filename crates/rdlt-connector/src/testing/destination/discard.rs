//! `D-DISCARD`: what no commit of the latest session publishes is never published.

use super::{Bench, STALE, commit, expect_rows, meta, rows};
use crate::commit::CommitMeta;
use crate::id::{CommitSeq, SegmentId};
use crate::testing::{Violation, bounded};

impl Bench<'_> {
    /// Neither an abandoned session's staging nor a fenced worker's late write reaches the
    /// latest session's commit.
    pub(super) async fn earlier_staging_is_discarded(&self) -> Result<(), Violation> {
        let abandoned = self.staged(self.destination, 1, &[2]).await?;
        drop(abandoned);
        let mut stale = self.open(self.destination, 2).await?;
        let mut stale_writer = self.writer(&mut stale.session).await?;
        let mut latest = self.open(self.peer, 3).await?;
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
        commit(
            &mut latest.session,
            &meta(self.load_id(3), latest.epoch, &[1], Vec::new()),
        )
        .await?;
        // Committing the abandoned session's segment may fail or succeed, but publishes nothing.
        let orphan = CommitMeta {
            commit_seq: CommitSeq::FIRST.next(),
            ..meta(self.load_id(3), latest.epoch, &[2], Vec::new())
        };
        drop(bounded("commit", latest.session.commit(&orphan)).await?);
        expect_rows(&self.published_rows().await?, &[1])?;
        self.abandoned_staging_is_discarded().await
    }

    /// What a session staged in a segment its commit abandons is never published, even by a
    /// commit that lists the segment after, and what it staged for a later commit is.
    async fn abandoned_staging_is_discarded(&self) -> Result<(), Violation> {
        let mut opened = self.open(self.destination, 4).await?;
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
            ..meta(self.load_id(4), opened.epoch, &[6], Vec::new())
        };
        commit(&mut opened.session, &abandoning).await?;
        // A segment staged for a later commit is no abandoned one: that commit publishes it.
        let later = CommitMeta {
            commit_seq: CommitSeq::FIRST.next(),
            ..meta(self.load_id(4), opened.epoch, &[7], Vec::new())
        };
        commit(&mut opened.session, &later).await?;
        // Listing the abandoned segment later may fail or succeed, but publishes nothing.
        let relisting = CommitMeta {
            commit_seq: CommitSeq::FIRST.next().next(),
            ..meta(self.load_id(4), opened.epoch, &[5], Vec::new())
        };
        drop(bounded("commit", opened.session.commit(&relisting)).await?);
        expect_rows(&self.published_rows().await?, &[1, 6, 7])
    }
}
