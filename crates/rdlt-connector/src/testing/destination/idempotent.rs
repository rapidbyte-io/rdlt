//! `D-IDEMPOTENT`: a commit sent again is answered with its receipt and publishes nothing, while
//! no later commit of its pipeline has a horizon past it, whatever another pipeline's says.

use super::{Bench, commit, expect_rows, meta};
use crate::commit::Horizon;
use crate::id::{CommitSeq, PipelineId};
use crate::testing::Violation;

impl Bench<'_> {
    /// Replays a committed load the way recovery does: another worker opens the same load,
    /// stages its segment again and re-commits the same `(load_id, commit_seq)`, which a later
    /// commit named as the oldest the engine may repeat, and which another pipeline's commit on
    /// the destination has a horizon past.
    pub(super) async fn recommits_are_idempotent(&self) -> Result<(), Violation> {
        let mut first = self.staged(self.destination, 1, &[1]).await?;
        let original = commit(
            &mut first.session,
            &meta(self.load_id(1), first.epoch, &[1], Vec::new()),
        )
        .await?;
        let mut later = meta(self.load_id(2), first.epoch, &[], Vec::new());
        later.horizon = Some(Horizon {
            load_id: self.load_id(1),
            commit_seq: CommitSeq::FIRST,
        });
        commit(&mut first.session, &later).await?;
        let other = PipelineId::parse(format!("{}_other", self.name()))
            .map_err(|error| Violation::from(format!("pipeline id: {error}")))?;
        let mut elsewhere = self.open_as(self.destination, other, 3).await?;
        let mut past = meta(self.load_id(3), elsewhere.epoch, &[], Vec::new());
        past.horizon = Some(Horizon {
            load_id: self.load_id(3),
            commit_seq: CommitSeq::FIRST,
        });
        commit(&mut elsewhere.session, &past).await?;
        let mut replay = self.staged(self.peer, 1, &[1]).await?;
        let replayed = commit(
            &mut replay.session,
            &meta(self.load_id(1), replay.epoch, &[1], Vec::new()),
        )
        .await?;
        if replayed != original {
            return Err(format!(
                "the re-commit returned {replayed:?}, not the stored receipt {original:?}"
            )
            .into());
        }
        expect_rows(&self.published_rows().await?, &[1])
    }
}
