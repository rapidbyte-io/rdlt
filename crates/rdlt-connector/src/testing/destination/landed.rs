//! `D-LANDED`: the commit whose key landed last, sent again, is answered with its receipt and
//! publishes nothing, from its own session or a newer one and whatever another pipeline commits
//! meanwhile; a commit that expects another key landed than the landed key fails fenced, coded
//! `commit_stale`, and publishes nothing.

use bytes::Bytes;

use super::{Bench, bounded, commit, expect_rows, keyed, meta};
use crate::commit::{CommitMeta, Receipt};
use crate::destination::OpenedSession;
use crate::error::{COMMIT_STALE, ConnectorErrorKind, Result};
use crate::id::PipelineId;
use crate::state::{StateChange, StateRecord};
use crate::testing::Violation;

impl Bench<'_> {
    /// Commits segment 1, and while another pipeline commits, sends that commit again from its
    /// own session, then from another worker's newer session that staged the segment again; from
    /// that session sends a commit expecting no commit landed, then commits two records in turn
    /// and sends the first of those commits again, a copy the second overtook, which must not
    /// put its record back.
    pub(super) async fn landed_keys_are_answered_and_stale_commits_refused(
        &self,
    ) -> Result<(), Violation> {
        let mut first = self.staged(self.destination, &[1]).await?;
        let sent = keyed(&first, meta(&[1], Vec::new()));
        let original = commit(&mut first, sent.clone()).await?;
        let other = PipelineId::parse(format!("{}_other", self.name()))
            .map_err(|error| Violation::from(format!("pipeline id: {error}")))?;
        let mut elsewhere = self.open_as(self.destination, other).await?;
        commit(&mut elsewhere, meta(&[], Vec::new())).await?;
        let again = answered("its own session", &mut first, &sent).await?;
        same(&again, &original, "its own session")?;
        let mut peer = self.staged(self.peer, &[1]).await?;
        let replayed = answered("a newer session", &mut peer, &sent).await?;
        same(&replayed, &original, "a newer session")?;
        expect_rows(&self.published_rows().await?, &[1])?;
        let unexpected = CommitMeta {
            after: None,
            ..keyed(&peer, meta(&[1], Vec::new()))
        };
        stale("expecting no commit landed", &mut peer, &unexpected).await?;
        expect_rows(&self.published_rows().await?, &[1])?;
        let overtaken = keyed(&peer, meta(&[], vec![put(b"1")]));
        commit(&mut peer, overtaken.clone()).await?;
        commit(&mut peer, meta(&[], vec![put(b"2")])).await?;
        stale("a later commit overtook", &mut peer, &overtaken).await?;
        let kept = self.open(self.destination).await?.state;
        if kept.contains(&record(b"1")) {
            return Err(format!("the overtaken commit put its record back: {kept:?}").into());
        }
        Ok(())
    }
}

/// The record a clause commits holding `value`.
fn record(value: &'static [u8]) -> StateRecord {
    StateRecord {
        key: "certify".to_owned(),
        value: Bytes::from_static(value),
    }
}

/// The change putting [`record`] of `value`.
fn put(value: &'static [u8]) -> StateChange {
    StateChange::Put(record(value))
}

/// What `opened`'s session answers `meta` with, sent again from `whose` session.
async fn answered(
    whose: &str,
    opened: &mut OpenedSession,
    meta: &CommitMeta,
) -> Result<Receipt, Violation> {
    bounded("commit", opened.session.commit(meta))
        .await?
        .map_err(|error| Violation::from(format!("the landed commit sent from {whose}: {error}")))
}

/// Checks that the landed commit, sent again from `whose` session, was answered with its receipt.
fn same(answered: &Receipt, original: &Receipt, whose: &str) -> Result<(), Violation> {
    if answered == original {
        return Ok(());
    }
    Err(format!(
        "the landed commit sent again from {whose} was answered with {answered:?}, not its \
         receipt {original:?}"
    )
    .into())
}

/// Checks that `opened`'s session refuses `meta`, a commit `why` it is stale, as fenced and
/// coded `commit_stale`.
async fn stale(why: &str, opened: &mut OpenedSession, meta: &CommitMeta) -> Result<(), Violation> {
    let refused: Result<_> = bounded("commit", opened.session.commit(meta)).await?;
    match refused {
        Err(error)
            if error.kind() == ConnectorErrorKind::Fenced && error.code() == Some(COMMIT_STALE) =>
        {
            Ok(())
        }
        Err(error) => Err(format!(
            "a commit {why} failed with {:?} coded {:?}, not Fenced coded {COMMIT_STALE}: {error}",
            error.kind(),
            error.code()
        )
        .into()),
        Ok(receipt) => Err(format!("a commit {why} landed: {receipt:?}").into()),
    }
}
