//! The landed key: what a destination keeps of a pipeline in the row every commit locks, and how
//! it answers a commit by it.

#[cfg(kani)]
mod proofs;
#[cfg(test)]
mod tests;

use crate::commit::{CommitKey, CommitMeta, Receipt};
use crate::error::{COMMIT_STALE, ConnectorError, Result};
use crate::id::Epoch;

/// A pipeline's fence row: the epoch of its newest session and the receipt of the last commit
/// that landed.
///
/// A commit reads the row under its transaction's write lock, with no condition on the epoch,
/// asks [`answer`], and where the commit applies writes its own receipt into the row in the same
/// transaction: a destination that wrote it outside the commit's transaction could apply a
/// commit twice.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fence {
    /// The epoch of the pipeline's newest session.
    pub epoch: Epoch,
    /// The receipt of the last commit that landed; none before the first.
    pub landed: Option<Receipt>,
}

/// How a destination answers a commit, by its fence row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Answer<'a> {
    /// The commit landed already: its receipt is the answer, and nothing is published.
    Landed(&'a Receipt),
    /// A newer session fenced the commit's: nothing is published.
    Fenced,
    /// The commit expects another key landed than the landed key, as a copy a later commit
    /// overtook does: nothing is published.
    Stale,
    /// The commit applies.
    Apply,
}

/// How a destination whose fence row is `fence` answers the commit keyed `key` that expects
/// `after` landed: the landed key with its receipt, whichever session sends it; then a commit of
/// another epoch than the row's is fenced; then one expecting another key landed is stale; and
/// the rest apply.
pub fn answer(fence: &Fence, key: CommitKey, after: Option<CommitKey>) -> Answer<'_> {
    let landed = fence.landed.as_ref();
    if let Some(receipt) = landed.filter(|receipt| receipt.key == key) {
        return Answer::Landed(receipt);
    }
    if fence.epoch != key.epoch {
        return Answer::Fenced;
    }
    if landed.map(|receipt| receipt.key) != after {
        return Answer::Stale;
    }
    Answer::Apply
}

impl Fence {
    /// What a destination does with `meta` before it publishes anything, as [`answer`] decides:
    /// the receipt of a commit that landed already, which it answers with and publishes nothing,
    /// or none for a commit to apply.
    ///
    /// # Errors
    ///
    /// `Fenced` where a newer session fenced the commit's, and `Fenced` coded `commit_stale`
    /// where the commit expects another key landed than the landed key.
    pub fn decide(&self, meta: &CommitMeta) -> Result<Option<Receipt>> {
        match answer(self, meta.key, meta.after) {
            Answer::Landed(receipt) => Ok(Some(receipt.clone())),
            Answer::Apply => Ok(None),
            Answer::Fenced => Err(ConnectorError::fenced(format!(
                "the pipeline is at epoch {}, past the commit's {}",
                self.epoch, meta.key.epoch
            ))),
            Answer::Stale => {
                let landed = self.landed.as_ref().map(|receipt| receipt.key);
                Err(ConnectorError::fenced(format!(
                    "{} expects {} landed, and {} did",
                    meta.key,
                    shown(meta.after),
                    shown(landed)
                ))
                .with_code(COMMIT_STALE))
            }
        }
    }
}

/// `key` as a message names it.
fn shown(key: Option<CommitKey>) -> String {
    key.map_or_else(|| "no commit".to_owned(), |key| key.to_string())
}
