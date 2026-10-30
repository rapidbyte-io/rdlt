//! `D-RETRY`: a commit whose try failed is sent again, the same commit by the same key, from its
//! session, with the session's other work between: a try lost before it landed leaves the
//! commit to land whole once; one that landed is answered with its receipt and publishes nothing
//! again; and one that failed after its generation was swapped in published nothing, and lands
//! whole once what failed it is gone.

use super::dropped::dropping_of;
use super::evolving::GENERATION;
use super::{Bench, commit, expect_rows, keyed, meta, rows};
use crate::commit::{CommitMeta, DroppedTable, Receipt};
use crate::destination::{DestinationWriter, OpenedSession, TableRef};
use crate::error::{Result, TABLE_OWNED};
use crate::id::SegmentId;
use crate::testing::{Violation, bounded, bounded_call};

impl Bench<'_> {
    /// Sends again a commit whose first try was lost before it landed, one whose try landed and
    /// whose answer was lost, and, where the destination replaces and drops tables, one that
    /// failed after it swapped its generation in.
    pub(super) async fn failed_commits_are_taken_again(&self) -> Result<(), Violation> {
        self.lost_tries_are_taken_again().await?;
        let capabilities = self.destination.capabilities();
        if capabilities.write_modes.replace && capabilities.drop_tables {
            self.commits_failed_after_their_swap_are_taken_again()
                .await?;
        }
        Ok(())
    }

    /// Commits segment 1 whose first try was lost before it landed, then segment 2 whose try
    /// landed and whose answer was lost, each sent again once its session applied its table's
    /// creation again and opened a writer of another table, then segment 3.
    async fn lost_tries_are_taken_again(&self) -> Result<(), Violation> {
        let mut opened = self.staged(self.destination, &[1, 2, 3]).await?;
        let before = keyed(&opened, meta(&[1], Vec::new()));
        self.between(&mut opened, "lost_before").await?;
        let landed = sent("lost before it landed", &mut opened, &before).await?;
        if landed.key != before.key {
            return Err(format!("{} was answered with {}", before.key, landed.key).into());
        }
        expect_rows(&self.published_rows().await?, &[1])?;
        let after = keyed(&opened, meta(&[2], Vec::new()));
        let first = sent("that landed", &mut opened, &after).await?;
        self.between(&mut opened, "lost_after").await?;
        let second = sent("whose answer was lost", &mut opened, &after).await?;
        if second != first {
            return Err(format!(
                "a landed commit sent again from its session was answered with {second:?}, not \
                 its receipt {first:?}"
            )
            .into());
        }
        expect_rows(&self.published_rows().await?, &[1, 2])?;
        commit(&mut opened, meta(&[3], Vec::new())).await?;
        expect_rows(&self.published_rows().await?, &[1, 2, 3])
    }

    /// What a session does between two tries of a commit: applies its table's creation again and
    /// opens a writer of another table, called after `suffix`.
    async fn between(&self, opened: &mut OpenedSession, suffix: &str) -> Result<(), Violation> {
        self.writer(&mut opened.session).await?;
        self.writer_of(&mut opened.session, &self.other_table(suffix))
            .await?;
        Ok(())
    }

    /// Commits a generation of a table of its own whose commit also drops a table another
    /// pipeline owns, which fails it, as the drop comes after the swap where a destination drops
    /// as it commits; once that pipeline dropped its table, the same commit lands whole.
    async fn commits_failed_after_their_swap_are_taken_again(&self) -> Result<(), Violation> {
        let base = self.other_table("swapped");
        let owned = self.other_table("owned");
        let mut other = self.other().await?;
        let mut writer = self.writer_of(&mut other.session, &owned).await?;
        stage(&mut writer, 1).await?;
        commit(&mut other, meta(&[1], Vec::new())).await?;
        let mut opened = self.open(self.destination).await?;
        let mut writer = self.writer_of(&mut opened.session, &base).await?;
        stage(&mut writer, 2).await?;
        commit(&mut opened, meta(&[2], Vec::new())).await?;
        let generation = TableRef {
            generation: Some(GENERATION),
            ..base.clone()
        };
        let mut writer = bounded_call("writer", opened.session.writer(&generation)).await?;
        stage(&mut writer, 3).await?;
        let swapping = keyed(
            &opened,
            CommitMeta {
                finish_generations: vec![(base.path.clone(), GENERATION)],
                drop_tables: vec![DroppedTable {
                    path: owned.path.clone(),
                    name: owned.name.clone(),
                }],
                ..meta(&[3], Vec::new())
            },
        );
        // A destination that lets the drop through fails `D-DROP`; nothing failed here to see.
        if bounded("commit", opened.session.commit(&swapping))
            .await?
            .is_ok()
        {
            return Err(Violation::unobserved(
                "a commit dropping another pipeline's table landed, so none failed after its swap",
            ));
        }
        expect_rows(&self.rows_of(&base).await?, &[2])?;
        commit(&mut other, dropping_of(&owned)).await?;
        let again = bounded("commit", opened.session.commit(&swapping)).await?;
        if again
            .as_ref()
            .is_err_and(|error| error.code() == Some(TABLE_OWNED))
        {
            return Err(Violation::unobserved(
                "the table the commit drops stayed another pipeline's once that pipeline dropped it",
            ));
        }
        again.map_err(|error| {
            Violation::from(format!("a commit that failed after its swap: {error}"))
        })?;
        expect_rows(&self.rows_of(&base).await?, &[3])
    }
}

/// Stages segment `segment`'s rows through `writer`, durably.
async fn stage(writer: &mut Box<dyn DestinationWriter>, segment: u64) -> Result<(), Violation> {
    bounded_call("write", writer.write(SegmentId(segment), rows(segment))).await?;
    bounded_call("flush", writer.flush()).await?;
    Ok(())
}

/// The receipt `opened`'s session answers `meta` with, a commit `which`; it is the session's last
/// that landed.
async fn sent(
    which: &str,
    opened: &mut OpenedSession,
    meta: &CommitMeta,
) -> Result<Receipt, Violation> {
    let receipt = bounded("commit", opened.session.commit(meta))
        .await?
        .map_err(|error| Violation::from(format!("a commit {which}: {error}")))?;
    opened.landed = Some(receipt.clone());
    Ok(receipt)
}
