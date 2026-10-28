//! `D-DROP`: a commit drops the tables it names, releasing them to any pipeline.

use super::{Bench, commit, expect_rows, meta, rows, try_commit};
use crate::commit::DroppedTable;
use crate::error::{ConnectorError, ConnectorErrorKind, TABLE_OWNED};
use crate::id::{PipelineId, SegmentId};
use crate::testing::{Violation, bounded};
use crate::{CommitMeta, OpenContext, OpenedSession};

impl Bench<'_> {
    /// A commit dropping the clause's table leaves nothing of it; dropping it again changes
    /// nothing; a session opened before the drop may not claim the table again; another pipeline
    /// may then create, load and own a table of that name; and the first pipeline's later drop of
    /// it is refused as `table_owned`.
    pub(super) async fn drops_release_tables(&self) -> Result<(), Violation> {
        let mut owner = self.staged(self.destination, &[1]).await?;
        commit(&mut owner, meta(&[1], Vec::new())).await?;
        if self.probe.reads() {
            expect_rows(&self.published_rows().await?, &[1])?;
        }
        for _ in 0..2 {
            let mut dropping = self.open(self.destination).await?;
            let dropped = self.dropping();
            commit(&mut dropping, dropped).await?;
            if self.probe.reads() {
                expect_rows(&self.published_rows().await?, &[])?;
            }
        }
        // The session that loaded the table before the drop may no longer claim it.
        let stale = owner.session.writer(&self.table()).await.map(drop);
        fenced(stale)?;
        let mut other = self.other().await?;
        let mut writer = self.writer(&mut other.session).await?;
        writer
            .write(SegmentId(2), rows(2))
            .await
            .map_err(|error| Violation::from(format!("another pipeline's write: {error}")))?;
        writer
            .flush()
            .await
            .map_err(|error| Violation::from(format!("another pipeline's flush: {error}")))?;
        commit(&mut other, meta(&[2], Vec::new())).await?;
        if self.probe.reads() {
            expect_rows(&self.published_rows().await?, &[2])?;
        }
        let mut late = self.open(self.destination).await?;
        let dropped = self.dropping();
        let refused = try_commit(&mut late, dropped).await?;
        owned(refused.map(drop))?;
        if self.probe.reads() {
            expect_rows(&self.published_rows().await?, &[2])?;
        }
        Ok(())
    }

    /// A commit that drops the clause's table.
    pub(super) fn dropping(&self) -> CommitMeta {
        let table = self.table();
        CommitMeta {
            drop_tables: vec![DroppedTable {
                path: table.path,
                name: table.name,
            }],
            ..meta(&[], Vec::new())
        }
    }

    /// A session of another pipeline.
    async fn other(&self) -> Result<OpenedSession, Violation> {
        let context = OpenContext {
            pipeline: PipelineId::parse(format!("{}_next", self.name()))
                .expect("certification pipeline ids are valid"),
        };
        bounded("open", self.peer.open(&context))
            .await?
            .map_err(|error| Violation::from(format!("another pipeline's open: {error}")))
    }
}

/// Whether a claim by a session a newer one fenced was refused as fenced.
fn fenced(outcome: Result<(), ConnectorError>) -> Result<(), Violation> {
    match outcome {
        Err(error) if error.kind() == ConnectorErrorKind::Fenced => Ok(()),
        Err(error) => Err(format!(
            "a fenced session's claim of a dropped table failed with {:?}, not Fenced: {error}",
            error.kind()
        )
        .into()),
        Ok(()) => Err("a fenced session claimed a table a drop released".into()),
    }
}

/// Whether a drop of another pipeline's table was refused as `table_owned`.
fn owned(outcome: Result<(), ConnectorError>) -> Result<(), Violation> {
    match outcome {
        Err(error)
            if error.kind() == ConnectorErrorKind::Config && error.code() == Some(TABLE_OWNED) =>
        {
            Ok(())
        }
        Err(error) => Err(format!(
            "dropping another pipeline's table failed with {:?} {:?}, not Config table_owned: \
             {error}",
            error.kind(),
            error.code()
        )
        .into()),
        Ok(()) => Err("dropping another pipeline's table succeeded".into()),
    }
}
