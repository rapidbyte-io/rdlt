//! `D-DROP`: a commit drops the tables it names, releasing them to any pipeline.

use super::{Bench, commit, expect_rows, meta, rows};
use crate::commit::DroppedTable;
use crate::error::{ConnectorError, ConnectorErrorKind};
use crate::id::{Epoch, PipelineId, SegmentId};
use crate::testing::{Violation, bounded};
use crate::{CommitMeta, OpenContext, OpenedSession};

impl Bench<'_> {
    /// A commit dropping the clause's table leaves nothing of it; dropping it again changes
    /// nothing; a session opened before the drop may not claim the table again; another pipeline
    /// may then create, load and own a table of that name; and the first pipeline's later drop of
    /// it is refused as `table_owned`.
    pub(super) async fn drops_release_tables(&self) -> Result<(), Violation> {
        let mut owner = self.staged(self.destination, 1, &[1]).await?;
        commit(
            &mut owner.session,
            &meta(self.load_id(1), owner.epoch, &[1], Vec::new()),
        )
        .await?;
        if self.probe.reads() {
            expect_rows(&self.published_rows().await?, &[1])?;
        }
        for load in [2, 3] {
            let mut dropping = self.open(self.destination, load).await?;
            let dropped = self.dropping(load, dropping.epoch);
            commit(&mut dropping.session, &dropped).await?;
            if self.probe.reads() {
                expect_rows(&self.published_rows().await?, &[])?;
            }
        }
        // The session that loaded the table before the drop may no longer claim it.
        let stale = owner.session.writer(&self.table()).await.map(drop);
        fenced(stale)?;
        let mut other = self.other(4).await?;
        let mut writer = self.writer(&mut other.session).await?;
        writer
            .write(SegmentId(2), rows(2))
            .await
            .map_err(|error| Violation::from(format!("another pipeline's write: {error}")))?;
        writer
            .flush()
            .await
            .map_err(|error| Violation::from(format!("another pipeline's flush: {error}")))?;
        commit(
            &mut other.session,
            &meta(self.load_id(4), other.epoch, &[2], Vec::new()),
        )
        .await?;
        if self.probe.reads() {
            expect_rows(&self.published_rows().await?, &[2])?;
        }
        let mut late = self.open(self.destination, 5).await?;
        let dropped = self.dropping(5, late.epoch);
        let refused = bounded("commit", late.session.commit(&dropped)).await?;
        owned(refused.map(drop))?;
        if self.probe.reads() {
            expect_rows(&self.published_rows().await?, &[2])?;
        }
        Ok(())
    }

    /// The commit of `load`, in a session opened at `epoch`, that drops the clause's table.
    fn dropping(&self, load: u8, epoch: Epoch) -> CommitMeta {
        let table = self.table();
        CommitMeta {
            drop_tables: vec![DroppedTable {
                path: table.path,
                name: table.name,
            }],
            ..meta(self.load_id(load), epoch, &[], Vec::new())
        }
    }

    /// A session of another pipeline, for `load`.
    async fn other(&self, load: u8) -> Result<OpenedSession, Violation> {
        let context = OpenContext {
            pipeline: PipelineId::parse(format!("{}_next", self.name()))
                .expect("certification pipeline ids are valid"),
            load_id: self.load_id(load),
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
            if error.kind() == ConnectorErrorKind::Config
                && error.code() == Some("table_owned") =>
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
