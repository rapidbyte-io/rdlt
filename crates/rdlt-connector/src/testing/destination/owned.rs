//! `D-OWNED`: a table belongs to the pipeline that created it.

use super::{Bench, commit, expect_rows, meta, try_commit};
use crate::error::{ConnectorError, ConnectorErrorKind, TABLE_OWNED};
use crate::id::{GenerationId, PipelineId};
use crate::testing::{Violation, bounded};
use crate::{CommitMeta, OpenContext, TableChange};

impl Bench<'_> {
    /// Another pipeline's schema change and writer for the clause's table are refused as
    /// `table_owned`, its generation swap too unless it changes nothing, and the owner keeps
    /// loading it: it creates the table again, writes and commits, and, where the probe reads
    /// them, both its commits' rows are published.
    pub(super) async fn tables_belong_to_their_pipeline(&self) -> Result<(), Violation> {
        let mut owner = self.staged(self.destination, &[1]).await?;
        commit(&mut owner, meta(&[1], Vec::new())).await?;
        let other = PipelineId::parse(format!("{}_other", self.name()))
            .expect("certification pipeline ids are valid");
        let context = OpenContext { pipeline: other };
        let mut intruder = bounded("open", self.peer.open(&context))
            .await?
            .map_err(|error| Violation::from(format!("open: {error}")))?;
        let create = TableChange::Create {
            table: self.table(),
            schema: super::schema(),
        };
        owned("apply_schema", intruder.session.apply_schema(&create).await)?;
        owned(
            "writer",
            intruder.session.writer(&self.table()).await.map(drop),
        )?;
        // A generation of the table swapped in by another pipeline would replace the owner's
        // rows: refused, or leaving them as they were.
        let swap = CommitMeta {
            finish_generations: vec![(self.table().path, GenerationId(1))],
            ..meta(&[], Vec::new())
        };
        match try_commit(&mut intruder, swap).await? {
            Ok(_) => {}
            refused => owned("generation swap", refused.map(drop))?,
        }
        let mut again = self.staged(self.destination, &[2]).await?;
        commit(&mut again, meta(&[2], Vec::new())).await?;
        if self.probe.reads() {
            expect_rows(&self.published_rows().await?, &[1, 2])?;
        }
        Ok(())
    }
}

/// Whether `called` refused another pipeline's table as `table_owned`.
fn owned(call: &str, outcome: Result<(), ConnectorError>) -> Result<(), Violation> {
    match outcome {
        Err(error)
            if error.kind() == ConnectorErrorKind::Config && error.code() == Some(TABLE_OWNED) =>
        {
            Ok(())
        }
        Err(error) => Err(format!(
            "another pipeline's {call} failed with {:?} {:?}, not Config table_owned: {error}",
            error.kind(),
            error.code()
        )
        .into()),
        Ok(()) => Err(format!("another pipeline's {call} of the table succeeded").into()),
    }
}
