//! `D-OWNED`: a table belongs to the pipeline that created it.

use super::{Bench, commit, expect_rows, meta};
use crate::error::{ConnectorError, ConnectorErrorKind};
use crate::id::PipelineId;
use crate::testing::{Violation, bounded};
use crate::{OpenContext, TableChange};

impl Bench<'_> {
    /// Another pipeline's schema change and writer for the clause's table are refused as
    /// `table_owned`, the owner's rows stay published, and the owner may create it again.
    pub(super) async fn tables_belong_to_their_pipeline(&self) -> Result<(), Violation> {
        let mut owner = self.staged(self.destination, 1, &[1]).await?;
        commit(
            &mut owner.session,
            &meta(self.load_id(1), owner.epoch, &[1], Vec::new()),
        )
        .await?;
        let other = PipelineId::parse(format!("{}_other", self.name()))
            .expect("certification pipeline ids are valid");
        let context = OpenContext {
            pipeline: other,
            load_id: self.load_id(2),
        };
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
        expect_rows(&self.published_rows().await?, &[1])?;
        let mut again = self.open(self.destination, 3).await?;
        bounded("apply_schema", again.session.apply_schema(&create))
            .await?
            .map_err(|error| Violation::from(format!("the owner's apply_schema: {error}")))
    }
}

/// Whether `called` refused another pipeline's table as `table_owned`.
fn owned(call: &str, outcome: Result<(), ConnectorError>) -> Result<(), Violation> {
    match outcome {
        Err(error)
            if error.kind() == ConnectorErrorKind::Config
                && error.code() == Some("table_owned") =>
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
