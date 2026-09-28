//! `D-FENCE`: a session opened before the latest one cannot commit.

use super::{Bench, expect_ids, meta};
use crate::error::ConnectorErrorKind;
use crate::testing::{Violation, bounded};

impl Bench<'_> {
    /// A worker's session is fenced by an open through another connection.
    pub(super) async fn stale_sessions_are_fenced(&self) -> Result<(), Violation> {
        let mut stale = self.staged(self.destination, 1, &[1]).await?;
        let _latest = self.open(self.peer, 2).await?;
        let meta = meta(self.load_id(1), stale.epoch, &[1], Vec::new());
        match bounded("commit", stale.session.commit(&meta)).await? {
            Err(error) if error.kind() == ConnectorErrorKind::Fenced => {
                expect_ids(&self.published_ids().await?, &[])
            }
            Err(error) => Err(format!(
                "the stale commit failed with {:?}, not Fenced: {error}",
                error.kind()
            )
            .into()),
            Ok(_) => Err("a session opened before the latest one committed".into()),
        }
    }
}
