//! `D-FENCE`: a session opened before the latest one cannot commit.

use super::{Bench, expect_rows, meta, try_commit};
use crate::error::ConnectorErrorKind;
use crate::testing::Violation;

impl Bench<'_> {
    /// A worker's session is fenced by an open through another connection.
    pub(super) async fn stale_sessions_are_fenced(&self) -> Result<(), Violation> {
        let mut stale = self.staged(self.destination, &[1]).await?;
        let _latest = self.open(self.peer).await?;
        match try_commit(&mut stale, meta(&[1], Vec::new())).await? {
            Err(error) if error.kind() == ConnectorErrorKind::Fenced => {
                expect_rows(&self.published_rows().await?, &[])
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
