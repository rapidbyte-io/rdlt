//! `D-CHECK`: a check agrees with opening a session.

use super::Bench;
use crate::testing::{Violation, bounded, bounded_call};

impl Bench<'_> {
    /// Checks the destination, and opens and closes a session; they must agree.
    pub(super) async fn check_agrees_with_open(&self) -> Result<(), Violation> {
        let checked = bounded_call("check", self.destination.check()).await;
        let opened = match self.open(self.destination, 1).await {
            Ok(opened) => bounded("close", opened.session.close())
                .await?
                .map_err(|error| Violation::from(format!("close: {error}"))),
            Err(violation) => Err(violation),
        };
        match (checked, opened) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(Violation { reason, .. }), Ok(())) => {
                Err(format!("check failed ({reason}), yet a session opened").into())
            }
            (Ok(()), Err(Violation { reason, .. })) => {
                Err(format!("check succeeded, yet opening a session failed: {reason}").into())
            }
            (Err(violation), Err(_)) => Err(violation.of("check failed")),
        }
    }
}
