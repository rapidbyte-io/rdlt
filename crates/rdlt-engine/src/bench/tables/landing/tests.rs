use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use rdlt_connector::{BoxFuture, Capabilities, Destination, OpenContext, OpenedSession, Result};

use super::{Landing, until_landed};
use crate::env::{Env, InlineEnv};

/// A destination that carries three calls at once.
struct Carrying(Capabilities);

impl Destination for Carrying {
    fn capabilities(&self) -> &Capabilities {
        &self.0
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn open<'a>(&'a self, _context: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        unreachable!("the calls a destination carries need no session")
    }

    fn max_calls(&self) -> Option<NonZeroUsize> {
        NonZeroUsize::new(3)
    }
}

#[test]
fn a_landing_destination_carries_as_many_calls_as_its_destination() {
    let carrying = Arc::new(Carrying(Capabilities::minimal()));
    let landing = Landing::new(carrying, Arc::default());
    assert_eq!(landing.max_calls(), NonZeroUsize::new(3));
}

#[tokio::test(start_paused = true)]
async fn a_wait_for_rows_ends_once_they_have_landed_or_at_its_limit() {
    let (landed, limit) = (AtomicU64::new(5), Duration::from_secs(30));
    let started = InlineEnv.instant();
    let waited = || InlineEnv.instant().saturating_duration_since(started);
    until_landed(&landed, 5, limit).await;
    assert_eq!(waited(), Duration::ZERO, "every row had landed");
    until_landed(&landed, 6, limit).await;
    assert!(waited() >= limit, "a row never landed: {:?}", waited());
}
