//! The wait for gathered pushes' deadline, kept across the ingest loop's turns while the
//! deadline stays where it is.

use std::time::Instant;

use crate::env::{Env, Sleep};

/// The sleep until the coalescer's deadline: made once for a deadline, and made again only once
/// the deadline moves or has passed.
#[derive(Default)]
pub(super) struct Due {
    until: Option<(Instant, Sleep)>,
}

impl Due {
    /// Follows `deadline`: keeps the sleep made for it, makes one on `env` where it moved, and
    /// drops it where there is none.
    pub(super) fn follow(&mut self, deadline: Option<Instant>, env: &dyn Env) {
        let Some(deadline) = deadline else {
            self.until = None;
            return;
        };
        if self
            .until
            .as_ref()
            .is_some_and(|(until, _)| *until == deadline)
        {
            return;
        }
        let wait = deadline.saturating_duration_since(env.instant());
        self.until = Some((deadline, env.sleep(wait)));
    }

    /// Completes once the deadline followed has passed, and forgets it; never where none is.
    pub(super) async fn passed(&mut self) {
        match &mut self.until {
            Some((_, sleep)) => {
                sleep.await;
                self.until = None;
            }
            None => std::future::pending().await,
        }
    }
}

#[cfg(test)]
mod tests;
