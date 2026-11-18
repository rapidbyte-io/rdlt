//! A clause's outcome: what the destination declares, the identifiers its rules give and what
//! its probe reads settle it before the clause's check runs.

use super::{Bench, clauses, evolving};
use crate::testing::{Outcome, outcome, timed};

/// Why a clause that reads published data is not observed without a probe that reads it.
const UNREAD: &str = "the destination's published data cannot be read";

impl Bench<'_> {
    /// The outcome of the clause `id`, where `unnamed` says why the destination's rules leave a
    /// table or column the clauses name no identifier, if they do.
    pub(super) async fn judged(&self, id: &str, unnamed: Option<&str>) -> Outcome {
        let unread = !self.probe.reads() && clauses::PROBED.contains(&id);
        let named = !clauses::NAMELESS.contains(&id);
        let inapplicable = evolving::inapplicable(self.destination, id);
        let unobserved = evolving::unobserved(self.destination, id);
        match (inapplicable, unobserved, unnamed) {
            (Some(reason), _, _) => Outcome::Inapplicable(reason.into()),
            (None, Some(reason), _) => Outcome::Unobserved(reason.into()),
            (None, None, Some(reason)) if named => Outcome::Unobserved(reason.into()),
            (None, None, _) if unread => Outcome::Unobserved(UNREAD.into()),
            (None, None, _) => outcome(timed(self.check(id)).await),
        }
    }
}
