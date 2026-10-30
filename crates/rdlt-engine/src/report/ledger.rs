//! A run's attempts as it makes them, each folded into its report as it ends, so a run that
//! retries for ever stays bounded.

#[cfg(test)]
mod tests;

use rdlt_connector::PipelineId;

use super::{AttemptRecord, Pending, Report};
use crate::error::Error;

/// A run's report so far, and a failed attempt's commit whose answer was lost.
#[derive(Debug)]
pub(crate) struct Ledger {
    report: Report,
    /// A failed attempt's commit in flight, credited to it once a later attempt's open finds its
    /// key landed.
    unresolved: Option<Pending>,
}

/// What a commit credited once a later attempt found it landed published.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Credit {
    pub(crate) rows: u64,
    pub(crate) bytes: u64,
}

impl Ledger {
    /// The ledger of a run of `pipeline` that has made no attempt.
    pub(crate) fn new(pipeline: PipelineId) -> Self {
        Self {
            report: Report::new(pipeline),
            unresolved: None,
        }
    }

    /// Records `attempt`, which `failed` or not: whether a commit of it moved the load on, a
    /// commit that left every partition where it stood and published nothing being no progress,
    /// and what it credited.
    ///
    /// A failed attempt's commit in flight is credited, to the attempt whose epoch its key names,
    /// once a later attempt's open finds that key the last that landed; where it finds another,
    /// the commit never landed or a later one landed on it, and goes uncredited. An attempt that
    /// never opened found nothing, so the commit stays in flight for the next.
    ///
    /// # Errors
    ///
    /// `receipt_overflow` where the attempt's receipts take the run's totals past what they
    /// hold.
    pub(crate) fn record(
        &mut self,
        mut attempt: AttemptRecord,
        failed: bool,
    ) -> Result<(bool, Option<Credit>), Error> {
        let log = &mut attempt.log;
        let mut credited = None;
        if let Some((_, landed)) = &log.opened
            && let Some(pending) = self.unresolved.take()
            && let Some(commit) = landed.as_ref().and_then(|landed| pending.landed(landed))
        {
            let credit = Credit {
                rows: commit.receipt.rows,
                bytes: commit.receipt.bytes,
            };
            self.report.credit(commit)?;
            credited = Some(credit);
        }
        if failed && let Some(pending) = log.pending.take() {
            self.unresolved = Some(pending);
        }
        let progressed = log.progressed;
        self.report.absorb(attempt)?;
        Ok((progressed, credited))
    }

    /// The run's report so far, of the attempts that ended.
    pub(crate) fn report(&self) -> &Report {
        &self.report
    }

    /// The run's report of the attempts that ended, the ledger given up.
    pub(crate) fn into_report(self) -> Report {
        self.report
    }
}
