//! A certification as the command line runs it: bounded, ended by the signals that end a
//! process, and leaving nothing it spawned however it ends.

#[cfg(test)]
mod tests;

use std::future::Future;
use std::time::Instant;

use rdlt_certify::{Observed, Report, Target, unfinished};
use rdlt_host::{Interrupts, Lingering, StopsSpawned};
use tokio::runtime::Runtime;

use super::Ended;
use crate::Exit;
use crate::stopping::STOPPING;

/// Why a role's clauses fail when the certification's timeout ends it.
const OVERDUE: &str = "the certification took longer than its timeout: see --timeout";

/// A role's certification, as the command line runs it.
pub(super) type Certifying<'a> = std::pin::Pin<&'a mut dyn Future<Output = Report>>;

/// What runs a certification: the runtime its roles are certified on, what hears the signals
/// that end it, on a runtime of its own that outlives the first, and its bound.
pub(super) struct Session {
    runtime: Runtime,
    signals: Runtime,
    interrupts: Interrupts,
    until: Option<Instant>,
}

impl Session {
    /// A session bounded by `until`, hearing signals from here on: one ends the certification,
    /// which then stops what it spawned.
    pub(super) fn start(until: Option<Instant>) -> Result<Self, Ended> {
        let failed = |error| Ended(Exit::Io, format!("starting the runtime failed: {error}"));
        let cores = rdlt::Cores::try_from_host().map_err(|error| {
            Ended(
                Exit::Io,
                format!("the host's count of cores is unknown: {error}"),
            )
        })?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(cores.runtime_workers().get())
            .enable_all()
            .build()
            .map_err(failed)?;
        let mut signals = tokio::runtime::Builder::new_multi_thread();
        let signals = signals
            .worker_threads(1)
            .enable_io()
            .build()
            .map_err(failed)?;
        let interrupts = {
            let _signals = signals.enter();
            Interrupts::listen()
                .map_err(|error| Ended(Exit::Io, format!("no signal is heard: {error}")))?
        };
        Ok(Self {
            runtime,
            signals,
            interrupts,
            until,
        })
    }

    /// The report of `target` certified as `role` by `certifying`, which tells `observed` what
    /// it finds: cut where the bound passed, and no connector started once it has.
    pub(super) fn ran_to(
        &mut self,
        target: &Target,
        role: rdlt_connector::Role,
        observed: &Observed,
        certifying: Certifying<'_>,
    ) -> Result<Report, Ended> {
        let until = self.until;
        if until.is_some_and(|until| Instant::now() >= until) {
            return Ok(unfinished(target, role, observed, OVERDUE));
        }
        let interrupts = &mut self.interrupts;
        let report = self.runtime.block_on(async {
            tokio::select! {
                biased;
                status = interrupts.heard() => Err(interrupted(status)),
                report = within(until, certifying) => Ok(report),
            }
        })?;
        Ok(report.unwrap_or_else(|| unfinished(target, role, observed, OVERDUE)))
    }

    /// Stops what the certification spawned, through `stops`, and answers what lingers, and
    /// the exit status of the last signal heard, `heard` being that of one that ended the
    /// certification; the certification's runtime is dropped first, so nothing it left running
    /// starts a connector again.
    pub(super) fn stopped(
        self,
        stops: StopsSpawned,
        heard: Option<i32>,
    ) -> (Result<(), Lingering>, Option<i32>) {
        let Self {
            runtime,
            signals,
            mut interrupts,
            ..
        } = self;
        runtime.shutdown_timeout(STOPPING);
        crate::stopping::stopped(&signals, stops, &mut interrupts, heard)
    }
}

/// The end of a certification that was interrupted, or asked to terminate, with the exit
/// `status` a process so ended has.
fn interrupted(status: i32) -> Ended {
    Ended(
        Exit::Signalled(status),
        "interrupted: nothing more is certified".to_owned(),
    )
}

/// The exit status of the signal that ended a certification with `ended`, if one did.
pub(super) fn signalled(ended: &Ended) -> Option<i32> {
    match ended.0 {
        Exit::Signalled(status) => Some(status),
        _ => None,
    }
}

/// What a certification that `certified` amounts to once what it spawned `stopped`, and
/// `heard` is the exit status of the last signal heard, if one was: its reports only when it
/// ran to its end, nothing interrupted it and nothing it spawned lingers.
///
/// A signal's exit status stands before any other, and the certification's own failure
/// before a group that lingers, which is said beside it all the same.
pub(super) fn ending<T>(
    certified: Result<T, Ended>,
    stopped: Result<(), Lingering>,
    heard: Option<i32>,
) -> Result<T, Ended> {
    let Ended(code, message) = match (certified, heard) {
        (Err(ended), Some(status)) if signalled(&ended).is_some() => interrupted(status),
        (Err(ended), _) => ended,
        (Ok(_), Some(status)) => interrupted(status),
        (Ok(reports), None) => match stopped {
            Ok(()) => return Ok(reports),
            Err(lingering) => return Err(Ended(Exit::Io, lingering.to_string())),
        },
    };
    match stopped {
        Ok(()) => Err(Ended(code, message)),
        Err(lingering) => Err(Ended(code, format!("{message}; {lingering}"))),
    }
}

/// `certifying`'s report, unless `until` comes first.
pub(super) async fn within(
    until: Option<Instant>,
    certifying: impl Future<Output = Report>,
) -> Option<Report> {
    match until {
        Some(until) => tokio::time::timeout_at(until.into(), certifying).await.ok(),
        None => Some(certifying.await),
    }
}
