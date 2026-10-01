//! Conformance clauses for connectors reached over the wire protocol (§20.8).
//!
//! The source and destination clauses of `rdlt_connector::testing` run through the protocol
//! against a connector served in this process, spawned from its binary, or listening at an
//! endpoint; the protocol's own clauses run through a client that speaks it raw.
//!
//! ```no_run
//! # async fn example() {
//! use rdlt_certify::{Target, certify_source};
//! use rdlt_connector::serve::Served;
//! # let served = Served::new();
//! let report = certify_source(&Target::served(served), serde_json::json!({})).await;
//! report.assert_passed();
//! # }
//! ```

#![forbid(unsafe_code)]

mod acknowledged;
mod connect;
mod kill;
mod limits;
mod protocol;
mod published;
mod registry;
mod report;
mod target;

pub use kill::KILL_CLAUSES;
pub use limits::RUN_TIMEOUT;
pub use protocol::PROTOCOL_CLAUSES;
pub use published::{ReadBackProbe, read_back};
pub use rdlt_connector::testing::{
    Clause, ClauseResult, DESTINATION_CLAUSES, Observed, Outcome, Probe, REASON_BYTES, Reason,
    Report, SOURCE_CLAUSES, Unprobed, Verdict,
};
pub use registry::{Family, clauses, markdown};
pub use report::{json, plain};
pub use target::Target;

use rdlt_connector::Role;
use rdlt_connector::testing::{
    certify_destination_factory_observed, certify_source_factory_observed,
};
use rdlt_connector::{DestinationFactory, SourceFactory};

/// Certifies the source `target` reaches, with `config`: the protocol's clauses, the source
/// clauses, each over connections of its own, then the kill clause.
///
/// No clause applies to a connector that serves no source.
pub async fn certify_source(target: &Target, config: serde_json::Value) -> Report {
    certify_source_observed(target, config, &Observed::new()).await
}

/// Certifies the source `target` reaches as [`certify_source`] does, telling `observed` each
/// clause's result as its check ends, for [`unfinished`] to report should it be cut.
pub async fn certify_source_observed(
    target: &Target,
    config: serde_json::Value,
    observed: &Observed,
) -> Report {
    match connect::Factory::new(target, Role::Source, &config).await {
        Ok(factory) => {
            let protocol = protocol::check(target, Role::Source, &config, observed).await;
            let mut report =
                certify_source_factory_observed(&factory, config.clone(), observed).await;
            report.results.splice(0..0, protocol);
            let id = &SourceFactory::spec(&factory).id;
            let killed = kill::source(target, id, &config).await;
            observed.tell(killed.clone());
            report.results.push(killed);
            report
        }
        Err(unmet) => {
            let families = [
                PROTOCOL_CLAUSES,
                SOURCE_CLAUSES,
                kill::clauses(Role::Source),
            ];
            let report = unmet.report(target, &families);
            report
                .results
                .iter()
                .cloned()
                .for_each(|result| observed.tell(result));
            report
        }
    }
}

/// The report of a certification of `target` as `role` that was cut, for `reason`: what
/// `observed` was told before the cut, the clause it was cut in failed, and those it never
/// reached not observed.
pub fn unfinished(target: &Target, role: Role, observed: &Observed, reason: &str) -> Report {
    let (clauses, kill) = match role {
        Role::Source => (SOURCE_CLAUSES, kill::clauses(Role::Source)),
        Role::Destination => (DESTINATION_CLAUSES, kill::clauses(Role::Destination)),
    };
    let mut results = observed.results();
    let left = [PROTOCOL_CLAUSES, clauses, kill].into_iter().flatten();
    for (index, clause) in left.skip(results.len()).enumerate() {
        let outcome = if index == 0 {
            Outcome::Failed(Reason::new(format_args!(
                "{reason}, as this clause was checked"
            )))
        } else {
            Outcome::Unobserved(Reason::new(format_args!("{reason}, before this clause")))
        };
        results.push(ClauseResult {
            clause: *clause,
            outcome,
        });
    }
    Report {
        connector: observed.connector().unwrap_or_else(|| target.describe()),
        results,
    }
}

/// Certifies the destination `target` reaches, with `config`, reading what it published through
/// `probe` ([`Unprobed`] when nothing can read it): the protocol's clauses, the destination
/// clauses, then the kill clause.
///
/// No clause applies to a connector that serves no destination.
pub async fn certify_destination(
    target: &Target,
    config: serde_json::Value,
    probe: &dyn Probe,
) -> Report {
    certify_destination_observed(target, config, probe, &Observed::new()).await
}

/// Certifies the destination `target` reaches as [`certify_destination`] does, telling
/// `observed` each clause's result as its check ends, for [`unfinished`] to report should it be
/// cut.
pub async fn certify_destination_observed(
    target: &Target,
    config: serde_json::Value,
    probe: &dyn Probe,
    observed: &Observed,
) -> Report {
    match connect::Factory::new(target, Role::Destination, &config).await {
        Ok(factory) => {
            let protocol = protocol::check(target, Role::Destination, &config, observed).await;
            let certifying =
                certify_destination_factory_observed(&factory, config.clone(), probe, observed);
            let mut report = certifying.await;
            report.results.splice(0..0, protocol);
            let id = &DestinationFactory::spec(&factory).id;
            let killed = kill::destination(target, id, &config, probe).await;
            observed.tell(killed.clone());
            report.results.push(killed);
            report
        }
        Err(unmet) => {
            let families = [
                PROTOCOL_CLAUSES,
                DESTINATION_CLAUSES,
                kill::clauses(Role::Destination),
            ];
            let report = unmet.report(target, &families);
            report
                .results
                .iter()
                .cloned()
                .for_each(|result| observed.tell(result));
            report
        }
    }
}
