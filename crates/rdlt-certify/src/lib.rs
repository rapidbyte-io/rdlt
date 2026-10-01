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
    Clause, ClauseResult, DESTINATION_CLAUSES, Outcome, Probe, REASON_BYTES, Reason, Report,
    SOURCE_CLAUSES, Unprobed, Verdict,
};
pub use registry::{Family, clauses, markdown};
pub use report::{json, plain};
pub use target::Target;

use rdlt_connector::Role;
use rdlt_connector::testing::{certify_destination_factory, certify_source_factory};
use rdlt_connector::{DestinationFactory, SourceFactory};

/// Certifies the source `target` reaches, with `config`: the protocol's clauses, the source
/// clauses, each over connections of its own, then the kill clause.
///
/// No clause applies to a connector that serves no source.
pub async fn certify_source(target: &Target, config: serde_json::Value) -> Report {
    match connect::Factory::new(target, Role::Source, &config).await {
        Ok(factory) => {
            let protocol = protocol::check(target, Role::Source, &config).await;
            let mut report = certify_source_factory(&factory, config.clone()).await;
            report.results.splice(0..0, protocol);
            let id = &SourceFactory::spec(&factory).id;
            report.results.push(kill::source(target, id, &config).await);
            report
        }
        Err(unmet) => unmet.report(
            target,
            &[
                PROTOCOL_CLAUSES,
                SOURCE_CLAUSES,
                kill::clauses(Role::Source),
            ],
        ),
    }
}

/// A report of every clause of `role`, each failed for `reason`: what a certification of
/// `target` that could not be finished reports, as one cut at a deadline.
pub fn unfinished(target: &Target, role: Role, reason: &str) -> Report {
    let (clauses, kill) = match role {
        Role::Source => (SOURCE_CLAUSES, kill::clauses(Role::Source)),
        Role::Destination => (DESTINATION_CLAUSES, kill::clauses(Role::Destination)),
    };
    let failed = |clause: &Clause| ClauseResult {
        clause: *clause,
        outcome: Outcome::Failed(Reason::new(reason)),
    };
    Report {
        connector: target.describe(),
        results: [PROTOCOL_CLAUSES, clauses, kill]
            .into_iter()
            .flatten()
            .map(failed)
            .collect(),
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
    match connect::Factory::new(target, Role::Destination, &config).await {
        Ok(factory) => {
            let protocol = protocol::check(target, Role::Destination, &config).await;
            let mut report = certify_destination_factory(&factory, config.clone(), probe).await;
            report.results.splice(0..0, protocol);
            let id = &DestinationFactory::spec(&factory).id;
            report
                .results
                .push(kill::destination(target, id, &config, probe).await);
            report
        }
        Err(unmet) => unmet.report(
            target,
            &[
                PROTOCOL_CLAUSES,
                DESTINATION_CLAUSES,
                kill::clauses(Role::Destination),
            ],
        ),
    }
}
