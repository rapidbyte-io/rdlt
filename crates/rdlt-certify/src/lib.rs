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

mod connect;
mod protocol;
mod published;
mod registry;
mod report;
mod target;

pub use protocol::PROTOCOL_CLAUSES;
pub use published::{ReadBack, read_back};
pub use rdlt_connector::testing::{
    Clause, ClauseResult, DESTINATION_CLAUSES, Outcome, Probe, Report, SOURCE_CLAUSES, Unprobed,
};
pub use registry::{Family, clauses, markdown};
pub use report::{json, plain};
pub use target::Target;

use rdlt_connector::Role;
use rdlt_connector::testing::{certify_destination_factory, certify_source_factory};

/// Certifies the source `target` reaches, with `config`: the protocol's clauses, then the source
/// clauses, each over connections of its own.
///
/// A connector that serves no source has every clause skipped.
pub async fn certify_source(target: &Target, config: serde_json::Value) -> Report {
    match connect::Factory::new(target, Role::Source, &config).await {
        Ok(factory) => {
            let protocol = protocol::check(target, Role::Source, &config).await;
            let mut report = certify_source_factory(&factory, config).await;
            report.results.splice(0..0, protocol);
            report
        }
        Err(unmet) => unmet.report(target, &[PROTOCOL_CLAUSES, SOURCE_CLAUSES]),
    }
}

/// Certifies the destination `target` reaches, with `config`, reading what it published through
/// `probe` ([`Unprobed`] when nothing can read it): the protocol's clauses, then the destination
/// clauses.
///
/// A connector that serves no destination has every clause skipped.
pub async fn certify_destination(
    target: &Target,
    config: serde_json::Value,
    probe: &dyn Probe,
) -> Report {
    match connect::Factory::new(target, Role::Destination, &config).await {
        Ok(factory) => {
            let protocol = protocol::check(target, Role::Destination, &config).await;
            let mut report = certify_destination_factory(&factory, config, probe).await;
            report.results.splice(0..0, protocol);
            report
        }
        Err(unmet) => unmet.report(target, &[PROTOCOL_CLAUSES, DESTINATION_CLAUSES]),
    }
}
