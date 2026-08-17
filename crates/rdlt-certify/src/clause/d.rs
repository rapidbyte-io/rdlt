//! Destination certification: spawn the target's binary and certify it
//! over the wire — the protocol clauses a destination faces (P1/P13
//! self-probed, P2/P4 through the provider, P3/P7 on a raw handshake,
//! P8–P12 on raw sessions over the live socket — all judged in
//! [`crate::clause::p`]) plus the testkit's destination conformance
//! clauses D1–D6/D8 reused against the managed adapter, folded here.
//!
//! The D-reuse rides a settling adapter, not the managed destination
//! raw: the in-process suite assumes dropping a session releases it
//! synchronously (its D4 arm drops one session and immediately opens
//! the next), but over the wire the server must observe the stream end
//! and run its close before the one session slot frees. The adapter's
//! `open` retries exactly the typed one-session refusal within the
//! reclaim window, masking nothing a clause certifies — a connector
//! that never reclaims still fails, and P9 certifies the reclaim
//! explicitly.
//!
//! Every clause rides under the 30 s clause timeout — a stalling
//! connector FAILS the clause, the certifier never hangs — and no
//! failure message ever carries config bytes.

use async_trait::async_trait;
use rdlt_connector::core::id::{LoadId, PipelineId};
use rdlt_connector::destination::{Capabilities, Destination, LoadSession, OpenContext};
use rdlt_connector::error::DestinationError;
use rdlt_connector::spec::ConnectorSpec;
use rdlt_connector_client::destination::Remote;
use rdlt_connector_client::error::Error as ClientError;
use rdlt_connector_client::handshake::Role;
use rdlt_runtime::local::Local;
use rdlt_runtime::managed::Managed;
use rdlt_runtime::provider::Provider;
use rdlt_testkit::conformance::destination::{self, TableProbe};

use crate::clause::p;
use crate::clock;
use crate::report::{self, Report};
use crate::target::{self, Target};

/// The D-clauses the reused testkit suite covers — its module doc's
/// exact set (D7 has no check there yet; renumbering is forbidden).
pub const CLAUSES: [&str; 7] = ["D1", "D2", "D3", "D4", "D5", "D6", "D8"];

/// The skip reason every read-back clause carries when no probe was
/// supplied — certification never silently narrows to a smaller
/// passing set. Shared with the kill matrix's destination arms
/// ([`crate::clause::k`]): their convergence assert is a read-back
/// too.
pub const NO_PROBE_SKIP: &str = "no table probe supplied — read-back clauses need one; pass --probe-cmd '<sh line>' \
     (the library API takes a TableProbe directly). Single-writer stores (duckdb) refuse \
     every open beside the live connector, a read-only one included — probe a COPY: copy \
     the store file plus its WAL sidecar, then count in the copy";

/// The skip reason D8 carries for a destination that declares no merge
/// capability — the suite asserts D8 only for merge-capable
/// destinations, so folding it into the asserted set would mint a
/// `Pass` for a clause that never ran. The ONE spelling merge-less
/// connectors' certify cells assert against (a spelling copied into
/// suites drifts).
pub const NO_MERGE_SKIP: &str = "the destination does not declare the merge capability — D8 certifies merge upsert and was \
     not exercised";

/// Certify `target` as a DESTINATION connector. Never hangs and never
/// panics on connector misbehavior: every clause's outcome — including
/// "the binary is not a connector at all" — is a report entry.
///
/// `probe` is the read-back seam the D-suite needs (row counting is the
/// one thing the SPI cannot do). With `None`, the D-clauses are
/// Skip-with-reason — never silently dropped, never vacuously passed —
/// while the probe-independent P-clauses still certify.
pub async fn certify(target: &Target, probe: Option<&dyn TableProbe>) -> Report {
    let mut report = Report::default();

    // P1 and P13 probe on their own spawns and write their entries
    // here, before any cascade point; P13's child is dead-and-reaped
    // before the wire spawn below opens any single-writer store.
    p::report_p1(&mut report, target, Role::Destination).await;
    p::report_p13(&mut report, target, Role::Destination).await;

    let provider = Local::new();

    // The Spec reply feeds P4 below — and, for a path-only target,
    // identity: the operator named a binary, not an id, so the id the
    // wire handshake verifies strictly is learned from the connector's
    // own report.
    let spec = target::fetch_spec(&provider, &target.requirement, Role::Destination).await;

    // Everything past the self-probed clauses (whose probes already
    // wrote their entries) runs over a verified handshake; without
    // one, every remaining clause fails with the one cause.
    // `post_wire` is the same cascade AFTER the wire block has judged
    // (or failed) P3/P7 on its own spawn — those two already carry
    // their entries by then, and a cascade re-failing them would write
    // a second verdict per clause.
    let post_wire = || {
        p::GENERIC
            .into_iter()
            .filter(|clause| !p::SELF_PROBED.contains(clause))
            .chain(CLAUSES)
            .chain(p::SESSION)
    };
    let downstream = || post_wire().chain(p::DEST_WIRE);

    let requirement = match target::resolved_requirement(&target.requirement, &spec) {
        Ok(requirement) => requirement,
        Err(why) => {
            for clause in downstream() {
                report.fail(clause, why.clone());
            }
            return report;
        }
    };

    // The wire clauses P3/P7 ride their OWN spawn, and that spawn runs
    // BEFORE the managed adapter exists and is killed-and-REAPED before
    // it spawns: a single-writer destination holds an exclusive
    // cross-process lock on its store from handshake to process death,
    // so two live processes handshaking the same config cannot coexist
    // — the second's open would be refused by the store, failing
    // clauses about the WIRE with a cause that is really the
    // certifier's own process overlap. Sequencing the two spawns is
    // free for every multi-writer destination and is what makes
    // single-writer ones certifiable at all.
    p::wire_clauses(&mut report, &requirement, Role::Destination, &target.config).await;

    // The certification subject: one managed destination, spawned
    // honestly through the provider (resolution is part of the bar).
    let managed = tokio::time::timeout(
        report::CLAUSE_TIMEOUT,
        provider.destination(&requirement, &target.config),
    )
    .await;
    // The SPI's `Destination` demands `'static`, so the settling
    // adapter OWNS the managed destination for the rest of the run —
    // `.inner` is the raw adapter wherever settling must not apply.
    let managed = match managed {
        Ok(Ok(managed)) => SettledDestination { inner: managed },
        Ok(Err(error)) => {
            let why =
                format!("the provider could not spawn the connector as a destination: {error}");
            for clause in post_wire() {
                report.fail(clause, why.clone());
            }
            return report;
        }
        Err(_elapsed) => {
            for clause in post_wire() {
                report.fail(clause, report::timed_out());
            }
            return report;
        }
    };

    // P2 — typed config refusal, probed on its own spawn with a
    // one-unknown-field document.
    let bogus = serde_json::json!({ "__rdlt_certify_bogus__": true });
    p::report_p2(
        &mut report,
        tokio::time::timeout(
            report::CLAUSE_TIMEOUT,
            provider.destination(&requirement, &bogus),
        )
        .await,
    );

    // P4 — the pre-handshake Spec: name/version non-empty and a JSON
    // -object config schema, answered with no config at all.
    p::report_p4(&mut report, &spec);

    // D-reuse — the testkit's destination conformance suite, verbatim,
    // against the (settling) managed adapter: the wire is certified by
    // the SAME clauses an in-process connector answers to. The fold
    // reads the suite's concluded record: clauses an abort left
    // unreached come out NOT-REACHED and refuse certification — never
    // a vacuous Pass, and never a Skip that would let the run pass.
    // D8 is asserted only when the connector declares merge —
    // otherwise the suite never ran it, and the honest verdict is a
    // Skip.
    match probe {
        Some(probe) => {
            let merge = managed.capabilities().merge;
            // The clause budget bounds SPI traffic ALONE: the probe
            // clock stops while a count runs — each count carries its
            // own probe budget and fails naming itself, so a
            // slow-but-legal probe cannot exhaust a suite budget that
            // was never sized for it.
            let (metered, probe_clock) = clock::StopClockProbe::new(probe);
            match clock::timeout_excluding_probe(
                report::CLAUSE_TIMEOUT,
                &probe_clock,
                destination::verify(&managed, &metered),
            )
            .await
            {
                Ok(outcome) => {
                    // The report's absorb renders skips honestly, so
                    // this caller ACKNOWLEDGES them by name.
                    let (failures, skips, concluded) = outcome.tolerating_skips();
                    if merge {
                        report.absorb(failures, skips, report::Concluded(&concluded), &CLAUSES);
                    } else {
                        // CLAUSES minus D8, derived — a hand copy is
                        // the drift the skip arm already shed.
                        let without_d8: Vec<&'static str> = CLAUSES
                            .into_iter()
                            .filter(|clause| *clause != "D8")
                            .collect();
                        report.absorb(failures, skips, report::Concluded(&concluded), &without_d8);
                        report.skip("D8", NO_MERGE_SKIP.to_string());
                    }
                }
                Err(_elapsed) => {
                    for clause in CLAUSES {
                        report.fail(clause, report::timed_out());
                    }
                }
            }
        }
        None => {
            for clause in CLAUSES {
                report.skip(clause, NO_PROBE_SKIP.to_string());
            }
        }
    }

    // P8–P12 need the LIVE socket — the provider's guard carries it.
    let socket = managed
        .inner
        .guard()
        .map(|guard| guard.socket_path().to_path_buf());
    p::session(&mut report, socket.as_deref()).await;

    report
}

/// Is `error` the wire's one-session refusal — a FATAL wrapping the
/// transport `FailedPrecondition` status? Typed end to end: the
/// judgment reads the tonic code through the client's own error type,
/// never a rendered message.
fn is_session_ceiling_refusal(error: &DestinationError) -> bool {
    let DestinationError::Fatal(cause) = error else {
        return false;
    };
    matches!(
        cause.downcast_ref::<ClientError>(),
        Some(ClientError::Transport(status))
            if status.code() == tonic::Code::FailedPrecondition
    )
}

/// Open a session, retrying exactly the one-session refusal within
/// [`p::RECLAIM_WINDOW`] — the wire-honest equivalent of the
/// in-process assumption that a dropped predecessor has already
/// released its slot. Any OTHER failure surfaces immediately, and
/// exhausting the window surfaces the refusal itself.
async fn settle_open(
    dest: &Managed<Remote>,
    pipeline: &str,
    load_id: &str,
) -> Result<Box<dyn LoadSession>, DestinationError> {
    let deadline = tokio::time::Instant::now() + p::RECLAIM_WINDOW;
    loop {
        let context = OpenContext::new(PipelineId::new(pipeline), LoadId::new(load_id));
        match dest.open(context).await {
            Ok(session) => return Ok(session),
            Err(error)
                if is_session_ceiling_refusal(&error) && tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(p::RECLAIM_POLL).await;
            }
            Err(error) => return Err(error),
        }
    }
}

/// The D-reuse subject: the managed destination with the settling
/// `open` — see the module doc for why the suite cannot run against
/// the raw adapter. Owns the managed destination because the SPI's
/// `Destination` demands `'static`.
struct SettledDestination {
    inner: Managed<Remote>,
}

#[async_trait]
impl Destination for SettledDestination {
    fn spec(&self) -> ConnectorSpec {
        self.inner.spec()
    }

    async fn check(&self) -> Result<(), DestinationError> {
        self.inner.check().await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn open(&self, context: OpenContext) -> Result<Box<dyn LoadSession>, DestinationError> {
        settle_open(
            &self.inner,
            context.pipeline.as_str(),
            context.load_id.as_str(),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `CLAUSES` keeps the report's render order, but its SET must
    /// equal the testkit suite's own asserted set — a clause added to
    /// the suite without a certify entry (or the other way round) would
    /// silently narrow one side's report; the drift fails here by name
    /// instead.
    #[test]
    fn dest_clauses_cover_exactly_the_testkit_suites_asserted_set() {
        use std::collections::BTreeSet;
        let report_side: BTreeSet<&str> = CLAUSES.into_iter().collect();
        let suite_side: BTreeSet<&str> = rdlt_testkit::conformance::destination::ASSERTED_CLAUSES
            .into_iter()
            .collect();
        assert_eq!(report_side, suite_side);
    }
}
