//! Source certification: spawn the target's binary and certify it over
//! the wire — the role-generic protocol clauses (P1 handshake-line
//! discipline, P2 typed config refusal, P4 pre-handshake Spec — the
//! probes live in [`crate::target`]), the wire clauses judged on raw
//! frames below the adapters (P3 identity/skew, P7 the v0 state-format
//! map, P5 one-batch, P6 error-frame shape — [`crate::wire`]), plus
//! the testkit's source conformance clauses (S1/S2/S4) reused against
//! the managed adapter.
//!
//! Every clause rides under [`CLAUSE_TIMEOUT`] — a stalling connector
//! FAILS the clause, the certifier never hangs — and no failure message
//! ever carries config bytes.

use rdlt_runtime::{ConnectorProvider, LocalBinaryConnectorProvider, Role};
use rdlt_testkit::conformance::source::verify_source;

use crate::report::{CLAUSE_TIMEOUT, Report, timed_out};
use crate::target::{
    Target, fetch_spec, probe_handshake_line, report_p2, report_p4, resolved_requirement,
};
use crate::wire;

/// The S-clauses the reused testkit suite asserts — its module doc's
/// exact set.
const SOURCE_CLAUSES: [&str; 3] = ["S1", "S2", "S4"];

/// Certify `target` as a SOURCE connector. Never hangs and never
/// panics on connector misbehavior: every clause's outcome — including
/// "the binary is not a connector at all" — is a report entry.
pub async fn certify_source(target: &Target) -> Report {
    let mut report = Report::default();

    // P1 — the handshake-line discipline, probed on a direct spawn whose
    // only purpose is P1; certification re-spawns cleanly afterward.
    match tokio::time::timeout(CLAUSE_TIMEOUT, probe_handshake_line(target, Role::Source)).await {
        Ok(Ok(())) => report.pass("P1"),
        Ok(Err(why)) => report.fail("P1", why),
        Err(_elapsed) => report.fail("P1", timed_out()),
    }

    let provider = LocalBinaryConnectorProvider::new();

    // The Spec reply feeds P4 below — and, for a path-only target,
    // identity: the operator named a binary, not an id, so the id the
    // wire handshake verifies strictly (D-039-2) is learned from the
    // connector's own report.
    let spec = fetch_spec(&provider, &target.requirement).await;

    let requirement = match resolved_requirement(&target.requirement, &spec) {
        Ok(requirement) => requirement,
        Err(why) => {
            // No identity means no verified handshake can happen at all:
            // everything past P1 fails with the one cause.
            for clause in ["P2", "P4"]
                .into_iter()
                .chain(SOURCE_CLAUSES)
                .chain(wire::SOURCE_WIRE_CLAUSES)
            {
                report.fail(clause, why.clone());
            }
            return report;
        }
    };

    // The certification subject: one managed source, spawned honestly
    // through the provider (resolution is part of the bar).
    let managed = tokio::time::timeout(
        CLAUSE_TIMEOUT,
        provider.source(&requirement, &target.config),
    )
    .await;
    let managed = match managed {
        Ok(Ok(managed)) => managed,
        Ok(Err(error)) => {
            let why = format!("the provider could not spawn the connector as a source: {error}");
            for clause in ["P2", "P4"]
                .into_iter()
                .chain(SOURCE_CLAUSES)
                .chain(wire::SOURCE_WIRE_CLAUSES)
            {
                report.fail(clause, why.clone());
            }
            return report;
        }
        Err(_elapsed) => {
            for clause in ["P2", "P4"]
                .into_iter()
                .chain(SOURCE_CLAUSES)
                .chain(wire::SOURCE_WIRE_CLAUSES)
            {
                report.fail(clause, timed_out());
            }
            return report;
        }
    };

    // P2 — typed config refusal, probed on its own spawn with a
    // one-unknown-field document.
    let bogus = serde_json::json!({ "__rdlt_certify_bogus__": true });
    report_p2(
        &mut report,
        tokio::time::timeout(CLAUSE_TIMEOUT, provider.source(&requirement, &bogus)).await,
    );

    // P4 — the pre-handshake Spec: name/version non-empty and a JSON
    // -object config schema, answered with no config at all.
    report_p4(&mut report, &spec);

    // The wire clauses — P3/P7 from one raw handshake, P5/P6 on
    // observed frames — ride their OWN spawn: the kit watches actual
    // frames BELOW the adapters (a misbehaving server must not hide
    // behind our client's good manners), and the managed adapter's
    // process has already spent its one handshake.
    match tokio::time::timeout(
        CLAUSE_TIMEOUT,
        wire::attach_for(&requirement, Role::Source, &target.config),
    )
    .await
    {
        Ok(Ok(mut probe)) => {
            wire::certify_source_wire(&mut report, &mut probe, &requirement.id).await;
        }
        Ok(Err(why)) => {
            for clause in wire::SOURCE_WIRE_CLAUSES {
                report.fail(clause, why.clone());
            }
        }
        Err(_elapsed) => {
            for clause in wire::SOURCE_WIRE_CLAUSES {
                report.fail(clause, timed_out());
            }
        }
    }

    // S-reuse — the testkit's source conformance suite, verbatim,
    // against the managed adapter: the wire is certified by the SAME
    // clauses an in-process connector answers to.
    match tokio::time::timeout(CLAUSE_TIMEOUT, verify_source(&managed)).await {
        Ok(failures) => report.absorb(failures, &SOURCE_CLAUSES),
        Err(_elapsed) => {
            for clause in SOURCE_CLAUSES {
                report.fail(clause, timed_out());
            }
        }
    }

    report
}
