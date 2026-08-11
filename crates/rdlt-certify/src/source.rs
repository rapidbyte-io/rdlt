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
use rdlt_testkit::conformance::{ConformanceFailure, ConformanceSkip};

use crate::report::{CLAUSE_TIMEOUT, Report, timed_out};
use crate::target::{
    GENERIC_CLAUSES, Target, fetch_spec, probe_handshake_line, report_p2, report_p4,
    resolved_requirement,
};
use crate::wire;

/// The S-clauses the reused testkit suite asserts — its module doc's
/// exact set. Public (re-exported at the crate root) because the
/// certifier CLI's skip-acknowledgment gate keys on exactly this set:
/// a skip among these clauses refuses certification unless the
/// operator acknowledges its stream by name.
pub const SOURCE_CLAUSES: [&str; 3] = ["S1", "S2", "S4"];

/// Certify `target` as a SOURCE connector. Never hangs and never
/// panics on connector misbehavior: every clause's outcome — including
/// "the binary is not a connector at all" — is a report entry.
///
/// `accept_skips` is the snapshot-source acknowledgment, BY STREAM
/// NAME and strict by default (round-4 put the guard HERE, not only in
/// the CLI; round-12 made it name-scoped — a blanket acknowledgment
/// accepted for one genuine snapshot stream also folded a REGRESSED
/// co-stream green): an S-suite skip folds as a FAILURE naming its
/// stream and the acknowledgment unless that stream is named in
/// `accept_skips`, so a library caller gating on [`Report::passed`]
/// refuses a source that never checkpoints exactly as the CLI does.
/// `Report::passed` itself deliberately keeps treating `Skip` as
/// passing — the destination's no-probe and no-merge skips are choices
/// the operator already made.
pub async fn certify_source(target: &Target, accept_skips: &[&str]) -> Report {
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

    // Everything past P1 (whose probe already wrote its entry) runs
    // over a verified handshake; without one, every remaining clause
    // fails with the one cause.
    let downstream = || {
        GENERIC_CLAUSES
            .into_iter()
            .filter(|clause| *clause != "P1")
            .chain(SOURCE_CLAUSES)
            .chain(wire::SOURCE_WIRE_CLAUSES)
    };

    let requirement = match resolved_requirement(&target.requirement, &spec) {
        Ok(requirement) => requirement,
        Err(why) => {
            for clause in downstream() {
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
            for clause in downstream() {
                report.fail(clause, why.clone());
            }
            return report;
        }
        Err(_elapsed) => {
            for clause in downstream() {
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
    // The child parks in a shared slot so the timeout arm can claim
    // and REAP it (the destination block's round-3 discipline; a
    // timed-out future only drops, and kill_on_drop only SENDS the
    // signal — the abandoned connector process must not outlive the
    // certification that spawned it).
    let slot = wire::ChildSlot::default();
    match tokio::time::timeout(
        CLAUSE_TIMEOUT,
        wire::attach_for(&requirement, Role::Source, &target.config, &slot),
    )
    .await
    {
        Ok(Ok(mut probe)) => {
            wire::certify_source_wire(&mut report, &mut probe, &requirement.id).await;
            probe.kill().await;
        }
        Ok(Err(why)) => {
            for clause in wire::SOURCE_WIRE_CLAUSES {
                report.fail(clause, why.clone());
            }
        }
        Err(_elapsed) => {
            wire::reap_parked(&slot).await;
            for clause in wire::SOURCE_WIRE_CLAUSES {
                report.fail(clause, timed_out());
            }
        }
    }

    // S-reuse — the testkit's source conformance suite, verbatim,
    // against the managed adapter: the wire is certified by the SAME
    // clauses an in-process connector answers to. Acknowledged skips
    // fold as Skip entries — an honestly-declared snapshot stream's S2
    // renders with its reason, never as a vacuous Pass; UNACKNOWLEDGED
    // ones fold as failures naming the acknowledgment, so the report
    // itself refuses (the S2 skip is reachable by DEFAULT-absent
    // cursor_field — a source that merely forgot checkpointing must
    // not certify).
    match tokio::time::timeout(CLAUSE_TIMEOUT, verify_source(&managed)).await {
        Ok(outcome) => {
            // Both arms are EXPLICIT consumptions of the outcome
            // (round-7: the fields went private so failures cannot be
            // read past the skip guard): the acknowledgment takes the
            // skips by name; strict promotes each through the
            // testkit's one fold spelling plus the acknowledgment
            // tail.
            let (mut failures, skips) = outcome.tolerating_skips();
            let (promoted, acknowledged) = fold_acknowledged(skips, accept_skips);
            failures.extend(promoted);
            report.absorb(failures, acknowledged, &SOURCE_CLAUSES)
        }
        Err(_elapsed) => {
            for clause in SOURCE_CLAUSES {
                report.fail(clause, timed_out());
            }
        }
    }

    report
}

/// The stream an S-suite skip's reason names — the acknowledgment
/// matches on it. Every suite skip reason opens ``stream `<name>` ...``,
/// so the name is the first backtick-quoted token.
fn skip_stream(reason: &str) -> Option<&str> {
    reason.split('`').nth(1)
}

/// Fold the S-suite's skips through the NAME-scoped acknowledgment
/// (round-12 — the acknowledgment was a blanket bool, so accepting a
/// genuine snapshot stream also folded a REGRESSED co-stream green):
/// a skip whose named stream is acknowledged stays an honest Skip;
/// every other skip promotes to a failure naming its stream and the
/// name-taking acknowledgment.
fn fold_acknowledged(
    skips: Vec<ConformanceSkip>,
    accept_skips: &[&str],
) -> (Vec<ConformanceFailure>, Vec<ConformanceSkip>) {
    let (acknowledged, promoted): (Vec<_>, Vec<_>) = skips.into_iter().partition(|skip| {
        skip_stream(&skip.reason).is_some_and(|stream| accept_skips.contains(&stream))
    });
    let failures = promoted
        .into_iter()
        .map(|skip| {
            let stream = skip_stream(&skip.reason).unwrap_or("<unnamed>").to_owned();
            let mut failure = skip.into_failure();
            failure.message.push_str(&format!(
                " — a skipped source clause is not certified evidence (a source that \
                 never checkpoints looks identical to one that forgot resume); \
                 acknowledge a snapshot stream BY NAME with accept_skips \
                 (CLI: --accept-skips {stream})"
            ));
            failure
        })
        .collect();
    (failures, acknowledged)
}

#[cfg(test)]
mod acknowledgment_tests {
    //! The name-scoped gate, pinned pure (round-12 red pin): two
    //! cursor-less streams, ONE acknowledged — the other must still
    //! fail by name, because a blanket acknowledgment is exactly how a
    //! regressed CDC stream certifies green beside a genuine snapshot
    //! stream.

    use super::*;

    fn s2_skip(stream: &str) -> ConformanceSkip {
        ConformanceSkip {
            clause: "S2",
            reason: format!(
                "stream `{stream}` declares no cursor_field and never checkpoints — an \
                 honest snapshot stream: there is no resume to certify, and every run \
                 re-reads everything"
            ),
        }
    }

    #[test]
    fn the_skip_reason_names_its_stream_between_the_first_backticks() {
        assert_eq!(skip_stream(&s2_skip("orders").reason), Some("orders"));
        assert_eq!(skip_stream("no backticks at all"), None);
    }

    #[test]
    fn an_unacknowledged_co_stream_still_fails_by_name() {
        let (failures, acknowledged) = fold_acknowledged(
            vec![s2_skip("snapshot_ok"), s2_skip("cdc_regressed")],
            &["snapshot_ok"],
        );
        assert_eq!(acknowledged.len(), 1, "{acknowledged:?}");
        assert!(
            skip_stream(&acknowledged[0].reason) == Some("snapshot_ok"),
            "the named stream's skip stays an honest skip: {acknowledged:?}"
        );
        assert_eq!(failures.len(), 1, "{failures:?}");
        assert!(
            failures[0].message.contains("`cdc_regressed`")
                && failures[0].message.contains("--accept-skips cdc_regressed")
                && failures[0].message.starts_with("not exercised: "),
            "the unacknowledged stream fails BY NAME, spelling out its own \
             acknowledgment: {}",
            failures[0].message
        );
    }

    #[test]
    fn naming_every_stream_accepts_every_skip() {
        let (failures, acknowledged) =
            fold_acknowledged(vec![s2_skip("a"), s2_skip("b")], &["a", "b"]);
        assert!(failures.is_empty(), "{failures:?}");
        assert_eq!(acknowledged.len(), 2, "both skips render honestly");
    }
}
