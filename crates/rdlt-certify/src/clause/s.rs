//! Source certification: spawn the target's binary and certify it over
//! the wire — the protocol clauses a source faces (P1/P13 self-probed,
//! P2/P4 through the provider, P3/P7/P5/P6 on raw frames — all judged
//! in [`crate::clause::p`]) plus the testkit's source conformance
//! clauses S1/S2/S4 reused against the managed adapter, folded here.
//!
//! Every clause rides under the 30 s clause timeout — a stalling
//! connector FAILS the clause, the certifier never hangs — and no
//! failure message ever carries config bytes.

use rdlt_connector_client::handshake::Role;
use rdlt_runtime::local::Local;
use rdlt_runtime::provider::Provider;
use rdlt_testkit::conformance::{Failure, Skip, source};

use crate::clause::p;
use crate::report::{self, Report};
use crate::target::{self, Target};

/// The S-clauses the reused testkit suite asserts — its module doc's
/// exact set. The skip-acknowledgment gate keys on exactly this set: a
/// skip among these clauses refuses certification unless the operator
/// acknowledges its stream by name.
pub const CLAUSES: [&str; 3] = ["S1", "S2", "S4"];

/// Certify `target` as a SOURCE connector. Never hangs and never
/// panics on connector misbehavior: every clause's outcome — including
/// "the binary is not a connector at all" — is a report entry.
///
/// `accept_skips` is the snapshot-source acknowledgment, BY STREAM
/// NAME and strict by default: an S-suite skip folds as a FAILURE
/// naming its stream and the acknowledgment unless that stream is
/// named here, so a library caller gating on [`Report::passed`]
/// refuses a source that never checkpoints exactly as the CLI does; a
/// blanket acknowledgment would fold a REGRESSED co-stream green beside
/// a genuine snapshot stream. `Report::passed` itself keeps treating
/// `Skip` as passing — the destination's no-probe and no-merge skips
/// are choices the operator already made.
pub async fn certify(target: &Target, accept_skips: &[&str]) -> Report {
    let mut report = Report::default();

    // P1 and P13 probe on their own spawns and write their entries
    // here, before any cascade point.
    p::report_p1(&mut report, target, Role::Source).await;
    p::report_p13(&mut report, target, Role::Source).await;

    let provider = Local::new();

    // The Spec reply feeds P4 below — and, for a path-only target,
    // identity: the operator named a binary, not an id, so the id the
    // wire handshake verifies strictly is learned from the connector's
    // own report.
    let spec = target::fetch_spec(&provider, &target.requirement, Role::Source).await;

    // Everything past the self-probed clauses (whose probes already
    // wrote their entries) runs over a verified handshake; without
    // one, every remaining clause fails with the one cause.
    let downstream = || {
        p::GENERIC
            .into_iter()
            .filter(|clause| !p::SELF_PROBED.contains(clause))
            .chain(CLAUSES)
            .chain(p::SOURCE_WIRE)
    };

    let requirement = match target::resolved_requirement(&target.requirement, &spec) {
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
        report::CLAUSE_TIMEOUT,
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
            provider.source(&requirement, &bogus),
        )
        .await,
    );

    // P4 — the pre-handshake Spec: name/version non-empty and a JSON
    // -object config schema, answered with no config at all.
    p::report_p4(&mut report, &spec);

    // The wire clauses P3/P7/P5/P6 on their OWN spawn, below the
    // adapters.
    p::wire_clauses(&mut report, &requirement, Role::Source, &target.config).await;

    // S-reuse — the testkit's source conformance suite, verbatim,
    // against the managed adapter: the wire is certified by the SAME
    // clauses an in-process connector answers to. Acknowledged skips
    // fold as Skip entries — an honestly-declared snapshot stream's S2
    // renders with its reason, never as a vacuous Pass; UNACKNOWLEDGED
    // ones fold as failures naming the acknowledgment, so the report
    // itself refuses (the S2 skip is reachable by DEFAULT-absent
    // cursor_field — a source that merely forgot checkpointing must
    // not certify).
    match tokio::time::timeout(report::CLAUSE_TIMEOUT, source::verify(&managed)).await {
        Ok(outcome) => {
            // Both arms are EXPLICIT consumptions of the outcome: the
            // acknowledgment takes the skips by name; strict promotes
            // each through the testkit's one fold spelling plus the
            // acknowledgment tail.
            let (mut failures, skips, concluded) = outcome.tolerating_skips();
            let (promoted, acknowledged) = fold_acknowledged(skips, accept_skips);
            failures.extend(promoted);
            report.absorb(
                failures,
                acknowledged,
                report::Concluded(&concluded),
                &CLAUSES,
            )
        }
        Err(_elapsed) => {
            for clause in CLAUSES {
                report.fail(clause, report::timed_out());
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

/// Fold the S-suite's skips through the NAME-scoped acknowledgment: a
/// skip whose named stream is acknowledged stays an honest Skip; every
/// other skip promotes to a failure naming its stream and the
/// name-taking acknowledgment.
fn fold_acknowledged(skips: Vec<Skip>, accept_skips: &[&str]) -> (Vec<Failure>, Vec<Skip>) {
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
    //! The name-scoped gate, pinned pure: two cursor-less streams, ONE
    //! acknowledged — the other must still fail by name, because a
    //! blanket acknowledgment is exactly how a regressed CDC stream
    //! certifies green beside a genuine snapshot stream.

    use super::*;

    fn s2_skip(stream: &str) -> Skip {
        Skip {
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

    /// `CLAUSES` keeps the report's render order, but its SET must
    /// equal the testkit suite's own asserted set — a clause added to
    /// the suite without a certify entry (or the other way round) would
    /// silently narrow one side's report; the drift fails here by name
    /// instead.
    #[test]
    fn source_clauses_cover_exactly_the_testkit_suites_asserted_set() {
        use std::collections::BTreeSet;
        let report_side: BTreeSet<&str> = CLAUSES.into_iter().collect();
        let suite_side: BTreeSet<&str> = source::ASSERTED_CLAUSES.into_iter().collect();
        assert_eq!(report_side, suite_side);
    }
}
