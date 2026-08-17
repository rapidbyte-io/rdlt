//! The destination headline: the REAL reference connector bin,
//! certified as a destination over the wire — the P-clauses probed on
//! live processes (including the handshake-borne wire clauses P3/P7,
//! P8's one-session ceiling, P9's abandonment reclaim, P10's
//! Backend-direct order book, P11's one-batch write rule and P12's
//! error-frame text discipline), and the testkit's D-clauses reused
//! against the managed adapter with a jsonl read-back probe. The
//! probe-less run proves certification never silently narrows: the
//! unexercisable D-clauses come out Skip-with-reason, never Pass and
//! never Fail.

use rdlt_certify::clause::{d, p};
use rdlt_certify::report::{self, Verdict};
use rdlt_certify::target::Target;
use serde_json::json;

use super::support::NO_PROBE_REASON;
use super::support::bins::built_bin;
use super::support::probe::JsonlDirProbe;

fn reference_target(out_root: &std::path::Path) -> Target {
    let config = json!({ "path": out_root.display().to_string() });
    Target::resolve_path(built_bin("rdlt-connector-reference"), config)
}

/// A conformant destination certifies clean: every clause has an entry,
/// nothing fails, and the only non-`Pass` entries are the two honest
/// skips — D8 (the reference destination declares no merge capability)
/// and P13 (the reference serves both roles, so there is no unserved
/// role to refuse).
#[tokio::test]
async fn the_reference_destination_certifies_clean_with_a_probe() {
    let out = tempfile::tempdir().expect("tempdir");
    let probe = JsonlDirProbe {
        root: out.path().to_path_buf(),
    };

    let report = d::certify(&reference_target(out.path()), Some(&probe)).await;

    report::assert_all_pass(
        &report,
        &[
            "D1", "D2", "D3", "D4", "D5", "D6", "P1", "P2", "P3", "P4", "P7", "P8", "P9", "P10",
            "P11", "P12",
        ],
        &[
            ("D8", d::NO_MERGE_SKIP),
            ("P13", p::DESTINATION_DUAL_ROLE_SKIP),
        ],
    );
}

/// Without a probe the read-back D-clauses are skipped WITH the reason
/// — never silently narrowed to a smaller passing set, and never
/// failed. The probe-independent clauses still certify.
#[tokio::test]
async fn a_probe_less_run_skips_the_read_back_clauses_with_the_reason() {
    let out = tempfile::tempdir().expect("tempdir");

    let report = d::certify(&reference_target(out.path()), None).await;

    for clause in ["D1", "D2", "D3", "D4", "D5", "D6", "D8"] {
        let entry = report
            .entries
            .iter()
            .find(|entry| entry.clause == clause)
            .unwrap_or_else(|| panic!("clause {clause} has no entry"));
        match &entry.verdict {
            Verdict::Skip(reason) => assert_eq!(reason, NO_PROBE_REASON),
            other => panic!("without a probe, {clause} must skip with the reason, not {other:?}"),
        }
    }
    for clause in [
        "P1", "P2", "P3", "P4", "P7", "P8", "P9", "P10", "P11", "P12",
    ] {
        let entry = report
            .entries
            .iter()
            .find(|entry| entry.clause == clause)
            .unwrap_or_else(|| panic!("clause {clause} has no entry"));
        assert!(
            matches!(entry.verdict, Verdict::Pass),
            "{clause} is probe-independent and must still certify:\n{}",
            report.render_text()
        );
    }
    // P13 is probe-independent too, but the dual-role reference earns
    // its announced skip rather than a Pass.
    let p13 = report
        .entries
        .iter()
        .find(|entry| entry.clause == "P13")
        .expect("clause P13 has an entry");
    match &p13.verdict {
        Verdict::Skip(reason) => assert_eq!(reason, p::DESTINATION_DUAL_ROLE_SKIP),
        other => panic!("a dual-role connector's P13 must skip, not {other:?}"),
    }
    assert!(
        report.passed(),
        "a probe-less run must not FAIL anything:\n{}",
        report.render_text()
    );
}
