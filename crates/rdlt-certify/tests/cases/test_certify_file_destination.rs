//! The destination headline: the REAL file connector bin, certified as
//! a destination over the wire — the P-clauses probed on live processes
//! (including the handshake-borne wire clauses P3/P7, P8's one-session
//! ceiling, P9's abandonment reclaim and P10's Backend-direct order
//! book), and the testkit's D-clauses reused against the managed adapter with
//! a jsonl read-back probe. The probe-less run proves certification
//! never silently narrows: the unexercisable D-clauses come out
//! Skip-with-reason, never Pass and never Fail.

use rdlt_certify::{Target, Verdict, certify_destination};
use serde_json::json;

use super::support::bins::built_bin;
use super::support::probe::JsonlDirProbe;

/// The skip reason a probe-less run stamps on every D-clause.
const NO_PROBE_REASON: &str = "no table probe supplied — read-back clauses need one; the library API accepts a \
     TableProbe (the bin gains --probe when a portable probe format exists)";

/// The skip reason D8 carries when the destination declares no merge
/// capability — the testkit asserts D8 only for merge-capable
/// destinations, and the file destination is not one.
const NO_MERGE_REASON: &str = "the destination does not declare the merge capability — D8 certifies merge upsert and was \
     not exercised";

fn file_target(out_root: &std::path::Path) -> Target {
    let config = json!({
        "path": out_root.display().to_string(),
        "format": "jsonl",
    });
    Target::resolve_path(built_bin("rdlt-connector-file"), config)
}

/// A conformant destination certifies clean: every clause has an entry,
/// nothing fails, and the only non-`Pass` is D8's honest skip (the file
/// destination declares no merge capability, so D8 cannot be
/// exercised).
#[tokio::test]
async fn the_file_destination_certifies_clean_with_a_probe() {
    let out = tempfile::tempdir().expect("tempdir");
    let probe = JsonlDirProbe {
        root: out.path().to_path_buf(),
    };

    let report = certify_destination(&file_target(out.path()), Some(&probe)).await;

    let clauses: Vec<&str> = report.entries.iter().map(|entry| entry.clause).collect();
    for clause in [
        "D1", "D2", "D3", "D4", "D5", "D6", "D8", "P1", "P2", "P3", "P4", "P7", "P8", "P9", "P10",
    ] {
        assert!(
            clauses.contains(&clause),
            "clause {clause} has no entry — asserted set was {clauses:?}"
        );
    }
    for entry in &report.entries {
        match (entry.clause, &entry.verdict) {
            ("D8", Verdict::Skip(reason)) => assert_eq!(reason, NO_MERGE_REASON),
            ("D8", other) => panic!(
                "the file destination declares no merge capability, so D8 must be an honest \
                 skip, not {other:?}"
            ),
            (_, Verdict::Pass) => {}
            (clause, verdict) => panic!(
                "a conformant destination must certify clean — {clause} came out \
                 {verdict:?}:\n{}",
                report.render_text()
            ),
        }
    }
    assert!(
        report.passed(),
        "no Fail entries:\n{}",
        report.render_text()
    );
}

/// Without a probe the read-back D-clauses are skipped WITH the reason
/// — never silently narrowed to a smaller passing set, and never
/// failed. The probe-independent clauses still certify.
#[tokio::test]
async fn a_probe_less_run_skips_the_read_back_clauses_with_the_reason() {
    let out = tempfile::tempdir().expect("tempdir");

    let report = certify_destination(&file_target(out.path()), None).await;

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
    for clause in ["P1", "P2", "P3", "P4", "P7", "P8", "P9", "P10"] {
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
    assert!(
        report.passed(),
        "a probe-less run must not FAIL anything:\n{}",
        report.render_text()
    );
}
