//! The headline: the REAL file connector bin, certified as a source
//! over the wire — spawned by path (the id learned from its own Spec
//! reply), the P-clauses probed on live processes (including the wire
//! clauses P3/P5/P6/P7, judged on raw frames below the adapters), and
//! the testkit's S-clauses reused against the managed adapter. A
//! conformant connector comes out all-Pass — P5 vacuously so here (the
//! jsonl source serves raw_json frames, never arrow), which is the
//! clause's recorded posture for JSON-native sources.
//!
//! Certification runs TWICE in a row against the same target and the
//! same fixture directory — the certification bar's repeated element
//! (no one-shot-only passes): a connector must survive being certified
//! again from the state the first certification left behind.

use rdlt_certify::{Target, Verdict, certify_source};
use serde_json::json;

use super::support::bins::built_bin;

/// A conformant source certifies clean: every entry `Pass`, and the
/// asserted set covers the S-reuse clauses plus the protocol clauses
/// this task probes — TWICE in a row, same target, same fixture
/// directory (each pass spawns fresh connector processes; the shared
/// directory is the state both passes read).
#[tokio::test]
async fn the_file_source_certifies_all_pass() {
    let bin = built_bin("rdlt-connector-file");
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("rows.jsonl"),
        "{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n",
    )
    .expect("the fixture file writes");
    let config = json!({
        "streams": [{
            "name": "events",
            "format": "jsonl",
            "path": format!("{}/*.jsonl", dir.path().display()),
        }]
    });

    for attempt in 1..=2 {
        let report =
            certify_source(&Target::resolve_path(bin.clone(), config.clone()), false).await;

        let clauses: Vec<&str> = report.entries.iter().map(|entry| entry.clause).collect();
        for clause in ["S1", "S2", "S4", "P1", "P2", "P3", "P4", "P5", "P6", "P7"] {
            assert!(
                clauses.contains(&clause),
                "attempt {attempt}: clause {clause} has no entry — asserted set was {clauses:?}"
            );
        }
        assert!(
            report
                .entries
                .iter()
                .all(|entry| matches!(entry.verdict, Verdict::Pass)),
            "attempt {attempt}: a conformant source must certify all-Pass:\n{}",
            report.render_text()
        );
    }
}

/// THE LIBRARY-LAYER GUARD (round-4 fix): an unacknowledged S-suite
/// skip refuses at the REPORT, not only at the CLI — a library caller
/// gating on `Report::passed` shares the guard. The snapshot shape is
/// real: an empty glob reads no files, checkpoints never, and declares
/// no cursor field. Acknowledged, the same run passes with the skip
/// rendered honestly.
#[tokio::test]
async fn an_unacknowledged_source_skip_fails_the_report_itself() {
    use rdlt_certify::Verdict;

    let dir = tempfile::tempdir().expect("tempdir");
    let config = serde_json::json!({
        "streams": [{
            "name": "events",
            "format": "jsonl",
            "path": format!("{}/*.jsonl", dir.path().display()),
        }]
    });
    let bin = built_bin("rdlt-connector-file");

    let strict = certify_source(&Target::resolve_path(bin.clone(), config.clone()), false).await;
    assert!(
        !strict.passed(),
        "an unacknowledged snapshot source must not pass:\n{}",
        strict.render_text()
    );
    assert!(
        strict.entries.iter().any(|entry| entry.clause == "S2"
            && matches!(&entry.verdict, Verdict::Fail(why)
                if why.contains("--accept-skips") && why.contains("not exercised"))),
        "S2 fails naming the acknowledgment:\n{}",
        strict.render_text()
    );

    let acknowledged = certify_source(&Target::resolve_path(bin, config), true).await;
    assert!(
        acknowledged.passed(),
        "the acknowledged snapshot source passes:\n{}",
        acknowledged.render_text()
    );
    assert!(
        acknowledged
            .entries
            .iter()
            .any(|entry| entry.clause == "S2" && matches!(entry.verdict, Verdict::Skip(_))),
        "the acknowledged run renders the skip honestly:\n{}",
        acknowledged.render_text()
    );
}
