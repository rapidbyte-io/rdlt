//! The kill matrix over the REAL file connector bin: SIGKILL at every
//! message boundary of the K-vocabulary — K-S1/K-S2/K-S3 on the read
//! wire, K-D1..K-D6 on the session wire — asserting a typed error
//! surfaces (never a hang) and, for the destination arms, that a fresh
//! re-run converges to exactly-once under the read-back probe.
//!
//! The vacuity arms this suite carries itself: a probe rooted at the
//! WRONG directory must FAIL the convergence assert (the count judgment
//! is live, not decorative), and a probe-less run must Skip every
//! destination K-clause with the `NO_PROBE_SKIP` reason the matrix
//! shares with `certify_destination` (`src/destination.rs`) — never
//! silently narrow, never vacuously pass. The kill itself is proven
//! able to fail by K-D6's no-op arm: a kill that duplicated rows would
//! break its exact-count assert.

use std::path::Path;

use rdlt_certify::{Entry, Target, Verdict, kill_matrix_destination, kill_matrix_source};
use serde_json::json;

use super::support::bins::built_bin;
use super::support::probe::JsonlDirProbe;

/// The skip reason a probe-less run stamps on every destination
/// K-clause — `src/destination.rs`'s `NO_PROBE_SKIP` spelling,
/// byte-identical (that const is `pub(crate)`, so this pin restates it
/// from outside).
const NO_PROBE_REASON: &str = "no table probe supplied — read-back clauses need one; pass --probe-cmd '<sh line>' \
     (the library API takes a TableProbe directly). Single-writer stores (duckdb) refuse \
     every open beside the live connector, a read-only one included — probe a COPY: copy \
     the store file plus its WAL sidecar, then count in the copy";

/// Render entries the report way, for failure messages.
fn render(entries: &[Entry]) -> String {
    let mut out = String::new();
    for entry in entries {
        out.push_str(&match &entry.verdict {
            Verdict::Pass => format!("PASS {}\n", entry.clause),
            Verdict::Fail(why) => format!("FAIL {}: {why}\n", entry.clause),
            Verdict::Skip(why) => format!("SKIP {}: {why}\n", entry.clause),
        });
    }
    out
}

/// A jsonl fixture big enough that the read stream provably outlives
/// the kill: the kill-matrix source arms dial with a floored h2 window
/// (64 KiB), so a fixture WELL past that (60 files, 100 rows each,
/// ~3 KiB per file plus a growing checkpoint per file) guarantees the
/// server is still blocked mid-stream when the kill lands after frame 1
/// — a smaller fixture could be fully in flight before the SIGKILL and
/// end the stream cleanly instead of erroring.
fn write_source_fixture(dir: &Path) {
    for file in 0..60 {
        let mut text = String::new();
        for row in 0..100 {
            let id = file * 100 + row;
            text.push_str(&format!("{{\"id\":{id},\"name\":\"row-{id}\"}}\n"));
        }
        std::fs::write(dir.join(format!("rows-{file:02}.jsonl")), text)
            .expect("the fixture file writes");
    }
}

fn source_target(fixture_dir: &Path) -> Target {
    let config = json!({
        "streams": [{
            "name": "events",
            "format": "jsonl",
            "path": format!("{}/*.jsonl", fixture_dir.display()),
        }]
    });
    Target::resolve_path(built_bin("rdlt-connector-file"), config)
}

fn dest_target(out_root: &Path) -> Target {
    let config = json!({
        "path": out_root.display().to_string(),
        "format": "jsonl",
    });
    Target::resolve_path(built_bin("rdlt-connector-file"), config)
}

/// The source half: every boundary in K order, every arm Pass — the
/// killed connector's wire fails typed within the window, never hangs.
#[tokio::test]
async fn the_source_kill_matrix_passes_at_every_boundary() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_source_fixture(dir.path());

    let entries = kill_matrix_source(&source_target(dir.path())).await;

    let clauses: Vec<&str> = entries.iter().map(|entry| entry.clause).collect();
    assert_eq!(
        clauses,
        ["K-S1", "K-S2", "K-S3"],
        "the K-vocabulary is fixed, in order"
    );
    for entry in &entries {
        assert!(
            matches!(entry.verdict, Verdict::Pass),
            "every source kill arm must Pass:\n{}",
            render(&entries)
        );
    }
}

/// The destination half: every boundary in K order, every arm Pass —
/// typed error after the kill, and exactly-once convergence proven by a
/// fresh re-run under the real probe (K-D6's arm doubles as the
/// kill-can-fail proof: a kill that duplicated rows would break its
/// exact count).
#[tokio::test]
async fn the_destination_kill_matrix_passes_at_every_boundary() {
    let out = tempfile::tempdir().expect("tempdir");
    let probe = JsonlDirProbe {
        root: out.path().to_path_buf(),
    };

    let entries = kill_matrix_destination(&dest_target(out.path()), Some(&probe)).await;

    let clauses: Vec<&str> = entries.iter().map(|entry| entry.clause).collect();
    assert_eq!(
        clauses,
        ["K-D1", "K-D2", "K-D3", "K-D4", "K-D5", "K-D6"],
        "the K-vocabulary is fixed, in order"
    );
    for entry in &entries {
        assert!(
            matches!(entry.verdict, Verdict::Pass),
            "every destination kill arm must Pass:\n{}",
            render(&entries)
        );
    }
}

/// Without a probe the destination arms Skip with the `NO_PROBE_SKIP`
/// reason — convergence is a read-back and cannot be judged, and
/// certification never silently narrows to a smaller passing set.
#[tokio::test]
async fn a_probe_less_destination_matrix_skips_with_the_reason() {
    let out = tempfile::tempdir().expect("tempdir");

    let entries = kill_matrix_destination(&dest_target(out.path()), None).await;

    let clauses: Vec<&str> = entries.iter().map(|entry| entry.clause).collect();
    assert_eq!(clauses, ["K-D1", "K-D2", "K-D3", "K-D4", "K-D5", "K-D6"]);
    for entry in &entries {
        match &entry.verdict {
            Verdict::Skip(reason) => assert_eq!(reason, NO_PROBE_REASON, "{}", entry.clause),
            other => panic!(
                "without a probe, {} must skip with the reason, not {other:?}",
                entry.clause
            ),
        }
    }
}

/// THE VACUITY ARM: a probe rooted at the WRONG directory sees zero
/// rows, so every convergence assert must FAIL — proof the count
/// judgment can fail at all. K-D4's evidence is pinned by both ends
/// (round-13: the arm table carries the invocation's entropy suffix,
/// so the middle is no longer a constant); the matrix ran against a
/// real target whose re-runs genuinely landed rows, so a Pass here
/// could only mean the assert never looked.
#[tokio::test]
async fn a_wrong_probe_fails_every_convergence_assert() {
    let out = tempfile::tempdir().expect("tempdir");
    let elsewhere = tempfile::tempdir().expect("tempdir");
    let wrong = JsonlDirProbe {
        root: elsewhere.path().to_path_buf(),
    };

    let entries = kill_matrix_destination(&dest_target(out.path()), Some(&wrong)).await;

    for entry in &entries {
        assert!(
            matches!(entry.verdict, Verdict::Fail(_)),
            "a wrong-rooted probe must fail every arm's convergence assert:\n{}",
            render(&entries)
        );
    }
    let d4 = entries
        .iter()
        .find(|entry| entry.clause == "K-D4")
        .expect("K-D4 has an entry");
    match &d4.verdict {
        Verdict::Fail(why) => assert!(
            why.starts_with("convergence failed: table `k_d4_")
                && why.ends_with(
                    "holds 0 rows where the kill matrix wrote 3 — the re-run lost or \
                     duplicated rows"
                ),
            "the K-D4 evidence names its entropy-suffixed table and the exact counts: {why}"
        ),
        other => panic!("K-D4 must Fail under the wrong probe, got {other:?}"),
    }
}
