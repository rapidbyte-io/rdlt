//! THE KILL MATRIX (042 Task 6): the spawned duckdb bin SIGKILLed at
//! every K-D boundary against a hermetic tempdir database — the first
//! kill matrix on a SINGLE-WRITER file destination. All six arms of
//! the destination K-vocabulary: typed error on the dead wire, then
//! exactly-once convergence — a FRESH spawn re-runs the load and the
//! read-back must count the fixture rows EXACTLY (K-D6's no-op arm
//! doubles as the receipt-durability proof: the killed process's
//! receipt must survive in the file, which holds because duckdb's WAL
//! is flushed at commit — measured while this cell was written: a
//! committed row survives a SIGKILL and a sequential re-open reads
//! it).
//!
//! The read-back probe is `SnapshotCount` — the convergence run's
//! process is still alive holding the file lock when the count runs,
//! so a direct read-only open would be refused (`support::probe`'s
//! measurement). The lock also makes process reaping load-bearing
//! BETWEEN arms: the certifier kills-and-WAITS its spawns (the
//! convergence spawn included) before the next arm opens the same
//! file.

use rdlt_certify::{Entry, Target, Verdict, kill_matrix_destination};
use serde_json::json;

use super::support::probe::SnapshotCount;
use super::support::spawn::built_bin;

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

/// THE DESTINATION HALF: every boundary in K order, every arm a real
/// Pass — a probe is supplied and every boundary is reachable on this
/// destination, so no arm has a legitimate Skip.
#[tokio::test(flavor = "multi_thread")]
async fn the_destination_kill_matrix_passes_at_every_boundary() {
    let dir = tempfile::tempdir().expect("dir");
    let file = dir.path().join("kill.duckdb");
    let config = json!({ "path": file });
    let probe = SnapshotCount(file.clone());

    let entries =
        kill_matrix_destination(&Target::resolve_path(built_bin(), config), Some(&probe)).await;

    let clauses: Vec<&str> = entries.iter().map(|entry| entry.clause).collect();
    assert_eq!(
        clauses,
        ["K-D1", "K-D2", "K-D3", "K-D4", "K-D5", "K-D6"],
        "the K-vocabulary is fixed, in order"
    );
    assert!(
        entries
            .iter()
            .all(|entry| matches!(entry.verdict, Verdict::Pass)),
        "every kill arm must Pass:\n{}",
        render(&entries)
    );
}
