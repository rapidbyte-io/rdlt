//! THE KILL MATRIX (042 Task 7, D-042-3): the spawned iceberg bin
//! SIGKILLed at every K-D boundary against the live Polaris/RUSTFS
//! fixture — the kill matrix's first CATALOG destination. All six arms
//! of the destination K-vocabulary RUN LIVE FIRST (the defining rule):
//! typed error on the dead wire, then exactly-once convergence — a
//! FRESH spawn re-runs the load and the read-back must count the
//! fixture rows EXACTLY, the count read off the catalog's own snapshot
//! summaries.
//!
//! Skip-not-fail: without a container runtime the fixture announces
//! the skip and the cell returns — the 015 convention every live
//! iceberg cell rides.

use rdlt_certify::{Entry, Target, Verdict, kill_matrix_destination};

use super::common::{CatalogFixture, LiveProbe};
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

/// THE DESTINATION HALF: every boundary in K order, all six arms run
/// live (D-042-3 — the matrix is never narrowed on a counting
/// argument), every arm a real Pass.
#[tokio::test(flavor = "multi_thread")]
async fn the_destination_kill_matrix_passes_at_every_boundary() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "kill_wire";
    let config = fixture.doc(namespace);
    let probe = LiveProbe {
        fixture,
        namespace: namespace.into(),
    };

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
