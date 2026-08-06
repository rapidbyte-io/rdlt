//! The headline: the REAL file connector bin, certified as a source
//! over the wire — spawned by path (the id learned from its own Spec
//! reply), the P-clauses probed on live processes (including the wire
//! clauses P3/P5/P6/P7, judged on raw frames below the adapters), and
//! the testkit's S-clauses reused against the managed adapter. A
//! conformant connector comes out all-Pass — P5 vacuously so here (the
//! jsonl source serves raw_json frames, never arrow), which is the
//! clause's recorded posture for JSON-native sources.

use rdlt_certify::{Target, Verdict, certify_source};
use serde_json::json;

use super::support::bins::built_bin;

/// A conformant source certifies clean: every entry `Pass`, and the
/// asserted set covers the S-reuse clauses plus the protocol clauses
/// this task probes.
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

    let report = certify_source(&Target::resolve_path(bin, config)).await;

    let clauses: Vec<&str> = report.entries.iter().map(|entry| entry.clause).collect();
    for clause in ["S1", "S2", "S4", "P1", "P2", "P3", "P4", "P5", "P6", "P7"] {
        assert!(
            clauses.contains(&clause),
            "clause {clause} has no entry — asserted set was {clauses:?}"
        );
    }
    assert!(
        report
            .entries
            .iter()
            .all(|entry| matches!(entry.verdict, Verdict::Pass)),
        "a conformant source must certify all-Pass:\n{}",
        report.render_text()
    );
}
