//! The headline: the REAL reference connector bin, certified as a
//! source over the wire — spawned by path (the id learned from its own
//! Spec reply), the P-clauses probed on live processes (including the
//! wire clauses P3/P5/P6/P7, judged on raw frames below the adapters),
//! and the testkit's S-clauses reused against the managed adapter. A
//! conformant connector comes out all-Pass — P5 vacuously so here (the
//! reference source serves raw_json frames, never arrow), which is the
//! clause's recorded posture for JSON-native sources. This is the
//! in-gate certifier exercise: the certification stack keeps facing a
//! REAL spawned connector with the first-party connectors out of tree.
//!
//! Certification runs TWICE in a row against the same target and the
//! same fixture file — the certification bar's repeated element (no
//! one-shot-only passes): a connector must survive being certified
//! again from the state the first certification left behind.
//!
//! The snapshot-source acknowledgment gate (`--accept-skips`) has no
//! wire-tier pin here: the reference source checkpoints on EVERY read
//! by design — the resume conformance S1/S2 certify — so it can never
//! mint the S2 skip. The fold itself (unacknowledged → FAIL naming the
//! flag, wrong name inert, acknowledged → honest Skip) stays pinned
//! in-process at `clause::s`'s own tests.

use rdlt_certify::clause::{p, s};
use rdlt_certify::report;
use rdlt_certify::target::Target;
use serde_json::json;

use super::support::bins::built_bin;

/// A conformant source certifies clean: every asserted clause `Pass`
/// with P13 the dual-role connector's one announced skip (the
/// reference serves both roles, so there is no unserved role to
/// refuse) — TWICE in a row, same target, same fixture file (each pass
/// spawns fresh connector processes; the shared file is the state both
/// passes read).
#[tokio::test]
async fn the_reference_source_certifies_all_pass() {
    let bin = built_bin("rdlt-connector-reference");
    let dir = tempfile::tempdir().expect("tempdir");
    let file = dir.path().join("events.jsonl");
    std::fs::write(&file, "{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n").expect("the fixture file writes");
    let config = json!({ "path": file });

    for _attempt in 1..=2 {
        let report = s::certify(&Target::resolve_path(bin.clone(), config.clone()), &[]).await;

        report::assert_all_pass(
            &report,
            &["S1", "S2", "S4", "P1", "P2", "P3", "P4", "P5", "P6", "P7"],
            &[("P13", p::SOURCE_DUAL_ROLE_SKIP)],
        );
    }
}
