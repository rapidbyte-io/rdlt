//! THE CERTIFICATION CELLS: the reference connector certified over the
//! wire, BOTH roles. The REAL `rdlt-connector-reference` bin is
//! spawned by path and the certify library judges it — the testkit's
//! S-/D-clauses reused against the managed adapters, the role-generic
//! protocol clauses, and the session clauses on raw dials of the live
//! socket. Hermetic on tempdirs — no container runtime, no network —
//! so neither cell ever skips.

use rdlt_certify::clause::{d, p, s};
use rdlt_certify::report;
use rdlt_certify::target::Target;
use serde_json::json;

use super::common::DirProbe;

/// The built reference bin, located through the ONE spawn scaffold —
/// the mechanics (`CARGO_TARGET_DIR` resolution, the
/// `RDLT_BUILD_CONNECTOR_BINS` guard, the loud missing-bin refusal)
/// live in [`rdlt_testkit::spawn::built_connector_bin`], never copied.
fn built_bin() -> std::path::PathBuf {
    rdlt_testkit::spawn::built_connector_bin(env!("CARGO_MANIFEST_DIR"), env!("CARGO_PKG_NAME"))
}

/// THE SOURCE CELL: the built bin certifies all-Pass as a source over
/// the wire — S1/S2/S4 against a seeded jsonl file plus every protocol
/// clause a source can face; P13 is the dual-role connector's one
/// announced skip (both roles are served, so there is no unserved role
/// to refuse).
#[tokio::test(flavor = "multi_thread")]
async fn the_reference_source_certifies_all_pass() {
    let dir = tempfile::tempdir().expect("tempdir");
    let file = dir.path().join("events.jsonl");
    std::fs::write(&file, "{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n").expect("seed file");

    let report = s::certify(
        &Target::resolve_path(built_bin(), json!({"path": file})),
        &[],
    )
    .await;

    report::assert_all_pass(
        &report,
        &["S1", "S2", "S4", "P1", "P2", "P3", "P4", "P5", "P6", "P7"],
        &[("P13", p::SOURCE_DUAL_ROLE_SKIP)],
    );
}

/// THE DESTINATION CELL: the built bin certifies over the wire against
/// a tempdir — every clause a destination can face asserted present:
/// D1–D6 live, D8 the honest merge-false Skip, ALL TEN protocol
/// clauses including the write-side P11/P12, and P13 the dual-role
/// connector's announced skip.
#[tokio::test(flavor = "multi_thread")]
async fn the_reference_destination_certifies_all_pass_with_the_merge_skip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("out");
    let probe = DirProbe(out.clone());

    let report = d::certify(
        &Target::resolve_path(built_bin(), json!({"path": out})),
        Some(&probe),
    )
    .await;

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
