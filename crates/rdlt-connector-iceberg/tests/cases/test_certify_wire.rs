//! THE CERTIFICATION CELL (042 Task 7): iceberg certified over the
//! wire against the live Polaris/RUSTFS fixture. The REAL
//! `rdlt-connector-iceberg` bin is spawned by path, and the certify
//! library judges it: the role-generic protocol clauses (P1–P4), the
//! wire clauses on a raw handshake below the adapters (P3/P7), the
//! testkit's D-clauses reused against the managed adapter (D1–D6, with
//! D8 an honest SKIP — this destination declares `merge = false`, so
//! the merge clause never ran and a Pass would be minted, the file
//! destination's posture exactly), and the session clauses
//! P8/P9/P10/P11/P12 on raw dials of the live socket.
//!
//! Skip-not-fail: without a container runtime the fixture announces
//! the skip and the cell returns — the 015 convention every live
//! iceberg cell rides.

use rdlt_certify::{Target, Verdict, certify_destination};

use super::common::{CatalogFixture, LiveProbe};
use super::support::spawn::built_bin;

/// The skip reason D8 carries when the destination declares no merge
/// capability — the certifier asserts D8 only for merge-capable
/// destinations, and iceberg is not one (`merge = false`,
/// connector.rs).
const NO_MERGE_REASON: &str = "the destination does not declare the merge capability — D8 certifies merge upsert and was \
     not exercised";

/// THE DESTINATION CELL: the built iceberg bin certifies over the wire
/// against the live catalog — every clause a destination can face,
/// asserted present: D1–D6 live, D8 the honest merge-false Skip, and
/// ALL TEN protocol clauses P1/P2/P3/P4/P7/P8/P9/P10/P11/P12.
#[tokio::test(flavor = "multi_thread")]
async fn the_iceberg_destination_certifies_over_the_wire() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "certify_wire";
    let config = fixture.doc(namespace);
    let probe = LiveProbe {
        fixture,
        namespace: namespace.into(),
    };

    let report =
        certify_destination(&Target::resolve_path(built_bin(), config), Some(&probe)).await;

    let clauses: Vec<&str> = report.entries.iter().map(|entry| entry.clause).collect();
    for clause in [
        "D1", "D2", "D3", "D4", "D5", "D6", "D8", "P1", "P2", "P3", "P4", "P7", "P8", "P9", "P10",
        "P11", "P12",
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
                "iceberg declares no merge capability, so D8 must be an honest skip, not \
                 {other:?}"
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
