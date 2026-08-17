//! The reference connector answers the same in-process kits every
//! shipping connector answers, both roles, through the sdk shell — the
//! SPI face `serve` runs over.

use rdlt_connector_reference::{destination, source};
use rdlt_connector_sdk::spi::destination::Destination;
use rdlt_connector_sdk::spi::source::Source;
use rdlt_testkit::conformance::{self, assert_conformant};
use serde_json::json;

use super::support::DirProbe;

/// The source kit: deterministic reads, the resume law over every
/// checkpoint, cancellation — certified against the same clauses every
/// shipping source answers, with no skips tolerated.
#[tokio::test]
async fn the_source_kit_certifies_the_shell() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("events.jsonl");
    std::fs::write(&path, "{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n").expect("seed file");
    let shell = rdlt_connector_sdk::source::Shell::<source::connector::Reference>::from_value(
        json!({"path": path}),
    )
    .expect("valid config");
    assert_eq!(shell.spec().name, "io.rapidbyte.reference");
    assert_conformant(
        conformance::source::verify(&shell)
            .await
            .expecting_no_skips(),
    );
}

/// The destination kit: staging invisibility, atomic state, idempotent
/// re-commit, crashed-session teardown — certified with no skips (D8
/// is never asserted: this destination declares `merge = false`).
#[tokio::test]
async fn the_destination_kit_certifies_the_shell() {
    let dir = tempfile::tempdir().expect("tempdir");
    let shell =
        rdlt_connector_sdk::destination::Shell::<destination::connector::Reference>::from_value(
            json!({"path": dir.path()}),
        )
        .expect("valid config");
    assert_eq!(shell.spec().name, "io.rapidbyte.reference");
    let probe = DirProbe(dir.path().to_path_buf());
    assert_conformant(
        conformance::destination::verify(&shell, &probe)
            .await
            .expecting_no_skips(),
    );
}
