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

/// The honest-check clauses, live on the reference's own hostile
/// shapes: a source configured at a DIRECTORY and a destination
/// configured at a FILE behind a trailing slash each refuse fatal at
/// `check()` — the kit's D7/S5 certify exactly the seats the reference
/// hardened.
#[tokio::test]
async fn the_check_refusal_clauses_certify_the_shells() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().join("events");
    std::fs::create_dir(&data_dir).expect("a directory where the file should be");
    let source_shell =
        rdlt_connector_sdk::source::Shell::<source::connector::Reference>::from_value(
            json!({"path": data_dir}),
        )
        .expect("valid config");
    assert_conformant(
        conformance::source::verify_check_refusal(&source_shell)
            .await
            .expecting_no_skips(),
    );

    let occupied = dir.path().join("occupied");
    std::fs::write(&occupied, b"x").expect("seed the file");
    let trailing = format!("{}/", occupied.display());
    let dest_shell =
        rdlt_connector_sdk::destination::Shell::<destination::connector::Reference>::from_value(
            json!({"path": trailing}),
        )
        .expect("valid config");
    assert_conformant(
        conformance::destination::verify_check_refusal(&dest_shell)
            .await
            .expecting_no_skips(),
    );
}

/// The kit's own kill matrix: a LYING check — `Ok` on a misconfigured
/// target — must FAIL the clause. In-process doubles whose `check`
/// answers `Ok` unconditionally stand in for the lying connector; the
/// clause must name D7/S5 against them, proving the clause can fail.
#[tokio::test]
async fn a_lying_check_fails_the_clause() {
    struct LyingSource;
    #[async_trait::async_trait]
    impl Source for LyingSource {
        fn spec(&self) -> rdlt_connector_sdk::spi::spec::ConnectorSpec {
            rdlt_connector_sdk::spi::spec::ConnectorSpec::new("liar", "0.0.0")
        }
        async fn check(&self) -> Result<(), rdlt_connector_sdk::spi::error::SourceError> {
            Ok(())
        }
        async fn streams(
            &self,
        ) -> Result<
            Vec<rdlt_connector_sdk::spi::source::StreamSpec>,
            rdlt_connector_sdk::spi::error::SourceError,
        > {
            unreachable!("the clause drives check alone")
        }
        async fn read(
            &self,
            _request: rdlt_connector_sdk::spi::source::ReadRequest,
        ) -> Result<(), rdlt_connector_sdk::spi::error::SourceError> {
            unreachable!("the clause drives check alone")
        }
    }
    let failures = conformance::source::verify_check_refusal(&LyingSource)
        .await
        .expecting_no_skips();
    assert_eq!(failures.len(), 1, "the lying check fails exactly S5");
    assert_eq!(failures[0].clause, "S5");
    assert!(failures[0].message.contains("answered Ok"));

    struct LyingDestination;
    #[async_trait::async_trait]
    impl Destination for LyingDestination {
        fn spec(&self) -> rdlt_connector_sdk::spi::spec::ConnectorSpec {
            rdlt_connector_sdk::spi::spec::ConnectorSpec::new("liar", "0.0.0")
        }
        async fn check(&self) -> Result<(), rdlt_connector_sdk::spi::error::DestinationError> {
            Ok(())
        }
        fn capabilities(&self) -> rdlt_connector_sdk::spi::destination::Capabilities {
            rdlt_connector_sdk::spi::destination::Capabilities::default()
        }
        async fn open(
            &self,
            _context: rdlt_connector_sdk::spi::destination::OpenContext,
        ) -> Result<
            Box<dyn rdlt_connector_sdk::spi::destination::LoadSession>,
            rdlt_connector_sdk::spi::error::DestinationError,
        > {
            unreachable!("the clause drives check alone")
        }
    }
    let failures = conformance::destination::verify_check_refusal(&LyingDestination)
        .await
        .expecting_no_skips();
    assert_eq!(failures.len(), 1, "the lying check fails exactly D7");
    assert_eq!(failures[0].clause, "D7");
    assert!(failures[0].message.contains("answered Ok"));
}
