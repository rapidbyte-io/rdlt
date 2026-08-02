#![cfg(feature = "failpoints")]
//! The crash sweep — its own binary, container-backed, selected by
//! name from the gate. Placeholder module until the live fixture
//! lands; the registry check already runs ungated in the integration
//! umbrella (cases/test_gating.rs).

/// The registry self-check the sweep runs before spending container
/// minutes — the ungated twin lives in cases/test_gating.rs.
#[test]
fn the_registry_matches_the_sources() {
    rdlt_testkit::assert_registry_matches_sources(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .as_path(),
        &[rdlt_connector_iceberg_v2::destination::FAIL_POINTS],
    );
}
