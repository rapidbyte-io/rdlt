//! The registry-vs-sources pin, UNGATED: three registries over one
//! source tree, checked against their union (the 024 rule — never
//! "simplify" this to per-registry equality).

use rdlt_connector_file::destination::{FAIL_POINTS, S3_FAIL_POINTS};
use rdlt_connector_file::source;

#[test]
fn the_registries_match_the_sources() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    rdlt_testkit::assert_registry_matches_sources(
        &src,
        &[FAIL_POINTS, S3_FAIL_POINTS, source::FAIL_POINTS],
    );
}
