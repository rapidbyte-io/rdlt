//! Does the shared scanner see what a plain text search sees?
//!
//! A scanner is the one piece of this machinery that fails OPEN when wrong: it
//! finds fewer sites and every assertion still passes. So its own count is
//! checked against an independently-derived one before any registry trusts it.

use std::path::Path;

/// Distinct crash-point names the scanner must find beside an arming call, per
/// crate. Recorded independently — by reading the sources — so that a scanner
/// which quietly stopped finding sites is caught. A scanner is the one piece of
/// this machinery that fails OPEN when wrong: it finds less and every assertion
/// it feeds still passes.
///
/// These are DISTINCT NAMES, not call sites. A name armed at two places counts
/// once, because the registry lists names.
///
/// Some names are armed INDIRECTLY — the `crash_point!` takes a variable and the
/// literal lives at the constructor supplying it — so they are absent here by
/// design and covered instead by the "declared names must appear twice" half of
/// `assert_registry_matches_sources`.
/// The connector crates' rows (file 14, rest 3, iceberg 3, duckdb 2,
/// oracle 2, snowflake's two-spellings proof at 4, postgres's
/// indirect-arming proof at 11) moved with their crates to the
/// rdlt-connectors repository at the 044 cut and live in ITS gate:
/// `crates/examples-gate/tests/scanner_selfcheck.rs` there carries the
/// per-crate counts beside the sources they count, including the
/// two-spellings and indirect-arming evidence. The scanner itself is
/// shared (this crate rides both repos as the verification half), so
/// the selfcheck here keeps the one crate this workspace arms.
const EXPECTED_DIRECT_NAMES: &[(&str, usize)] = &[("rdlt-engine", 7)];

#[test]
fn the_scanner_finds_every_directly_armed_name() {
    let crates_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/ is the parent of this crate");
    for (crate_name, expected) in EXPECTED_DIRECT_NAMES {
        let src = crates_dir.join(crate_name).join("src");
        let found = rdlt_testkit::armed_crash_points(&src);
        assert_eq!(
            found.len(),
            *expected,
            "{crate_name}: scanner found {} distinct names, expected {expected}: {found:?}",
            found.len()
        );
    }
}

/// The vacuity guard: scanning a directory with no arming calls, against a
/// non-empty registry, must FAIL rather than agree.
///
/// This is the one way the whole registry check could itself pass while verifying
/// nothing — a mistyped path or an unrecognised arming spelling yields an empty
/// set, and an empty set trivially satisfies "everything armed is declared".
#[test]
#[should_panic(expected = "no crash-point sites found")]
fn scanning_nowhere_against_a_real_registry_fails() {
    let empty = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/memory");
    rdlt_testkit::assert_registry_matches_sources(&empty, &[&["some.point"]]);
}
