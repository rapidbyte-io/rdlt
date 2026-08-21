//! The S2 snapshot door: a stream that DECLARES no `cursor_field` and
//! never checkpoints is a snapshot stream by its own declaration — the
//! suite skips S2 with the reason instead of failing it, so an honest
//! snapshot source is certifiable. The door turns on the DECLARATION:
//! a stream that promises a cursor and still never checkpoints keeps
//! failing by name, and the strict fold first-party cells use promotes
//! any skip back to a failure.

use rdlt_testkit::conformance::source::verify_source;
use rdlt_testkit::{MemoryBatch, MemorySource, MemoryStream};
use serde_json::json;

/// One stream over `spec` whose batches never checkpoint.
fn checkpointless(spec: rdlt_connector::StreamSpec) -> MemorySource {
    MemorySource::new(vec![MemoryStream::new(
        spec,
        vec![MemoryBatch::new(vec![json!({"a": 1}), json!({"a": 2})])],
    )])
}

/// `cursor_field: None` + zero checkpoints: nothing violated, one S2
/// skip with the pinned reason — full-string, because the certifier
/// renders it verbatim on its SKIP line.
#[tokio::test]
async fn an_honest_snapshot_stream_skips_s2_with_the_reason() {
    let source = checkpointless(rdlt_connector::StreamSpec::new("events"));

    let outcome = verify_source(&source).await;
    // The explicit acknowledgment path — this cell's whole subject is
    // the skip's shape.
    let (failures, raw_skips, _concluded) = outcome.tolerating_skips();

    assert!(
        failures.is_empty(),
        "an honest snapshot stream violates nothing: {failures:?}"
    );
    let skips: Vec<(&str, &str)> = raw_skips
        .iter()
        .map(|skip| (skip.clause, skip.reason.as_str()))
        .collect();
    assert_eq!(
        skips,
        vec![(
            "S2",
            "stream `events` declares no cursor_field and never checkpoints — an honest \
             snapshot stream: there is no resume to certify, and every run re-reads everything"
        )]
    );
}

/// A DECLARED cursor with zero checkpoints is still the S2 violation,
/// spelled exactly as before — the stream promised resume and delivered
/// none.
#[tokio::test]
async fn a_cursored_stream_with_no_checkpoints_still_fails_s2() {
    let source = checkpointless(rdlt_connector::StreamSpec::new("events").with_cursor_field("a"));

    let outcome = verify_source(&source).await;
    let (raw_failures, skips, _concluded) = outcome.tolerating_skips();

    assert!(
        skips.is_empty(),
        "a broken cursor promise earns no skip: {skips:?}"
    );
    let failures: Vec<(&str, &str)> = raw_failures
        .iter()
        .map(|failure| (failure.clause, failure.message.as_str()))
        .collect();
    assert_eq!(
        failures,
        vec![(
            "S2",
            "stream `events` never checkpoints — resume (S1) cannot be certified and every \
             restart re-reads everything"
        )]
    );
}

/// The strict fold: `expecting_no_skips` promotes the skip to a failure
/// of its clause, so a first-party cell asserting full conformance
/// cannot go green because a stream quietly stopped declaring its
/// cursor.
#[tokio::test]
async fn expecting_no_skips_promotes_the_skip_to_a_failure() {
    let source = checkpointless(rdlt_connector::StreamSpec::new("events"));

    let failures = verify_source(&source).await.expecting_no_skips();

    assert_eq!(failures.len(), 1, "exactly the promoted skip: {failures:?}");
    assert_eq!(failures[0].clause, "S2");
    assert!(
        failures[0].message.starts_with("not exercised: "),
        "the promoted failure names the skip as unexercised: {}",
        failures[0].message
    );
}
