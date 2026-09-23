use std::error::Error as _;
use std::time::Duration;

use super::{ConnectorError, ConnectorErrorKind, LimitExceeded, ResultExt};

#[test]
fn only_transient_and_rate_limited_errors_are_retryable() {
    use ConnectorErrorKind as K;
    let retryable = [K::Transient, K::RateLimited];
    let final_kinds = [
        K::Config,
        K::Auth,
        K::Data,
        K::Unsupported,
        K::Fenced,
        K::Stopped,
        K::Internal,
    ];
    for kind in retryable {
        assert!(ConnectorError::new(kind, "x").is_retryable(), "{kind:?}");
    }
    for kind in final_kinds {
        assert!(!ConnectorError::new(kind, "x").is_retryable(), "{kind:?}");
    }
}

#[test]
fn classifying_a_foreign_error_keeps_it_as_the_cause() {
    let parsed: Result<u16, _> = "70000".parse::<u16>();
    let error = parsed.config("port is not a u16").unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Config);
    assert_eq!(error.to_string(), "port is not a u16");
    assert_eq!(
        error.source().unwrap().to_string(),
        "number too large to fit in target type"
    );
}

#[test]
fn every_classifier_sets_its_kind() {
    let failing = || Err::<(), _>(std::io::Error::other("boom"));
    let cases = [
        (failing().config("c"), ConnectorErrorKind::Config),
        (failing().auth("c"), ConnectorErrorKind::Auth),
        (failing().transient("c"), ConnectorErrorKind::Transient),
        (failing().data("c"), ConnectorErrorKind::Data),
        (failing().unsupported("c"), ConnectorErrorKind::Unsupported),
        (failing().internal("c"), ConnectorErrorKind::Internal),
    ];
    for (result, kind) in cases {
        assert_eq!(result.unwrap_err().kind(), kind);
    }
}

#[test]
fn limit_errors_carry_the_limit_and_a_code() {
    let limit = LimitExceeded {
        name: "cursor bytes",
        limit: 4,
        actual: 9,
    };
    let error = ConnectorError::exceeds(limit);
    assert_eq!(error.kind(), ConnectorErrorKind::Data);
    assert_eq!(error.code(), Some("limit_exceeded"));
    assert_eq!(error.limit(), Some(limit));
    assert_eq!(error.to_string(), "cursor bytes is 9, over the limit of 4");
}

#[test]
fn rate_limits_carry_the_requested_wait() {
    let error = ConnectorError::rate_limited("slow down", Some(Duration::from_secs(3)));
    assert_eq!(error.retry_after(), Some(Duration::from_secs(3)));
    assert_eq!(ConnectorError::data("x").retry_after(), None);
    assert_eq!(
        ConnectorError::fenced("x").kind(),
        ConnectorErrorKind::Fenced
    );
    assert_eq!(
        ConnectorError::stopped().kind(),
        ConnectorErrorKind::Stopped
    );
    assert_eq!(
        ConnectorError::internal("x").with_code("a.b").code(),
        Some("a.b")
    );
}
