use std::error::Error as _;
use std::time::Duration;

use super::{ConnectorError, ConnectorErrorKind, LimitExceeded, RETENTION_LOST, ResultExt};

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

#[test]
fn a_lost_retention_is_a_data_error_no_retry_finds() {
    let error = ConnectorError::retention_lost("offset 4 is gone");
    assert_eq!(error.kind(), ConnectorErrorKind::Data);
    assert_eq!(error.code(), Some(RETENTION_LOST));
    assert!(!error.is_retryable());
}

/// An error whose text is `text`, caused by `source`.
#[derive(Debug, thiserror::Error)]
#[error("{text}")]
struct Driver {
    text: String,
    #[source]
    source: Option<Box<Driver>>,
}

/// A chain of `depth` driver errors, each saying its depth and `said`.
fn chain(depth: usize, said: &str) -> Option<Box<Driver>> {
    (0..depth).rev().fold(None, |source, level| {
        let text = format!("level {level}: {said}");
        Some(Box::new(Driver { text, source }))
    })
}

fn causes(error: &ConnectorError) -> Vec<String> {
    let mut causes = Vec::new();
    let mut cause = error.source();
    while let Some(error) = cause {
        causes.push(error.to_string());
        cause = error.source();
    }
    causes
}

#[test]
fn an_error_a_host_received_keeps_what_classifies_it() {
    let limit = LimitExceeded {
        name: "max_batch_rows",
        limit: 1,
        actual: 2,
    };
    let raised = ConnectorError::exceeds(limit);
    let received = raised.received(&|text| text);
    assert_eq!(received.kind(), ConnectorErrorKind::Data);
    assert_eq!(received.code(), Some("limit_exceeded"));
    assert_eq!(received.limit(), Some(limit));
    assert_eq!(received.to_string(), raised.to_string());
    let wait = Some(Duration::from_secs(7));
    let received = ConnectorError::rate_limited("slow down", wait).received(&|text| text);
    assert_eq!(received.retry_after(), wait);
    assert!(received.is_retryable());
    assert!(received.source().is_none());
}

#[test]
fn an_error_a_host_received_shows_its_message_code_and_causes_and_obeys_none() {
    let hostile = "\u{1b}[2J\r INFO forged\n\u{202e}\u{200b}\u{9b}";
    let raised = ConnectorError::internal(hostile)
        .with_code(hostile)
        .with_source(*chain(3, hostile).expect("a chain"));
    let received = raised.received(&|text| text);
    let texts: Vec<String> = [received.to_string(), received.code().unwrap().to_owned()]
        .into_iter()
        .chain(causes(&received))
        .collect();
    assert_eq!(texts.len(), 5);
    for text in texts {
        assert!(
            text.is_ascii() && !text.chars().any(char::is_control),
            "{text:?}"
        );
        assert!(text.contains(r"\u{1b}[2J\r INFO forged"), "{text:?}");
    }
}

#[test]
fn an_error_a_host_received_is_bounded_in_text_and_in_causes() {
    use crate::limits::{MAX_ERROR_CAUSES, MAX_ERROR_CODE_BYTES, MAX_ERROR_TEXT_BYTES};
    let long = "x".repeat(MAX_ERROR_TEXT_BYTES * 4);
    for depth in [0, 1, MAX_ERROR_CAUSES, MAX_ERROR_CAUSES + 1, 1000] {
        let mut raised = ConnectorError::internal(&long).with_code(long.as_str());
        if let Some(source) = chain(depth, &long) {
            raised = raised.with_source(*source);
        }
        let received = raised.received(&|text| text);
        assert_eq!(received.to_string().len(), MAX_ERROR_TEXT_BYTES);
        assert_eq!(received.code().unwrap().len(), MAX_ERROR_CODE_BYTES);
        let causes = causes(&received);
        assert_eq!(causes.len(), depth.min(MAX_ERROR_CAUSES), "{depth}");
        for (level, cause) in causes.iter().enumerate() {
            assert_eq!(cause.len(), MAX_ERROR_TEXT_BYTES);
            // Outermost first, as they were.
            assert!(cause.starts_with(&format!("level {level}: ")), "{cause}");
        }
    }
}

#[test]
fn what_a_host_scrubs_is_gone_from_the_message_the_code_and_every_cause_before_any_is_cut() {
    use crate::limits::MAX_ERROR_TEXT_BYTES;
    // The secret straddles the point where the text is cut.
    let said = format!("{}hunter2", "x".repeat(MAX_ERROR_TEXT_BYTES - 3));
    let raised = ConnectorError::config(&said)
        .with_code("hunter2")
        .with_source(*chain(2, &said).expect("a chain"));
    let received = raised.received(&|text| text.replace("hunter2", "***"));
    let texts = [received.to_string(), received.code().unwrap().to_owned()];
    for text in texts.into_iter().chain(causes(&received)) {
        assert!(!text.contains("hun"), "{text}");
    }
    assert_eq!(received.code(), Some("***"));
}
