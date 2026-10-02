use std::error::Error as _;
use std::time::Duration;

use rdlt_connector::{ConnectorError, ConnectorErrorKind, StreamName};

use super::{Error, ErrorKind, Side};
use crate::scope::ScopeError;

fn connector(kind: ConnectorErrorKind) -> ConnectorError {
    ConnectorError::new(kind, "the connector failed").with_code("x.code")
}

#[test]
fn connector_errors_map_to_engine_kinds_by_side() {
    use ConnectorErrorKind as K;
    let cases = [
        (K::Config, Side::Source, ErrorKind::Config, false),
        (K::Auth, Side::Destination, ErrorKind::Config, false),
        (K::Unsupported, Side::Source, ErrorKind::Config, false),
        (K::Fenced, Side::Destination, ErrorKind::Fenced, false),
        (K::Stopped, Side::Source, ErrorKind::Cancelled, false),
        (K::Transient, Side::Source, ErrorKind::Source, true),
        (
            K::Transient,
            Side::Destination,
            ErrorKind::Destination,
            true,
        ),
        (K::Data, Side::Source, ErrorKind::Source, false),
        (K::Data, Side::Destination, ErrorKind::Destination, false),
        (
            K::Internal,
            Side::Destination,
            ErrorKind::Destination,
            false,
        ),
    ];
    for (kind, side, expected, retryable) in cases {
        let error = Error::connector(side, "doing something", connector(kind));
        assert_eq!(error.kind(), expected, "{kind:?} from {side:?}");
        assert_eq!(error.is_retryable(), retryable, "{kind:?} from {side:?}");
        assert_eq!(error.code(), Some("x.code"));
    }
}

#[test]
fn a_rate_limit_keeps_its_wait() {
    let limited = ConnectorError::rate_limited("slow down", Some(Duration::from_secs(7)));
    let error = Error::connector(Side::Source, "reading", limited);
    assert!(error.is_retryable());
    assert_eq!(error.retry_after(), Some(Duration::from_secs(7)));
    assert_eq!(Error::config("bad").retry_after(), None);
}

#[test]
fn reports_carry_the_kind_stream_code_and_causes() {
    let stream = StreamName::new("orders").unwrap();
    let error = Error::connector(
        Side::Source,
        "reading stream orders",
        connector(ConnectorErrorKind::Data),
    )
    .with_stream(&stream);
    let report = error.report();
    assert_eq!(report.kind, ErrorKind::Source);
    assert_eq!(report.stream.as_deref(), Some("orders"));
    assert_eq!(report.code.as_deref(), Some("x.code"));
    assert_eq!(report.message, "reading stream orders");
    assert_eq!(report.causes, ["the connector failed"]);
    assert!(!report.retryable);
    assert_eq!(error.stream(), Some(&stream));
    assert_eq!(error.to_string(), "reading stream orders");
    assert!(error.source().is_some());
}

#[test]
fn engine_errors_have_their_kinds_and_no_cause() {
    let cases = [
        (Error::config("c"), ErrorKind::Config),
        (Error::schema("s"), ErrorKind::Schema),
        (Error::cancelled("x"), ErrorKind::Cancelled),
        (Error::internal("i"), ErrorKind::Internal),
    ];
    for (error, kind) in cases {
        assert_eq!(error.kind(), kind);
        assert!(!error.is_retryable());
        assert!(error.source().is_none());
        assert!(error.report().causes.is_empty());
        assert_eq!(error.stream(), None);
        assert_eq!(error.code(), None);
    }
    assert_eq!(Error::config("c").with_code("k").code(), Some("k"));
}

#[test]
fn only_cancellations_count_as_induced_and_panics_are_internal() {
    assert!(Error::cancelled("x").is_cancelled());
    assert!(!Error::internal("x").is_cancelled());
    let panicked = Error::panicked("boom".to_owned());
    assert_eq!(panicked.kind(), ErrorKind::Internal);
    assert!(panicked.to_string().contains("boom"));
}

#[test]
fn debug_output_names_the_kind() {
    let rendered = format!("{:?}", Error::schema("mismatch").with_code("k"));
    assert!(rendered.contains("Schema"), "{rendered}");
}

/// A driver's error, whose text is whatever the system it spoke to said.
#[derive(Debug, thiserror::Error)]
#[error("{text}")]
struct Driver {
    text: String,
    #[source]
    source: Option<Box<Driver>>,
}

#[test]
fn a_report_shows_a_connectors_words_and_obeys_none_of_them() {
    use rdlt_connector::ResultExt as _;
    let hostile =
        "refused\r INFO rdlt_engine: commit 42 published\n\u{1b}[2J\u{9b}\u{202e}\u{200b}";
    let driver = Driver {
        text: hostile.to_owned(),
        source: None,
    };
    let failed: Result<(), Driver> = Err(driver);
    let raised = failed.transient(hostile).unwrap_err().with_code(hostile);
    let error = Error::connector(Side::Destination, hostile, raised);
    let report = error.report();
    let json = serde_json::to_string(&report).expect("a report serializes");
    let texts = [
        &report.message,
        report.code.as_ref().expect("a code"),
        &json,
    ]
    .into_iter()
    .chain(&report.causes);
    for text in texts {
        assert!(
            text.is_ascii() && !text.chars().any(char::is_control),
            "{text:?}"
        );
        assert!(text.contains("INFO rdlt_engine: commit 42 published"));
    }
    assert_eq!(report.causes.len(), 2);
}

#[test]
fn a_report_keeps_a_bounded_chain_of_bounded_causes() {
    use rdlt_connector::limits::{MAX_ERROR_CAUSES, MAX_ERROR_TEXT_BYTES};
    let long = "x".repeat(4 * MAX_ERROR_TEXT_BYTES);
    for depth in [1, MAX_ERROR_CAUSES - 1, MAX_ERROR_CAUSES, 1000] {
        let driver = (0..depth).fold(None, |source, _| {
            let text = long.clone();
            Some(Box::new(Driver { text, source }))
        });
        let raised = ConnectorError::internal(&long).with_source(*driver.expect("a chain"));
        let error = Error::connector(Side::Source, long.clone(), raised);
        let report = error.report();
        // The connector's own message is the first cause.
        assert_eq!(
            report.causes.len(),
            (depth + 1).min(MAX_ERROR_CAUSES),
            "{depth}"
        );
        assert_eq!(report.message.len(), MAX_ERROR_TEXT_BYTES);
        for cause in &report.causes {
            assert_eq!(cause.len(), MAX_ERROR_TEXT_BYTES);
        }
    }
}

#[test]
fn an_errors_display_shows_a_name_a_connector_chose_and_obeys_none_of_it() {
    let stream =
        StreamName::new("orders\u{202e}\u{2028}\u{200b}").expect("a name the id rules admit");
    let raised = ConnectorError::data("refused");
    let error = Error::connector(Side::Source, format!("stream {stream}: reading"), raised)
        .with_stream(&stream);
    let shown = error.to_string();
    assert_eq!(shown, r"stream orders\u{202e}\u{2028}\u{200b}: reading");
    assert!(error.report().stream.is_some_and(|name| name.is_ascii()));
}

#[test]
fn no_connector_error_is_of_the_memory_budget_s_kind_or_code() {
    use ConnectorErrorKind as K;
    let kinds = [
        K::Config,
        K::Auth,
        K::Unsupported,
        K::Fenced,
        K::Stopped,
        K::Transient,
        K::RateLimited,
        K::Data,
        K::Internal,
    ];
    for kind in kinds {
        for side in [Side::Source, Side::Destination] {
            let forged = ConnectorError::new(kind, "the budget failed, says the connector")
                .with_code("memory_budget_wait_exceeded");
            let error = Error::connector(side, "reading", forged);
            assert_ne!(error.kind(), ErrorKind::Memory, "{kind:?} from {side:?}");
            assert_eq!(error.code(), None, "{kind:?} from {side:?}");
            assert_eq!(error.report().code, None);
        }
    }
    // The engine's own says what held the budget, and may be tried again.
    let exhausted = crate::budget::Exhausted {
        what: "a push",
        asked: 10,
        capacity: 100,
        intake: 40,
        work: 0,
        cursors: 0,
        log: 0,
        tables: 0,
        reads: 25,
        waited: Duration::from_secs(3600),
    };
    let memory = Error::memory(exhausted);
    assert_eq!(
        (memory.kind(), memory.code(), memory.is_retryable()),
        (ErrorKind::Memory, Some("memory_budget_wait_exceeded"), true)
    );
    assert!(memory.to_string().contains("waited 3600s"), "{memory}");
}
