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
