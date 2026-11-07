use std::error::Error as _;
use std::fmt;

use rdlt_connector::StreamName;
use rdlt_connector::limits::MAX_ERROR_CAUSES;
use rdlt_connector::{ConnectorError, ConnectorErrorKind, ConnectorId, PipelineId, ReadMode, Role};
use rdlt_engine::{Code, EngineConfig, ErrorKind, Origin, PipelinePlan, StreamPlan, WriteMode};
use rdlt_host::ProviderError;

use super::Error;
use crate::codes;

fn id() -> ConnectorId {
    ConnectorId::parse("io.example.missing").unwrap()
}

#[test]
fn a_missing_connector_is_a_config_error_of_its_side_coded_as_the_host_codes_it() {
    let missing = ProviderError::NotFound {
        id: id(),
        source: None,
    };
    let error = Error::placement(Role::Source, missing);
    assert_eq!(error.kind(), ErrorKind::Config);
    assert_eq!(error.origin(), Origin::Source);
    assert_eq!(error.code(), Some(Code::new("connector_not_found")));
    let report = error.report();
    assert_eq!(
        (report.kind, report.code),
        (ErrorKind::Config, error.code())
    );
    assert_eq!(report.origin, Origin::Source);
    assert!(
        report.message.starts_with("placing the source: "),
        "{}",
        report.message
    );
}

/// Checks `error`, placing the connector of `role`, is of `kind` and `origin`, coded as the host
/// codes it, and keeps the host's error as its cause.
fn placed(role: Role, error: ProviderError, expected: (ErrorKind, Origin)) {
    let code = Code::new(error.code());
    let placing = Error::placement(role, error);
    assert_eq!((placing.kind(), placing.origin()), expected, "{placing:?}");
    assert_eq!(placing.code(), Some(code));
    assert!(
        placing.source().is_some(),
        "the host's error is kept as the cause"
    );
    assert!(!placing.is_retryable());
    assert_eq!(placing.connector_code(), None);
}

#[test]
fn a_connector_that_cannot_start_or_be_reached_fails_its_side() {
    let unstarted = || ProviderError::SpawnFailed {
        id: id(),
        path: "/opt/rdlt/bin/rdlt-connector-missing".into(),
        source: std::io::Error::other("exec format error"),
    };
    let unreachable = ProviderError::Unreachable {
        id: id(),
        endpoint: "db.example:7443".to_owned(),
        source: std::io::Error::other("connection refused"),
    };
    let untrusted = ProviderError::Tls {
        id: id(),
        endpoint: "db.example:7443".to_owned(),
        source: Box::new(std::io::Error::other("unknown issuer")),
    };
    let source = (ErrorKind::Source, Origin::Source);
    let destination = (ErrorKind::Destination, Origin::Destination);
    placed(Role::Destination, unstarted(), destination);
    placed(Role::Source, unstarted(), source);
    placed(Role::Source, unreachable, source);
    placed(Role::Destination, untrusted, destination);
    let broken = ConnectorError::internal("broke");
    let handshake = ProviderError::HandshakeFailed {
        id: id(),
        source: Box::new(broken),
    };
    placed(Role::Destination, handshake, destination);
}

#[test]
fn a_connector_refusing_its_config_and_a_placement_the_host_refuses_are_config() {
    let handshake = |error: ConnectorError| ProviderError::HandshakeFailed {
        id: id(),
        source: Box::new(error),
    };
    let unsupported = ConnectorError::new(ConnectorErrorKind::Unsupported, "no merge");
    let denied = ConnectorError::new(ConnectorErrorKind::Auth, "denied");
    let refused = ProviderError::Refused {
        id: id(),
        placement: "remote",
    };
    let config = |origin| (ErrorKind::Config, origin);
    let configured = handshake(ConnectorError::config("no such table"));
    placed(Role::Source, configured, config(Origin::Source));
    placed(Role::Source, handshake(unsupported), config(Origin::Source));
    placed(
        Role::Destination,
        handshake(denied),
        config(Origin::Destination),
    );
    placed(Role::Destination, refused, config(Origin::Destination));
}

#[test]
fn a_facade_error_reports_its_code_message_and_cause() {
    let error = Error::config_caused(
        codes::REFERENCE_INVALID,
        "the source connector's id is not one",
        std::io::Error::other("the cause"),
    );
    assert_eq!(error.kind(), ErrorKind::Config);
    assert_eq!(error.origin(), Origin::Engine);
    assert_eq!(error.code(), Some(codes::REFERENCE_INVALID));
    assert_eq!(error.to_string(), "the source connector's id is not one");
    assert_eq!(error.stream(), None);
    let report = error.report();
    assert_eq!(report.causes, ["the cause"]);
    assert_eq!(report.stream, None);
    assert_eq!(report.message, "the source connector's id is not one");
    assert!(!report.retryable);
    let debug = format!("{error:?}");
    for part in ["Config", "Engine", "reference_invalid", "is not one"] {
        assert!(debug.contains(part), "{debug}");
    }
}

/// An error whose chain of causes is `depth` long.
#[derive(Debug)]
struct Chain {
    depth: usize,
    next: Option<Box<Chain>>,
}

impl Chain {
    fn of(depth: usize) -> Self {
        let next = (depth > 1).then(|| Box::new(Self::of(depth - 1)));
        Self { depth, next }
    }
}

impl fmt::Display for Chain {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "cause {}", self.depth)
    }
}

impl std::error::Error for Chain {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.next
            .as_deref()
            .map(|next| next as &(dyn std::error::Error + 'static))
    }
}

#[test]
fn a_report_holds_at_most_the_causes_a_report_may() {
    let error = Error::config_caused(codes::DOCUMENT_INVALID, "deep", Chain::of(20));
    let causes = error.report().causes;
    assert_eq!(causes.len(), MAX_ERROR_CAUSES);
    assert_eq!(causes.first().map(String::as_str), Some("cause 20"));
    assert_eq!(causes.last().map(String::as_str), Some("cause 13"));
}

#[test]
fn an_engine_error_reports_as_the_engine_reports_it() {
    let engine = EngineConfig::builder().memory(0).build().unwrap_err();
    let report = engine.report();
    let (kind, origin, code) = (engine.kind(), engine.origin(), engine.code());
    let display = engine.to_string();
    let error = Error::engine(engine);
    assert_eq!(error.report(), report);
    assert_eq!(
        (error.kind(), error.origin(), error.code()),
        (kind, origin, code)
    );
    assert_eq!(error.kind(), ErrorKind::Config);
    assert_eq!(error.to_string(), display);
    assert_eq!((error.stream(), error.connector_code()), (None, None));
    assert!(!error.is_retryable());
    assert!(error.source().is_none());
    let debug = format!("{error:?}");
    for part in ["Config", "Engine", "config_invalid", &display] {
        assert!(debug.contains(part), "{debug}");
    }
}

#[test]
fn an_engine_error_names_its_stream_as_the_stream_displays() {
    let stream = StreamName::with_namespace("public", "orders").unwrap();
    let modes = StreamPlan::new(stream)
        .read(ReadMode::Incremental)
        .write(WriteMode::Replace);
    let refused = PipelinePlan::new(PipelineId::parse("modes").unwrap(), [modes]).unwrap_err();
    let error = Error::engine(refused);
    assert_eq!(error.stream(), Some("public.orders"));
    assert_eq!(error.report().stream.as_deref(), Some("public.orders"));
}

#[test]
fn a_stream_given_to_an_error_names_it_where_the_error_names_none() {
    let built = Error::config(codes::STREAM_INVALID, "refused")
        .in_stream("a")
        .in_stream("b");
    assert_eq!(built.stream(), Some("a"));
    assert_eq!(built.report().stream.as_deref(), Some("a"));
    let unnamed = EngineConfig::builder().memory(0).build().unwrap_err();
    let unnamed = Error::engine(unnamed).in_stream("orders");
    assert_eq!(unnamed.stream(), Some("orders"));
    assert_eq!(unnamed.report().stream.as_deref(), Some("orders"));
    let stream = StreamName::with_namespace("public", "orders").unwrap();
    let modes = StreamPlan::new(stream)
        .read(ReadMode::Incremental)
        .write(WriteMode::Replace);
    let refused = PipelinePlan::new(PipelineId::parse("modes").unwrap(), [modes]).unwrap_err();
    let named = Error::engine(refused).in_stream("other");
    assert_eq!(named.stream(), Some("public.orders"));
    assert_eq!(named.report().stream.as_deref(), Some("public.orders"));
}

#[test]
fn a_secret_that_does_not_resolve_is_config_coded_as_the_host_codes_it() {
    let refused = rdlt_host::SecretError::Refused {
        field: "key".to_owned(),
        kind: rdlt_host::SecretKind::Env,
    };
    let error = Error::secret(refused);
    assert_eq!(error.kind(), ErrorKind::Config);
    assert_eq!(error.code(), Some(Code::new("secret_refused")));
    assert!(error.source().is_some());
}

#[test]
fn a_connector_s_failure_keeps_its_code_apart_and_says_whether_to_try_again() {
    let transient = ConnectorError::new(ConnectorErrorKind::Transient, "busy").with_code("busy");
    let error = Error::connector(Role::Destination, "discovering the catalog", transient);
    assert_eq!(
        (error.kind(), error.origin()),
        (ErrorKind::Destination, Origin::Destination)
    );
    assert_eq!(error.code(), Some(codes::CONNECTOR_FAILED));
    assert_eq!(error.connector_code(), Some("busy"));
    assert!(error.is_retryable());
    assert_eq!(
        error.to_string(),
        "discovering the catalog: the destination failed"
    );
    let report = error.report();
    assert_eq!(report.connector_code.as_deref(), Some("busy"));
    assert!(report.retryable);
    assert_eq!(report.causes, ["busy"]);
    for (failure, kind) in [
        (
            ConnectorError::new(ConnectorErrorKind::Auth, "denied"),
            ErrorKind::Config,
        ),
        (ConnectorError::config("bad dsn"), ErrorKind::Config),
        (ConnectorError::data("broken row"), ErrorKind::Source),
    ] {
        let error = Error::connector(Role::Source, "reading", failure);
        assert_eq!((error.kind(), error.origin()), (kind, Origin::Source));
        assert!(!error.is_retryable());
        assert_eq!(error.connector_code(), None);
    }
}

#[test]
fn a_failure_of_the_facade_s_own_is_internal() {
    let cause = std::io::Error::other("no cores");
    let error = Error::internal(
        codes::ENVIRONMENT_UNAVAILABLE,
        "starting the compute pool",
        cause,
    );
    assert_eq!(
        (error.kind(), error.origin()),
        (ErrorKind::Internal, Origin::Engine)
    );
    assert_eq!(error.code(), Some(codes::ENVIRONMENT_UNAVAILABLE));
    assert_eq!(error.report().causes, ["no cores"]);
}
