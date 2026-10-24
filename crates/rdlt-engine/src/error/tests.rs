use std::error::Error as _;
use std::time::Duration;

use proptest::prelude::*;
use rdlt_connector::limits::MAX_ERROR_CODE_BYTES;
use rdlt_connector::{ConnectorError, ConnectorErrorKind, StreamName};

use super::{Error, ErrorKind, Origin, Side, codes};
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
        (K::Stopped, Side::Source, ErrorKind::Source, false),
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
        assert_eq!(error.code(), None);
        assert_eq!(error.connector_code(), Some("x.code"));
        assert_eq!(error.origin(), side.origin(), "{kind:?} from {side:?}");
        assert_eq!(error.report().origin, side.origin());
    }
}

#[test]
fn no_connector_error_has_the_engine_s_origin() {
    use ConnectorErrorKind as K;
    let kinds = [
        K::Config,
        K::Auth,
        K::Transient,
        K::RateLimited,
        K::Data,
        K::Unsupported,
        K::Fenced,
        K::Stopped,
        K::Internal,
    ];
    let codes = [
        None,
        Some("x.code"),
        Some("deadline_exceeded"),
        Some("state_invalid"),
    ];
    for kind in kinds {
        for code in codes {
            for (side, origin) in [
                (Side::Source, Origin::Source),
                (Side::Destination, Origin::Destination),
            ] {
                let mut raised = ConnectorError::new(kind, "the connector failed");
                if let Some(code) = code {
                    raised = raised.with_code(code);
                }
                let error = Error::connector(side, "doing something", raised);
                assert_eq!(error.origin(), origin, "{kind:?} {code:?} from {side:?}");
            }
        }
    }
}

#[test]
fn a_connector_error_coded_deadline_exceeded_is_the_connector_s() {
    for (side, origin) in [
        (Side::Source, Origin::Source),
        (Side::Destination, Origin::Destination),
    ] {
        // Its words and its cause's are the engine's wait's, and it is still the connector's.
        let said = "the commit took longer than the engine's wait of 30s";
        let cause = Driver {
            text: said.to_owned(),
            source: None,
        };
        let late = ConnectorError::new(ConnectorErrorKind::Transient, said)
            .with_code(rdlt_connector::DEADLINE_EXCEEDED)
            .with_source(cause);
        let error = Error::connector(side, "committing", late);
        assert_eq!(error.origin(), origin, "from {side:?}");
        assert!(error.is_retryable());
    }
}

#[test]
fn a_connector_cannot_claim_a_stop_or_a_fence_but_a_session_s_fence() {
    use ConnectorErrorKind as K;
    let kinds = [
        K::Config,
        K::Auth,
        K::Transient,
        K::RateLimited,
        K::Data,
        K::Unsupported,
        K::Fenced,
        K::Stopped,
        K::Internal,
    ];
    for kind in kinds {
        for side in [Side::Source, Side::Destination] {
            let mapped = Error::connector(side, "doing something", connector(kind)).kind();
            let fenced = kind == K::Fenced && side == Side::Destination;
            assert_eq!(
                mapped == ErrorKind::Fenced,
                fenced,
                "{kind:?} from {side:?}"
            );
            assert!(
                !matches!(
                    mapped,
                    ErrorKind::Cancelled | ErrorKind::Internal | ErrorKind::Wal | ErrorKind::Memory
                ),
                "{kind:?} from {side:?} is {mapped:?}"
            );
        }
    }
}

#[test]
fn only_a_rate_limit_keeps_the_wait_it_asks_for() {
    use rdlt_connector::wire::v1;
    let asking = |kind: v1::ErrorKind| {
        let wire = v1::Error {
            kind: kind as i32,
            message: "wait".to_owned(),
            code: None,
            retry_after: Some(v1::Duration {
                seconds: 9,
                nanos: 0,
            }),
            limit: None,
        };
        let error = ConnectorError::try_from(wire).unwrap();
        Error::connector(Side::Source, "reading", error).retry_after()
    };
    assert_eq!(
        asking(v1::ErrorKind::RateLimited),
        Some(Duration::from_secs(9))
    );
    assert_eq!(asking(v1::ErrorKind::Transient), None);
    assert_eq!(asking(v1::ErrorKind::Data), None);
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
    assert_eq!(report.code, None);
    assert_eq!(report.connector_code.as_deref(), Some("x.code"));
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
        assert_eq!(error.origin(), Origin::Engine);
        assert_eq!(error.report().origin, Origin::Engine);
        assert!(!error.is_retryable());
        assert!(error.source().is_none());
        assert!(error.report().causes.is_empty());
        assert_eq!(error.stream(), None);
        assert_eq!(error.code(), None);
        assert_eq!(error.connector_code(), None);
    }
    let coded = Error::config("c").with_code(codes::CONFIG_INVALID);
    assert_eq!(coded.code(), Some(codes::CONFIG_INVALID));
    assert_eq!(coded.report().code, Some(codes::CONFIG_INVALID));
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
    let rendered = format!(
        "{:?}",
        Error::schema("mismatch").with_code(codes::SCHEMA_FROZEN)
    );
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
        report.connector_code.as_ref().expect("a code"),
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
    // A name refuses what would hide or reorder text; a context of other text is shown.
    assert!(StreamName::new("orders\u{202e}").is_err());
    let stream = StreamName::new("orders").expect("a name");
    let raised = ConnectorError::data("refused");
    let context = "stream orders\u{202e}\u{2028}\u{200b}: reading";
    let error = Error::connector(Side::Source, context, raised).with_stream(&stream);
    let shown = error.to_string();
    assert_eq!(shown, r"stream orders\u{202e}\u{2028}\u{200b}: reading");
    assert!(error.report().stream.is_some_and(|name| name.is_ascii()));
}

#[test]
fn the_budget_s_error_says_what_held_it_and_may_be_tried_again() {
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
        control: 0,
        waited: Duration::from_secs(3600),
    };
    let memory = Error::memory(exhausted);
    assert_eq!(
        (
            memory.kind(),
            memory.origin(),
            memory.code(),
            memory.is_retryable()
        ),
        (
            ErrorKind::Memory,
            Origin::Engine,
            Some(codes::MEMORY_BUDGET_WAIT_EXCEEDED),
            true
        )
    );
    assert!(memory.to_string().contains("waited 3600s"), "{memory}");
}

#[test]
fn a_log_another_load_still_writes_is_waited_for_and_a_taken_one_is_not() {
    let load = rdlt_connector::LoadId::from_parts(std::time::UNIX_EPOCH, 3);
    let running = Error::wal_running(load);
    assert_eq!(running.code(), Some(codes::WAL_RUNNING));
    assert!(running.is_retryable(), "another attempt may end");
    let raced = Error::wal_opening_taken(load);
    assert_eq!(raced.code(), Some(codes::WAL_RUNNING));
    assert!(raced.is_retryable(), "another attempt begins");
    let fenced = Error::wal_fenced(load);
    assert_eq!(fenced.kind(), ErrorKind::Fenced);
    assert!(
        !fenced.is_retryable(),
        "another attempt took the load's place"
    );
}

#[test]
fn a_full_disk_under_the_log_is_retryable_and_says_so() {
    use std::io::{Error as Io, ErrorKind as Kind};
    for full in [Kind::StorageFull, Kind::QuotaExceeded] {
        let error = Error::from_wal(Io::from(full));
        assert_eq!(error.kind(), ErrorKind::Wal);
        assert_eq!(error.code(), Some(codes::WAL_STORAGE_FULL), "{full:?}");
        assert!(error.is_retryable(), "{full:?}");
    }
    for transient in [
        Kind::Interrupted,
        Kind::TimedOut,
        Kind::WouldBlock,
        Kind::ResourceBusy,
    ] {
        let error = Error::from_wal(Io::from(transient));
        assert_eq!(error.code(), None, "{transient:?}");
        assert!(error.is_retryable(), "{transient:?}");
    }
    for lasting in [Kind::PermissionDenied, Kind::InvalidData, Kind::Other] {
        let error = Error::from_wal(Io::from(lasting));
        assert_eq!(error.code(), None, "{lasting:?}");
        assert!(!error.is_retryable(), "{lasting:?}");
    }
}

#[test]
fn a_report_serializes_as_an_embedder_reads_it() {
    let stream = StreamName::new("orders").unwrap();
    let driver = Driver {
        text: "duplicate key value violates unique constraint".to_owned(),
        source: None,
    };
    let refused = ConnectorError::data("the batch was refused")
        .with_code("pg.unique_violation")
        .with_source(driver);
    let destination =
        Error::connector(Side::Destination, "writing a batch", refused).with_stream(&stream);
    let engine = Error::schema("stream orders: column total cannot hold text")
        .with_code(codes::SCHEMA_FROZEN)
        .with_stream(&stream);
    let reports = [destination.report(), engine.report()];
    insta::assert_snapshot!(serde_json::to_string_pretty(&reports).expect("reports serialize"));
}

#[test]
fn a_connector_s_code_is_never_the_engine_s() {
    let codes = [
        "x.code",
        "state_invalid",
        "deadline_exceeded",
        "memory_budget_wait_exceeded",
    ];
    for code in codes {
        for side in [Side::Source, Side::Destination] {
            let raised =
                ConnectorError::new(ConnectorErrorKind::Transient, "failed").with_code(code);
            let error = Error::connector(side, "doing something", raised);
            assert_eq!(
                (error.origin(), error.code(), error.connector_code()),
                (side.origin(), None, Some(code)),
                "{code} from {side:?}"
            );
            let report = error.report();
            assert_eq!(
                (report.code, report.connector_code.as_deref()),
                (None, Some(code))
            );
        }
    }
}

#[test]
fn the_engine_s_codes_read_as_their_text() {
    assert_eq!(codes::STATE_INVALID.as_str(), "state_invalid");
    assert_eq!(codes::STATE_INVALID.to_string(), "state_invalid");
    assert_eq!(
        serde_json::to_string(&codes::STATE_INVALID).unwrap(),
        r#""state_invalid""#
    );
    // Where the engine and a connector or the wire name one failure alike, the text is one.
    assert_eq!(
        codes::DEADLINE_EXCEEDED.as_str(),
        rdlt_connector::DEADLINE_EXCEEDED
    );
    assert_eq!(
        codes::LIMIT_EXCEEDED.as_str(),
        rdlt_wire::limits::LIMIT_EXCEEDED
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(256)))]

    /// Whatever code a connector's error carries, the engine keeps it as the connector's, shown
    /// as a connector's text is and within the limit of a code, and never as its own.
    #[test]
    fn any_code_a_connector_sends_is_shown_bounded_and_its_own(
        code in proptest::string::string_regex("(?s).{0,200}").unwrap(),
        destination in any::<bool>(),
    ) {
        let side = if destination { Side::Destination } else { Side::Source };
        let raised = ConnectorError::data("refused").with_code(code.as_str());
        let error = Error::connector(side, "doing something", raised);
        let shown = rdlt_connector::text::shown(&code, MAX_ERROR_CODE_BYTES);
        prop_assert_eq!(error.code(), None);
        prop_assert_eq!(error.origin(), side.origin());
        prop_assert_eq!(error.connector_code(), Some(shown.as_str()));
        prop_assert!(shown.len() <= MAX_ERROR_CODE_BYTES);
        prop_assert_eq!(error.report().connector_code, Some(shown));
    }
}
