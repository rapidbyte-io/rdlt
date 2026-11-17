use rdlt_connector::{ConnectorError, ConnectorErrorKind};

use super::{Spawned, Words};
use crate::remote::CONNECTOR_LOST;
use crate::secrets::{Redactions, SecretError, SecretFault, SecretKind};

#[test]
fn a_configuration_that_cannot_be_sent_again_says_neither_more_nor_less_than_its_cause() {
    for (cause, code) in [
        (SecretError::NotJson, "config_invalid"),
        (SecretError::TooMany { limit: 1 }, "config_invalid"),
    ] {
        let said = cause.to_string();
        let error = Spawned::Secret(cause).into_error();
        assert_eq!(error.code(), Some(code));
        assert_eq!(
            error.to_string(),
            "the connector's configuration could not be prepared"
        );
        let source = std::error::Error::source(&error).expect("its cause is kept");
        assert_eq!(source.to_string(), said);
    }
}

#[test]
fn a_respawn_that_does_not_start_in_time_is_lost_and_may_be_tried_again() {
    let late = std::io::Error::new(std::io::ErrorKind::TimedOut, "late");
    let error = Spawned::Io(late).into_error();
    assert_eq!(error.kind(), ConnectorErrorKind::Transient);
    assert_eq!(error.code(), Some(CONNECTOR_LOST));
    assert!(error.is_retryable());
}

#[test]
fn a_secret_store_that_fails_as_a_connector_starts_again_may_be_tried_again() {
    let unresolved = |source| SecretError::Unresolved {
        field: "password".to_owned(),
        kind: SecretKind::Named,
        source,
    };
    let failing = [
        SecretFault::Io(std::io::Error::other("too many open files")),
        SecretFault::Failed(Box::new(std::io::Error::other("unavailable"))),
    ];
    for fault in failing {
        let error = Spawned::Secret(unresolved(fault)).into_error();
        assert_eq!(error.kind(), ConnectorErrorKind::Transient, "{error:?}");
        assert_eq!(error.code(), Some("secret_unresolved"));
    }
    // What the store answers of the secret itself is the configuration's to mend.
    let answered = [
        SecretFault::Missing,
        SecretFault::Unsupported,
        SecretFault::NotText,
        SecretFault::TooLong { limit: 1 },
        SecretFault::Relative,
        SecretFault::NotRegular,
        SecretFault::Shared {
            owner: 0,
            mode: 0o644,
        },
    ];
    for fault in answered {
        let error = Spawned::Secret(unresolved(fault)).into_error();
        assert_eq!(error.kind(), ConnectorErrorKind::Config, "{error:?}");
        assert!(!error.is_retryable());
    }
    let refused = SecretError::Refused {
        field: "password".to_owned(),
        kind: SecretKind::Env,
    };
    let error = Spawned::Secret(refused).into_error();
    assert_eq!(error.kind(), ConnectorErrorKind::Config);
}

#[tokio::test]
async fn the_hosts_own_code_is_kept_whatever_a_connector_was_sent() {
    let redactions = Redactions::new();
    for value in ["lost", "connector", "_"] {
        redactions.add(value);
    }
    let words = Words {
        redactions,
        witness: None,
    };
    let lost = ConnectorError::new(ConnectorErrorKind::Transient, "the connector was lost")
        .with_code(CONNECTOR_LOST);
    let explained = words.explain::<()>(Err(lost)).await;
    let explained = explained.expect_err("it failed");
    assert_eq!(explained.code(), Some(CONNECTOR_LOST));
    assert_eq!(explained.to_string(), "the *** was ***");
}
