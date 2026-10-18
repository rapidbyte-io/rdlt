use rdlt_connector::ConnectorErrorKind;

use super::Spawned;
use crate::remote::CONNECTOR_LOST;
use crate::secrets::SecretError;

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
