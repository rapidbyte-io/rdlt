use super::Spawned;
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
