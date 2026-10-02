use std::error::Error as _;

use rdlt_connector::{ConnectorError, ConnectorErrorKind};

use super::received;
use crate::secrets::Redactions;

#[derive(Debug, thiserror::Error)]
#[error("error connecting to postgres://app:hunter2@db.internal/prod: refused\r\u{1b}[2J")]
struct Driver;

#[test]
fn an_error_is_kept_without_the_secrets_its_connector_was_sent_in_any_of_its_texts() {
    let redactions = Redactions::new();
    redactions.add("hunter2");
    let raised = ConnectorError::new(ConnectorErrorKind::Transient, "connecting with hunter2")
        .with_code("pg.hunter2")
        .with_source(Driver);
    let kept = received(&raised, &redactions);
    assert_eq!(kept.kind(), ConnectorErrorKind::Transient);
    assert_eq!(kept.to_string(), "connecting with ***");
    assert_eq!(kept.code(), Some("pg.***"));
    let cause = kept
        .source()
        .expect("the cause is kept, as text")
        .to_string();
    assert_eq!(
        cause,
        r"error connecting to postgres://app:***@db.internal/prod: refused\r\u{1b}[2J"
    );
    // The driver's own error, which could say it again, is not.
    assert!(
        kept.source()
            .expect("a cause")
            .downcast_ref::<Driver>()
            .is_none()
    );
}
