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

/// A destination whose session says, as it closes, the secret it was sent.
struct Telling;

/// A session that says the secret it was sent as it closes.
struct TellingSession;

impl rdlt_connector::Destination for Telling {
    fn capabilities(&self) -> &rdlt_connector::Capabilities {
        unreachable!("no test asks")
    }

    fn check(&self) -> rdlt_connector::BoxFuture<'_, rdlt_connector::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn open<'a>(
        &'a self,
        _context: &'a rdlt_connector::OpenContext,
    ) -> rdlt_connector::BoxFuture<'a, rdlt_connector::Result<rdlt_connector::OpenedSession>> {
        Box::pin(async {
            Ok(rdlt_connector::OpenedSession {
                session: Box::new(TellingSession),
                epoch: rdlt_connector::Epoch(1),
                state: Vec::new(),
            })
        })
    }
}

impl rdlt_connector::DestinationSession for TellingSession {
    fn apply_schema<'a>(
        &'a mut self,
        _change: &'a rdlt_connector::TableChange,
    ) -> rdlt_connector::BoxFuture<'a, rdlt_connector::Result<()>> {
        unreachable!("no test asks")
    }

    fn writer<'a>(
        &'a mut self,
        _table: &'a rdlt_connector::TableRef,
    ) -> rdlt_connector::BoxFuture<
        'a,
        rdlt_connector::Result<Box<dyn rdlt_connector::DestinationWriter>>,
    > {
        unreachable!("no test asks")
    }

    fn commit<'a>(
        &'a mut self,
        _meta: &'a rdlt_connector::CommitMeta,
    ) -> rdlt_connector::BoxFuture<'a, rdlt_connector::Result<rdlt_connector::Receipt>> {
        unreachable!("no test asks")
    }

    fn close(self: Box<Self>) -> rdlt_connector::BoxFuture<'static, rdlt_connector::Result<()>> {
        let told = ConnectorError::new(ConnectorErrorKind::Transient, "closing with hunter2");
        Box::pin(async move { Err(told) })
    }
}

#[tokio::test]
async fn a_session_a_guarded_destination_opens_is_guarded_too() {
    use rdlt_connector::Destination as _;
    let redactions = Redactions::new();
    redactions.add("hunter2");
    let guarded = super::Guarded {
        inner: Box::new(Telling) as Box<dyn rdlt_connector::Destination>,
        redactions,
    };
    let context = rdlt_connector::OpenContext {
        pipeline: rdlt_connector::PipelineId::parse("guarded").expect("a valid id"),
        load_id: rdlt_connector::LoadId::from_parts(std::time::SystemTime::UNIX_EPOCH, 0),
    };
    let opened = guarded.open(&context).await.expect("it opens");
    assert_eq!(opened.epoch, rdlt_connector::Epoch(1));
    let closed = opened.session.close().await.expect_err("it fails");
    assert_eq!(closed.to_string(), "closing with ***");
}
