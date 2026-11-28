use std::any::Any;
use std::convert::Infallible;
use std::future::{Ready, ready};
use std::task::{Context, Poll, Waker};

use rdlt_wire::tonic::body::Body;
use tower::Service as _;

use super::{Contained, panicked};
use crate::error::ConnectorErrorKind;

#[test]
fn a_call_that_panics_fails_as_internal_saying_what_the_panic_said_where_it_is_text() {
    let bugs = 2;
    // A panic of a literal holds a `&str`, a formatted one a `String`, and `panic_any` anything.
    let payloads: [(Box<dyn Any + Send>, Option<&str>); 3] = [
        (
            Box::new("the commit met a bug"),
            Some("the commit met a bug"),
        ),
        (
            Box::new(format!("the commit met {bugs} bugs")),
            Some("the commit met 2 bugs"),
        ),
        (Box::new(7_u8), None),
    ];
    for (payload, said) in payloads {
        let error = panicked(payload.as_ref());
        assert_eq!(error.kind(), ConnectorErrorKind::Internal, "{error}");
        assert_eq!(error.code(), None, "{error}");
        if let Some(said) = said {
            assert!(error.to_string().contains(said), "{error}");
        }
    }
}

/// A service that is never ready for a call.
struct Busy;

impl tower::Service<http::Request<()>> for Busy {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = Ready<Result<http::Response<Body>, Infallible>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Pending
    }

    fn call(&mut self, _request: http::Request<()>) -> Self::Future {
        ready(Ok(http::Response::new(Body::empty())))
    }
}

#[test]
fn the_calls_are_ready_when_the_service_answering_them_is() {
    let mut context = Context::from_waker(Waker::noop());
    assert!(Contained(Busy).poll_ready(&mut context).is_pending());
    let answering = tower::service_fn(|_: http::Request<()>| {
        ready(Ok::<_, Infallible>(http::Response::new(Body::empty())))
    });
    assert!(Contained(answering).poll_ready(&mut context).is_ready());
}
