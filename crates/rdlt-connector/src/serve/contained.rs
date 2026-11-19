//! A served call whose answer panics, answered as an internal error of the connector, as a read
//! or a write that panics is, rather than with a reset stream its host would read as a
//! transient failure of the connection and retry.

#[cfg(test)]
mod tests;

use std::any::Any;
use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::task::{Context, Poll};

use rdlt_wire::tonic::body::Body;

use crate::error::{ConnectorError, ConnectorErrorKind};
use crate::wire::status;

/// The served calls of `S`, each answered as an internal error where answering it panics.
#[derive(Clone)]
pub(super) struct Contained<S>(pub(super) S);

impl<S, B> tower::Service<http::Request<B>> for Contained<S>
where
    S: tower::Service<http::Request<B>, Response = http::Response<Body>>,
    S::Future: Unpin,
{
    type Response = http::Response<Body>;
    type Error = S::Error;
    type Future = Answer<S::Future>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), S::Error>> {
        self.0.poll_ready(context)
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        Answer(Some(self.0.call(request)))
    }
}

/// A call's answer being made; nothing once it is made or panicked.
pub(super) struct Answer<F>(Option<F>);

impl<F, E> Future for Answer<F>
where
    F: Future<Output = Result<http::Response<Body>, E>> + Unpin,
{
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let answering = self
            .0
            .as_mut()
            .expect("an answer is not polled once it is ready");
        let polled = panic::catch_unwind(AssertUnwindSafe(|| Pin::new(answering).poll(context)));
        let answered = match polled {
            Ok(Poll::Pending) => return Poll::Pending,
            Ok(Poll::Ready(answered)) => answered,
            // The status cuts the panic's message as it cuts every error's, and the host scrubs
            // it as it scrubs every error. What the call held, a session's lock among it, goes.
            Err(payload) => Ok(status(&panicked(payload.as_ref())).into_http()),
        };
        self.0 = None;
        Poll::Ready(answered)
    }
}

/// The internal error of a call whose answer panicked with `payload`, saying what the panic
/// said where that is text.
fn panicked(payload: &(dyn Any + Send)) -> ConnectorError {
    let said = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str));
    let message = match said {
        Some(said) => format!("the call panicked: {said}"),
        None => "the call panicked".to_owned(),
    };
    ConnectorError::new(ConnectorErrorKind::Internal, message)
}
