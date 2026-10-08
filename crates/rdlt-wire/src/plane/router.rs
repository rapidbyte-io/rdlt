//! A served connection's calls: each request held to its bounds, the data plane's served by a
//! [`Plane`], every other by the generated service.

#[cfg(test)]
mod tests;

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tonic::Status;
use tonic::body::Body;
use tonic::codegen::http;
use tonic::codegen::{BoxFuture, BoxStream, Service};

use super::Incoming;
use crate::bounded::Window;
use crate::limits::Limits;
use crate::v1;

/// The answers of a data-plane call, or the status that ends them.
pub type Answers<T> = BoxStream<T>;

/// A data-plane call being served, until its answers start.
pub type Serving<'a, T> = Pin<Box<dyn Future<Output = Result<Answers<T>, Status>> + Send + 'a>>;

/// What serves the data plane's calls.
pub trait Plane: Send + Sync + 'static {
    /// Serves a write of `frames`.
    ///
    /// # Errors
    ///
    /// A status the call fails with before any answer.
    fn write(&self, frames: Incoming<v1::WriteFrame>) -> Serving<'_, v1::WriteAck>;
}

/// A connection's service: each request held to its class's bounds and the connection's window
/// before anything reads it, the data plane's calls served by `P`, and the rest by `S`.
#[derive(Debug)]
#[expect(dead_code, reason = "a stub")]
pub struct Router<S, P> {
    inner: S,
    plane: Arc<P>,
    limits: Limits,
    window: Window,
}

impl<S: Clone, P> Clone for Router<S, P> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            plane: Arc::clone(&self.plane),
            limits: self.limits,
            window: self.window.clone(),
        }
    }
}

impl<S, P> Router<S, P> {
    /// Calls served by `plane` and `inner`, each request held to `limits` for its class, and
    /// what is still arriving to `window`.
    pub fn new(inner: S, plane: Arc<P>, limits: &Limits, window: Window) -> Self {
        Self {
            inner,
            plane,
            limits: *limits,
            window,
        }
    }
}

impl<S, P> Service<http::Request<Body>> for Router<S, P>
where
    S: Service<http::Request<Body>, Response = http::Response<Body>, Error = Infallible>,
    S::Future: Send + 'static,
    P: Plane,
{
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<http::Response<Body>, Infallible>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let _ = request;
        todo!()
    }
}
