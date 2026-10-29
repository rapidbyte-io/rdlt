//! A served connection's calls: each request held to its bounds before anything reads it, the
//! `Data` call's stream served by [`Streams`], and every other call by the generated service.

#[cfg(test)]
mod tests;

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tonic::body::Body;
use tonic::codegen::http;
use tonic::codegen::{BoxFuture, Service};

use crate::bounded::{Bounded, Bounds, Window};
use crate::limits::{Class, Limits};
use crate::stream::{DATA, Piped, piped};

/// What serves the streams of a connection's `Data` calls.
pub trait Streams: Send + Sync + 'static {
    /// Serves the data call `stream` carries, until it ends.
    fn serve(
        self: Arc<Self>,
        stream: Piped<Body>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
}

/// A connection's service: each request held to its class's bounds and the connection's window
/// before anything reads it, the `Data` call's stream served by `S`, and the rest by `C`.
#[derive(Debug)]
pub struct Router<C, S> {
    inner: C,
    streams: Arc<S>,
    limits: Limits,
    window: Window,
}

impl<C: Clone, S> Clone for Router<C, S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            streams: Arc::clone(&self.streams),
            limits: self.limits,
            window: self.window.clone(),
        }
    }
}

impl<C, S> Router<C, S> {
    /// Calls served by `inner`, each request held to `limits` for its class and what is still
    /// arriving to `window`, and the `Data` call's streams by `streams`.
    pub fn new(inner: C, streams: Arc<S>, limits: &Limits, window: Window) -> Self {
        Self {
            inner,
            streams,
            limits: *limits,
            window,
        }
    }
}

impl<C, S> Service<http::Request<Body>> for Router<C, S>
where
    C: Service<http::Request<Body>, Response = http::Response<Body>, Error = Infallible>,
    C::Future: Send + 'static,
    S: Streams,
{
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<http::Response<Body>, Infallible>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let path = request.uri().path();
        if path == DATA {
            let (stream, sent) = piped(request.into_body());
            // The stream's server outlives the call's headers: it ends with the stream.
            tokio::spawn(Arc::clone(&self.streams).serve(stream));
            let mut answer = http::Response::new(Body::new(sent));
            *answer.version_mut() = http::Version::HTTP_2;
            return Box::pin(async move { Ok(answer) });
        }
        let method = path.rsplit('/').next().unwrap_or(path);
        let class = Class::of_request(method);
        let bounds = Bounds::of(&self.limits, class, crate::scan::request(method));
        let window = Some(self.window.clone());
        let request = request.map(|body| Body::new(Bounded::new(body, bounds, window)));
        Box::pin(self.inner.call(request))
    }
}
