//! A call's body of messages as their chunks: each message's head, then its batch's body as the
//! bytes it already is.

use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::{Body, Frame};
use tonic::Status;
use tonic::codegen::tokio_stream::Stream;

use super::chained::Chained;

/// A body sending the messages of a stream, each as its chunks.
#[derive(Debug)]
pub struct Outgoing<M>(PhantomData<fn() -> M>);

impl<M: Chained + Send + 'static> Outgoing<M> {
    /// A request's body of `messages`, each at most `most` bytes, which ends after the last.
    pub fn request(messages: impl Stream<Item = M> + Send + 'static, most: usize) -> Self {
        let _ = (messages, most);
        todo!()
    }

    /// An answer's body of `messages`, each at most `most` bytes.
    pub fn answer(
        messages: Pin<Box<dyn Stream<Item = Result<M, Status>> + Send>>,
        most: usize,
    ) -> Self {
        let _ = (messages, most);
        todo!()
    }
}

impl<M: Chained + Send + 'static> Body for Outgoing<M> {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        todo!()
    }
}
