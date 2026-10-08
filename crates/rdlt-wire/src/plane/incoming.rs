//! The messages of a call's body, each decoded from the bytes its [`Bounded`] body passed on.

use std::marker::PhantomData;

use bytes::Bytes;
use prost::Message;
use tonic::Status;
use tonic::codegen::http;

use crate::bounded::Bounded;

/// A call's messages of `M`, read from its body.
#[derive(Debug)]
pub struct Incoming<M> {
    body: Bounded,
    message: PhantomData<fn() -> M>,
}

impl<M: Message + Default> Incoming<M> {
    /// The messages of a request's `body`.
    pub fn request(body: Bounded) -> Self {
        Self {
            body,
            message: PhantomData,
        }
    }

    /// The messages of `answer`.
    pub fn answer(answer: http::Response<Bounded>) -> Result<Self, Status> {
        Ok(Self::request(answer.into_body()))
    }

    /// The next message, or `None` once the call has ended well; after a failure, `None`.
    pub async fn message(&mut self) -> Result<Option<M>, Status> {
        todo!()
    }

    /// Releases the charge of the message passed on last: it has been decoded.
    pub fn release(&self) {
        todo!()
    }
}

/// The message of `message`'s bytes, prefix and all, its `bytes` fields slices of them.
pub(super) fn decoded<M: Message + Default>(message: Bytes) -> Result<M, Status> {
    let _ = message;
    todo!()
}
