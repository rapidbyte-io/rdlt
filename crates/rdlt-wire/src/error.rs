//! How receiving a frame fails: a limit refused it, or it was malformed.

use arrow_schema::ArrowError;

use crate::limits::Refusal;

/// The kind of Arrow frame an error concerns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Frame {
    /// A schema message.
    Schema,
    /// A record batch message.
    Batch,
    /// A dictionary batch message.
    Dictionary,
}

/// What is wrong with a malformed frame.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Problem {
    /// The header is not an IPC message flatbuffer.
    #[error("the header is not an IPC message")]
    NotAMessage,
    /// The header is a message of another kind.
    #[error("the header holds a {found} message")]
    Unexpected {
        /// The kind the header holds.
        found: &'static str,
    },
    /// The body's length differs from the length the header declares.
    #[error("the body is {actual} bytes, not the {declared} the header declares")]
    BodyLength {
        /// The length the header declares.
        declared: i64,
        /// The body's length.
        actual: u64,
    },
    /// A buffer the header describes lies outside the body.
    #[error(
        "buffer {index} at offset {offset} of {length} bytes lies outside the {body}-byte body"
    )]
    BufferOutOfBounds {
        /// The buffer's position among the message's buffers.
        index: usize,
        /// Its offset.
        offset: i64,
        /// Its length.
        length: i64,
        /// The body's length.
        body: u64,
    },
    /// The body is compressed, which no end negotiates.
    #[error("the body is compressed")]
    Compressed,
    /// A batch arrived before any schema.
    #[error("no schema was received")]
    NoSchema,
}

/// Receiving a frame failed.
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    /// A limit refused the frame.
    #[error(transparent)]
    Refused(#[from] Refusal),
    /// The frame is malformed.
    #[error("malformed {frame:?} frame: {problem}")]
    Malformed {
        /// The frame.
        frame: Frame,
        /// What is wrong with it.
        problem: Problem,
    },
    /// Arrow could not decode or encode the frame.
    #[error("cannot {verb} a {frame:?} frame", verb = if *.encoding { "encode" } else { "decode" })]
    Arrow {
        /// The frame.
        frame: Frame,
        /// Whether encoding, rather than decoding, failed.
        encoding: bool,
        /// Arrow's error.
        #[source]
        source: ArrowError,
    },
    /// Decoding the frame panicked.
    #[error("decoding a {frame:?} frame panicked: {message}")]
    Panicked {
        /// The frame.
        frame: Frame,
        /// The panic's message.
        message: String,
    },
}

impl WireError {
    /// A malformed `frame`, with its `problem`.
    pub(crate) fn malformed(frame: Frame, problem: Problem) -> Self {
        Self::Malformed { frame, problem }
    }
}
