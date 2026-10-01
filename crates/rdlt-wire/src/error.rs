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
    /// A buffer starts before the buffer described before it ends, so the two could share bytes.
    #[error("buffer {index} at offset {offset} starts before the previous buffer's end at {end}")]
    BufferOverlaps {
        /// The buffer's position among the message's buffers.
        index: usize,
        /// Its offset.
        offset: u64,
        /// Where the buffer before it ends.
        end: u64,
    },
    /// A buffer's offset is not a multiple of eight, the padding of the IPC format.
    #[error("buffer {index} at offset {offset} is not padded to eight bytes")]
    BufferUnaligned {
        /// The buffer's position among the message's buffers.
        index: usize,
        /// Its offset.
        offset: u64,
    },
    /// A buffer is shorter than the values its node declares need.
    #[error("buffer {index} of {length} bytes is shorter than the {needed} its node needs")]
    BufferTooShort {
        /// The buffer's position among the message's buffers.
        index: usize,
        /// Its length.
        length: u64,
        /// The bytes its node's values need.
        needed: u64,
    },
    /// The message describes fewer parts than its schema's columns need.
    #[error("the message lacks a {part} its schema needs")]
    Missing {
        /// The part that ran out.
        part: Part,
    },
    /// The message describes more parts than its schema's columns need.
    #[error("the message holds a {part} its schema does not need")]
    Unused {
        /// The part left over.
        part: Part,
    },
    /// A node's length or null count is negative, or it counts more nulls than values.
    #[error("node {index} declares {length} values, {nulls} of them null")]
    Node {
        /// The node's position among the message's nodes.
        index: usize,
        /// The length it declares.
        length: i64,
        /// The null count it declares.
        nulls: i64,
    },
    /// A view column declares a negative number of data buffers.
    #[error("a view column declares {count} data buffers")]
    VariadicCount {
        /// The count it declares.
        count: i64,
    },
    /// A view names a data buffer its column lacks, or bytes outside that buffer.
    #[error("view {index} of the column at node {node} names bytes outside its data buffers")]
    View {
        /// The column's node, by its position among the message's nodes.
        node: usize,
        /// The view's position in the column.
        index: usize,
    },
    /// A list view names items outside its child.
    #[error("list view {index} of the column at node {node} names items outside its child")]
    ListView {
        /// The column's node, by its position among the message's nodes.
        node: usize,
        /// The list view's position in the column.
        index: usize,
    },
    /// A dictionary batch carries an id no field of the schema names.
    #[error("no field of the schema uses dictionary {id}")]
    UnknownDictionary {
        /// The dictionary's id.
        id: i64,
    },
    /// The message is of a metadata version other than V5, which no end negotiates.
    #[error("the message is of metadata version {found}")]
    Version {
        /// The version's number in the IPC format.
        found: i16,
    },
    /// The schema is big-endian, which no end negotiates.
    #[error("the schema is big-endian")]
    BigEndian,
    /// The header's tables repeat, so it describes more than its bytes hold.
    #[error("the header describes more than its bytes hold")]
    Inflated,
    /// A dictionary batch extends the dictionary sent before it, which no end negotiates; a
    /// peer could otherwise grow a dictionary without bound.
    #[error("the dictionary batch is a delta")]
    DeltaDictionary,
    /// The body is compressed, which no end negotiates.
    #[error("the body is compressed")]
    Compressed,
    /// A batch arrived before any schema.
    #[error("no schema was received")]
    NoSchema,
}

/// A part of a record batch message that its schema's columns consume in order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Part {
    /// A field node.
    Node,
    /// A buffer.
    Buffer,
    /// A view column's count of data buffers.
    VariadicCount,
}

impl std::fmt::Display for Part {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Node => "field node",
            Self::Buffer => "buffer",
            Self::VariadicCount => "count of data buffers",
        })
    }
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
