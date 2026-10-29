//! A frame's head: the sixteen bytes before its body that say how long the body is, what it
//! holds, and what it belongs to, `len:u32le | kind:u8 | flags:u8 | 0:u16 | tag:u64le`.
//!
//! A batch frame's flags say whether its rows are changes (bit 0) and whether it is a dictionary
//! (bit 1): its receiver reserves a batch's body with what the dictionaries before it hold, and
//! refuses a frame whose message is not what its flags say.
//!
//! A head comes from the peer, so it is parsed before anything is reserved or read for its body:
//! a kind no frame is, flags or a tag its kind does not take, reserved bytes that are not zero, a
//! kind this end of the call does not send, and a body beyond its kind's bound are each refused.

#[cfg(kani)]
mod proofs;
#[cfg(test)]
mod tests;

use crate::limits::{Limit, Refusal};

/// Bytes: a frame's head.
pub const HEAD_BYTES: usize = 16;

/// The data calls, each a stream of its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Call {
    /// A read of one partition of a source.
    Read,
    /// A write of one table of a destination's session.
    Write,
    /// A read-back of what a destination published to a table, for certification.
    ReadPublished,
}

impl Call {
    /// Every call.
    pub const ALL: [Self; 3] = [Self::Read, Self::Write, Self::ReadPublished];

    /// The flags naming the call on its opening frames.
    pub const fn flags(self) -> u8 {
        match self {
            Self::Read => 1,
            Self::Write => 2,
            Self::ReadPublished => 3,
        }
    }

    /// The call `flags` names, where they name one.
    pub const fn of(flags: u8) -> Option<Self> {
        match flags {
            1 => Some(Self::Read),
            2 => Some(Self::Write),
            3 => Some(Self::ReadPublished),
            _ => None,
        }
    }
}

/// What a batch frame's rows are.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rows {
    /// Rows of the stream's schema.
    Arrow,
    /// Changes, as a change batch holds them.
    Changes,
}

/// What a frame holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// One Arrow IPC encapsulated message, a dictionary or a record batch, at the stream
    /// format's alignment.
    Batch {
        /// What its rows are.
        rows: Rows,
        /// Whether it is a dictionary, which batches after it use.
        dictionary: bool,
        /// Its schema epoch on a read, and its segment on a write.
        tag: u64,
    },
    /// A push's JSON text.
    Json,
    /// One protocol buffers message of the call's.
    Message {
        /// The call the frame opens, on the first frame a host sends and the first its connector
        /// answers.
        opens: Option<Call>,
    },
}

impl Kind {
    /// The kind's name, as refusals name it.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Batch { .. } => "batch",
            Self::Json => "json",
            Self::Message { .. } => "message",
        }
    }

    const fn code(self) -> u8 {
        match self {
            Self::Batch { .. } => 1,
            Self::Json => 2,
            Self::Message { .. } => 3,
        }
    }

    /// The kind a head's `code`, `flags` and `tag` say.
    fn parse(code: u8, flags: u8, tag: u64) -> Result<Self, Malformed> {
        let kind = match code {
            1 => {
                if flags > 3 {
                    let kind = "batch";
                    return Err(Malformed::Flags { kind, flags });
                }
                let rows = if flags & 1 == 0 {
                    Rows::Arrow
                } else {
                    Rows::Changes
                };
                let dictionary = flags & 2 != 0;
                return Ok(Self::Batch {
                    rows,
                    dictionary,
                    tag,
                });
            }
            2 if flags == 0 => Self::Json,
            2 => {
                return Err(Malformed::Flags {
                    kind: "json",
                    flags,
                });
            }
            3 => match (flags, Call::of(flags)) {
                (0, _) => Self::Message { opens: None },
                (_, Some(call)) => Self::Message { opens: Some(call) },
                (_, None) => {
                    return Err(Malformed::Flags {
                        kind: "message",
                        flags,
                    });
                }
            },
            other => return Err(Malformed::Kind(other)),
        };
        // Only a batch carries a tag.
        if tag != 0 {
            let kind = kind.name();
            return Err(Malformed::Tag { kind, tag });
        }
        Ok(kind)
    }
}

/// A frame's head.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Head {
    /// What the frame holds.
    pub kind: Kind,
    /// Bytes: its body's length.
    pub len: u32,
}

/// The most bytes a frame of one kind may take, and the limit a longer one passes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bound {
    /// Bytes: the most its body may take.
    pub bytes: u32,
    /// The limit it is refused by beyond them.
    pub limit: Limit,
}

/// What one end of a call sends: each kind of frame it sends within its bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bounds {
    /// Batch frames, where the end sends them.
    pub batch: Option<Bound>,
    /// JSON frames, where the end sends them.
    pub json: Option<Bound>,
    /// Message frames, which every end sends.
    pub message: Bound,
}

/// A head refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum HeadError {
    /// It is malformed.
    #[error("a malformed frame head: {0}")]
    Malformed(Malformed),
    /// Its body is beyond its kind's bound.
    #[error(transparent)]
    Refused(Refusal),
}

/// What is wrong with a malformed head.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Malformed {
    /// Its kind is none a frame has.
    #[error("kind {0}, which no frame is")]
    Kind(u8),
    /// Its flags are none its kind takes.
    #[error("a {kind} frame flagged {flags}")]
    Flags {
        /// The frame's kind.
        kind: &'static str,
        /// Its flags.
        flags: u8,
    },
    /// Its reserved bytes are not zero.
    #[error("reserved bytes of {0}, not zero")]
    Reserved(u16),
    /// It carries a tag its kind takes none of.
    #[error("a {kind} frame tagged {tag}")]
    Tag {
        /// The frame's kind.
        kind: &'static str,
        /// Its tag.
        tag: u64,
    },
    /// Its kind is one this end of the call does not send.
    #[error("a {0} frame, which this end of the call does not send")]
    Unexpected(&'static str),
}

impl Head {
    /// The head of a frame of `kind` whose body is `len` bytes.
    pub const fn new(kind: Kind, len: u32) -> Self {
        Self { kind, len }
    }

    /// The head's bytes.
    pub fn encode(self) -> [u8; HEAD_BYTES] {
        let (flags, tag) = match self.kind {
            Kind::Batch {
                rows,
                dictionary,
                tag,
            } => {
                let changes = match rows {
                    Rows::Arrow => 0,
                    Rows::Changes => 1,
                };
                (changes | u8::from(dictionary) << 1, tag)
            }
            Kind::Json => (0, 0),
            Kind::Message { opens } => (opens.map_or(0, Call::flags), 0),
        };
        let mut bytes = [0; HEAD_BYTES];
        bytes[..4].copy_from_slice(&self.len.to_le_bytes());
        bytes[4] = self.kind.code();
        bytes[5] = flags;
        bytes[8..].copy_from_slice(&tag.to_le_bytes());
        bytes
    }

    /// The head `bytes` hold, from an end of a call that sends within `bounds`.
    ///
    /// # Errors
    ///
    /// A [`HeadError`]: [`Malformed`] for a kind, flags, tag or reserved bytes no head of this end
    /// carries, and a refusal of the kind's limit for a body beyond its bound.
    pub fn parse(bytes: [u8; HEAD_BYTES], bounds: &Bounds) -> Result<Self, HeadError> {
        let [
            l0,
            l1,
            l2,
            l3,
            code,
            flags,
            r0,
            r1,
            t0,
            t1,
            t2,
            t3,
            t4,
            t5,
            t6,
            t7,
        ] = bytes;
        let len = u32::from_le_bytes([l0, l1, l2, l3]);
        let reserved = u16::from_le_bytes([r0, r1]);
        let tag = u64::from_le_bytes([t0, t1, t2, t3, t4, t5, t6, t7]);
        if reserved != 0 {
            return Err(HeadError::Malformed(Malformed::Reserved(reserved)));
        }
        let kind = Kind::parse(code, flags, tag).map_err(HeadError::Malformed)?;
        let bound = match kind {
            Kind::Batch { .. } => bounds.batch,
            Kind::Json => bounds.json,
            Kind::Message { .. } => Some(bounds.message),
        };
        let Some(bound) = bound else {
            return Err(HeadError::Malformed(Malformed::Unexpected(kind.name())));
        };
        if len > bound.bytes {
            return Err(HeadError::Refused(Refusal {
                field: bound.limit,
                limit: u64::from(bound.bytes),
                actual: u64::from(len),
            }));
        }
        Ok(Self { kind, len })
    }
}
