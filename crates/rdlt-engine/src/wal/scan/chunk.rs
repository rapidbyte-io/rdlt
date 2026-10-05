//! Reading one published chunk back, frame by frame, each checked whole: a chunk is written whole,
//! so anything in it that does not read is damage.

use rdlt_connector::{LoadId, PipelineId};

use super::{foreign, unreadable};
use crate::error::Error;
use crate::wal::frame::{self, End, Fence, Frame, HEAD, Header, PREAMBLE, Skimmed};
use crate::wal::store::{Chunk, WalStore};

/// Bytes: the most of a batch frame a scan reads at once.
const PIECE: u64 = 1 << 16;

/// A frame of a chunk, where it lies and what it holds, a batch's data left unread.
pub(super) struct Read {
    pub(super) offset: u64,
    pub(super) len: u64,
    pub(super) frame: Skimmed,
}

/// What a chunk is: its first frame and its last, and the frames between.
pub(super) struct Opened {
    pub(super) first: First,
    /// The frames between its first and its end.
    pub(super) frames: Vec<Read>,
    pub(super) end: End,
}

/// The first frame of a chunk.
pub(super) enum First {
    /// A chunk its load wrote.
    Header(Header),
    /// A chunk a replay wrote to take the log over.
    Fence(Fence),
}

/// Whose log is read, and how large a frame may be.
pub(super) struct Reading<'a> {
    pub(super) store: &'a dyn WalStore,
    pub(super) pipeline: &'a PipelineId,
    pub(super) load: LoadId,
    pub(super) frame_bytes: u64,
}

impl Reading<'_> {
    fn unreadable(&self, detail: &dyn std::fmt::Display) -> Error {
        unreadable(self.pipeline, self.load, detail)
    }

    /// Reads chunk `number`, `len` bytes long, whole: its preamble, then every frame, each
    /// refused before it is read where it announces more than a frame may hold.
    pub(super) async fn chunk(&self, number: u64, len: u64) -> Result<Opened, Error> {
        let chunk = Chunk {
            load: self.load,
            number,
        };
        let preamble = self.read(chunk, 0, PREAMBLE as u64).await?;
        frame::check_preamble(&preamble)
            .map_err(|error| self.unreadable(&format!("chunk {number}: {error}")))?;
        let mut offset = PREAMBLE as u64;
        let mut frames = Vec::new();
        while offset < len {
            let head = self.read(chunk, offset, HEAD as u64).await?;
            let payload = frame::payload_len(&head)
                .filter(|_| head.len() == HEAD)
                .ok_or_else(|| {
                    self.unreadable(&format!("chunk {number} ends in a frame's head"))
                })?;
            if payload > self.frame_bytes {
                let detail = format!(
                    "chunk {number} holds a frame of {payload} bytes at {offset}, beyond the \
                     {} a frame may hold",
                    self.frame_bytes
                );
                return Err(self.unreadable(&detail));
            }
            let frame_len = HEAD as u64 + payload;
            if offset + frame_len > len {
                let detail = format!("chunk {number} ends inside its frame at {offset}");
                return Err(self.unreadable(&detail));
            }
            let at =
                |error: Error| self.unreadable(&format!("chunk {number}, at {offset}: {error}"));
            let frame = if head[0] == frame::BATCH {
                self.batch(chunk, offset, &head, payload)
                    .await
                    .map_err(at)?
            } else {
                let bytes = self.read(chunk, offset, frame_len).await?;
                frame::skim(&bytes).map_err(at)?
            };
            frames.push(Read {
                offset,
                len: frame_len,
                frame,
            });
            offset += frame_len;
        }
        self.shaped(number, frames)
    }

    /// The header of the batch frame at `offset` of `chunk`, whose head is `head` and whose
    /// payload is `payload` bytes long, its checksum summed as it is read a piece at a time: a
    /// scan holds no more of it at once than [`PIECE`].
    async fn batch(
        &self,
        chunk: Chunk,
        offset: u64,
        head: &[u8],
        payload: u64,
    ) -> Result<Skimmed, Error> {
        let mut summing =
            frame::Summing::of(head).ok_or_else(|| Error::internal("a frame's head is short"))?;
        let start = offset + HEAD as u64;
        let mut header = None;
        let mut read = 0;
        while read < payload {
            let piece = self
                .read(chunk, start + read, PIECE.min(payload - read))
                .await?;
            if piece.is_empty() {
                return Err(Error::internal("the chunk ends inside a frame"));
            }
            summing.add(&piece);
            if header.is_none() {
                header = Some(frame::batch_header_in(&piece)?);
            }
            read += piece.len() as u64;
        }
        summing.check()?;
        let header = header.ok_or_else(|| Error::internal("a batch frame holds no header"))?;
        Ok(Skimmed::Batch(header))
    }

    async fn read(&self, chunk: Chunk, offset: u64, len: u64) -> Result<bytes::Bytes, Error> {
        let bytes = self
            .store
            .read(self.pipeline, chunk, offset, len)
            .await
            .map_err(Error::from_wal)?;
        Ok(bytes)
    }

    /// The chunk `number` whose frames are `frames`, checked to be shaped as a chunk is: a header
    /// or a fence of this log at this number first, one end last, and a commit or the closing
    /// frame before a header's end.
    fn shaped(&self, number: u64, mut frames: Vec<Read>) -> Result<Opened, Error> {
        let shapeless = |detail: &str| self.unreadable(&format!("chunk {number} {detail}"));
        let end = match frames.pop().map(|read| read.frame) {
            Some(Skimmed::Other(frame)) => match *frame {
                Frame::End(end) => end,
                _ => return Err(shapeless("does not end with its end")),
            },
            _ => return Err(shapeless("does not end with its end")),
        };
        if frames.is_empty() {
            return Err(shapeless("holds nothing before its end"));
        }
        let first = match frames.remove(0).frame {
            Skimmed::Other(frame) => match *frame {
                Frame::Header(header) => First::Header(header),
                Frame::Fence(fence) => First::Fence(fence),
                _ => return Err(shapeless("starts with neither a header nor a fence")),
            },
            Skimmed::Batch(_) => return Err(shapeless("starts with a batch")),
        };
        let (pipeline, load, at) = match &first {
            First::Header(header) => (&header.pipeline, header.load, header.chunk),
            First::Fence(fence) => (&fence.pipeline, fence.load, fence.chunk),
        };
        if pipeline != self.pipeline || load != self.load {
            return Err(foreign(self.pipeline, self.load, pipeline, load));
        }
        if at != number {
            return Err(shapeless(&format!("says it is chunk {at}")));
        }
        let closes = |read: &Read| match &read.frame {
            Skimmed::Other(frame) => {
                matches!(**frame, Frame::Commit(_) | Frame::Closed | Frame::Relieved)
            }
            Skimmed::Batch(_) => false,
        };
        // Seals and phases go with the commit that follows them: a closing or relieved chunk
        // holds none.
        let closing = frames.last().is_some_and(|read| {
            matches!(&read.frame, Skimmed::Other(frame) if matches!(**frame, Frame::Closed | Frame::Relieved))
        });
        let shaped = match &first {
            First::Fence(_) => frames.is_empty(),
            First::Header(_) => {
                frames.last().is_some_and(closes)
                    && frames[..frames.len() - 1]
                        .iter()
                        .all(|read| !(closes(read) || opens(read) || closing && seals(read)))
            }
        };
        if !shaped {
            return Err(shapeless("is not shaped as a chunk is"));
        }
        Ok(Opened { first, frames, end })
    }
}

/// Whether `read` is a frame only a chunk's first or last may be.
fn opens(read: &Read) -> bool {
    match &read.frame {
        Skimmed::Other(frame) => {
            matches!(**frame, Frame::Header(_) | Frame::Fence(_) | Frame::End(_))
        }
        Skimmed::Batch(_) => false,
    }
}

/// Whether `read` is a seal or a phase, which a commit takes.
fn seals(read: &Read) -> bool {
    matches!(&read.frame, Skimmed::Other(frame) if matches!(**frame, Frame::Seal(_) | Frame::Begun(_)))
}
