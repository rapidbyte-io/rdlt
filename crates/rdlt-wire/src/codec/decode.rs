//! Decodes one receiver's frames: a schema, bounded and validated once, then batches in it, each
//! checked as a whole before Arrow reads it where it arrived.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_ipc::MetadataVersion;
use arrow_ipc::reader::RecordBatchDecoder;
use arrow_schema::SchemaRef;
use bytes::Bytes;

use super::IpcFrame;
use super::contain::contained;
use super::in_place;
use super::measure::{Columns, measured, message, unexpected};
use super::shape::Shape;
use crate::error::{Frame, Problem, WireError};
use crate::limits::{Limits, MESSAGE_ROOM, count};

/// Decodes the frames one sender sends, within `limits`.
#[derive(Debug)]
pub struct Decoder {
    limits: Limits,
    columns: Option<Columns>,
    /// The schema message `columns` came from.
    message: Option<Bytes>,
    dictionaries: HashMap<i64, ArrayRef>,
    /// The bytes each dictionary held counts, its frame and the room beside it, by its id: at
    /// least the allocation it keeps alive.
    held: HashMap<i64, u64>,
}

impl Decoder {
    /// A decoder enforcing `limits`, before any schema, of frames each in a message of its own,
    /// which may take [`MESSAGE_ROOM`] beside it.
    pub fn new(limits: Limits) -> Self {
        Self {
            limits,
            columns: None,
            message: None,
            dictionaries: HashMap::new(),
            held: HashMap::new(),
        }
    }

    /// The limits it decodes within.
    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    /// Bytes: what the dictionaries held count, at least the allocations they keep alive, which
    /// batches decoded from now on share and whoever charges a budget charges while the decoder
    /// lives.
    pub fn dictionary_bytes(&self) -> u64 {
        self.held.values().copied().fold(0, u64::saturating_add)
    }

    /// The schema the frames that follow are in, from its IPC schema message.
    ///
    /// The schema and dictionaries received before it are forgotten, whether or not this one is
    /// admitted. A message the same, byte for byte, as the message the decoder's schema came
    /// from is returned as that same schema, so batches decoded either side of it share it.
    /// Schemas that compare equal are not enough: they may name their dictionaries by other ids.
    ///
    /// # Errors
    ///
    /// A [`WireError`] when the message is too large, malformed, or its schema beyond the limits.
    pub fn schema(&mut self, ipc_schema: &Bytes) -> Result<SchemaRef, WireError> {
        // A refused schema ends the schema before it too: no batch is read under either.
        let before = self.columns.take().zip(self.message.take());
        self.dictionaries.clear();
        self.held.clear();
        self.limits.admit_schema(ipc_schema.len())?;
        // The message held again is the schema held: the batches of a sender that sends its
        // schema again keep a single schema alive between them, not a schema each.
        if let Some((columns, message)) = before
            && message == ipc_schema
        {
            let schema = Arc::clone(columns.schema());
            (self.columns, self.message) = (Some(columns), Some(message));
            return Ok(schema);
        }
        let message = message(Frame::Schema, ipc_schema, self.limits.nesting_depth)?;
        let Some(fb) = message.header_as_schema() else {
            return Err(unexpected(Frame::Schema, &message));
        };
        super::schema::admit(fb, &self.limits)?;
        let schema = contained(Frame::Schema, || Ok(arrow_ipc::convert::fb_to_schema(fb)))?;
        let schema = Arc::new(schema);
        self.columns = Some(Columns::new(Arc::clone(&schema)));
        // Copied, so the decoder keeps the message's bytes alive and no buffer they lie in.
        self.message = Some(Bytes::copy_from_slice(ipc_schema));
        Ok(schema)
    }

    /// Decodes `frame`: a record batch in the current schema, or `None` for a dictionary batch,
    /// which later batches use.
    ///
    /// A dictionary replaces any of its id, and is held until the next schema: the
    /// dictionaries held together are within [`Limits::dictionary_bytes`].
    ///
    /// # Errors
    ///
    /// A [`WireError`] when the frame is too large, arrives before a schema, is malformed, holds
    /// more than the limits allow, or Arrow cannot decode it; decoding never panics.
    pub fn frame(&mut self, frame: &IpcFrame) -> Result<Option<RecordBatch>, WireError> {
        self.shaped(frame).map(|(batch, _)| batch)
    }

    /// Checks `frame` as [`Decoder::frame`] does before decoding it, and returns what it counts
    /// toward the dictionaries a receiver holds: its header and body, and the room beside them
    /// in the message it arrives in.
    ///
    /// # Errors
    ///
    /// As [`Decoder::frame`], for every check made before Arrow reads anything but where its
    /// buffers lie.
    pub fn measure(&self, frame: &IpcFrame) -> Result<u64, WireError> {
        measured(self.columns.as_ref(), &self.limits, frame)?;
        Ok(counted(frame))
    }

    /// Decodes `frame` as [`Decoder::frame`] does, and measures what it holds.
    ///
    /// # Errors
    ///
    /// As [`Decoder::frame`].
    pub fn shaped(&mut self, frame: &IpcFrame) -> Result<(Option<RecordBatch>, Shape), WireError> {
        let measured = measured(self.columns.as_ref(), &self.limits, frame)?;
        let malformed = |problem| WireError::malformed(measured.frame, problem);
        let body = in_place::body(frame);
        // Before Arrow reads anything: a buffer off its alignment is a broken sender's.
        if let Some(problem) = in_place::misaligned(&body, &measured.walked.spans) {
            return Err(malformed(problem));
        }
        let shape = Shape {
            values: measured.walked.values,
            view_bytes: measured.walked.view_bytes,
            held_bytes: in_place::held(frame),
        };
        let counted = counted(frame);
        if let Some(id) = measured.dictionary {
            // Counted before it is decoded: a dictionary replaces any of its id.
            admit_dictionary(&self.limits, &self.held, id, counted)?;
        }
        let rewritten = measured
            .walked
            .emptied
            .then(|| in_place::rewritten(measured.batch, &measured.walked));
        let batch = match &rewritten {
            Some(message) => flatbuffers::root::<arrow_ipc::RecordBatch<'_>>(message)
                .map_err(|_| malformed(Problem::NotAMessage))?,
            None => measured.batch,
        };
        let dictionaries = &self.dictionaries;
        let decoded = contained(measured.frame, || {
            RecordBatchDecoder::try_new(
                &body,
                batch,
                measured.columns,
                dictionaries,
                &MetadataVersion::V5,
            )?
            .with_require_alignment(true)
            .read_record_batch()
        })?;
        let Some(id) = measured.dictionary else {
            return Ok((Some(decoded), shape));
        };
        if let Some(values) = decoded.columns().first() {
            self.dictionaries.insert(id, Arc::clone(values));
            self.held.insert(id, counted);
        }
        Ok((None, shape))
    }
}

/// Bytes: what `frame` counts toward the dictionaries a receiver holds, its header and body and
/// [`MESSAGE_ROOM`] beside them, at least the allocation a receiver holds it in.
pub(crate) fn counted(frame: &IpcFrame) -> u64 {
    count(frame.header.len())
        .saturating_add(count(frame.body.len()))
        .saturating_add(MESSAGE_ROOM)
}

/// Admits a dictionary of id `id` that takes `bytes` beside `held`, the dictionaries a receiver
/// holds by id, which it replaces any of its id among: its sender and its receiver both count by
/// this, so a dictionary one sends the other holds.
///
/// # Errors
///
/// A [`WireError::Refused`] where the dictionaries held together would pass
/// [`Limits::held_dictionary_bytes`].
pub(crate) fn admit_dictionary(
    limits: &Limits,
    held: &HashMap<i64, u64>,
    id: i64,
    bytes: u64,
) -> Result<(), WireError> {
    let others = held
        .iter()
        .filter(|(held, _)| **held != id)
        .map(|(_, bytes)| *bytes)
        .fold(0, u64::saturating_add);
    limits.admit_dictionaries(others.saturating_add(bytes))?;
    Ok(())
}
