//! Decodes one receiver's frames: a schema, bounded and validated once, then batches in it, each
//! checked as a whole before Arrow reads it.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_ipc::MetadataVersion;
use arrow_ipc::reader::RecordBatchDecoder;
use arrow_schema::SchemaRef;
use bytes::Bytes;

use super::IpcFrame;
use super::contain::contained;
use super::measure::{Columns, measured, message, unexpected};
use super::relocate::{held_bytes, relocated};
use super::shape::Shape;
use crate::error::{Frame, Problem, WireError};
use crate::limits::Limits;

/// Decodes the frames one sender sends, within `limits`.
#[derive(Debug)]
pub struct Decoder {
    limits: Limits,
    columns: Option<Columns>,
    /// The schema message `columns` came from.
    message: Option<Bytes>,
    dictionaries: HashMap<i64, ArrayRef>,
    /// The bytes of the allocation each dictionary held was decoded into, by its id.
    held: HashMap<i64, u64>,
}

impl Decoder {
    /// A decoder enforcing `limits`, before any schema.
    pub fn new(limits: Limits) -> Self {
        Self {
            limits,
            columns: None,
            message: None,
            dictionaries: HashMap::new(),
            held: HashMap::new(),
        }
    }

    /// Bytes: the allocations of the dictionaries held, which batches decoded from now on share
    /// and whoever charges a budget charges while the decoder lives.
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

    /// Bytes: the allocation decoding `frame` would make, measured without making it, so a
    /// receiver can reserve it first.
    ///
    /// # Errors
    ///
    /// As [`Decoder::frame`], for every check made before anything is copied.
    pub fn held(&self, frame: &IpcFrame) -> Result<u64, WireError> {
        let measured = measured(self.columns.as_ref(), &self.limits, frame)?;
        Ok(held_bytes(&measured.walked))
    }

    /// Decodes `frame` as [`Decoder::frame`] does, and measures what it holds.
    ///
    /// # Errors
    ///
    /// As [`Decoder::frame`].
    pub fn shaped(&mut self, frame: &IpcFrame) -> Result<(Option<RecordBatch>, Shape), WireError> {
        let measured = measured(self.columns.as_ref(), &self.limits, frame)?;
        let malformed = |problem| WireError::malformed(measured.frame, problem);
        let shape = Shape {
            values: measured.walked.values,
            view_bytes: measured.walked.view_bytes,
            held_bytes: held_bytes(&measured.walked),
        };
        if let Some(id) = measured.dictionary {
            // Counted before anything is copied: a dictionary replaces any of its id.
            admit_dictionary(&self.limits, &self.held, id, shape.held_bytes)?;
        }
        let relocated = relocated(measured.batch, &measured.walked);
        let batch = relocated
            .batch()
            .ok_or_else(|| malformed(Problem::NotAMessage))?;
        let dictionaries = &self.dictionaries;
        let decoded = contained(measured.frame, || {
            RecordBatchDecoder::try_new(
                &relocated.body,
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
            self.held.insert(id, shape.held_bytes);
        }
        Ok((None, shape))
    }
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
