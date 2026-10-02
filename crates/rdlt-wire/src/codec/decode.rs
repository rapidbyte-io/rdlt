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
use super::relocate::relocated;
use super::shape::Shape;
use crate::error::{Frame, Problem, WireError};
use crate::limits::Limits;

/// Decodes the frames one sender sends, within `limits`.
#[derive(Debug)]
pub struct Decoder {
    limits: Limits,
    columns: Option<Columns>,
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
    /// admitted. A schema equal to the one it replaces is returned as that same schema, so
    /// batches decoded either side of it share it.
    ///
    /// # Errors
    ///
    /// A [`WireError`] when the message is too large, malformed, or its schema beyond the limits.
    pub fn schema(&mut self, ipc_schema: &Bytes) -> Result<SchemaRef, WireError> {
        // A refused schema ends the schema before it too: no batch is read under either.
        let before = self.columns.take();
        self.dictionaries.clear();
        self.held.clear();
        self.limits.admit_schema(ipc_schema.len())?;
        let message = message(Frame::Schema, ipc_schema, self.limits.nesting_depth)?;
        let Some(fb) = message.header_as_schema() else {
            return Err(unexpected(Frame::Schema, &message));
        };
        super::schema::admit(fb, &self.limits)?;
        let schema = contained(Frame::Schema, || Ok(arrow_ipc::convert::fb_to_schema(fb)))?;
        // An equal schema is returned as the schema held: the batches of a sender that sends
        // its schema again keep a single schema alive between them, not a schema each.
        if let Some(before) = before
            && **before.schema() == schema
        {
            let held = Arc::clone(before.schema());
            self.columns = Some(before);
            return Ok(held);
        }
        let schema = Arc::new(schema);
        self.columns = Some(Columns::new(Arc::clone(&schema)));
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

    /// Decodes `frame` as [`Decoder::frame`] does, and measures what it holds.
    ///
    /// # Errors
    ///
    /// As [`Decoder::frame`].
    pub fn shaped(&mut self, frame: &IpcFrame) -> Result<(Option<RecordBatch>, Shape), WireError> {
        let measured = measured(self.columns.as_ref(), &self.limits, frame)?;
        let malformed = |problem| WireError::malformed(measured.frame, problem);
        let relocated = relocated(measured.batch, &measured.walked);
        let shape = Shape {
            values: measured.walked.values,
            view_bytes: measured.walked.view_bytes,
            held_bytes: u64::try_from(relocated.body.capacity()).unwrap_or(u64::MAX),
        };
        if let Some(id) = measured.dictionary {
            // Counted before Arrow builds it: a dictionary replaces any of its id.
            let others = self
                .dictionary_bytes()
                .saturating_sub(self.held.get(&id).copied().unwrap_or(0));
            self.limits
                .admit_dictionaries(others.saturating_add(shape.held_bytes))?;
        }
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
