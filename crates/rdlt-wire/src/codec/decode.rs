//! Decodes one receiver's frames: a schema, bounded and validated once, then batches in it, each
//! checked as a whole before Arrow reads it.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_ipc::reader::RecordBatchDecoder;
use arrow_ipc::{Message, MetadataVersion};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use bytes::Bytes;
use flatbuffers::InvalidFlatbuffer;

use super::IpcFrame;
use super::contain::contained;
use super::framing::{framing, kind};
use super::relocate::relocated;
use super::shape::{Shape, walk};
use crate::error::{Frame, Problem, WireError};
use crate::limits::Limits;

/// Decodes the frames one sender sends, within `limits`.
#[derive(Debug)]
pub struct Decoder {
    limits: Limits,
    schema: Option<SchemaRef>,
    /// Each dictionary's values as a batch of one column, by the dictionary's id.
    values: HashMap<i64, SchemaRef>,
    dictionaries: HashMap<i64, ArrayRef>,
}

impl Decoder {
    /// A decoder enforcing `limits`, before any schema.
    pub fn new(limits: Limits) -> Self {
        Self {
            limits,
            schema: None,
            values: HashMap::new(),
            dictionaries: HashMap::new(),
        }
    }

    /// The schema the frames that follow are in, from its IPC schema message; dictionaries
    /// received before it are forgotten.
    ///
    /// # Errors
    ///
    /// A [`WireError`] when the message is too large, malformed, or its schema beyond the limits.
    pub fn schema(&mut self, ipc_schema: &Bytes) -> Result<SchemaRef, WireError> {
        self.limits.admit_schema(ipc_schema.len())?;
        let message = message(Frame::Schema, ipc_schema, self.limits.nesting_depth)?;
        let Some(fb) = message.header_as_schema() else {
            return Err(unexpected(Frame::Schema, &message));
        };
        super::schema::admit(fb, &self.limits)?;
        let schema = contained(Frame::Schema, || Ok(arrow_ipc::convert::fb_to_schema(fb)))?;
        let schema = Arc::new(schema);
        self.values.clear();
        dictionaries(schema.fields(), &mut self.values);
        self.schema = Some(Arc::clone(&schema));
        self.dictionaries.clear();
        Ok(schema)
    }

    /// Decodes `frame`: a record batch in the current schema, or `None` for a dictionary batch,
    /// which later batches use.
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
        self.limits
            .admit_frame(frame.header.len().saturating_add(frame.body.len()))?;
        let message = message(Frame::Batch, &frame.header, self.limits.nesting_depth)?;
        let framed = framing(&message, frame.body.len())?;
        let malformed = |problem| WireError::malformed(framed.frame, problem);
        let Some(schema) = &self.schema else {
            return Err(malformed(Problem::NoSchema));
        };
        let rows = u64::try_from(framed.batch.length()).unwrap_or(u64::MAX);
        Limits::admit("batch rows", self.limits.batch_rows, rows)?;
        let columns = match framed.dictionary {
            None => Arc::clone(schema),
            Some(id) => match self.values.get(&id) {
                Some(values) => Arc::clone(values),
                None => return Err(malformed(Problem::UnknownDictionary { id })),
            },
        };
        let types = columns.fields().iter().map(|field| field.data_type());
        let walked = walk(framed.frame, framed.batch, types, &frame.body, &self.limits)?;
        let relocated = relocated(framed.batch, &walked);
        let shape = Shape {
            values: walked.values,
            view_bytes: walked.view_bytes,
            held_bytes: u64::try_from(relocated.body.capacity()).unwrap_or(u64::MAX),
        };
        let batch = relocated
            .batch()
            .ok_or_else(|| malformed(Problem::NotAMessage))?;
        let dictionaries = &self.dictionaries;
        let decoded = contained(framed.frame, || {
            RecordBatchDecoder::try_new(
                &relocated.body,
                batch,
                columns,
                dictionaries,
                &MetadataVersion::V5,
            )?
            .with_require_alignment(true)
            .read_record_batch()
        })?;
        let Some(id) = framed.dictionary else {
            return Ok((Some(decoded), shape));
        };
        if let Some(values) = decoded.columns().first() {
            self.dictionaries.insert(id, Arc::clone(values));
        }
        Ok((None, shape))
    }
}

/// A `frame` whose header holds another kind of message.
fn unexpected(frame: Frame, message: &Message<'_>) -> WireError {
    WireError::malformed(
        frame,
        Problem::Unexpected {
            found: kind(message),
        },
    )
}

/// The IPC message `header` holds, of the metadata version both ends speak.
///
/// It is verified to a depth that fits a schema nested `depth` levels, so a schema deeper than
/// that is refused by the nesting limit rather than as no message, and within what a message of
/// its length can hold: a table every four bytes, and sixteen times its bytes once every table
/// and string is counted wherever it repeats.
fn message(frame: Frame, header: &[u8], depth: u64) -> Result<Message<'_>, WireError> {
    let depth = usize::try_from(depth).unwrap_or(usize::MAX);
    let options = flatbuffers::VerifierOptions {
        max_depth: depth.saturating_mul(4).saturating_add(64),
        max_tables: header.len() / 4,
        max_apparent_size: header.len().saturating_mul(16),
        ..flatbuffers::VerifierOptions::default()
    };
    let message =
        arrow_ipc::root_as_message_with_opts(&options, header).map_err(|error| match error {
            InvalidFlatbuffer::TooManyTables | InvalidFlatbuffer::ApparentSizeTooLarge => {
                WireError::malformed(frame, Problem::Inflated)
            }
            _ => WireError::malformed(frame, Problem::NotAMessage),
        })?;
    if message.version() != MetadataVersion::V5 {
        let found = message.version().0;
        return Err(WireError::malformed(frame, Problem::Version { found }));
    }
    Ok(message)
}

/// Records, for each dictionary `fields` or the fields nested in them use, its values as a batch
/// of one column; the first field naming an id gives its dictionary's type, as in Arrow's reader.
fn dictionaries(fields: &Fields, values: &mut HashMap<i64, SchemaRef>) {
    for field in fields {
        if let DataType::Dictionary(_, value) = field.data_type() {
            #[expect(
                deprecated,
                reason = "Arrow's reader finds a column's dictionary by this id"
            )]
            let id = field.dict_id();
            if let Some(id) = id {
                values.entry(id).or_insert_with(|| {
                    let column = Field::new("", value.as_ref().clone(), true);
                    Arc::new(Schema::new(vec![column]))
                });
            }
        }
        nested(field.data_type(), values);
    }
}

/// Records the dictionaries of the fields nested in a value of `data_type`.
fn nested(data_type: &DataType, values: &mut HashMap<i64, SchemaRef>) {
    match data_type {
        DataType::Struct(fields) => dictionaries(fields, values),
        DataType::Union(fields, _) => {
            let fields: Fields = fields.iter().map(|(_, field)| Arc::clone(field)).collect();
            dictionaries(&fields, values);
        }
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::ListView(item)
        | DataType::LargeListView(item)
        | DataType::FixedSizeList(item, _)
        | DataType::Map(item, _) => dictionaries(&Fields::from(vec![Arc::clone(item)]), values),
        DataType::RunEndEncoded(ends, item) => {
            dictionaries(
                &Fields::from(vec![Arc::clone(ends), Arc::clone(item)]),
                values,
            );
        }
        DataType::Dictionary(_, value) => nested(value, values),
        _ => {}
    }
}
