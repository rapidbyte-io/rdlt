//! Decodes one receiver's frames: a schema, validated once, then batches in it.

use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_buffer::Buffer;
use arrow_ipc::{Message, MessageHeader, MetadataVersion};
use arrow_schema::{DataType, Field, SchemaRef};
use bytes::Bytes;

use super::IpcFrame;
use super::precheck::{framing, kind};
use crate::error::{Frame, Problem, WireError};
use crate::limits::Limits;

/// Decodes the frames one sender sends, within `limits`.
#[derive(Debug)]
pub struct Decoder {
    limits: Limits,
    schema: Option<SchemaRef>,
    version: MetadataVersion,
    dictionaries: HashMap<i64, ArrayRef>,
}

impl Decoder {
    /// A decoder enforcing `limits`, before any schema.
    pub fn new(limits: Limits) -> Self {
        Self {
            limits,
            schema: None,
            version: MetadataVersion::V5,
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
        self.limits.admit_frame(ipc_schema.len())?;
        let message = message(Frame::Schema, ipc_schema, self.limits.nesting_depth)?;
        let Some(fb) = message.header_as_schema() else {
            return Err(WireError::malformed(
                Frame::Schema,
                Problem::Unexpected {
                    found: kind(&message),
                },
            ));
        };
        let schema = contained(Frame::Schema, || Ok(arrow_ipc::convert::fb_to_schema(fb)))?;
        let (columns, depth) = measure(schema.fields());
        Limits::admit("schema columns", self.limits.schema_columns, columns)?;
        Limits::admit("nesting depth", self.limits.nesting_depth, depth)?;
        let schema = Arc::new(schema);
        self.schema = Some(Arc::clone(&schema));
        self.version = message.version();
        self.dictionaries.clear();
        Ok(schema)
    }

    /// Decodes `frame`: a record batch in the current schema, or `None` for a dictionary batch,
    /// which later batches use.
    ///
    /// # Errors
    ///
    /// A [`WireError`] when the frame is too large, arrives before a schema, is malformed, holds
    /// more rows than the limit, or Arrow cannot decode it; decoding never panics.
    pub fn frame(&mut self, frame: &IpcFrame) -> Result<Option<RecordBatch>, WireError> {
        self.limits
            .admit_frame(frame.header.len().saturating_add(frame.body.len()))?;
        let message = message(Frame::Batch, &frame.header, self.limits.nesting_depth)?;
        let (kind_of, rows) = match message.header_type() {
            MessageHeader::RecordBatch => (
                Frame::Batch,
                message.header_as_record_batch().map(|batch| batch.length()),
            ),
            MessageHeader::DictionaryBatch => (
                Frame::Dictionary,
                message
                    .header_as_dictionary_batch()
                    .and_then(|dictionary| dictionary.data())
                    .map(|data| data.length()),
            ),
            _ => return Err(unexpected(Frame::Batch, &message)),
        };
        let Some(schema) = self.schema.clone() else {
            return Err(WireError::malformed(kind_of, Problem::NoSchema));
        };
        // A node's values need at least a bit of body each, but for nulls and runs, which
        // need none: those are bounded by the row limit.
        let bits = u64::try_from(frame.body.len())
            .unwrap_or(u64::MAX)
            .saturating_mul(8);
        framing(
            kind_of,
            &message,
            &frame.body,
            self.limits.batch_rows.max(bits),
        )?;
        let rows = rows
            .and_then(|rows| u64::try_from(rows).ok())
            .unwrap_or(u64::MAX);
        Limits::admit("batch rows", self.limits.batch_rows, rows)?;
        let body = Buffer::from(frame.body.clone());
        let version = self.version;
        if let Some(dictionary) = message.header_as_dictionary_batch() {
            let dictionaries = &mut self.dictionaries;
            contained(Frame::Dictionary, || {
                arrow_ipc::reader::read_dictionary(
                    &body,
                    dictionary,
                    &schema,
                    dictionaries,
                    &version,
                )
            })?;
            return Ok(None);
        }
        let batch = message
            .header_as_record_batch()
            .ok_or_else(|| unexpected(Frame::Batch, &message))?;
        let dictionaries = &self.dictionaries;
        contained(Frame::Batch, || {
            arrow_ipc::reader::read_record_batch(&body, batch, schema, dictionaries, None, &version)
        })
        .map(Some)
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

/// The IPC message `header` holds, verified to a depth that fits a schema nested `depth` levels,
/// so a schema deeper than that is refused by the nesting limit rather than as no message.
fn message(frame: Frame, header: &[u8], depth: u64) -> Result<Message<'_>, WireError> {
    let depth = usize::try_from(depth).unwrap_or(usize::MAX);
    let options = flatbuffers::VerifierOptions {
        max_depth: depth.saturating_mul(4).saturating_add(64),
        ..flatbuffers::VerifierOptions::default()
    };
    arrow_ipc::root_as_message_with_opts(&options, header)
        .map_err(|_| WireError::malformed(frame, Problem::NotAMessage))
}

/// Runs `decode`, turning an Arrow error or a panic into a [`WireError`].
fn contained<T>(
    frame: Frame,
    decode: impl FnOnce() -> Result<T, arrow_schema::ArrowError>,
) -> Result<T, WireError> {
    match catch_unwind(AssertUnwindSafe(decode)) {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(source)) => Err(WireError::Arrow {
            frame,
            encoding: false,
            source,
        }),
        Err(payload) => {
            let message = payload
                .downcast_ref::<&str>()
                .map(|message| (*message).to_owned())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_default();
            Err(WireError::Panicked { frame, message })
        }
    }
}

/// How many columns `fields` hold, nested ones included, and how deep they nest.
pub(super) fn measure<'a>(fields: impl IntoIterator<Item = &'a Arc<Field>>) -> (u64, u64) {
    fields.into_iter().fold((0, 0), |(columns, depth), field| {
        let (inner_columns, inner_depth) = measure(children(field.data_type()));
        (columns + 1 + inner_columns, depth.max(1 + inner_depth))
    })
}

/// The fields nested in a value of `data_type`.
fn children(data_type: &DataType) -> Vec<&Arc<Field>> {
    match data_type {
        DataType::Struct(fields) => fields.iter().collect(),
        DataType::Union(fields, _) => fields.iter().map(|(_, field)| field).collect(),
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::ListView(item)
        | DataType::LargeListView(item)
        | DataType::FixedSizeList(item, _)
        | DataType::Map(item, _) => vec![item],
        DataType::RunEndEncoded(ends, values) => vec![ends, values],
        DataType::Dictionary(_, values) => children(values),
        _ => Vec::new(),
    }
}
