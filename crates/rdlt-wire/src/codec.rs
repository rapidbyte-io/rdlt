//! Arrow batches on the wire, in Arrow Flight's `FlightData` layout: a schema once per schema
//! epoch, then for each batch the dictionary batches it needs that were not sent, then the batch.

mod compact;
mod contain;
mod decode;
mod framing;
mod measure;
mod relocate;
mod schema;
mod shape;
mod split;
#[cfg(test)]
mod tests;
mod weigh;

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions};
use arrow_schema::{DataType, Schema};
use bytes::Bytes;

use self::measure::Columns;
use crate::error::{Frame, Problem, WireError};

pub use decode::Decoder;
pub use shape::Shape;
pub use split::Cut;
pub use weigh::{Weigher, Weight};

/// One IPC message: its flatbuffer header and its body buffers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IpcFrame {
    /// The IPC message flatbuffer.
    pub header: Bytes,
    /// The message's body buffers.
    pub body: Bytes,
}

/// Encodes one sender's batches: a schema, then batches in it.
#[derive(Debug)]
pub struct Encoder {
    generator: IpcDataGenerator,
    tracker: DictionaryTracker,
    options: IpcWriteOptions,
    context: IpcWriteContext,
    /// The columns of the schema last encoded, as its receiver converts it.
    columns: Option<Columns>,
    /// The dictionaries its receiver holds since that schema, by id: what each takes there.
    held: HashMap<i64, u64>,
    /// How many batches were encoded, for tests of what a cut costs.
    #[cfg(test)]
    encodes: usize,
}

impl Default for Encoder {
    fn default() -> Self {
        Self {
            generator: IpcDataGenerator {},
            tracker: DictionaryTracker::new(false),
            options: IpcWriteOptions::default(),
            context: IpcWriteContext::default(),
            columns: None,
            held: HashMap::new(),
            #[cfg(test)]
            encodes: 0,
        }
    }
}

impl Encoder {
    /// The IPC schema message opening a new schema epoch for `schema`; every dictionary is sent
    /// again after it.
    ///
    /// # Errors
    ///
    /// [`WireError::Malformed`] when the schema holds a dictionary of dictionaries, which no
    /// schema message describes: nothing is to be sent for it, and the epoch before it is over.
    pub fn schema(&mut self, schema: &Schema) -> Result<Bytes, WireError> {
        self.columns = None;
        self.held.clear();
        if schema
            .fields()
            .iter()
            .any(|field| twice_keyed(field.data_type()))
        {
            return Err(WireError::malformed(
                Frame::Schema,
                Problem::DictionaryOfDictionaries,
            ));
        }
        self.tracker = DictionaryTracker::new(false);
        let encoded = self.generator.schema_to_bytes_with_dictionary_tracker(
            schema,
            &mut self.tracker,
            &self.options,
        );
        // Read back as its receiver reads it, verified as deep as the schema nests.
        let message = measure::verified(Frame::Schema, &encoded.ipc_message, nesting(schema))?;
        let read = message
            .header_as_schema()
            .ok_or_else(|| measure::unexpected(Frame::Schema, &message))?;
        self.columns = Some(Columns::new(Arc::new(arrow_ipc::convert::fb_to_schema(
            read,
        ))));
        Ok(Bytes::from(encoded.ipc_message))
    }

    /// The frames of `batch`, which must be in the schema last encoded: the dictionaries it needs
    /// that differ from those sent, then the batch.
    ///
    /// # Errors
    ///
    /// [`WireError::Arrow`] when Arrow cannot encode the batch; [`WireError::Malformed`] when no
    /// schema was encoded for it.
    pub fn batch(&mut self, batch: &RecordBatch) -> Result<Vec<IpcFrame>, WireError> {
        let (mut frames, batch) = self.encoded(batch)?;
        frames.push(batch);
        Ok(frames)
    }

    /// The dictionaries `batch` needs that differ from those sent, and its own frame.
    fn encoded(&mut self, batch: &RecordBatch) -> Result<(Vec<IpcFrame>, IpcFrame), WireError> {
        if self.columns.is_none() {
            return Err(WireError::malformed(Frame::Batch, Problem::NoSchema));
        }
        #[cfg(test)]
        {
            self.encodes += 1;
        }
        let (dictionaries, encoded) = self
            .generator
            .encode(batch, &mut self.tracker, &self.options, &mut self.context)
            .map_err(|source| WireError::Arrow {
                frame: Frame::Batch,
                encoding: true,
                source,
            })?;
        let frame = |encoded: arrow_ipc::writer::EncodedData| IpcFrame {
            header: Bytes::from(encoded.ipc_message),
            body: Bytes::from(encoded.arrow_data),
        };
        Ok((
            dictionaries.into_iter().map(frame).collect(),
            frame(encoded),
        ))
    }
}

/// Bytes: what the schema message carrying `schema` takes, as a sender encodes it, which its
/// receiver holds to [`Limits::schema_bytes`](crate::Limits::schema_bytes).
pub fn schema_message_bytes(schema: &Schema) -> usize {
    let mut tracker = DictionaryTracker::new(false);
    IpcDataGenerator {}
        .schema_to_bytes_with_dictionary_tracker(schema, &mut tracker, &IpcWriteOptions::default())
        .ipc_message
        .len()
}

/// Whether `data_type`, or a type nested in it, is a dictionary whose values are a dictionary.
fn twice_keyed(data_type: &DataType) -> bool {
    match data_type {
        DataType::Dictionary(_, values) => {
            matches!(values.as_ref(), DataType::Dictionary(..)) || twice_keyed(values)
        }
        DataType::Struct(fields) => fields.iter().any(|field| twice_keyed(field.data_type())),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| twice_keyed(field.data_type())),
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::ListView(item)
        | DataType::LargeListView(item)
        | DataType::FixedSizeList(item, _)
        | DataType::Map(item, _) => twice_keyed(item.data_type()),
        DataType::RunEndEncoded(_, values) => twice_keyed(values.data_type()),
        _ => false,
    }
}

/// How deep `schema`'s fields nest, counting a top-level field as the first level, as the
/// nesting limit counts them.
fn nesting(schema: &Schema) -> u64 {
    let mut deepest = 0;
    let mut fields: Vec<(&arrow_schema::Field, u64)> = schema
        .fields()
        .iter()
        .map(|field| (field.as_ref(), 1))
        .collect();
    while let Some((field, depth)) = fields.pop() {
        deepest = deepest.max(depth);
        let below = children(field.data_type()).map(|child| (child, depth.saturating_add(1)));
        fields.extend(below);
    }
    deepest
}

/// The fields one level below a field of `data_type`.
fn children(data_type: &DataType) -> Box<dyn Iterator<Item = &arrow_schema::Field> + '_> {
    match data_type {
        DataType::Struct(fields) => Box::new(fields.iter().map(AsRef::as_ref)),
        DataType::Union(fields, _) => Box::new(fields.iter().map(|(_, field)| field.as_ref())),
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::ListView(item)
        | DataType::LargeListView(item)
        | DataType::FixedSizeList(item, _)
        | DataType::Map(item, _) => Box::new(std::iter::once(item.as_ref())),
        DataType::RunEndEncoded(ends, values) => {
            Box::new([ends.as_ref(), values.as_ref()].into_iter())
        }
        DataType::Dictionary(_, values) => children(values),
        _ => Box::new(std::iter::empty()),
    }
}
