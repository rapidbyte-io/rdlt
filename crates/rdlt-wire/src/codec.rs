//! Arrow batches on the wire, in Arrow Flight's `FlightData` layout: a schema once per schema
//! epoch, then for each batch the dictionary batches it needs that were not sent, then the batch.

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

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions};
use arrow_schema::Schema;
use bytes::Bytes;

use self::measure::Columns;
use crate::error::{Frame, WireError};

pub use decode::Decoder;
pub use shape::Shape;

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
}

impl Default for Encoder {
    fn default() -> Self {
        Self {
            generator: IpcDataGenerator {},
            tracker: DictionaryTracker::new(false),
            options: IpcWriteOptions::default(),
            context: IpcWriteContext::default(),
            columns: None,
        }
    }
}

impl Encoder {
    /// The IPC schema message opening a new schema epoch for `schema`; every dictionary is sent
    /// again after it.
    pub fn schema(&mut self, schema: &Schema) -> Bytes {
        self.tracker = DictionaryTracker::new(false);
        let encoded = self.generator.schema_to_bytes_with_dictionary_tracker(
            schema,
            &mut self.tracker,
            &self.options,
        );
        self.columns = arrow_ipc::root_as_message(&encoded.ipc_message)
            .ok()
            .and_then(|message| message.header_as_schema())
            .map(|schema| Columns::new(Arc::new(arrow_ipc::convert::fb_to_schema(schema))));
        Bytes::from(encoded.ipc_message)
    }

    /// The frames of `batch`, which must be in the schema last encoded: the dictionaries it needs
    /// that differ from those sent, then the batch.
    ///
    /// # Errors
    ///
    /// [`WireError::Arrow`] when Arrow cannot encode the batch.
    pub fn batch(&mut self, batch: &RecordBatch) -> Result<Vec<IpcFrame>, WireError> {
        let (mut frames, batch) = self.encoded(batch)?;
        frames.push(batch);
        Ok(frames)
    }

    /// The dictionaries `batch` needs that differ from those sent, and its own frame.
    fn encoded(&mut self, batch: &RecordBatch) -> Result<(Vec<IpcFrame>, IpcFrame), WireError> {
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
