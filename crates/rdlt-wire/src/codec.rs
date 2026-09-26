//! Arrow batches on the wire, in Arrow Flight's `FlightData` layout: a schema once per schema
//! epoch, then for each batch the dictionary batches it needs that were not sent, then the batch.

mod decode;
mod precheck;
#[cfg(test)]
mod tests;

use arrow_array::RecordBatch;
use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions};
use arrow_schema::Schema;
use bytes::Bytes;

use crate::error::{Frame, WireError};

pub use decode::Decoder;

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
}

impl Default for Encoder {
    fn default() -> Self {
        Self {
            generator: IpcDataGenerator {},
            tracker: DictionaryTracker::new(false),
            options: IpcWriteOptions::default(),
            context: IpcWriteContext::default(),
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
        Bytes::from(encoded.ipc_message)
    }

    /// The frames of `batch`, which must be in the schema last encoded: the dictionaries it needs
    /// that differ from those sent, then the batch.
    ///
    /// # Errors
    ///
    /// [`WireError::Arrow`] when Arrow cannot encode the batch.
    pub fn batch(&mut self, batch: &RecordBatch) -> Result<Vec<IpcFrame>, WireError> {
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
        let mut frames: Vec<IpcFrame> = dictionaries.into_iter().map(frame).collect();
        frames.push(frame(encoded));
        Ok(frames)
    }
}
