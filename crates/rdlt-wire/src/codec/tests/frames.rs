//! Record batch and dictionary batch messages taken apart and rebuilt, for frames no
//! encoder following the format sends.

use arrow_array::RecordBatch;
use arrow_ipc::{MessageHeader, MetadataVersion};
use arrow_schema::Schema;
use bytes::Bytes;

use crate::codec::{Decoder, Encoder, IpcFrame};
use crate::error::{Problem, WireError};
use crate::limits::{Limits, Refusal};

/// What a record batch or dictionary batch message declares.
#[derive(Clone, Debug)]
pub(crate) struct Parts {
    /// The message's metadata version.
    pub(crate) version: MetadataVersion,
    /// The body length it declares.
    pub(crate) body: i64,
    /// The rows it declares.
    pub(crate) length: i64,
    /// Each node's length and null count.
    pub(crate) nodes: Vec<(i64, i64)>,
    /// Each buffer's offset and length.
    pub(crate) buffers: Vec<(i64, i64)>,
    /// Each view column's count of data buffers.
    pub(crate) variadic: Vec<i64>,
    /// A dictionary batch's id, and whether it is a delta.
    pub(crate) dictionary: Option<(i64, bool)>,
    /// Whether the message says its body is compressed.
    pub(crate) compressed: bool,
}

impl Parts {
    /// A record batch message of `length` rows over a body of `body` bytes, with no nodes or
    /// buffers yet.
    pub(crate) fn batch(length: i64, body: i64) -> Self {
        Self {
            version: MetadataVersion::V5,
            body,
            length,
            nodes: Vec::new(),
            buffers: Vec::new(),
            variadic: Vec::new(),
            dictionary: None,
            compressed: false,
        }
    }

    /// What the message in `header` declares.
    pub(crate) fn of(header: &Bytes) -> Self {
        let message = arrow_ipc::root_as_message(header).unwrap();
        let (batch, dictionary) = match message.header_as_dictionary_batch() {
            Some(dictionary) => (
                dictionary.data().unwrap(),
                Some((dictionary.id(), dictionary.isDelta())),
            ),
            None => (message.header_as_record_batch().unwrap(), None),
        };
        let nodes = batch.nodes().unwrap().iter();
        let buffers = batch.buffers().unwrap().iter();
        Self {
            version: message.version(),
            body: message.bodyLength(),
            length: batch.length(),
            nodes: nodes
                .map(|node| (node.length(), node.null_count()))
                .collect(),
            buffers: buffers
                .map(|buffer| (buffer.offset(), buffer.length()))
                .collect(),
            variadic: batch.variadicBufferCounts().into_iter().flatten().collect(),
            dictionary,
            compressed: batch.compression().is_some(),
        }
    }

    /// The message declaring these parts.
    pub(crate) fn header(&self) -> Bytes {
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let nodes: Vec<_> = self
            .nodes
            .iter()
            .map(|(length, nulls)| arrow_ipc::FieldNode::new(*length, *nulls))
            .collect();
        let buffers: Vec<_> = self
            .buffers
            .iter()
            .map(|(offset, length)| arrow_ipc::Buffer::new(*offset, *length))
            .collect();
        let (nodes, buffers) = (fbb.create_vector(&nodes), fbb.create_vector(&buffers));
        let variadic = (!self.variadic.is_empty()).then(|| fbb.create_vector(&self.variadic));
        let compression = self
            .compressed
            .then(|| arrow_ipc::BodyCompressionBuilder::new(&mut fbb).finish());
        let mut batch = arrow_ipc::RecordBatchBuilder::new(&mut fbb);
        batch.add_length(self.length);
        batch.add_nodes(nodes);
        batch.add_buffers(buffers);
        if let Some(variadic) = variadic {
            batch.add_variadicBufferCounts(variadic);
        }
        if let Some(compression) = compression {
            batch.add_compression(compression);
        }
        let batch = batch.finish();
        let (kind, header) = match self.dictionary {
            Some((id, delta)) => {
                let mut dictionary = arrow_ipc::DictionaryBatchBuilder::new(&mut fbb);
                dictionary.add_id(id);
                dictionary.add_data(batch);
                dictionary.add_isDelta(delta);
                (
                    MessageHeader::DictionaryBatch,
                    dictionary.finish().as_union_value(),
                )
            }
            None => (MessageHeader::RecordBatch, batch.as_union_value()),
        };
        let mut message = arrow_ipc::MessageBuilder::new(&mut fbb);
        message.add_version(self.version);
        message.add_header_type(kind);
        message.add_bodyLength(self.body);
        message.add_header(header);
        let message = message.finish();
        fbb.finish(message, None);
        Bytes::copy_from_slice(fbb.finished_data())
    }
}

/// A decoder within `limits` that received `schema`.
pub(crate) fn decoder(schema: &Schema, limits: Limits) -> Decoder {
    let mut decoder = Decoder::new(limits);
    decoder.schema(&Encoder::default().schema(schema)).unwrap();
    decoder
}

/// A decoder within `limits` that received `batch`'s schema, and the frames of `batch`.
pub(crate) fn sent(batch: &RecordBatch, limits: Limits) -> (Decoder, Vec<IpcFrame>) {
    let mut encoder = Encoder::default();
    let mut decoder = Decoder::new(limits);
    decoder.schema(&encoder.schema(&batch.schema())).unwrap();
    (decoder, encoder.batch(batch).unwrap())
}

/// The last of `frames` with its header's parts changed by `change`.
pub(crate) fn changed(frames: &[IpcFrame], change: impl FnOnce(&mut Parts)) -> IpcFrame {
    let frame = frames.last().unwrap();
    let mut parts = Parts::of(&frame.header);
    change(&mut parts);
    IpcFrame {
        header: parts.header(),
        body: frame.body.clone(),
    }
}

/// The error `decoded` is; a frame that decoded fails the test without printing its batch.
pub(crate) fn refused<T>(decoded: Result<T, WireError>) -> WireError {
    match decoded {
        Ok(_) => panic!("the frame was decoded"),
        Err(error) => error,
    }
}

/// The limit's refusal `decoded` is.
pub(crate) fn refusal<T>(decoded: Result<T, WireError>) -> Refusal {
    match refused(decoded) {
        WireError::Refused(refusal) => refusal,
        other => panic!("{other}"),
    }
}

/// What is wrong with the malformed frame `decoded` is.
pub(crate) fn problem<T>(decoded: Result<T, WireError>) -> Problem {
    match refused(decoded) {
        WireError::Malformed { problem, .. } => problem,
        other => panic!("{other}"),
    }
}
