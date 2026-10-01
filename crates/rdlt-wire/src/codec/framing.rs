//! Checks a batch message's framing: it is a record batch or a dictionary batch, its body is as
//! long as it declares, and it uses nothing no end negotiates.

use arrow_ipc::{Message, RecordBatch};

use crate::error::{Frame, Problem, WireError};

/// A batch message's kind, what it describes, and a dictionary batch's id.
pub(super) struct Framed<'a> {
    /// Whether the message is a record batch or a dictionary batch.
    pub(super) frame: Frame,
    /// The nodes and buffers it describes.
    pub(super) batch: RecordBatch<'a>,
    /// The id of the dictionary it replaces, when it is a dictionary batch.
    pub(super) dictionary: Option<i64>,
}

/// What `message`, received with a body of `body` bytes, describes.
pub(super) fn framing<'a>(message: &Message<'a>, body: usize) -> Result<Framed<'a>, WireError> {
    let (frame, batch, dictionary) = if let Some(batch) = message.header_as_record_batch() {
        (Frame::Batch, Some(batch), None)
    } else if let Some(dictionary) = message.header_as_dictionary_batch() {
        if dictionary.isDelta() {
            return Err(WireError::malformed(
                Frame::Dictionary,
                Problem::DeltaDictionary,
            ));
        }
        (Frame::Dictionary, dictionary.data(), Some(dictionary.id()))
    } else {
        (Frame::Batch, None, None)
    };
    let malformed = |problem| WireError::malformed(frame, problem);
    let Some(batch) = batch else {
        return Err(malformed(Problem::Unexpected {
            found: kind(message),
        }));
    };
    let actual = u64::try_from(body).unwrap_or(u64::MAX);
    let declared = message.bodyLength();
    if u64::try_from(declared).ok() != Some(actual) {
        return Err(malformed(Problem::BodyLength { declared, actual }));
    }
    if batch.compression().is_some() {
        return Err(malformed(Problem::Compressed));
    }
    Ok(Framed {
        frame,
        batch,
        dictionary,
    })
}

/// The kind of message `message` holds, for errors.
pub(super) fn kind(message: &Message<'_>) -> &'static str {
    message.header_type().variant_name().unwrap_or("unknown")
}
