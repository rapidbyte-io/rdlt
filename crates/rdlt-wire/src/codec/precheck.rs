//! Checks a message's framing before Arrow reads it: its body is as long as it declares, and
//! every buffer it describes lies within the body.

use arrow_ipc::Message;

use crate::error::{Frame, Problem, WireError};

/// Checks that `message`, received with `body`, frames its buffers within the body.
pub(super) fn framing(frame: Frame, message: &Message<'_>, body: &[u8]) -> Result<(), WireError> {
    let malformed = |problem| WireError::malformed(frame, problem);
    let actual = u64::try_from(body.len()).unwrap_or(u64::MAX);
    let declared = message.bodyLength();
    if u64::try_from(declared).ok() != Some(actual) {
        return Err(malformed(Problem::BodyLength { declared, actual }));
    }
    let batch = match frame {
        Frame::Batch => message.header_as_record_batch(),
        Frame::Dictionary => message
            .header_as_dictionary_batch()
            .and_then(|dictionary| dictionary.data()),
        Frame::Schema => None,
    };
    let Some(batch) = batch else {
        return Err(malformed(Problem::Unexpected {
            found: kind(message),
        }));
    };
    if batch.compression().is_some() {
        return Err(malformed(Problem::Compressed));
    }
    for (index, buffer) in batch.buffers().into_iter().flatten().enumerate() {
        let (offset, length) = (buffer.offset(), buffer.length());
        let end = offset
            .checked_add(length)
            .and_then(|end| u64::try_from(end).ok());
        let within = offset >= 0 && length >= 0 && end.is_some_and(|end| end <= actual);
        if !within {
            return Err(malformed(Problem::BufferOutOfBounds {
                index,
                offset,
                length,
                body: actual,
            }));
        }
    }
    Ok(())
}

/// The kind of message `message` holds, for errors.
pub(super) fn kind(message: &Message<'_>) -> &'static str {
    message.header_type().variant_name().unwrap_or("unknown")
}
