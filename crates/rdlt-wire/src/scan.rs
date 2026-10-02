//! What a message decodes to, measured from its bytes before it is decoded.
//!
//! A decoder builds what a message's fields become before anything checks them, many times the
//! bytes they took: an empty entry of a repeated field takes two bytes and becomes a whole
//! message in memory. The scan walks a message's encoding by its form, the size of each message
//! type and the kind of each of its fields, generated from the `.proto` files, allocating
//! nothing, and counts what decoding it would hold at its peak: each message its size, three times
//! over for the entries of a repeated field, as a vector that grows holds its old entries beside
//! twice as many, and each string and byte field its length, or the least a vector of bytes
//! allocates. That is no less than what decoding holds, so a message whose count passes a bound
//! is refused before it is decoded.

#[cfg(test)]
mod tests;

#[expect(
    clippy::match_same_arms,
    reason = "the forms' lookups name each method, whatever form it shares"
)]
#[path = "generated/forms.rs"]
mod forms;

pub use forms::{request, response};

/// The form of a message type: its size in memory and its fields, by number.
#[derive(Debug)]
pub struct Form {
    /// The message's name.
    pub name: &'static str,
    size: usize,
    fields: &'static [Field],
}

/// A field of a message type.
#[derive(Debug)]
struct Field {
    number: u64,
    kind: Kind,
    repeated: bool,
}

/// What a field holds.
#[derive(Debug)]
enum Kind {
    /// A message of this form.
    Message(&'static Form),
    /// Text, held at its length.
    String,
    /// Bytes, held at their length.
    Bytes,
    /// A number of this many bytes in memory.
    Scalar(usize),
}

/// Bytes: the least a vector of bytes allocates once it holds any.
const LEAST: usize = 8;

/// Levels a message's encoding may nest, a top-level message the first: the protocol's own
/// messages nest a few levels, and a deeper encoding is refused rather than walked.
const MAX_DEPTH: usize = 16;

/// Why a message's encoding could not be scanned.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Unscanned {
    /// It is not a protocol buffer encoding.
    #[error("the message is not an encoding protocol buffers decode")]
    Malformed,
    /// It nests deeper than any of the protocol's messages.
    #[error("the message nests deeper than {MAX_DEPTH} levels")]
    Deep,
}

/// What decoding `message`, an encoding of `form`, holds in memory: the message, and all its
/// fields hold; once that passes `bound`, some count beyond it.
///
/// # Errors
///
/// [`Unscanned`] for an encoding that does not decode, or nests too deep.
pub fn decoded(form: &Form, message: &[u8], bound: usize) -> Result<usize, Unscanned> {
    let mut count = Count {
        held: form.size,
        bound,
    };
    count.walk(form, message, 1)?;
    Ok(count.held)
}

/// What a scan has counted, and where it may stop.
struct Count {
    held: usize,
    bound: usize,
}

impl Count {
    /// Counts what `bytes`, an encoding of `form` at `depth`, holds beside the form's own size.
    fn walk(&mut self, form: &Form, mut bytes: &[u8], depth: usize) -> Result<(), Unscanned> {
        if depth > MAX_DEPTH {
            return Err(Unscanned::Deep);
        }
        while !bytes.is_empty() && self.held <= self.bound {
            let key = varint(&mut bytes)?;
            let field = form.fields.iter().find(|field| field.number == key >> 3);
            match key & 7 {
                0 => {
                    varint(&mut bytes)?;
                }
                1 => {
                    take(&mut bytes, 8)?;
                }
                5 => {
                    take(&mut bytes, 4)?;
                }
                2 => {
                    let length =
                        usize::try_from(varint(&mut bytes)?).map_err(|_| Unscanned::Malformed)?;
                    let payload = take(&mut bytes, length)?;
                    if let Some(field) = field {
                        self.delimited(field, payload, depth)?;
                    }
                    continue;
                }
                _ => return Err(Unscanned::Malformed),
            }
            // A number of a repeated field, one unpacked entry.
            if let Some(Field {
                kind: Kind::Scalar(size),
                repeated: true,
                ..
            }) = field
            {
                self.hold(size.saturating_mul(3));
            }
        }
        Ok(())
    }

    /// Counts `payload`, a length-delimited value of `field`.
    fn delimited(&mut self, field: &Field, payload: &[u8], depth: usize) -> Result<(), Unscanned> {
        // An entry of a repeated field is held in a vector of up to twice the entries, beside
        // the vector it grew from.
        let entry = |size: usize| {
            if field.repeated {
                size.saturating_mul(3)
            } else {
                0
            }
        };
        match field.kind {
            Kind::Message(inner) => {
                self.hold(entry(inner.size));
                self.walk(inner, payload, depth + 1)
            }
            // A text or bytes takes at least the least a vector of bytes allocates.
            Kind::String => {
                self.hold(entry(size_of::<String>()).saturating_add(payload.len().max(LEAST)));
                Ok(())
            }
            Kind::Bytes => {
                self.hold(
                    entry(size_of::<bytes::Bytes>()).saturating_add(payload.len().max(LEAST)),
                );
                Ok(())
            }
            // Packed numbers, each at least a byte.
            Kind::Scalar(size) => {
                self.hold(payload.len().saturating_mul(size).saturating_mul(3));
                Ok(())
            }
        }
    }

    fn hold(&mut self, bytes: usize) {
        self.held = self.held.saturating_add(bytes);
    }
}

/// Takes a varint from the front of `bytes`.
fn varint(bytes: &mut &[u8]) -> Result<u64, Unscanned> {
    let mut value = 0_u64;
    for (index, byte) in bytes.iter().take(10).enumerate() {
        value |= u64::from(byte & 0x7f) << (7 * index);
        if byte & 0x80 == 0 {
            *bytes = &bytes[index + 1..];
            return Ok(value);
        }
    }
    Err(Unscanned::Malformed)
}

/// Takes `length` bytes from the front of `bytes`.
fn take<'a>(bytes: &mut &'a [u8], length: usize) -> Result<&'a [u8], Unscanned> {
    if bytes.len() < length {
        return Err(Unscanned::Malformed);
    }
    let (taken, rest) = bytes.split_at(length);
    *bytes = rest;
    Ok(taken)
}
