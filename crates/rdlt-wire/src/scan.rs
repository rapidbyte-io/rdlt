//! What a message decodes to, measured from its bytes before it is decoded.
//!
//! A decoder builds what a message's fields become before anything checks them, many times the
//! bytes they took: an empty entry of a repeated field takes two bytes and becomes a whole
//! message in memory. The scan walks a message's encoding by its form, the size of each message
//! type and the kind of each of its fields, generated from the `.proto` files, allocating
//! nothing, and counts what decoding it would hold at its peak: each message its size, four times
//! over for each entry of a repeated field, eight for one-byte numbers, as a vector allocates
//! room for four entries at its first and holds its old entries beside twice as many as it grows;
//! each string and byte field its length, or the least a vector of bytes allocates, and a string
//! set again, alone or in a message set again, twice that, as the room the decoder reuses for it
//! grows to twice what it held beside what it held; and a field the form does not know its
//! bytes. That is no less than what decoding holds, so a message whose
//! count passes a bound is refused before it is decoded, and one the scan cannot walk is refused
//! too.

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

/// Entries a vector of elements of more than a byte allocates room for at its first.
///
/// One of bytes allocates [`LEAST`]. Growing, a vector holds its old entries beside twice as
/// many: at most this many times its entries, whatever their count.
const FIRST_ROOM: usize = 4;

/// Levels an encoding may nest, a top-level message the first: as many as protocol buffers'
/// decoder takes, messages and groups alike, and one more, so whatever it decodes is walked.
const MAX_DEPTH: usize = 101;

/// Why a message's encoding could not be scanned.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Unscanned {
    /// It is not a protocol buffer encoding.
    #[error("the message is not an encoding protocol buffers decode")]
    Malformed,
    /// It nests deeper than protocol buffers' decoder takes.
    #[error("the message nests deeper than {MAX_DEPTH} levels")]
    Deep,
}

/// What decoding `message`, an encoding of `form`, holds in memory: the message, and all its
/// fields hold; once that passes `bound`, some count beyond it.
///
/// Every encoding protocol buffers' decoder takes is scanned, fields the form does not know
/// counted at their bytes, groups among them.
///
/// # Errors
///
/// [`Unscanned`] for an encoding that does not decode, or nests too deep.
pub fn decoded(form: &Form, message: &[u8], bound: usize) -> Result<usize, Unscanned> {
    let mut count = Count {
        held: form.size,
        bound,
    };
    count.walk(form, message, 1, false)?;
    Ok(count.held)
}

/// What a scan has counted, and where it may stop.
struct Count {
    held: usize,
    bound: usize,
}

/// A field's key: its number and how its value is encoded.
struct Key {
    number: u64,
    wire: u64,
}

impl Count {
    /// Counts what `bytes`, an encoding of `form` at `depth`, holds beside the form's own size;
    /// `merging` where the message was set before, so its fields are decoded into what it holds.
    fn walk(
        &mut self,
        form: &Form,
        mut bytes: &[u8],
        depth: usize,
        merging: bool,
    ) -> Result<(), Unscanned> {
        if depth > MAX_DEPTH {
            return Err(Unscanned::Deep);
        }
        // The fields set so far, by their place in the form.
        let mut set = 0_u64;
        while !bytes.is_empty() && self.held <= self.bound {
            let key = key(&mut bytes)?;
            let found = form
                .fields
                .iter()
                .enumerate()
                .find(|(_, field)| field.number == key.number);
            let Some((index, field)) = found else {
                // A field the form does not know, held at its bytes.
                let before = bytes.len();
                skip(&key, &mut bytes, depth)?;
                self.hold(before - bytes.len());
                continue;
            };
            if key.wire == 2 {
                let length =
                    usize::try_from(varint(&mut bytes)?).map_err(|_| Unscanned::Malformed)?;
                let payload = take(&mut bytes, length)?;
                let again = set_before(&mut set, index) || merging;
                self.delimited(field, payload, depth, again)?;
                continue;
            }
            skip(&key, &mut bytes, depth)?;
            // A number of a repeated field, one unpacked entry.
            if let (Kind::Scalar(size), true) = (&field.kind, field.repeated) {
                self.hold(entries(*size, 1));
            }
        }
        Ok(())
    }

    /// Counts `payload`, a length-delimited value of `field`, `again` where the field was set
    /// before in the message it is decoded into.
    fn delimited(
        &mut self,
        field: &Field,
        payload: &[u8],
        depth: usize,
        again: bool,
    ) -> Result<(), Unscanned> {
        // An entry of a repeated field is held in a vector, and is a value of its own.
        let entry = |size: usize| if field.repeated { entries(size, 1) } else { 0 };
        let again = again && !field.repeated;
        match field.kind {
            Kind::Message(inner) => {
                self.hold(entry(inner.size));
                self.walk(inner, payload, depth + 1, again)
            }
            // A text or bytes takes at least the least a vector of bytes allocates. A text set
            // again is decoded into the room the last took, which grows to twice that, or to the
            // text, beside the room it grew from.
            Kind::String => {
                let text = payload.len().max(LEAST);
                let text = if again { text.saturating_mul(2) } else { text };
                self.hold(entry(size_of::<String>()).saturating_add(text));
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
                self.hold(entries(size, payload.len()));
                Ok(())
            }
        }
    }

    fn hold(&mut self, bytes: usize) {
        self.held = self.held.saturating_add(bytes);
    }
}

/// Whether the field at `index` of a form was set before, noting in `set` that it is now; one
/// past the first 64 is taken to have been.
fn set_before(set: &mut u64, index: usize) -> bool {
    let Some(bit) = u32::try_from(index)
        .ok()
        .and_then(|index| 1_u64.checked_shl(index))
    else {
        return true;
    };
    let before = *set & bit != 0;
    *set |= bit;
    before
}

/// What `count` entries of `size` bytes hold in a vector at its peak.
fn entries(size: usize, count: usize) -> usize {
    let room = if size == 1 { LEAST } else { FIRST_ROOM };
    size.saturating_mul(count).saturating_mul(room)
}

/// Takes a field's key from the front of `bytes`, as protocol buffers' decoder takes one.
fn key(bytes: &mut &[u8]) -> Result<Key, Unscanned> {
    let key = varint(bytes)?;
    let (number, wire) = (key >> 3, key & 7);
    // A key within 32 bits, as the decoder takes it: a number within 29.
    if number > u64::from(u32::MAX >> 3) || number == 0 || wire > 5 {
        return Err(Unscanned::Malformed);
    }
    Ok(Key { number, wire })
}

/// Skips the value of a field of `key` at the front of `bytes`, a group's fields among it, at
/// `depth`.
fn skip(key: &Key, bytes: &mut &[u8], depth: usize) -> Result<(), Unscanned> {
    match key.wire {
        0 => drop(varint(bytes)?),
        1 => drop(take(bytes, 8)?),
        5 => drop(take(bytes, 4)?),
        2 => {
            let length = usize::try_from(varint(bytes)?).map_err(|_| Unscanned::Malformed)?;
            take(bytes, length)?;
        }
        3 => {
            if depth >= MAX_DEPTH {
                return Err(Unscanned::Deep);
            }
            loop {
                let inner = self::key(bytes)?;
                if inner.wire == 4 {
                    if inner.number != key.number {
                        return Err(Unscanned::Malformed);
                    }
                    break;
                }
                skip(&inner, bytes, depth + 1)?;
            }
        }
        // A group's end where none began.
        _ => return Err(Unscanned::Malformed),
    }
    Ok(())
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

/// What decoding `bytes` as each message the protocol's calls carry holds, against what the
/// scan counts: for tests and fuzzing to hold the scan to the decoder.
#[doc(hidden)]
pub mod differential {
    use prost::Message;

    use super::{Form, Unscanned, decoded, request, response};
    use crate::v1;

    /// How a decoder of one message type took an encoding.
    #[derive(Debug)]
    pub struct Decoding {
        /// The type's form.
        pub form: &'static Form,
        /// Whether the decoder took the encoding.
        pub decoded: bool,
        /// What the decoder held at its peak, as `measured` measured it.
        pub held: usize,
        /// What the scan counted.
        pub counted: Result<usize, Unscanned>,
    }

    /// Decodes `bytes` as `M`: whether it decoded.
    fn decodes<M: Message + Default>(bytes: &[u8]) -> bool {
        M::decode(bytes).is_ok()
    }

    /// A message type's form, where it has one, and its decoder.
    type Decoder = (Option<&'static Form>, fn(&[u8]) -> bool);

    /// The messages the protocol's calls carry: each's form and its decoder.
    fn messages() -> Vec<Decoder> {
        vec![
            (request("Handshake"), decodes::<v1::HandshakeRequest>),
            (request("Configure"), decodes::<v1::ConfigureRequest>),
            (request("Check"), decodes::<v1::CheckRequest>),
            (request("Discover"), decodes::<v1::DiscoverRequest>),
            (request("Plan"), decodes::<v1::PlanRequest>),
            (request("Read"), decodes::<v1::ReadControl>),
            (request("Committed"), decodes::<v1::CommittedRequest>),
            (request("Open"), decodes::<v1::OpenRequest>),
            (request("ApplySchema"), decodes::<v1::ApplySchemaRequest>),
            (request("Write"), decodes::<v1::WriteFrame>),
            (request("Commit"), decodes::<v1::CommitRequest>),
            (request("Close"), decodes::<v1::CloseRequest>),
            (request("Heartbeat"), decodes::<v1::Ping>),
            (
                request("ReadPublished"),
                decodes::<v1::ReadPublishedRequest>,
            ),
            (
                request("ReadAcknowledged"),
                decodes::<v1::ReadAcknowledgedRequest>,
            ),
            (response("Handshake"), decodes::<v1::HandshakeResponse>),
            (response("Configure"), decodes::<v1::ConfigureResponse>),
            (response("Check"), decodes::<v1::CheckResponse>),
            (response("Discover"), decodes::<v1::Catalog>),
            (response("Plan"), decodes::<v1::PlanResponse>),
            (response("Read"), decodes::<v1::ReadFrame>),
            (response("Committed"), decodes::<v1::CommittedResponse>),
            (response("Open"), decodes::<v1::OpenResponse>),
            (response("ApplySchema"), decodes::<v1::ApplySchemaResponse>),
            (response("Write"), decodes::<v1::WriteAck>),
            (response("Commit"), decodes::<v1::Receipt>),
            (response("Close"), decodes::<v1::CloseResponse>),
            (response("Heartbeat"), decodes::<v1::Pong>),
            (
                response("ReadAcknowledged"),
                decodes::<v1::ReadAcknowledgedResponse>,
            ),
        ]
    }

    /// How many message types [`decoding`] takes.
    pub fn count() -> usize {
        messages().len()
    }

    /// `bytes` decoded as message type `which`, modulo [`count`], what the decoder held at its
    /// peak as `measured` measures what the function it is given holds, and what the scan counts.
    pub fn decoding(
        which: usize,
        bytes: &[u8],
        measured: &dyn Fn(&mut dyn FnMut()) -> usize,
    ) -> Option<Decoding> {
        let messages = messages();
        let (form, decode) = messages.get(which % messages.len())?;
        let form = (*form)?;
        let mut took = false;
        let held = measured(&mut || took = decode(bytes));
        Some(Decoding {
            form,
            decoded: took,
            held,
            counted: decoded(form, bytes, usize::MAX),
        })
    }
}
