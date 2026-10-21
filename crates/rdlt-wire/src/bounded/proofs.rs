//! Proof that a call's body reads each message's prefix within its bound, for every input within
//! the proof's bound.

use tonic::Code;

use super::{Bounded, Bounds, PREFIX};
use crate::limits::Class;

/// The most bytes a proof feeds a body at once.
const BYTES: usize = 16;

/// Formats nothing: what a refusal says has no bearing on whether a prefix is refused.
#[expect(
    dead_code,
    reason = "Kani's stub attributes name it, which rustc does not count as a use"
)]
fn unformatted(_: std::fmt::Arguments<'_>) -> String {
    String::new()
}

/// A body over a call that sends nothing more, its messages held to `wire` bytes on the wire
/// and to no form.
fn body(wire: usize) -> Bounded {
    let bounds = Bounds {
        class: Class::Catalog,
        form: None,
        wire,
        decoded: usize::MAX,
    };
    Bounded::new(tonic::body::Body::empty(), bounds, None)
}

/// Any bytes, up to `BYTES` of them.
fn any_bytes() -> Vec<u8> {
    let bytes: [u8; BYTES] = kani::any();
    bytes[..kani::any_where(|len: &usize| *len <= BYTES)].to_vec()
}

/// The length the prefix at the start of `bytes` declares, where it is whole.
fn declared(bytes: &[u8]) -> Option<usize> {
    let prefix = bytes.get(..PREFIX)?;
    let length = u32::from_be_bytes([prefix[1], prefix[2], prefix[3], prefix[4]]);
    Some(usize::try_from(length).unwrap_or(usize::MAX))
}

/// A prefix read from the start of any bytes is nothing until it is whole, then the length it
/// declares where that is within the wire bound, and a refusal beyond it.
///
/// Bound: every run of up to `BYTES` bytes, and every wire bound. A refusal's message is
/// formatted as nothing.
#[kani::proof]
#[kani::unwind(17)]
#[kani::stub(std::fmt::format, unformatted)]
fn a_prefix_is_read_from_any_bytes_within_its_bound() {
    let wire: usize = kani::any();
    let bytes = any_bytes();
    match (body(wire).declared_in(&bytes), declared(&bytes)) {
        (Ok(None), None) => {}
        (Ok(Some(length)), Some(declared)) => {
            assert_eq!(length, declared);
            assert!(length <= wire);
        }
        (Err(status), Some(declared)) => {
            assert!(declared > wire);
            assert_eq!(status.code(), Code::OutOfRange);
        }
        _ => panic!("a prefix is read as nothing exactly while it is not whole"),
    }
}
