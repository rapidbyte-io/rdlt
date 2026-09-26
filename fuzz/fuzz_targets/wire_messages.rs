//! Decoding untrusted protocol messages never panics, and whatever converts into the contract's
//! types converts back to the same message.

#![no_main]

use libfuzzer_sys::fuzz_target;
use rdlt_connector::wire::v1;
use rdlt_connector::{
    Capabilities, Catalog, CommitMeta, ConnectorError, Receipt, StreamState, TableChange,
};
use rdlt_wire::prost::Message;

/// Decodes `bytes` as `W`, and if it converts into `T`, checks the round trip back.
fn check<T, W>(bytes: &[u8])
where
    W: Message + Default + for<'a> From<&'a T>,
    T: TryFrom<W> + PartialEq + std::fmt::Debug,
{
    let Ok(message) = W::decode(bytes) else { return };
    let Ok(value) = T::try_from(message) else { return };
    let again = W::from(&value);
    let value_again = T::try_from(W::decode(again.encode_to_vec().as_slice()).expect("decodes"));
    assert_eq!(value_again.ok(), Some(value), "the round trip changed the value");
}

fuzz_target!(|input: (u8, Vec<u8>)| {
    let (which, bytes) = input;
    match which % 16 {
        0 => check::<Catalog, v1::Catalog>(&bytes),
        1 => check::<Capabilities, v1::Capabilities>(&bytes),
        2 => check::<StreamState, v1::StreamState>(&bytes),
        3 => check::<TableChange, v1::TableChange>(&bytes),
        4 => check::<CommitMeta, v1::CommitMeta>(&bytes),
        5 => check::<Receipt, v1::Receipt>(&bytes),
        6 => {
            let Ok(error) = v1::Error::decode(bytes.as_slice()) else { return };
            let _ = ConnectorError::try_from(error);
        }
        7 => drop(v1::ReadControl::decode(bytes.as_slice())),
        8 => drop(v1::ReadFrame::decode(bytes.as_slice())),
        9 => drop(v1::WriteFrame::decode(bytes.as_slice())),
        10 => drop(v1::WriteAck::decode(bytes.as_slice())),
        11 => drop(v1::HandshakeRequest::decode(bytes.as_slice())),
        12 => drop(v1::HandshakeResponse::decode(bytes.as_slice())),
        13 => drop(v1::OpenResponse::decode(bytes.as_slice())),
        14 => drop(v1::PlanRequest::decode(bytes.as_slice())),
        _ => drop(v1::CommitRequest::decode(bytes.as_slice())),
    }
});
