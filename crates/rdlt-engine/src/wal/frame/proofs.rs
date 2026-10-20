//! Proofs that a chunk's preamble and a frame's head read from any bytes as only their own
//! writing does, for every input within each proof's bound.

use super::{HEAD, PREAMBLE, Summing, check_preamble, opened, payload_len, preamble};

/// The most bytes a proof reads.
const BYTES: usize = 16;

/// The most bytes of payload a proof's frame holds.
const PAYLOAD: usize = 4;

/// Formats nothing: what a refusal says has no bearing on whether bytes are refused.
fn unformatted(_: std::fmt::Arguments<'_>) -> String {
    String::new()
}

/// Any bytes, up to `most` of them.
fn any_bytes(most: usize) -> Vec<u8> {
    let bytes: [u8; BYTES] = kani::any();
    bytes[..kani::any_where(|len: &usize| *len <= most.min(BYTES))].to_vec()
}

/// CRC-32C (Castagnoli) of `data` after `crc`, a bit at a time, as `crc32c_append` defines it.
fn reference_append(crc: u32, data: &[u8]) -> u32 {
    let mut crc = !crc;
    for byte in data {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0x82F6_3B78
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

/// CRC-32C of `data`, as `crc32c` defines it.
fn reference(data: &[u8]) -> u32 {
    reference_append(0, data)
}

/// The length a head announces is read from any bytes: nothing until bytes 1 to 4 have
/// arrived, then those bytes as a little-endian `u32`.
///
/// Bound: every run of up to `BYTES` bytes.
#[kani::proof]
#[kani::unwind(17)]
fn a_head_announces_its_payload_s_length_from_any_bytes() {
    let head = any_bytes(BYTES);
    match payload_len(&head) {
        None => assert!(head.len() < 5),
        Some(len) => {
            assert!(head.len() >= 5);
            let announced = u32::from_le_bytes([head[1], head[2], head[3], head[4]]);
            assert_eq!(len, u64::from(announced));
        }
    }
}

/// A chunk is accepted from any bytes only where they start with this format's own preamble.
///
/// Bound: every run of up to `BYTES` bytes. A refusal's message is formatted as nothing.
/// The checksum is a bitwise CRC-32C in place of the crate's, which detects the CPU.
#[kani::stub(crc32c::crc32c, reference)]
#[kani::stub(crc32c::crc32c_append, reference_append)]
#[kani::proof]
#[kani::unwind(17)]
#[kani::stub(std::fmt::format, unformatted)]
fn only_this_format_s_preamble_is_accepted_from_any_bytes() {
    let bytes = any_bytes(BYTES);
    let accepted = check_preamble(&bytes).is_ok();
    assert_eq!(accepted, bytes.get(..PREAMBLE) == Some(&preamble()[..]));
}

/// Summing a frame's checksum from its head and its payload in two pieces cut anywhere, as a
/// scan reads a long payload, accepts exactly the frames reading it whole accepts.
///
/// Bound: every run of up to `HEAD` + `PAYLOAD` bytes whose head announces what follows it,
/// cut at any point of its payload. A refusal's message is formatted as nothing. The checksum is
/// a bitwise CRC-32C in place of the crate's, which detects the CPU.
#[kani::stub(crc32c::crc32c, reference)]
#[kani::stub(crc32c::crc32c_append, reference_append)]
#[kani::proof]
#[kani::unwind(17)]
#[kani::stub(std::fmt::format, unformatted)]
fn summing_a_frame_in_pieces_agrees_with_reading_it_whole() {
    let bytes = any_bytes(HEAD + PAYLOAD);
    kani::assume(bytes.len() >= HEAD);
    let announced = payload_len(&bytes).expect("a whole head");
    kani::assume(usize::try_from(announced).ok() == Some(bytes.len() - HEAD));
    let mut summing = Summing::of(&bytes[..HEAD]).expect("a whole head");
    let payload = &bytes[HEAD..];
    let cut = kani::any_where(|cut: &usize| *cut <= payload.len());
    summing.add(&payload[..cut]);
    summing.add(&payload[cut..]);
    assert_eq!(summing.check().is_ok(), opened(&bytes).is_ok());
    assert!(Summing::of(&bytes[..HEAD - 1]).is_none());
}

/// The reference is CRC-32C: it gives the algorithm's standard check value, and appending
/// is summing the whole.
///
/// Bound: none for the check value, a concrete input; any split of a 9-byte input.
#[kani::proof]
#[kani::unwind(10)]
fn the_reference_checksum_is_crc32c() {
    assert_eq!(reference(b"123456789"), 0xE306_9283);
    let data: [u8; 9] = kani::any();
    let cut = kani::any_where(|cut: &usize| *cut <= data.len());
    assert_eq!(
        reference_append(reference(&data[..cut]), &data[cut..]),
        reference(&data)
    );
}
