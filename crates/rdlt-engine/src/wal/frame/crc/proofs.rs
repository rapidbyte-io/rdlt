//! Proof that the bitwise checksum the log's proofs stub in is CRC-32C.

use super::super::proofs::{reference, reference_append};

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
