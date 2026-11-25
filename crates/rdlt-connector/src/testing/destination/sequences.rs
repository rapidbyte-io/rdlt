//! The sequences the clauses write: ordered as the engine's are, and spread over their bytes so a
//! destination that orders them otherwise orders some pair wrongly.

#[cfg(test)]
mod tests;

/// The sequence the clauses write for their `n`th row of a key: 16 bytes, ordered bytewise as `n`
/// is, laid out as the engine's are, a segment's eight bytes then a row's.
///
/// The row's half alone orders them the other way, and the segment's crosses the top bit between
/// 7 and 8, so comparing the row's half alone, or comparing signed, flips a winner.
pub(super) fn sequence(n: u8) -> [u8; 16] {
    let segment = u64::MAX / 2 - 7 + u64::from(n);
    let row = u64::MAX - u64::from(n);
    let mut sequence = [0; 16];
    sequence[..8].copy_from_slice(&segment.to_be_bytes());
    sequence[8..].copy_from_slice(&row.to_be_bytes());
    sequence
}

/// The `n` whose [`sequence`] `bytes` are; none for bytes no clause writes.
pub(super) fn ordinal(bytes: &[u8]) -> Option<u8> {
    let row = u64::from_be_bytes(bytes.get(8..)?.try_into().ok()?);
    let n = u8::try_from(u64::MAX - row).ok()?;
    (sequence(n).as_slice() == bytes).then_some(n)
}
