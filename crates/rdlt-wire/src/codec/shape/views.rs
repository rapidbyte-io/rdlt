//! What a column's views name: the bytes of string and binary views, and the items of list
//! views.

/// Bytes a view holds in itself rather than in a data buffer.
const INLINE: u64 = 12;

/// Bits `at` to `at + 32` of `view`.
fn word(view: u128, at: u32) -> u64 {
    u64::try_from((view >> at) & u128::from(u32::MAX)).unwrap_or(u64::MAX)
}

/// The bytes the views in `views` name in data buffers of `data` bytes each, counted once a
/// view.
///
/// # Errors
///
/// The position of the first view naming a buffer `data` lacks, or bytes beyond one.
pub(super) fn named(views: &[u8], data: &[u64]) -> Result<u64, usize> {
    let mut bytes: u64 = 0;
    for (index, view) in views.as_chunks::<16>().0.iter().enumerate() {
        let view = u128::from_le_bytes(*view);
        let (length, buffer, offset) = (word(view, 0), word(view, 64), word(view, 96));
        if length <= INLINE {
            continue;
        }
        let within = usize::try_from(buffer)
            .ok()
            .and_then(|buffer| data.get(buffer))
            .is_some_and(|size| offset + length <= *size);
        if !within {
            return Err(index);
        }
        bytes = bytes.saturating_add(length);
    }
    Ok(bytes)
}

/// The items the list views of `offsets` and `sizes` name in a child of `child` items, counted
/// once a view; `read` reads one offset or size.
///
/// # Errors
///
/// The position of the first list view with a negative offset or size, or naming items beyond
/// the child.
pub(super) fn listed<const N: usize>(
    offsets: &[u8],
    sizes: &[u8],
    child: u64,
    read: fn([u8; N]) -> i64,
) -> Result<u64, usize> {
    let mut items: u64 = 0;
    let views = offsets
        .as_chunks::<N>()
        .0
        .iter()
        .zip(sizes.as_chunks::<N>().0);
    for (index, (offset, size)) in views.enumerate() {
        let within = u64::try_from(read(*offset))
            .ok()
            .zip(u64::try_from(read(*size)).ok())
            .filter(|(offset, size)| offset.checked_add(*size).is_some_and(|end| end <= child));
        let Some((_, size)) = within else {
            return Err(index);
        };
        items = items.saturating_add(size);
    }
    Ok(items)
}
