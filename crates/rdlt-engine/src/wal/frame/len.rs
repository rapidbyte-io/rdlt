//! The lengths of the frames a log keeps room for, counted without encoding them.

use super::{HEAD, Header, unencoded};
use crate::error::Error;

/// Bytes: the frame of `header`, as `Frame::encode` writes it, counted without writing it.
pub(crate) fn header_len(header: &Header) -> Result<u64, Error> {
    let mut counted = Counted(0);
    serde_json::to_writer(&mut counted, header).map_err(|error| unencoded(&error))?;
    Ok(framed_len(counted.0))
}

/// Bytes: the end frame naming `live` chunks and `received` commits, as `Frame::encode` writes
/// it.
pub(crate) fn end_len(
    live: impl IntoIterator<Item = u64>,
    received: impl IntoIterator<Item = u64>,
) -> u64 {
    let names = u64::try_from(r#"{"live":,"received":}"#.len()).unwrap_or(u64::MAX);
    framed_len(
        names
            .saturating_add(listed(live))
            .saturating_add(listed(received)),
    )
}

/// Bytes: a frame whose payload takes `payload` bytes.
fn framed_len(payload: u64) -> u64 {
    payload.saturating_add(u64::try_from(HEAD).unwrap_or(u64::MAX))
}

/// Bytes: `numbers` as a JSON array, its brackets and the commas between them included.
fn listed(numbers: impl IntoIterator<Item = u64>) -> u64 {
    let (count, digits) = numbers
        .into_iter()
        .fold((0_u64, 0_u64), |(count, digits), number| {
            let width = number.checked_ilog10().map_or(1, |log| u64::from(log) + 1);
            (count + 1, digits + width)
        });
    2 + digits + count.saturating_sub(1)
}

/// A writer that keeps only how many bytes it was given.
struct Counted(u64);

impl std::io::Write for Counted {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        self.0 = self.0.saturating_add(len);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
