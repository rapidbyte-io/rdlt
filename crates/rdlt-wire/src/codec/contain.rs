//! Contains a panic of Arrow's readers: the decoder refuses the frame with a typed error, and
//! the panic is not printed.

#[cfg(test)]
mod tests;

use std::any::Any;
use std::cell::Cell;
use std::panic::{self, AssertUnwindSafe, catch_unwind};
use std::sync::Once;

use crate::error::{Frame, WireError};
use crate::limits::PANIC_TEXT_BYTES;

thread_local! {
    /// Whether this thread is decoding inside [`contained`].
    static CONTAINING: Cell<bool> = const { Cell::new(false) };
}

/// Installs the hook that keeps a contained panic quiet.
static QUIET: Once = Once::new();

/// Runs `decode`, turning an Arrow error or a panic into a [`WireError`].
///
/// The first call wraps the process's panic hook: a panic this function contains skips the hook,
/// and any other reaches the hook installed before.
pub(super) fn contained<T>(
    frame: Frame,
    decode: impl FnOnce() -> Result<T, arrow_schema::ArrowError>,
) -> Result<T, WireError> {
    QUIET.call_once(|| {
        let previous = panic::take_hook();
        panic::set_hook(Box::new(move |info| {
            if !CONTAINING.get() {
                previous(info);
            }
        }));
    });
    CONTAINING.set(true);
    let decoded = catch_unwind(AssertUnwindSafe(decode));
    CONTAINING.set(false);
    match decoded {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(source)) => Err(WireError::Arrow {
            frame,
            encoding: false,
            source,
        }),
        Err(payload) => Err(WireError::Panicked {
            frame,
            message: text(payload.as_ref()),
        }),
    }
}

/// The text of a panic's `payload`, cut to [`PANIC_TEXT_BYTES`]: its printable ASCII as it is, any
/// other character escaped.
fn text(payload: &(dyn Any + Send)) -> String {
    let raw = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or_default();
    let limit = usize::try_from(PANIC_TEXT_BYTES).unwrap_or(usize::MAX);
    let mut text = String::new();
    for char in raw.chars() {
        let before = text.len();
        if char.is_ascii_graphic() || char == ' ' {
            text.push(char);
        } else {
            text.extend(char.escape_default());
        }
        if text.len() > limit {
            text.truncate(before);
            break;
        }
    }
    text
}
