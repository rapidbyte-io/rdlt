//! The panic hook is the process's, so each of these tests needs a process of its own, as
//! nextest gives it.

use std::panic::{self, PanicHookInfo};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use arrow_schema::ArrowError;

use super::contained;
use crate::error::{Frame, WireError};
use crate::limits::PANIC_TEXT_BYTES;

/// Counts the panics of this thread that reach the hook it installs, as the process's hook.
fn counting() -> Arc<AtomicUsize> {
    let (count, thread) = (Arc::new(AtomicUsize::new(0)), thread::current().id());
    let seen = Arc::clone(&count);
    let hook = move |_: &PanicHookInfo<'_>| {
        if thread::current().id() == thread {
            seen.fetch_add(1, Ordering::SeqCst);
        }
    };
    panic::set_hook(Box::new(hook));
    count
}

fn panicking(text: &str) -> WireError {
    let text = text.to_owned();
    contained(Frame::Batch, move || -> Result<(), ArrowError> {
        panic!("{text}")
    })
    .unwrap_err()
}

#[test]
fn a_contained_panic_does_not_reach_the_panic_hook() {
    let reached = counting();
    let error = panicking("a reader's panic");
    assert!(matches!(
        error,
        WireError::Panicked {
            frame: Frame::Batch,
            ..
        }
    ));
    assert_eq!(reached.load(Ordering::SeqCst), 0);
}

#[test]
fn a_panic_outside_the_decoder_reaches_the_hook_installed_before_it() {
    let reached = counting();
    drop(panicking("contained"));
    let escaped = panic::catch_unwind(|| panic!("not the decoder's"));
    assert!(escaped.is_err());
    assert_eq!(reached.load(Ordering::SeqCst), 1);
    // The decoder's own results never touch the hook either.
    let fine = contained(Frame::Batch, || Ok::<_, ArrowError>(7));
    assert_eq!(fine.ok(), Some(7));
    let refused = contained(Frame::Batch, || {
        Err::<(), _>(ArrowError::IpcError("refused".to_owned()))
    });
    assert!(matches!(refused, Err(WireError::Arrow { .. })));
    assert_eq!(reached.load(Ordering::SeqCst), 1);
}

#[test]
fn a_panic_on_another_thread_reaches_the_hook_while_one_is_contained() {
    let (count, seen) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let counted = Arc::clone(&count);
    panic::set_hook(Box::new(move |_| {
        counted.fetch_add(1, Ordering::SeqCst);
    }));
    let inner = Arc::clone(&seen);
    let contained = contained(Frame::Batch, move || -> Result<(), ArrowError> {
        let other = thread::spawn(|| panic!("another thread's"));
        assert!(other.join().is_err());
        inner.store(count.load(Ordering::SeqCst), Ordering::SeqCst);
        Ok(())
    });
    assert!(contained.is_ok());
    assert_eq!(seen.load(Ordering::SeqCst), 1);
}

#[test]
fn a_contained_panics_text_is_bounded_and_escaped() {
    let limit = usize::try_from(PANIC_TEXT_BYTES).unwrap();
    let message = |error: WireError| match error {
        WireError::Panicked { message, .. } => message,
        other => panic!("{other}"),
    };
    assert_eq!(message(panicking("plain text ~!")), "plain text ~!");
    let escaped = message(panicking("a\u{1b}[31m\nb\u{202e}"));
    assert_eq!(escaped, "a\\u{1b}[31m\\nb\\u{202e}");
    // A panic's payload may be a `&'static str` as well as a `String`.
    let fixed = contained(Frame::Schema, || -> Result<(), ArrowError> {
        panic!("fixed")
    });
    assert_eq!(message(fixed.unwrap_err()), "fixed");
    let other = contained(Frame::Schema, || -> Result<(), ArrowError> {
        panic::panic_any(7_u8)
    });
    assert_eq!(message(other.unwrap_err()), "");
    for long in ["x".repeat(limit + 1), "\n".repeat(limit), "é".repeat(limit)] {
        let cut = message(panicking(&long));
        assert!(cut.len() <= limit, "{} bytes", cut.len());
        assert!(cut.len() + 6 > limit, "{} bytes", cut.len());
        assert!(cut.chars().all(|char| char.is_ascii_graphic()));
    }
    assert_eq!(message(panicking(&"x".repeat(limit))).len(), limit);
}
