//! The panic hook is the process's: these tests take turns at it, each installing a hook that
//! counts its own panics and passes on any other, with the decoder's wrapped around it as on
//! the decoder's first use.

use std::panic::{self, PanicHookInfo};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::thread;

use arrow_schema::ArrowError;

use super::{contained, quieted};
use crate::error::{Frame, WireError};
use crate::limits::PANIC_TEXT_BYTES;

/// Held by the test whose hook is installed.
static TURN: Mutex<()> = Mutex::new(());

/// The panics that reached a test's hook, while it has its turn at the process's.
struct Reached {
    count: Arc<AtomicUsize>,
    _turn: MutexGuard<'static, ()>,
}

impl Reached {
    fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}

/// Counts the panics that reach the hook of threads `counted` holds for; those of other threads,
/// other tests' among them, go to the hook installed before.
fn counting(counted: impl Fn(&thread::Thread) -> bool + Send + Sync + 'static) -> Reached {
    // The decoder wraps the hook once, on whichever thread decodes first: that is over before
    // this test takes the hook, so the two cannot each take it and one lose the other's.
    super::QUIET.call_once(quieted);
    let turn = TURN.lock().unwrap_or_else(PoisonError::into_inner);
    let count = Arc::new(AtomicUsize::new(0));
    let (seen, before) = (Arc::clone(&count), panic::take_hook());
    panic::set_hook(Box::new(move |info: &PanicHookInfo<'_>| {
        if counted(&thread::current()) {
            seen.fetch_add(1, Ordering::SeqCst);
        } else {
            before(info);
        }
    }));
    quieted();
    Reached { count, _turn: turn }
}

/// Counts the panics of the calling thread.
fn counting_mine() -> Reached {
    let mine = thread::current().id();
    counting(move |thread| thread.id() == mine)
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
    let reached = counting_mine();
    let error = panicking("a reader's panic");
    assert!(matches!(
        error,
        WireError::Panicked {
            frame: Frame::Batch,
            ..
        }
    ));
    assert_eq!(reached.count(), 0);
}

#[test]
fn a_panic_outside_the_decoder_reaches_the_hook_installed_before_it() {
    let reached = counting_mine();
    drop(panicking("contained"));
    let escaped = panic::catch_unwind(|| panic!("not the decoder's"));
    assert!(escaped.is_err());
    assert_eq!(reached.count(), 1);
    // The decoder's own results never touch the hook either.
    let fine = contained(Frame::Batch, || Ok::<_, ArrowError>(7));
    assert_eq!(fine.ok(), Some(7));
    let refused = contained(Frame::Batch, || {
        Err::<(), _>(ArrowError::IpcError("refused".to_owned()))
    });
    assert!(matches!(refused, Err(WireError::Arrow { .. })));
    assert_eq!(reached.count(), 1);
}

#[test]
fn a_panic_on_another_thread_reaches_the_hook_while_one_is_contained() {
    const OTHER: &str = "another thread, panicking while a decode is contained";
    let reached = counting(|thread| thread.name() == Some(OTHER));
    let contained = contained(Frame::Batch, || -> Result<usize, ArrowError> {
        let other = thread::Builder::new().name(OTHER.to_owned());
        let other = other.spawn(|| panic!("another thread's")).unwrap();
        assert!(other.join().is_err());
        Ok(reached.count())
    });
    assert_eq!(contained.ok(), Some(1));
}

#[test]
fn a_contained_panics_text_is_bounded_and_escaped() {
    let limit = usize::try_from(PANIC_TEXT_BYTES).unwrap();
    let message = |error: WireError| match error {
        WireError::Panicked { message, .. } => message,
        other => panic!("{other}"),
    };
    assert_eq!(message(panicking("plain text ~!")), "plain text ~!");
    // Quotes and backslashes are printable: they stay as they are.
    assert_eq!(message(panicking(r#"it's "a\b""#)), r#"it's "a\b""#);
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
