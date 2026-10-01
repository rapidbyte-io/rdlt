//! A record, for tests, of every step a directory handle makes durable or undoes, in order, and
//! a fault to end them at a chosen step.
//!
//! A step is recorded once it has taken place, so the record is the order of what happened to
//! the disk and not of what was asked for. A fault refuses the step it is set at before the
//! step takes place; a crash refuses that step and every step after it, as a process that died
//! there takes none.
//!
//! The record holds that a step was taken and in which order. It cannot hold that the call
//! behind a recorded sync reached the disk: nothing in a test can see that.

use std::cell::{Cell, RefCell};
use std::io;
use std::path::PathBuf;

/// One step, and the path it is taken on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Step {
    MakeDir(PathBuf),
    Create(PathBuf),
    SyncFile(PathBuf),
    SyncDir(PathBuf),
    /// A link made at the path, to a file being published.
    Link(PathBuf),
    /// A rename onto the path.
    Rename(PathBuf),
    Remove(PathBuf),
    RemoveDir(PathBuf),
}

/// Which step of those taken since it was set a fault refuses, and whether every later one too.
#[derive(Clone, Copy, Debug)]
struct Fault {
    at: usize,
    crash: bool,
}

thread_local! {
    static STEPS: RefCell<Vec<Step>> = const { RefCell::new(Vec::new()) };
    static FAULT: Cell<Option<Fault>> = const { Cell::new(None) };
    static TAKEN: Cell<usize> = const { Cell::new(0) };
    static REFUSED: Cell<bool> = const { Cell::new(false) };
}

/// Counts `step` as the next to take, and refuses it where a fault says so.
pub(crate) fn attempt(step: &Step) -> io::Result<()> {
    let taken = TAKEN.get();
    TAKEN.set(taken + 1);
    let refused = FAULT
        .get()
        .is_some_and(|fault| taken == fault.at || (fault.crash && taken > fault.at));
    if refused {
        REFUSED.set(true);
        return Err(io::Error::other(format!("a fault refused {step:?}")));
    }
    Ok(())
}

/// Records `step`, which took place.
pub(crate) fn done(step: Step) {
    STEPS.with(|steps| steps.borrow_mut().push(step));
}

/// Forgets the steps recorded and any fault.
pub(crate) fn clear() {
    STEPS.with(|steps| steps.borrow_mut().clear());
    FAULT.set(None);
    TAKEN.set(0);
    REFUSED.set(false);
}

/// Whether a fault refused a step since the record was last cleared.
pub(crate) fn refused() -> bool {
    REFUSED.get()
}

/// The steps taken since the record was last cleared.
pub(crate) fn steps() -> Vec<Step> {
    STEPS.with(|steps| steps.borrow().clone())
}

/// The directories synced since the record was last cleared, in order.
pub(crate) fn synced() -> Vec<PathBuf> {
    steps()
        .into_iter()
        .filter_map(|step| match step {
            Step::SyncDir(path) => Some(path),
            _ => None,
        })
        .collect()
}

/// Clears the record and refuses step `at` of those that follow, counted from zero.
pub(crate) fn fail_at(at: usize) {
    clear();
    FAULT.set(Some(Fault { at, crash: false }));
}

/// Clears the record and refuses step `at` of those that follow and every step after it.
pub(crate) fn crash_at(at: usize) {
    clear();
    FAULT.set(Some(Fault { at, crash: true }));
}
