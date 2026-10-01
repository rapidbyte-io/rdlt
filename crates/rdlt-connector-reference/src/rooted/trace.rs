//! A record, for tests, of every step a directory handle makes durable or undoes, in order, and
//! a fault to end them at a chosen step.
//!
//! A step is recorded before it is taken. A fault refuses the step it is set at; a crash refuses
//! that step and every step after it, as a process that died there takes none.

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

/// Records `step`, and refuses it where a fault says so.
pub(crate) fn step(step: Step) -> io::Result<()> {
    let taken = TAKEN.get();
    TAKEN.set(taken + 1);
    let refused = FAULT
        .get()
        .is_some_and(|fault| taken == fault.at || (fault.crash && taken > fault.at));
    if refused {
        REFUSED.set(true);
        return Err(io::Error::other(format!("a fault refused {step:?}")));
    }
    STEPS.with(|steps| steps.borrow_mut().push(step));
    Ok(())
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
