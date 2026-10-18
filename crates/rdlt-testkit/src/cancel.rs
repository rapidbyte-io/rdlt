//! Cancels a call at each point it waits, and checks what the call left.
//!
//! A caller drops a call part way, as a `select!` or a timeout does, and may then call the same
//! object again: what the dropped call changed before it waited must leave the object whole, or
//! leave it refusing as its contract says. [`at_every_point`] runs an operation once to its end,
//! counting the times it waits, and then once for each of them, dropping it there; after every
//! run, a check calls the object again and holds it to its model.
//!
//! Each run builds afresh on a runtime of its own whose clock is paused, so runs are alike up to
//! where one is cut, and a run that never ends fails at the paused clock's bound rather than
//! hanging.
//!
//! Two things are not checked. A run that waits at other points than the first, with as many
//! waits, is cut somewhere else unseen, so an operation must wait alike on every run: no unbiased
//! `select!`, no watch with several waiters. A panic in a task the operation spawns is held by
//! its handle and never reaches the harness, so a spawned fake reports its own failures.

#[cfg(test)]
mod tests;

use std::future::{Future, poll_fn};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::pin::pin;
use std::task::Poll;
use std::time::Duration;

/// The most times an operation may wait: one that waits more fails, since it would no longer be
/// cut at every point.
pub const POINTS: usize = 256;

/// How long one run of an operation and its check may take on the paused clock.
const RUN: Duration = Duration::from_mins(60);

/// How a run of an operation ended.
#[derive(Debug, PartialEq, Eq)]
pub enum Ended<T> {
    /// It returned this.
    Returned(T),
    /// It was dropped at one of its waits.
    Cancelled {
        /// Which wait, counting from one.
        at: usize,
    },
}

/// Runs `operation` on what `make` builds, to its end and then cut at each time it waits, has
/// `check` go on with what each run left, and returns how many waits it was cut at.
///
/// A wait is a poll of the operation that returns pending. Every run builds afresh with `make`,
/// on a current-thread runtime of its own whose clock is paused, and must wait as the first run
/// did up to where it is cut. `check` is told how the run ended, and uses the object as a caller
/// would after a call it dropped: it calls again, and asserts the object's model.
///
/// # Example
///
/// ```
/// use rdlt_testkit::cancel::{Ended, at_every_point};
///
/// // A counter whose call yields between its two steps: cut there, it has taken one.
/// struct Counter(u32);
///
/// impl Counter {
///     async fn twice(&mut self) {
///         self.0 += 1;
///         tokio::task::yield_now().await;
///         self.0 += 1;
///     }
/// }
///
/// let points = at_every_point(
///     async || Counter(0),
///     async |counter: &mut Counter| counter.twice().await,
///     async |counter: &mut Counter, ended: Ended<()>| {
///         let taken = if ended == Ended::Returned(()) { 2 } else { 1 };
///         assert_eq!(counter.0, taken);
///         counter.twice().await;
///         assert_eq!(counter.0, taken + 2);
///     },
/// );
/// assert_eq!(points, 1);
/// ```
///
/// # Panics
///
/// Where it is called within a tokio runtime; where the operation never waits, waits more than
/// [`POINTS`] times, or returns before the wait it is cut at when run again; where a run and its check take more than
/// an hour of the paused clock; and where `check` panics. Each message names the run it failed in.
pub fn at_every_point<S, T>(
    mut make: impl AsyncFnMut() -> S,
    mut operation: impl AsyncFnMut(&mut S) -> T,
    mut check: impl AsyncFnMut(&mut S, Ended<T>),
) -> usize {
    let points = paused("run to its end", async {
        let mut state = make().await;
        let (ended, waits) = run(operation(&mut state), None).await;
        check(&mut state, ended).await;
        waits
    });
    assert!(
        points > 0,
        "the operation never waited: no point to cut it at"
    );
    for at in 1..=points {
        paused(&format!("cancelled at wait {at} of {points}"), async {
            let mut state = make().await;
            let (ended, waits) = run(operation(&mut state), Some(at)).await;
            assert!(
                matches!(ended, Ended::Cancelled { .. }),
                "the operation ran otherwise than its first run: it returned after {waits} waits"
            );
            check(&mut state, ended).await;
        });
    }
    points
}

/// Polls `operation` until it returns or, where `cut` names one, until that wait, then drops it;
/// returns how it ended and how many times it waited.
async fn run<T>(operation: impl Future<Output = T>, cut: Option<usize>) -> (Ended<T>, usize) {
    let mut operation = pin!(operation);
    let mut waits = 0;
    let ended = poll_fn(|context| match operation.as_mut().poll(context) {
        Poll::Ready(value) => Poll::Ready(Ended::Returned(value)),
        Poll::Pending => {
            waits += 1;
            assert!(
                waits <= POINTS,
                "the operation waited more than {POINTS} times"
            );
            if cut == Some(waits) {
                Poll::Ready(Ended::Cancelled { at: waits })
            } else {
                Poll::Pending
            }
        }
    })
    .await;
    (ended, waits)
}

/// Runs `work` within an hour on a runtime of its own whose clock is paused; a panic in it is
/// raised again prefixed with `point`.
fn paused<F: Future>(point: &str, work: F) -> F::Output {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("a current-thread runtime builds");
    let ran = catch_unwind(AssertUnwindSafe(|| {
        runtime.block_on(async {
            tokio::time::timeout(RUN, work).await.unwrap_or_else(|_| {
                panic!("the run did not end within {RUN:?} of the paused clock")
            })
        })
    }));
    ran.unwrap_or_else(|panic| {
        let why = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied());
        match why {
            Some(why) => panic!("{point}: {why}"),
            None => resume_unwind(panic),
        }
    })
}
