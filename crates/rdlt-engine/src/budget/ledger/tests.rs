use std::sync::Arc;

use parking_lot::Mutex;
use proptest::prelude::*;
use tokio::sync::oneshot;

use super::super::Reservation;
use super::{Class, Ledger, WAITING, admit_waiting};

/// A budget whose shares are round, as `budget/tests.rs` describes it.
const BUDGET: u64 = 6_400;

/// The classes a request may wait in.
const ASKED: [Class; 4] = [Class::Intake, Class::Work, Class::Cursor, Class::Tables];

#[derive(Clone, Debug)]
enum Step {
    /// A request of `ASKED[class]` for `percent` of what one of its class may take.
    Ask(usize, u64),
    /// A held reservation released, or a queued request given up, at `place` modulo their count.
    Let(usize),
    /// A holder of queued pieces starts pressing, or the latest stops.
    Press(bool),
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        (0..ASKED.len(), 1_u64..=100).prop_map(|(class, percent)| Step::Ask(class, percent)),
        (0_usize..64).prop_map(Step::Let),
        any::<bool>().prop_map(Step::Press),
    ]
}

/// What the ledger should say: whether pressure is on, and whether a cursor or a table's
/// records wait.
fn said(ledger: &Ledger) -> (bool, bool) {
    let waits = |class: Class| {
        let place = WAITING.iter().position(|waiting| *waiting == class);
        place.is_some_and(|place| !ledger.waiting[place].is_empty())
    };
    (
        ledger.pressing > 0 || waits(Class::Work) || waits(Class::Intake),
        waits(Class::Cursor) || waits(Class::Tables),
    )
}

proptest! {
    /// Each change of what the ledger says is sent once, and every receiver holds what it says.
    #[test]
    fn the_ledger_sends_each_change_of_what_it_says_once(
        steps in proptest::collection::vec(step(), 1..64),
    ) {
        let shared = Arc::new(Mutex::new(Ledger::new(BUDGET)));
        let mut held: Vec<Reservation> = Vec::new();
        let mut queued: Vec<(u64, oneshot::Receiver<Reservation>)> = Vec::new();
        let (mut pressings, mut last, mut changes) = (0_usize, (false, false), 0_u64);
        for step in steps {
            match step {
                Step::Ask(class, percent) => {
                    let class = ASKED[class];
                    let mut ledger = shared.lock();
                    let limit = match class {
                        Class::Intake => ledger.shares.intake,
                        Class::Work => ledger.shares.request,
                        Class::Cursor => ledger.shares.cursors,
                        _ => ledger.shares.tables,
                    };
                    let bytes = (limit * percent / 100).max(1);
                    if ledger.open(class, bytes) {
                        ledger.reserve(class, bytes);
                        drop(ledger);
                        held.push(Reservation::of(&shared, class, bytes));
                    } else if let Some(waiting) = ledger.wait(class, bytes) {
                        queued.push(waiting);
                    }
                }
                Step::Let(place) if !held.is_empty() && (place % 2 == 0 || queued.is_empty()) => {
                    // Released outside the lock: a reservation's drop takes it.
                    drop(held.swap_remove(place % held.len()));
                }
                Step::Let(place) if !queued.is_empty() => {
                    let (id, receiver) = queued.swap_remove(place % queued.len());
                    {
                        let mut ledger = shared.lock();
                        ledger.forget(id);
                        admit_waiting(&shared, &mut ledger);
                    }
                    drop(receiver);
                }
                Step::Press(true) => {
                    shared.lock().pressing(true);
                    pressings += 1;
                }
                Step::Press(false) if pressings > 0 => {
                    shared.lock().pressing(false);
                    pressings -= 1;
                }
                Step::Let(_) | Step::Press(false) => {}
            }
            // Requests admitted while others were released now hold their bytes.
            queued.retain_mut(|(_, receiver)| match receiver.try_recv() {
                Ok(reservation) => {
                    held.push(reservation);
                    false
                }
                Err(oneshot::error::TryRecvError::Empty) => true,
                Err(oneshot::error::TryRecvError::Closed) => false,
            });
            let ledger = shared.lock();
            let now = said(&ledger);
            changes += u64::from(now.0 != last.0) + u64::from(now.1 != last.1);
            last = now;
            prop_assert_eq!(ledger.pressed.subscribe().borrow_and_update(), now.0);
            prop_assert_eq!(ledger.cursor_waits.subscribe().borrow_and_update(), now.1);
            prop_assert_eq!(ledger.signals, changes);
        }
    }
}
