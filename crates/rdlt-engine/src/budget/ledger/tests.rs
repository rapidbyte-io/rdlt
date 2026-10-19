use std::sync::Arc;

use parking_lot::Mutex;
use proptest::prelude::*;
use tokio::sync::oneshot;

use super::super::{Lent, Reservation};
use super::{Class, Ledger, WAITING, admit_waiting};

/// A budget whose shares are round, as `budget/tests.rs` describes it.
const BUDGET: u64 = 6_400;

/// The classes a request may wait in.
const ASKED: [Class; 4] = [Class::Intake, Class::Work, Class::Cursor, Class::Tables];

#[derive(Clone, Debug)]
enum Step {
    /// A request of the class at `class` for `percent` of what one of its class may take.
    Ask(usize, u64),
    /// A held reservation released, or a queued request given up, at `place` modulo their count.
    Let(usize),
    /// A holder of queued pieces starts pressing, or the latest stops.
    Press(bool),
}

/// Steps that ask in one of `classes` classes.
fn step(classes: usize) -> impl Strategy<Value = Step> {
    prop_oneof![
        (0..classes, 1_u64..=100).prop_map(|(class, percent)| Step::Ask(class, percent)),
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
        steps in proptest::collection::vec(step(ASKED.len()), 1..64),
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

/// Every class a request may be of.
const CLASSES: [Class; 7] = [
    Class::Intake,
    Class::Work,
    Class::Cursor,
    Class::Log,
    Class::Tables,
    Class::Read,
    Class::Control,
];

/// Whether every share holds no more than it may: pushes within theirs, pushes and lowering
/// within the data, and each other share within its own.
fn within(ledger: &Ledger) -> bool {
    let shares = &ledger.shares;
    ledger.intake <= shares.intake
        && u128::from(ledger.intake) + u128::from(ledger.work) <= u128::from(shares.data)
        && ledger.cursors <= shares.cursors
        && ledger.log <= shares.log
        && ledger.tables <= shares.tables
        && ledger.reads <= shares.reads
        && ledger.control <= shares.control
}

/// `permille` thousandths of `share`.
fn part(share: u64, permille: u64) -> u64 {
    u64::try_from(u128::from(share) * u128::from(permille) / 1000).expect("within the share")
}

/// Budgets of every size: small, around a power of two, and up to the largest.
fn capacity() -> impl Strategy<Value = u64> {
    prop_oneof![
        0_u64..1 << 20,
        (0_u32..64).prop_map(|bits| 1_u64 << bits),
        any::<u64>()
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(256)))]

    /// Asks, lets and presses in any order keep every share within it, never reserve beyond
    /// the budget, and leave nothing reserved once every reservation is dropped.
    #[test]
    fn every_share_stays_within_it_and_letting_all_go_returns_to_none(
        steps in proptest::collection::vec(step(CLASSES.len()), 1..64),
    ) {
        let shared = Arc::new(Mutex::new(Ledger::new(BUDGET)));
        let mut held: Vec<Reservation> = Vec::new();
        let mut queued: Vec<(u64, oneshot::Receiver<Reservation>)> = Vec::new();
        let mut pressing = 0_usize;
        for step in steps {
            match step {
                Step::Ask(class, percent) => {
                    let class = CLASSES[class];
                    let mut ledger = shared.lock();
                    let limit = ledger
                        .too_large(class, u64::MAX)
                        .map_or(u64::MAX, |refused| refused.limit);
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
                    let mut reservation = held.swap_remove(place % held.len());
                    if place % 3 == 0 {
                        held.push(reservation.split(reservation.bytes() / 2));
                    }
                    if place % 5 == 0 {
                        reservation.shrink(reservation.bytes() / 3);
                    }
                    drop(reservation);
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
                Step::Press(start) if start || pressing > 0 => {
                    let mut ledger = shared.lock();
                    ledger.pressing(start);
                    admit_waiting(&shared, &mut ledger);
                    drop(ledger);
                    pressing = if start { pressing + 1 } else { pressing - 1 };
                }
                Step::Press(_) | Step::Let(_) => {}
            }
            queued.retain_mut(|(_, receiver)| match receiver.try_recv() {
                Ok(reservation) => {
                    held.push(reservation);
                    false
                }
                Err(oneshot::error::TryRecvError::Empty) => true,
                Err(oneshot::error::TryRecvError::Closed) => false,
            });
            let ledger = shared.lock();
            prop_assert!(within(&ledger));
            prop_assert!(ledger.reserved() <= ledger.capacity);
            let holding: u128 = held
                .iter()
                .map(|reservation| u128::from(reservation.bytes()))
                .sum();
            prop_assert_eq!(u128::from(ledger.reserved()), holding);
        }
        drop(queued);
        drop(held);
        prop_assert_eq!(shared.lock().reserved(), 0);
    }

    /// Waiting requests are admitted the first of each share in turn, only where it fits and
    /// a push only while no request for lowering waits, from any state within the shares, the
    /// abandoned ones given back.
    #[test]
    fn waiting_requests_are_admitted_only_where_they_fit(
        capacity in capacity(),
        held in proptest::array::uniform7(0_u64..=1000),
        waiters in proptest::collection::vec((0..WAITING.len(), 1_u64..=1000, any::<bool>()), 0..4),
        freed in (0..CLASSES.len(), 0_u64..=1000),
    ) {
        let shared = Arc::new(Mutex::new(Ledger::new(capacity)));
        // Dropped after the lock, should a check fail under it: a reservation a waiter holds
        // takes the lock to go back.
        let mut waiting = Vec::new();
        let mut ledger = shared.lock();
        let shares = ledger.shares;
        ledger.intake = part(shares.intake, held[0]);
        ledger.work = part(shares.data - ledger.intake, held[1]);
        ledger.cursors = part(shares.cursors, held[2]);
        ledger.log = part(shares.log, held[3]);
        ledger.tables = part(shares.tables, held[4]);
        ledger.reads = part(shares.reads, held[5]);
        ledger.control = part(shares.control, held[6]);
        for (class, permille, alive) in waiters {
            let class = WAITING[class];
            let limit = ledger.too_large(class, u64::MAX).map_or(u64::MAX, |refused| refused.limit);
            let bytes = part(limit, permille);
            let (_, receiver) = ledger.wait(class, bytes).expect("a class that waits is queued");
            waiting.push((class, bytes, alive.then_some(receiver)));
        }
        let class = CLASSES[freed.0];
        let holds = *ledger.held(class);
        ledger.release(class, part(holds, freed.1));
        let before = ledger.reserved();
        admit_waiting(&shared, &mut ledger);
        prop_assert!(within(&ledger));
        let work = WAITING.iter().position(|class| *class == Class::Work).expect("lowering waits");
        let lowering_waits = !ledger.waiting[work].is_empty();
        for (place, class) in WAITING.into_iter().enumerate() {
            if let Some(front) = ledger.waiting[place].front() {
                prop_assert!(
                    (class == Class::Intake && lowering_waits) || !ledger.fits(class, front.bytes)
                );
            }
        }
        let after = ledger.reserved();
        drop(ledger);
        let mut received = Vec::new();
        let mut admitted = Vec::new();
        for (_, bytes, receiver) in &mut waiting {
            let taken = receiver.as_mut().and_then(|receiver| receiver.try_recv().ok());
            admitted.push(taken.is_some());
            if let Some(reservation) = taken {
                prop_assert_eq!(reservation.bytes(), *bytes);
                received.push(reservation);
            }
        }
        let sum: u128 = received.iter().map(|taken| u128::from(taken.bytes())).sum();
        prop_assert_eq!(u128::from(after), u128::from(before) + sum);
        for later in 0..waiting.len() {
            for earlier in 0..later {
                let (alive, same) = (
                    waiting[earlier].2.is_some(),
                    waiting[earlier].0 == waiting[later].0,
                );
                prop_assert!(!admitted[later] || !alive || !same || admitted[earlier]);
            }
        }
    }
}

/// One change to a lent reservation and the parts taken from it.
#[derive(Clone, Debug)]
enum Lend {
    /// A part of `permille` thousandths of what is lent.
    Take(u64),
    /// The part at `place` modulo their count shrinks to `permille` thousandths of its bytes.
    Shrink(usize, u64),
    /// The part at `place` modulo their count is split at `permille` thousandths of its bytes.
    Split(usize, u64),
    /// The part at `place` modulo their count goes back.
    Drop(usize),
    /// The lent reservation lends no more.
    Close,
}

fn lend() -> impl Strategy<Value = Lend> {
    prop_oneof![
        (0_u64..=1000).prop_map(Lend::Take),
        (0_usize..8, 0_u64..=1000).prop_map(|(place, permille)| Lend::Shrink(place, permille)),
        (0_usize..8, 0_u64..=1000).prop_map(|(place, permille)| Lend::Split(place, permille)),
        (0_usize..8).prop_map(Lend::Drop),
        Just(Lend::Close),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(256)))]

    /// A lent reservation keeps all its bytes reserved until it is closed, then only what its
    /// parts hold, whichever order parts are taken, shrunk, split, released and the lending
    /// closed in; once all are gone nothing is reserved.
    #[test]
    fn a_lent_reservation_gives_back_all_it_held_whichever_order(
        class in 0..CLASSES.len(),
        permille in 1_u64..=1000,
        steps in proptest::collection::vec(lend(), 0..24),
    ) {
        let shared = Arc::new(Mutex::new(Ledger::new(BUDGET)));
        let class = CLASSES[class];
        let lent: Lent = {
            let mut ledger = shared.lock();
            let limit = ledger
                .too_large(class, u64::MAX)
                .map_or(u64::MAX, |refused| refused.limit);
            let bytes = part(limit, permille).max(1);
            prop_assert!(ledger.open(class, bytes));
            ledger.reserve(class, bytes);
            drop(ledger);
            Reservation::of(&shared, class, bytes).lend()
        };
        let mut parts: Vec<Reservation> = Vec::new();
        let mut closed = false;
        for step in steps {
            match step {
                Lend::Take(permille) => {
                    if let Some(taken) = lent.try_take(part(lent.bytes(), permille)) {
                        parts.push(taken);
                    }
                }
                Lend::Shrink(place, permille) if !parts.is_empty() => {
                    let place = place % parts.len();
                    let bytes = part(parts[place].bytes(), permille);
                    parts[place].shrink(bytes);
                }
                Lend::Split(place, permille) if !parts.is_empty() => {
                    let place = place % parts.len();
                    let bytes = part(parts[place].bytes(), permille);
                    let split = parts[place].split(bytes);
                    parts.push(split);
                }
                Lend::Drop(place) if !parts.is_empty() => {
                    let place = place % parts.len();
                    drop(parts.swap_remove(place));
                }
                Lend::Close => {
                    lent.close();
                    closed = true;
                }
                Lend::Shrink(..) | Lend::Split(..) | Lend::Drop(_) => {}
            }
            let holding: u128 = parts.iter().map(|part| u128::from(part.bytes())).sum();
            let reserved = u128::from(shared.lock().reserved());
            if closed {
                prop_assert_eq!(reserved, holding);
            } else {
                prop_assert_eq!(reserved, u128::from(lent.bytes()));
            }
        }
        drop(parts);
        drop(lent);
        prop_assert_eq!(shared.lock().reserved(), 0);
    }
}
