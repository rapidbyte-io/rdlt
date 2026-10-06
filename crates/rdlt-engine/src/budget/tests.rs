#![expect(
    clippy::disallowed_methods,
    reason = "tests drive tokio's paused clock and tasks directly"
)]

use std::future::Future;
use std::time::Duration;

use super::{Denied, Exhausted, MemoryBudget, Shares, TooLarge};

/// A budget whose shares are round: 100 for cursors, 400 for the log, 200 for tables' records,
/// 1,600 for reads, 400 for answers, and 3,700 for data, of which a request for lowering takes
/// 1,600 at most and pushes 2,100.
const BUDGET: u64 = 6_400;

const HOUR: Duration = Duration::from_secs(3600);

/// A budget of `capacity` whose requests wait an hour at most.
fn bounded(capacity: u64) -> MemoryBudget {
    let env = std::sync::Arc::new(crate::env::SystemEnv::one_core());
    MemoryBudget::new(capacity).within(env, HOUR)
}

/// Whether `request` is still waiting after a second.
async fn waits<T>(request: &mut (impl Future<Output = T> + Unpin)) -> bool {
    tokio::select! {
        biased;
        _ = request => false,
        () = tokio::time::sleep(Duration::from_secs(1)) => true,
    }
}

/// Whether `budget` calls for a commit within a second.
async fn due(budget: &MemoryBudget) -> bool {
    tokio::select! {
        biased;
        () = budget.cursor_waits() => true,
        () = tokio::time::sleep(Duration::from_secs(1)) => false,
    }
}

/// Whether `budget` signals pressure within a second.
async fn felt(budget: &MemoryBudget) -> bool {
    tokio::select! {
        biased;
        () = budget.pressed() => true,
        () = tokio::time::sleep(Duration::from_secs(1)) => false,
    }
}

#[test]
fn the_shares_of_a_budget_never_pass_it() {
    assert_eq!(
        Shares::of(BUDGET),
        Shares {
            cursors: 100,
            log: 400,
            tables: 200,
            reads: 1_600,
            control: 400,
            data: 3_700,
            request: 1_600,
            intake: 2_100,
            piece: 1_600,
        }
    );
    let default = Shares::of(256 << 20);
    assert_eq!(default.cursors, 4 << 20);
    assert_eq!(default.log, 16 << 20);
    assert_eq!(default.tables, 8 << 20);
    assert_eq!(default.reads, 64 << 20);
    assert_eq!(default.control, 16 << 20);
    assert_eq!(default.piece, 16 << 20);
    for capacity in [0, 1, 3, 63, 64, 1_000, 65_536, 1 << 20, 256 << 20, u64::MAX] {
        let shares = Shares::of(capacity);
        let all = u128::from(shares.cursors)
            + u128::from(shares.log)
            + u128::from(shares.tables)
            + u128::from(shares.reads)
            + u128::from(shares.control)
            + u128::from(shares.data);
        assert_eq!(all, u128::from(capacity), "of {capacity}");
        assert_eq!(shares.intake + shares.request, shares.data, "of {capacity}");
        assert!(shares.piece <= shares.request, "of {capacity}");
    }
}

#[tokio::test]
async fn requests_that_fit_are_admitted_at_once_and_released_when_dropped() {
    let budget = MemoryBudget::new(BUDGET);
    let first = budget.acquire(1_000).await.unwrap();
    let second = budget.acquire(700).await.unwrap();
    assert_eq!((first.bytes(), second.bytes()), (1_000, 700));
    assert_eq!(budget.reserved(), 1_700);
    drop(first);
    assert_eq!(budget.reserved(), 700);
    drop(second);
    assert_eq!(budget.reserved(), 0);
    assert_eq!(budget.peak(), 1_700);
}

#[tokio::test(start_paused = true)]
async fn a_request_for_more_than_its_share_takes_is_refused_and_never_cut_down() {
    let budget = bounded(BUDGET);
    let large = |what, asked, limit| Denied::TooLarge(TooLarge { what, asked, limit });
    assert_eq!(
        budget.acquire(2_101).await.unwrap_err(),
        large("a push", 2_101, 2_100)
    );
    assert_eq!(
        budget.acquire_working(1_601).await.unwrap_err(),
        large("lowering", 1_601, 1_600)
    );
    assert!(budget.try_acquire_working(1_601).is_none());
    assert_eq!(
        budget.acquire_cursor(101).await.unwrap_err(),
        large("a cursor", 101, 100)
    );
    assert_eq!(
        budget.acquire_log(401).await.unwrap_err(),
        large("the log's frames or staging", 401, 400)
    );
    assert_eq!(
        budget.acquire_tables(201).await.unwrap_err(),
        large("a table's records", 201, 200)
    );
    assert_eq!(
        budget.acquire_control(401).await.unwrap_err(),
        large("decoding an answer", 401, 400)
    );
    assert_eq!(
        budget.keep(1_601).unwrap_err(),
        TooLarge {
            what: "what reads keep",
            asked: 1_601,
            limit: 1_600
        }
    );
    // Nothing was reserved for any of them, and each share admits all it may hold.
    assert_eq!((budget.reserved(), budget.peak()), (0, 0));
    let held = (
        budget.acquire(2_100).await.unwrap(),
        budget.acquire_working(1_600).await.unwrap(),
        budget.acquire_cursor(100).await.unwrap(),
        budget.acquire_log(400).await.unwrap(),
        budget.acquire_tables(200).await.unwrap(),
        budget.keep(1_600).unwrap(),
        budget.acquire_control(400).await.unwrap(),
    );
    assert_eq!((budget.reserved(), budget.peak()), (BUDGET, BUDGET));
    drop(held);
    assert_eq!(budget.reserved(), 0);
}

#[tokio::test(start_paused = true)]
async fn requests_for_lowering_never_pass_the_budget_however_many_ask() {
    // Sixteen partitions each ask for eight pieces of a sixteenth of the budget.
    let budget = bounded(16 << 20);
    let piece = budget.shares().piece;
    let mut held = Vec::new();
    for _ in 0..16 * 8 {
        if let Some(reserved) = budget.try_acquire_working(piece) {
            held.push(reserved);
        }
        assert!(budget.reserved() <= budget.capacity());
    }
    assert_eq!(budget.reserved(), budget.shares().data / piece * piece);
    assert!(budget.peak() <= budget.capacity());
    // One more waits for a piece to be written.
    let more = budget.acquire_working(piece);
    tokio::pin!(more);
    assert!(waits(&mut more).await);
    held.pop();
    assert_eq!(more.await.unwrap().bytes(), piece);
    assert!(budget.peak() <= budget.capacity());
}

#[tokio::test(start_paused = true)]
async fn a_request_waits_until_enough_is_released() {
    let budget = MemoryBudget::new(BUDGET);
    let held = budget.acquire(1_200).await.unwrap();
    let waiting = budget.acquire(1_000);
    tokio::pin!(waiting);
    assert!(waits(&mut waiting).await, "1,000 pass what pushes may take");
    drop(held);
    let admitted = waiting.await.unwrap();
    assert_eq!((admitted.bytes(), budget.reserved()), (1_000, 1_000));
}

#[tokio::test(start_paused = true)]
async fn waiting_requests_are_admitted_in_arrival_order() {
    let budget = MemoryBudget::new(BUDGET);
    let held = budget.acquire(2_100).await.unwrap();
    let order = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
    let large = {
        let (budget, order) = (budget.clone(), std::sync::Arc::clone(&order));
        async move {
            let reservation = budget.acquire(1_600).await.unwrap();
            order.lock().push("large");
            reservation
        }
    };
    let small = {
        let (budget, order) = (budget.clone(), std::sync::Arc::clone(&order));
        async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            let reservation = budget.acquire(5).await.unwrap();
            order.lock().push("small");
            reservation
        }
    };
    let release = async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        drop(held);
    };
    let (large, small, ()) = tokio::join!(large, small, release);
    assert_eq!(*order.lock(), ["large", "small"]);
    assert_eq!(budget.reserved(), large.bytes() + small.bytes());
}

#[tokio::test(start_paused = true)]
async fn pushes_leave_a_request_for_lowering_its_room() {
    let budget = MemoryBudget::new(BUDGET);
    // Pushes take all they may.
    let pushes = budget.acquire(2_100).await.unwrap();
    let more = budget.acquire(1);
    tokio::pin!(more);
    assert!(waits(&mut more).await, "pushes never take a request's room");
    // The largest request for lowering fits beside them at once.
    let piece = budget.acquire_working(1_600).await.unwrap();
    assert_eq!(budget.reserved(), 3_700);
    assert!(budget.try_acquire_working(1).is_none());
    drop(piece);
    assert!(waits(&mut more).await, "lowering's room is not a push's");
    drop(pushes);
    assert_eq!(more.await.unwrap().bytes(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_push_never_takes_the_room_a_request_for_lowering_waits_for() {
    let budget = MemoryBudget::new(BUDGET);
    let pushes = budget.acquire(800).await.unwrap();
    let first = budget.acquire_working(1_600).await.unwrap();
    let second = budget.acquire_working(1_200).await.unwrap();
    // 100 bytes of data are left: a request for lowering waits, and a push that would fit them
    // waits behind it.
    let piece = budget.acquire_working(200);
    tokio::pin!(piece);
    assert!(waits(&mut piece).await);
    let push = budget.acquire(100);
    tokio::pin!(push);
    assert!(waits(&mut push).await, "a push goes after lowering");
    assert!(
        budget.try_acquire_working(50).is_none(),
        "nor past its turn"
    );
    // Room a push would fit and the request does not is kept for the request.
    let mut pushes = pushes;
    pushes.shrink(750);
    assert!(waits(&mut push).await, "the room is lowering's");
    drop(first);
    let piece = piece.await.unwrap();
    let push = push.await.unwrap();
    assert_eq!((piece.bytes(), push.bytes()), (200, 100));
    drop((pushes, second));
}

#[tokio::test(start_paused = true)]
async fn a_cursor_is_admitted_from_its_own_share_whatever_data_and_reads_hold() {
    let budget = bounded(BUDGET);
    let data = (
        budget.acquire(2_100).await.unwrap(),
        budget.acquire_working(1_600).await.unwrap(),
        budget.keep(1_600).unwrap(),
        budget.acquire_log(400).await.unwrap(),
        budget.acquire_tables(200).await.unwrap(),
        budget.acquire_control(400).await.unwrap(),
    );
    let push = budget.acquire(1);
    tokio::pin!(push);
    assert!(waits(&mut push).await);
    // A checkpoint waits behind no push.
    let started = tokio::time::Instant::now();
    let cursor = budget.acquire_cursor(100).await.unwrap();
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert_eq!(budget.reserved(), BUDGET);
    // Its share full, a cursor waits for a commit, and data cannot lend it room.
    let next = budget.acquire_cursor(1);
    tokio::pin!(next);
    drop(data);
    assert!(waits(&mut next).await, "only a commit releases cursors");
    drop(cursor);
    assert_eq!(next.await.unwrap().bytes(), 1);
}

#[tokio::test(start_paused = true)]
async fn sixteen_reads_keeping_all_they_may_leave_pushes_and_checkpoints_flowing() {
    // The default budget, and sixteen reads each keeping its part of the reads' share.
    let budget = bounded(256 << 20);
    let shares = budget.shares();
    let kept: Vec<_> = (0..16)
        .map(|_| budget.keep(shares.reads / 16).unwrap())
        .collect();
    assert_eq!(budget.reserved(), shares.reads);
    // One byte more is refused at once: no read waits, and none passes the share.
    assert_eq!(budget.keep(1).unwrap_err().limit, shares.reads);
    let started = tokio::time::Instant::now();
    let cursor = budget.acquire_cursor(4 << 20).await.unwrap();
    let push = budget.acquire(shares.intake).await.unwrap();
    let piece = budget.acquire_working(shares.request).await.unwrap();
    let frame = budget.acquire_log(shares.log).await.unwrap();
    let tables = budget.acquire_tables(shares.tables).await.unwrap();
    let answer = budget.acquire_control(shares.control).await.unwrap();
    assert_eq!(started.elapsed(), Duration::ZERO);
    assert_eq!(budget.reserved(), budget.capacity());
    assert_eq!(budget.peak(), budget.capacity());
    drop((kept, cursor, push, piece, frame, tables, answer));
    assert_eq!(budget.reserved(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_cursor_that_waits_calls_for_a_commit_and_presses_no_one() {
    let budget = MemoryBudget::new(BUDGET);
    let held = budget.acquire_cursor(60).await.unwrap();
    assert!(!due(&budget).await, "no cursor waits");
    let waiting = budget.acquire_cursor(60);
    tokio::pin!(waiting);
    assert_eq!(budget.waits(), (0, 0));
    tokio::select! {
        biased;
        _ = &mut waiting => panic!("two cursors of 60 pass a share of 100"),
        due = due(&budget) => assert!(due, "a cursor waits for a commit"),
    }
    assert!(!felt(&budget).await, "no write releases a cursor");
    drop(held);
    assert_eq!(waiting.await.unwrap().bytes(), 60);
    assert!(!due(&budget).await, "the cursor was admitted");
    assert_eq!(budget.waits(), (0, 1));
}

#[tokio::test(start_paused = true)]
async fn an_abandoned_request_releases_what_it_was_granted() {
    let budget = MemoryBudget::new(BUDGET);
    let held = budget.acquire(2_100).await.unwrap();
    {
        let abandoned = budget.acquire(1_000);
        tokio::pin!(abandoned);
        assert!(waits(&mut abandoned).await);
    }
    let waiting = budget.acquire(1_200);
    drop(held);
    let admitted = waiting.await.unwrap();
    assert_eq!(budget.reserved(), 1_200);
    drop(admitted);
    assert_eq!(budget.reserved(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_waiter_that_gave_up_never_counts_toward_the_peak() {
    let budget = MemoryBudget::new(BUDGET);
    let held = budget.acquire(1_000).await.unwrap();
    {
        let abandoned = budget.acquire(1_500);
        tokio::pin!(abandoned);
        assert!(waits(&mut abandoned).await);
    }
    drop(held);
    assert_eq!(budget.reserved(), 0);
    assert_eq!(budget.peak(), 1_000);
}

#[tokio::test]
async fn debug_output_shows_capacity_and_use() {
    let budget = MemoryBudget::new(BUDGET);
    let reservation = budget.acquire(3).await.unwrap();
    assert!(format!("{budget:?}").contains("capacity: 6400"));
    assert!(format!("{budget:?}").contains("reserved: 3"));
    assert_eq!(format!("{reservation:?}"), "Reservation(3)");
}

#[tokio::test(start_paused = true)]
async fn pressure_is_felt_while_a_request_waits_and_eases_once_it_is_admitted() {
    let budget = MemoryBudget::new(BUDGET);
    let held = budget.acquire(1_500).await.unwrap();
    assert!(!felt(&budget).await, "nothing waits");
    let waiting = budget.acquire(900);
    tokio::pin!(waiting);
    tokio::select! {
        biased;
        _ = &mut waiting => panic!("900 pass what pushes may take"),
        pressed = felt(&budget) => assert!(pressed, "a push waits"),
    }
    drop(held);
    let admitted = waiting.await.unwrap();
    assert!(!felt(&budget).await, "the request was admitted");
    assert_eq!(budget.waits(), (1, 0));
    // A request for lowering presses as a push does.
    let work = (
        budget.acquire_working(1_600).await.unwrap(),
        budget.acquire_working(1_200).await.unwrap(),
    );
    let piece = budget.acquire_working(1_600);
    tokio::pin!(piece);
    tokio::select! {
        biased;
        _ = &mut piece => panic!("three requests pass the data beside a push"),
        pressed = felt(&budget) => assert!(pressed, "lowering waits"),
    }
    drop((admitted, work));
}

#[tokio::test(start_paused = true)]
async fn a_holder_of_queued_pieces_presses_for_their_writes_while_it_waits() {
    let budget = MemoryBudget::new(BUDGET);
    assert!(!felt(&budget).await);
    let first = budget.pressing();
    let second = budget.pressing();
    assert!(felt(&budget).await);
    drop(first);
    assert!(felt(&budget).await, "one still waits");
    drop(second);
    assert!(!felt(&budget).await);
}

#[tokio::test(start_paused = true)]
async fn a_request_waits_no_longer_than_the_deadline_and_says_what_held_the_budget() {
    let budget = bounded(BUDGET);
    let read = budget.keep(700).unwrap();
    let cursor = budget.acquire_cursor(20).await.unwrap();
    let frame = budget.acquire_log(30).await.unwrap();
    let pushes = budget.acquire(1_600).await.unwrap();
    let work = budget.acquire_working(1_500).await.unwrap();
    let started = tokio::time::Instant::now();
    let exhausted = budget.acquire(600).await.unwrap_err();
    assert_eq!(started.elapsed(), HOUR);
    let expected = Exhausted {
        what: "a push",
        asked: 600,
        capacity: BUDGET,
        intake: 1_600,
        work: 1_500,
        cursors: 20,
        log: 30,
        tables: 0,
        reads: 700,
        control: 0,
        waited: HOUR,
    };
    assert_eq!(exhausted, Denied::Exhausted(expected));
    let said = exhausted.to_string();
    assert!(said.contains("waited 3600s"), "{said}");
    assert!(said.contains("700 are kept by reads"), "{said}");
    // The request left the queue: nothing was reserved for it, and nothing waits behind it.
    assert_eq!(budget.reserved(), 3_850);
    let next = budget.acquire(500).await.unwrap();
    drop((read, cursor, frame, pushes, work, next));
    assert_eq!(budget.reserved(), 0);
}

#[tokio::test(start_paused = true)]
async fn every_share_that_waits_ends_its_wait_at_the_deadline() {
    let budget = bounded(BUDGET);
    let held = (
        budget.acquire(2_100).await.unwrap(),
        budget.acquire_working(1_600).await.unwrap(),
        budget.acquire_cursor(100).await.unwrap(),
        budget.acquire_log(400).await.unwrap(),
        budget.acquire_tables(200).await.unwrap(),
        budget.acquire_control(400).await.unwrap(),
    );
    let started = tokio::time::Instant::now();
    let (push, work, cursor, frame, tables, answer) = tokio::join!(
        budget.acquire(1),
        budget.acquire_working(1),
        budget.acquire_cursor(1),
        budget.acquire_log(1),
        budget.acquire_tables(1),
        budget.acquire_control(1),
    );
    assert_eq!(started.elapsed(), HOUR);
    for (denied, what) in [
        (push.unwrap_err(), "a push"),
        (work.unwrap_err(), "lowering"),
        (cursor.unwrap_err(), "a cursor"),
        (frame.unwrap_err(), "the log's frames or staging"),
        (tables.unwrap_err(), "a table's records"),
        (answer.unwrap_err(), "decoding an answer"),
    ] {
        let Denied::Exhausted(exhausted) = denied else {
            panic!("{what} was refused, not waited for: {denied}");
        };
        assert_eq!((exhausted.what, exhausted.waited), (what, HOUR));
    }
    assert_eq!(budget.reserved(), 4_800);
    assert!(!felt(&budget).await, "no request waits any more");
    drop(held);
}

#[tokio::test(start_paused = true)]
async fn a_request_admitted_before_the_deadline_is_not_failed_by_it() {
    let budget = bounded(BUDGET);
    let held = budget.acquire(2_100).await.unwrap();
    let release = async {
        tokio::time::sleep(Duration::from_secs(3_599)).await;
        drop(held);
    };
    let (admitted, ()) = tokio::join!(budget.acquire(2_100), release);
    assert_eq!(admitted.unwrap().bytes(), 2_100);
}

#[tokio::test(start_paused = true)]
async fn a_request_nobody_waits_for_leaves_the_queue_at_once() {
    let budget = MemoryBudget::new(BUDGET);
    let held = budget.acquire(1_200).await.unwrap();
    {
        let abandoned = budget.acquire(1_000);
        tokio::pin!(abandoned);
        tokio::select! {
            biased;
            _ = &mut abandoned => panic!("1,000 pass what pushes may take"),
            pressed = felt(&budget) => assert!(pressed, "a request waits"),
        }
    }
    // Nothing waits any more: no one is pressed, and a request that fits is not behind it.
    assert!(!felt(&budget).await);
    let admitted = tokio::time::timeout(Duration::from_secs(1), budget.acquire(500)).await;
    let admitted = admitted.unwrap().unwrap();
    assert_eq!((admitted.bytes(), budget.reserved()), (500, 1_700));
    drop(held);
}

#[tokio::test(start_paused = true)]
async fn a_reservation_shrinks_and_never_grows() {
    let budget = MemoryBudget::new(BUDGET);
    let mut piece = budget.acquire(1_600).await.unwrap();
    let waiting = budget.acquire(700);
    tokio::pin!(waiting);
    assert!(waits(&mut waiting).await);
    piece.shrink(1_000);
    let admitted = waiting.await.unwrap();
    assert_eq!(admitted.bytes(), 700);
    assert_eq!((piece.bytes(), budget.reserved()), (1_000, 1_700));
    piece.shrink(2_000);
    assert_eq!((piece.bytes(), budget.reserved()), (1_000, 1_700));
    assert_eq!(budget.peak(), 1_700);
    drop((piece, admitted));
    assert_eq!(budget.reserved(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_part_split_off_a_reservation_is_released_on_its_own() {
    let budget = MemoryBudget::new(BUDGET);
    let mut whole = budget.acquire_working(1_000).await.unwrap();
    let part = whole.split(300);
    assert_eq!(
        (whole.bytes(), part.bytes(), budget.reserved()),
        (700, 300, 1_000)
    );
    let rest = whole.split(5_000);
    assert_eq!(
        (whole.bytes(), rest.bytes(), budget.reserved()),
        (0, 700, 1_000)
    );
    drop(part);
    assert_eq!(budget.reserved(), 700);
    drop(whole);
    assert_eq!(budget.reserved(), 700);
    drop(rest);
    assert_eq!((budget.reserved(), budget.peak()), (0, 1_000));
}

#[tokio::test]
async fn a_waiter_gone_before_it_is_admitted_gives_back_what_it_was_admitted() {
    let budget = MemoryBudget::new(6_400);
    // The cursors' share, a hundred bytes, held whole while a waiter queues behind it.
    let holding = budget.acquire_cursor(100).await.unwrap();
    let (_, receiver) = budget
        .shared
        .lock()
        .wait(super::Class::Cursor, 60)
        .expect("a share with room for it some day");
    drop(receiver);
    // Releasing admits the waiter, which is gone: what it was admitted goes back to the share
    // under the lock its admission holds, without counting toward the peak.
    let (done, released) = std::sync::mpsc::channel();
    let releasing = std::thread::spawn(move || {
        drop(holding);
        done.send(()).ok();
    });
    released
        .recv_timeout(Duration::from_secs(5))
        .expect("releasing returns");
    releasing.join().unwrap();
    assert_eq!(budget.reserved(), 0);
    assert_eq!(budget.peak(), 100);
    let again = tokio::time::timeout(Duration::from_secs(5), budget.acquire_cursor(100));
    assert_eq!(again.await.unwrap().unwrap().bytes(), 100);
}
