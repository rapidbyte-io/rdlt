use std::hint::black_box;

use super::{
    Held, IN_PROCESS, IN_PROCESS_LOGGED, NETWORK, NETWORK_LOGGED, QUEUED, Window, allowance,
    installed, queued,
};
use crate::seed::Seed;

#[test]
fn a_window_measures_the_most_its_thread_held_beyond_where_it_began() {
    let before = black_box(vec![0_u8; 4 << 20]);
    let window = Window::open();
    drop(black_box(vec![0_u8; 1 << 20]));
    let kept = black_box(vec![0_u8; 1 << 10]);
    assert_eq!(window.close(), 1 << 20);
    drop((before, kept));
}

#[test]
fn a_window_leaves_out_what_another_thread_allocates() {
    let window = Window::open();
    std::thread::scope(|scope| {
        scope.spawn(|| drop(black_box(vec![0_u8; 8 << 20])));
    });
    let held = window.close();
    assert!(held < 1 << 20, "held {held}");
}

#[test]
fn a_window_inside_another_leaves_the_outer_window_its_own_peak() {
    let outer = Window::open();
    drop(black_box(vec![0_u8; 2 << 20]));
    let inner = Window::open();
    drop(black_box(vec![0_u8; 1 << 20]));
    assert_eq!(inner.close(), 1 << 20);
    let held = outer.close();
    assert!(held >= 2 << 20, "held {held}");
}

#[test]
fn the_unit_tests_count_their_threads_allocations() {
    assert!(installed());
}

fn held(held: u64, bound: u64) -> Held {
    Held {
        held,
        reserved: bound,
        dropped: 0,
        pushes: 0,
        queued: 0,
    }
}

#[test]
fn the_bound_is_every_term_the_runs_account_for() {
    let held = Held {
        held: 0,
        reserved: 1000,
        dropped: 500,
        pushes: 30,
        queued: 7,
    };
    assert_eq!(held.bound(), 1000 + 500 + 30 + 7);
}

#[test]
fn the_bound_saturates_rather_than_wrapping() {
    let held = Held {
        pushes: u64::MAX,
        ..held(0, 1000)
    };
    assert_eq!(held.bound(), u64::MAX);
    assert_eq!(queued(u64::MAX, u64::MAX, 1), u64::MAX);
}

#[test]
fn a_read_over_the_network_holds_its_queued_pushes_and_two_frames_of_credit() {
    let push = 10 << 20;
    assert_eq!(queued(3, push, 4 << 20), 3 * (QUEUED * push + 2 * push));
}

#[test]
fn a_read_of_small_pushes_holds_the_credit_floor_in_transport() {
    let (push, floor) = (500, 4 << 20);
    assert_eq!(queued(3, push, floor), 3 * (QUEUED * push + floor));
    assert_eq!(queued(0, push, floor), 0);
}

#[test]
fn a_timing_line_records_the_bound_term_by_term() {
    let mut fields = serde_json::Map::new();
    Held {
        held: 9,
        reserved: 1,
        dropped: 2,
        pushes: 3,
        queued: 4,
    }
    .record(&mut fields);
    let recorded = |name: &str| fields[name].as_u64();
    assert_eq!(recorded("held_bytes"), Some(9));
    assert_eq!(recorded("bound_bytes"), Some(10));
    assert_eq!(recorded("reserved_bytes"), Some(1));
    assert_eq!(recorded("dropped_bytes"), Some(2));
    assert_eq!(recorded("pushes_bytes"), Some(3));
    assert_eq!(recorded("queued_bytes"), Some(4));
}

#[test]
fn the_worst_attempt_is_the_one_that_passed_its_bound_by_most() {
    let within = held(10, 100);
    let past = held(150, 100);
    let further = held(300, 200);
    assert_eq!(Held::worse(None, within), within);
    assert_eq!(Held::worse(Some(within), past), past);
    assert_eq!(Held::worse(Some(past), within), past);
    // A tie keeps the earlier attempt.
    assert_eq!(Held::worse(Some(further), held(51, 1)), further);
    // A smaller bound does not make an attempt worse.
    assert_eq!(Held::worse(Some(past), held(140, 10)), held(140, 10));
}

#[test]
fn an_attempt_within_its_bound_and_the_allowance_passes() {
    held(150, 100).within(Seed::new(7), 2, 50);
    held(10, 100).within(Seed::new(7), 2, 0);
}

#[test]
#[should_panic(expected = "seed 7: phase 2: an attempt's thread held 151 bytes")]
fn an_attempt_past_its_bound_and_the_allowance_fails_naming_its_seed_and_phase() {
    held(151, 100).within(Seed::new(7), 2, 50);
}

#[test]
fn an_allowance_is_found_apart_by_placement_and_log_where_work_is_on_one_thread() {
    let seed = Seed::new(7);
    assert_eq!(allowance(seed, true, false, false), Some(IN_PROCESS));
    assert_eq!(allowance(seed, true, false, true), Some(IN_PROCESS_LOGGED));
    assert_eq!(allowance(seed, true, true, false), Some(NETWORK));
    assert_eq!(allowance(seed, true, true, true), Some(NETWORK_LOGGED));
    assert_eq!(allowance(seed, false, false, false), None);
}
