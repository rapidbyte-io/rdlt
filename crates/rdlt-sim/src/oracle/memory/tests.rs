use std::hint::black_box;

use super::{
    Account, Held, IN_PROCESS, IN_PROCESS_LOGGED, NETWORK, NETWORK_LOGGED, Window, allowance,
    installed,
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

fn account() -> Account {
    Account {
        reserved: 1000,
        crashed: 0,
        started: 2,
        budget: 500,
        partitions: 3,
        pushes: 1,
        push: 10,
    }
}

#[test]
fn the_bound_is_the_reserved_peaks_and_a_push_a_partition_a_run() {
    assert_eq!(account().bound(), 1000 + 2 * 3 * 10);
}

#[test]
fn the_bound_gains_a_budget_for_each_run_dropped() {
    let account = Account {
        crashed: 2,
        ..account()
    };
    assert_eq!(account.bound(), 1000 + 2 * 500 + 2 * 3 * 10);
}

#[test]
fn the_bound_gains_the_pushes_a_connector_queues_over_the_network() {
    let account = Account {
        pushes: 1 + super::QUEUED,
        ..account()
    };
    assert_eq!(account.bound(), 1000 + 2 * 3 * 33 * 10);
}

#[test]
fn the_bound_saturates_rather_than_wrapping() {
    let account = Account {
        started: u64::MAX,
        push: u64::MAX,
        ..account()
    };
    assert_eq!(account.bound(), u64::MAX);
}

#[test]
fn the_worst_attempt_is_the_one_that_passed_its_bound_by_most() {
    let within = Held {
        held: 10,
        bound: 100,
    };
    let past = Held {
        held: 150,
        bound: 100,
    };
    let further = Held {
        held: 300,
        bound: 200,
    };
    assert_eq!(Held::worse(None, within), within);
    assert_eq!(Held::worse(Some(within), past), past);
    assert_eq!(Held::worse(Some(past), within), past);
    // A tie keeps the earlier attempt.
    assert_eq!(
        Held::worse(Some(further), Held { held: 51, bound: 1 }),
        further
    );
    // A smaller bound does not make an attempt worse.
    assert_eq!(
        Held::worse(
            Some(past),
            Held {
                held: 140,
                bound: 10
            }
        ),
        Held {
            held: 140,
            bound: 10
        }
    );
}

#[test]
fn an_attempt_within_its_bound_and_the_allowance_passes() {
    Held {
        held: 150,
        bound: 100,
    }
    .within(Seed::new(7), 2, 50);
    Held {
        held: 10,
        bound: 100,
    }
    .within(Seed::new(7), 2, 0);
}

#[test]
#[should_panic(expected = "seed 7: phase 2: an attempt's thread held 151 bytes")]
fn an_attempt_past_its_bound_and_the_allowance_fails_naming_its_seed_and_phase() {
    Held {
        held: 151,
        bound: 100,
    }
    .within(Seed::new(7), 2, 50);
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
