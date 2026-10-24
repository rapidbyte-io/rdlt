use std::backtrace::Backtrace;
use std::hint::black_box;

use alloc_tracker::Session;

use super::{
    CHANNEL, Counterweight, Held, IN_PROCESS, Kept, NETWORK, QUEUED, Rise, Tracing, Window,
    account, allowance, charged, freed, installed, queued,
};
use crate::seed::Seed;
use crate::world::Largest;

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

#[test]
fn a_window_under_a_counterweight_measures_what_its_thread_holds_beyond_its_charge() {
    let weight = Counterweight::hang(charged(), 64 << 20, None);
    let window = Window::open();
    // Charged before it is held, and given back once it is freed: never beyond its charge.
    account(1 << 20);
    let charged = black_box(vec![0_u8; 1 << 20]);
    drop(charged);
    account(-(1 << 20));
    assert!(window.close() < 1 << 10);
    assert!(!weight.lift().short);
}

#[test]
fn a_charge_given_back_before_what_it_held_is_freed_shows_in_the_window() {
    let weight = Counterweight::hang(charged(), 64 << 20, None);
    let window = Window::open();
    account(1 << 20);
    let charged = black_box(vec![0_u8; 1 << 20]);
    account(-(1 << 20));
    drop(charged);
    let held = window.close();
    assert!(
        (1 << 20..(1 << 20) + (1 << 10)).contains(&held),
        "held {held}"
    );
    assert!(!weight.lift().short);
}

#[test]
fn what_is_held_before_it_is_charged_shows_in_the_window() {
    let weight = Counterweight::hang(charged(), 64 << 20, None);
    let window = Window::open();
    let uncharged = black_box(vec![0_u8; 1 << 20]);
    account(1 << 20);
    drop(uncharged);
    account(-(1 << 20));
    let held = window.close();
    assert!(
        (1 << 20..(1 << 20) + (1 << 10)).contains(&held),
        "held {held}"
    );
    assert!(!weight.lift().short);
}

#[test]
fn a_counterweight_charged_past_its_room_says_so_and_another_hangs_after_it() {
    let weight = Counterweight::hang(charged(), 1 << 10, None);
    account(2 << 10);
    assert!(weight.lift().short);
    account(-(2 << 10));
    let weight = Counterweight::hang(charged(), 1 << 10, None);
    assert!(!weight.lift().short);
}

#[test]
fn the_worst_attempt_is_the_one_that_passed_what_its_reads_hold_by_most() {
    let within = Held {
        held: 10,
        queued: 100,
    };
    let past = Held {
        held: 150,
        queued: 100,
    };
    let further = Held {
        held: 300,
        queued: 200,
    };
    assert_eq!(Held::worse(None, within), within);
    assert_eq!(Held::worse(Some(within), past), past);
    assert_eq!(Held::worse(Some(past), within), past);
    // A tie keeps the earlier attempt.
    let tied = Held {
        held: 101,
        queued: 1,
    };
    assert_eq!(Held::worse(Some(further), tied), further);
}

#[test]
fn a_timing_line_records_what_was_held_and_what_reads_hold() {
    let mut fields = serde_json::Map::new();
    Held { held: 9, queued: 4 }.record(&mut fields);
    assert_eq!(fields["held_bytes"].as_u64(), Some(9));
    assert_eq!(fields["queued_bytes"].as_u64(), Some(4));
}

#[test]
fn an_attempt_within_what_its_reads_hold_and_the_allowance_passes() {
    Held {
        held: 150,
        queued: 100,
    }
    .within(Seed::new(7), 2, 50, &[]);
    Held {
        held: 10,
        queued: 100,
    }
    .within(Seed::new(7), 2, 0, &[]);
}

#[test]
#[should_panic(expected = "seed 7: phase 2: an attempt's thread held 151 bytes")]
fn an_attempt_past_what_its_reads_hold_and_the_allowance_fails_naming_its_seed_and_phase() {
    Held {
        held: 151,
        queued: 100,
    }
    .within(Seed::new(7), 2, 50, &[]);
}

#[test]
#[should_panic(expected = "replay it with RDLT_SIM_TRACE=1")]
fn an_attempt_not_traced_says_how_to_name_the_change_after_which_it_held_too_much() {
    Held {
        held: 151,
        queued: 100,
    }
    .within(Seed::new(7), 2, 50, &[]);
}

fn rise(change: u64, most: u64) -> Rise {
    Rise {
        change,
        by: Some(-9),
        most,
        before: Backtrace::disabled(),
        at: Backtrace::disabled(),
    }
}

#[test]
#[should_panic(expected = "it held 160 bytes beyond its charge once change 4 of the charge, by -9")]
fn a_traced_attempt_names_the_first_change_after_which_it_held_too_much() {
    let rises = [rise(2, 120), rise(4, 160), rise(6, 200)];
    Held {
        held: 200,
        queued: 100,
    }
    .within(Seed::new(7), 2, 50, &rises);
}

fn tracing(spans: usize) -> Tracing {
    let sessions = (0..spans)
        .map(|_| {
            let session = Session::new().no_stdout().no_file();
            let operation = session.operation("change");
            (session, operation)
        })
        .collect();
    Tracing {
        sessions,
        read: Vec::with_capacity(spans),
    }
}

#[test]
fn a_trace_keeps_the_change_after_which_the_thread_held_beyond_its_charge() {
    let tracing = tracing(8);
    let window = Window::open();
    let weight = Counterweight::hang(charged(), 64 << 20, Some(tracing));
    account(2 << 20);
    let charged = black_box(vec![0_u8; 2 << 20]);
    account(-(2 << 20));
    drop(charged);
    account(1);
    account(-1);
    let lifted = weight.lift();
    drop(window);
    let [rise] = lifted.rises.as_slice() else {
        panic!("{:?}", lifted.rises);
    };
    assert_eq!((rise.change, rise.by), (2, Some(-(2 << 20))));
    assert!(
        (2 << 20..(2 << 20) + (64 << 10)).contains(&rise.most),
        "{}",
        rise.most
    );
}

#[test]
fn a_trace_keeps_the_change_after_which_small_holds_crossed_a_mebibyte() {
    let window = Window::open();
    let weight = Counterweight::hang(charged(), 64 << 20, Some(tracing(8)));
    let first = black_box(vec![0_u8; 600 << 10]);
    account(1);
    let second = black_box(vec![0_u8; 600 << 10]);
    account(1);
    account(-2);
    drop((first, second));
    let lifted = weight.lift();
    drop(window);
    let changes: Vec<u64> = lifted.rises.iter().map(|rise| rise.change).collect();
    assert_eq!(changes, [2]);
}

#[test]
fn a_trace_reads_what_is_held_as_the_attempt_ends() {
    let window = Window::open();
    let weight = Counterweight::hang(charged(), 64 << 20, Some(tracing(8)));
    account(1);
    let held = black_box(vec![0_u8; 2 << 20]);
    let lifted = weight.lift();
    drop((window, held));
    let [rise] = lifted.rises.as_slice() else {
        panic!("{:?}", lifted.rises);
    };
    assert_eq!(rise.by, None);
    assert!(
        rise.named().contains("by the attempt's end"),
        "{}",
        rise.named()
    );
}

#[test]
fn a_trace_reads_no_change_past_its_spans() {
    let weight = Counterweight::hang(charged(), 64 << 20, Some(tracing(1)));
    account(2 << 20);
    let charged = black_box(vec![0_u8; 2 << 20]);
    account(-(2 << 20));
    drop(charged);
    assert!(weight.lift().rises.is_empty());
}

#[test]
fn an_allowance_is_found_by_placement_where_work_is_on_one_thread() {
    let seed = Seed::new(7);
    assert_eq!(allowance(seed, true, false, false), Some(IN_PROCESS));
    assert_eq!(allowance(seed, true, true, false), Some(NETWORK));
    assert_eq!(allowance(seed, false, false, false), None);
    assert_eq!(allowance(seed, true, true, true), None);
}

#[test]
fn a_read_of_pushes_beyond_a_frame_holds_one_in_its_channel_and_two_frames_of_credit() {
    let (push, cursor, frame) = (10 << 20, 100, 4 << 20);
    let largest = Largest { push, cursor };
    assert_eq!(
        queued(3, largest, frame, 4 << 20),
        3 * (QUEUED * push + frame + push + CHANNEL * cursor + 2 * push)
    );
}

#[test]
fn a_read_of_small_pushes_holds_its_channel_s_events_and_the_credit_floor() {
    let (push, frame, floor) = (500, 4 << 20, 4 << 20);
    let largest = Largest { push, cursor: 20 };
    assert_eq!(
        queued(3, largest, frame, floor),
        3 * (QUEUED * push + CHANNEL * push + floor)
    );
    assert_eq!(queued(0, largest, frame, floor), 0);
}

#[test]
fn a_cursor_larger_than_every_push_is_a_read_s_largest_event() {
    let largest = Largest {
        push: 10,
        cursor: 1_000,
    };
    assert_eq!(
        queued(1, largest, 4 << 20, 1),
        QUEUED * 1_000 + CHANNEL * 1_000 + 2 * 1_000
    );
}

#[test]
fn what_reads_hold_saturates_rather_than_wrapping() {
    let largest = Largest {
        push: u64::MAX,
        cursor: u64::MAX,
    };
    assert_eq!(queued(u64::MAX, largest, u64::MAX, 1), u64::MAX);
}

#[test]
fn freed_measures_what_dropping_a_value_gives_back() {
    let value = (black_box(vec![0_u8; 3_000]), black_box(vec![0_u8; 500]));
    assert_eq!(freed(value), 3_500);
    assert_eq!(freed(()), 0);
}

#[test]
fn what_the_world_keeps_counts_with_the_charge_while_it_is_kept() {
    let before = charged();
    let window = Window::open();
    let weight = Counterweight::hang(before, 64 << 20, None);
    // Staged under the engine's charge, kept by the world once published, then let go.
    account(1 << 20);
    let stored = black_box(vec![0_u8; 1 << 20]);
    let kept = Kept::held(1 << 20);
    account(-(1 << 20));
    assert_eq!(charged() - before, 1 << 20);
    // The bytes go before what kept them, as a store's entry drops them.
    drop((stored, kept));
    assert!(!weight.lift().short);
    let held = window.close().saturating_sub(64 << 20);
    assert!(held < 64 << 10, "held {held}");
    assert_eq!(charged(), before);
}

#[test]
fn what_dropping_a_value_gives_back_is_measured_unseen_by_a_window() {
    let value = black_box(vec![0_u8; 1 << 20]);
    let window = Window::open();
    let weight = Counterweight::hang(charged(), 512 << 20, None);
    let bytes = freed(value);
    assert!(!weight.lift().short);
    let held = window.close().saturating_sub(512 << 20);
    assert_eq!(bytes, 1 << 20);
    assert!(held < 64 << 10, "held {held}");
}

#[test]
fn a_copy_of_what_is_kept_is_counted_as_made_here() {
    let before = charged();
    let kept = Kept::held(100);
    let copy = kept.clone();
    drop(kept);
    assert_eq!(charged(), before);
    drop(copy);
    assert_eq!(charged(), before);
}
