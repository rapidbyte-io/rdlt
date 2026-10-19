use std::hint::black_box;

use super::{Window, installed};

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
