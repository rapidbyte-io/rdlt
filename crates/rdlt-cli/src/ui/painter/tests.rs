use std::io::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use super::{Live, Paint, Painter, Std, frame_of};
use crate::args::Color;
use crate::output::FINISHING;
use crate::ui::live::View;
use crate::ui::{Asked, Found, Looks};

fn looks() -> Looks {
    let asked = Asked {
        color: Color::Never,
        quiet: false,
        events_on_stdout: false,
        verbose_logs: false,
    };
    Looks::resolved(asked, &Found::default())
}

/// A painter whose queue nothing reads, as one whose thread is stuck on the terminal is; the
/// receiver, kept by the caller, holds the queue full.
fn stuck_painter() -> (Painter, Receiver<Paint>) {
    let (sender, receiver) = mpsc::sync_channel(0);
    let (_done, ended) = oneshot::channel();
    let thread = std::thread::spawn(|| {});
    let painter = Painter {
        sender,
        ended,
        thread,
        stuck: AtomicBool::new(true),
        lost_logs: Arc::default(),
    };
    (painter, receiver)
}

#[test]
fn above_waiting_returns_at_once_from_a_painter_known_to_be_stuck() {
    let (painter, _queue) = stuck_painter();
    let started = Instant::now();
    painter.above_waiting(Std::Out, "lost");
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "it waited {:?} on a painter already known to be stuck",
        started.elapsed()
    );
}

#[test]
fn log_lines_the_painter_has_no_room_for_are_counted() {
    let (painter, _queue) = stuck_painter();
    let mut lines = painter.log_lines();
    for _ in 0..3 {
        lines.write_all(b"a line\n").expect("a write never fails");
    }
    assert_eq!(painter.lost_logs.load(Ordering::Relaxed), 3);
}

/// The log writer in the global subscriber lives until the process ends, so a clone of the
/// sender must not keep the painter waiting for a disconnect that never comes.
#[tokio::test]
async fn a_painter_ends_at_once_though_the_log_writer_keeps_its_sender() {
    let painter = Painter::start(looks()).expect("the thread starts");
    let _kept = painter.log_lines();
    painter.lost_logs.store(2, Ordering::Relaxed);
    let started = Instant::now();
    assert_eq!(painter.finished().await, Some(2));
    assert!(
        started.elapsed() < FINISHING / 2,
        "it took {:?} to end",
        started.elapsed()
    );
}

#[test]
fn a_clear_nobody_acknowledges_is_given_up_on_within_its_bound_and_marks_the_painter_stuck() {
    let (mut painter, _queue) = stuck_painter();
    painter.stuck = AtomicBool::new(false);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .expect("a runtime");
    runtime.block_on(painter.cleared());
    assert!(
        painter.stuck(),
        "a clear no one acknowledges marks it stuck"
    );
    runtime.block_on(painter.cleared());
}

#[test]
fn the_spinner_s_frame_counts_the_time_since_the_painter_began() {
    let began = Instant::now()
        .checked_sub(Duration::from_millis(9_950))
        .expect("the clock has run for that long");
    let frame = frame_of(began);
    assert!((95..=130).contains(&frame), "{frame}");
}

#[test]
fn only_a_stop_is_drawn_before_the_region_goes() {
    let task = Live {
        view: Some(View::Task("placing".to_owned())),
        undrawn: true,
        bar: None,
    };
    assert!(!task.shows_an_undrawn_stop());
}
