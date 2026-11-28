use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, StringViewArray};
use rdlt_wire::Limits;

use super::{Answered, Read, Sent, Weighed, answered, pieces};
use crate::testing::limits::BARRIER_SLACK;

/// A read that sent `events` in order: `p` a push, `c` a checkpoint, `a` a checkpoint answering
/// barrier 1 and `b` one answering barrier 2, which was raised where `|` stands.
fn read(events: &str) -> Read {
    let mut read = Read::default();
    for event in events.chars() {
        match event {
            '|' => read.raised = Some(read.sent.len()),
            'p' => read.sent.push(Sent::Push),
            'a' => read.sent.push(Sent::Checkpoint(Some(1))),
            'b' => read.sent.push(Sent::Checkpoint(Some(2))),
            _ => read.sent.push(Sent::Checkpoint(None)),
        }
    }
    read
}

#[test]
fn a_read_answers_a_barrier_pending_as_it_starts_before_its_last_push() {
    let cases = [
        // Nothing pushed, or one push answered after it, shows nothing.
        ("", Answered::Unseen),
        ("c", Answered::Unseen),
        ("a", Answered::Unseen),
        ("pa", Answered::Unseen),
        ("p", Answered::Never),
        ("pcpc", Answered::Never),
        ("ap", Answered::InTime),
        ("pap", Answered::InTime),
        ("papc", Answered::InTime),
        ("pcpap", Answered::InTime),
        (
            "ppa",
            Answered::Late {
                barrier: 1,
                pushes: 2,
            },
        ),
        (
            "pcpa",
            Answered::Late {
                barrier: 1,
                pushes: 2,
            },
        ),
        (
            "pcppac",
            Answered::Late {
                barrier: 1,
                pushes: 3,
            },
        ),
    ];
    for (events, expected) in cases {
        assert_eq!(answered(&read(events), 0), expected, "{events}");
    }
}

#[test]
fn the_slack_counts_what_rdlts_serving_end_and_host_hold_of_a_read() {
    // Five events at the ends, the serving end's queue of sixteen, and two in a Python source's
    // bridge.
    assert_eq!(BARRIER_SLACK, 5 + 16 + 2);
}

#[test]
fn the_pieces_a_cut_adds_are_bounded_by_every_limit_it_cuts_at() {
    let limits = Limits {
        batch_rows: 1024,
        batch_values: 4000,
        frame_bytes: 1000,
        ..Limits::default()
    };
    let after = |rows, values, frame_bytes, view_bytes| Weighed {
        rows,
        values,
        frame_bytes,
        view_bytes,
        overhead: 0,
    };
    let beside = |frame_bytes, overhead| Weighed {
        frame_bytes,
        overhead,
        ..Weighed::default()
    };
    let cases = [
        // Nothing, or no more than a limit's half of it: no piece more.
        (after(0, 0, 0, 0), 0),
        (after(1023, 2000, 250, 500), 0),
        // A row limit's pieces hold it whole; the others' fewer than two to a limit.
        (after(4096, 0, 0, 0), 4),
        (after(0, 0, 1500, 0), 2),
        (after(0, 0, 0, 1501), 3),
        // Each value weighs a validity bit more in its frame, whole bytes of them.
        (after(0, 1, 500, 0), 1),
        (after(0, 2000, 251, 0), 1),
        (after(0, 6001, 0, 0), 3 + 1),
        (after(2048, 8000, 500, 2000), 2 + 3 + 2 + 3),
        // A frame's bytes are weighed against the room its rows have in it.
        (beside(600, 500), 2),
        (beside(3, 999), 5),
        // No room: nothing bounds the pieces, whatever they weigh.
        (beside(3, 1000), usize::MAX),
        (beside(0, 1000), usize::MAX),
        (beside(3, 1001), usize::MAX),
    ];
    let wrong: Vec<_> = cases
        .iter()
        .map(|(weighed, expected)| (weighed, pieces(&limits, weighed), *expected))
        .filter(|(_, bound, expected)| bound != expected)
        .collect();
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn a_batch_weighs_its_rows_values_and_bytes_as_its_receiver_counts_them() {
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let names: ArrayRef = Arc::new(StringArray::from(vec!["a", "bb", "ccc"]));
    let views: ArrayRef = Arc::new(StringViewArray::from(vec![
        "a string longer than a view holds inline",
        "b",
        "c",
    ]));
    let narrow = RecordBatch::try_from_iter([("id", Arc::clone(&ids))]).expect("a batch");
    let wide = RecordBatch::try_from_iter([("id", ids), ("name", names), ("view", views)])
        .expect("a batch");
    let weigh = |batch: &RecordBatch| {
        let mut weigher = rdlt_wire::Weigher::new(batch);
        weigher.begin();
        (weigher.weigh_rows(0..3), weigher.overhead())
    };
    let (thin, beside_narrow) = weigh(&narrow);
    let (thick, beside_wide) = weigh(&wide);
    let mut weighed = Weighed::default();
    for batch in [&narrow, &wide, &wide, &narrow] {
        weighed.add(batch);
    }
    // Weights add up; a frame takes beside its rows what the widest schema's takes.
    assert_eq!(
        weighed,
        Weighed {
            rows: 12,
            values: 2 * (thin.values + thick.values),
            frame_bytes: 2 * (thin.frame_bytes() + thick.frame_bytes()),
            view_bytes: 2 * thick.view_bytes,
            overhead: beside_wide,
        }
    );
    assert!(thick.view_bytes > 0 && thick.values >= 9, "{thick:?}");
    assert!(beside_narrow < beside_wide);
}

#[test]
fn a_cut_read_has_its_second_barrier_judged_past_the_pieces_it_adds() {
    // Pieces add to the pushes a source may send before it sees the barrier.
    let beyond = format!("ap|{}", "p".repeat(BARRIER_SLACK + 3));
    assert_ne!(answered(&read(&beyond), 2), Answered::InTime);
    assert_eq!(answered(&read(&beyond), 3), Answered::InTime);
    // Pieces nothing bounds leave it unjudged.
    assert_eq!(answered(&read(&beyond), usize::MAX), Answered::InTime);
}

#[test]
fn a_barrier_raised_during_a_read_is_judged_against_the_pushes_sent_once_it_could_be_seen() {
    let within = "p".repeat(BARRIER_SLACK);
    let beyond = "p".repeat(BARRIER_SLACK + 1);
    let late = Answered::Late {
        barrier: 2,
        pushes: BARRIER_SLACK + 1,
    };
    let never = Answered::Unanswered {
        pushes: BARRIER_SLACK + 1,
    };
    let cases = [
        // The pushes within the slack the source may have sent before it saw the barrier.
        (format!("ap|{within}"), Answered::InTime),
        (format!("ap|{within}b"), Answered::InTime),
        (format!("ap|{beyond}"), never),
        (format!("ap|{beyond}b"), late),
        (format!("ap|pb{beyond}"), Answered::InTime),
        (format!("ap|{within}bp"), Answered::InTime),
        // A checkpoint answering a later barrier answers the earlier too.
        (format!("pbp|{beyond}"), Answered::InTime),
    ];
    for (events, expected) in cases {
        assert_eq!(answered(&read(&events), 0), expected, "{events}");
    }
}
