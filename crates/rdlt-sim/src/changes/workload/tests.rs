use rdlt_connector::ChangeOp;
use rdlt_engine::{DeleteMode, OnTruncate, WriteMode};

use super::{ChangeStream, ChangeWorkload, Event, Value};
use crate::changes::{Filter, Filtered};
use crate::rng::SplitMix64;
use crate::swarm::Features;

/// The workload `seed` draws, with sources that forget or not.
fn drawn(seed: u64, forgetting: bool) -> ChangeWorkload {
    let features = Features {
        forgetting,
        ..Features::ALL
    };
    ChangeWorkload::generate(
        &mut SplitMix64::new(seed),
        features,
        &mut SplitMix64::new(!seed),
    )
}

#[test]
fn every_other_change_stream_forgets_only_where_the_seed_draws_sources_that_forget() {
    let mut forgetting = 0;
    for seed in 0..500 {
        let remembering = drawn(seed, false);
        let forgetful = drawn(seed, true);
        assert!(remembering.streams.iter().all(|stream| stream.replayable));
        assert_eq!(forgetful.streams.len(), remembering.streams.len());
        let streams = forgetful.streams.iter().zip(&remembering.streams);
        for (index, (forgetful, remembering)) in streams.enumerate() {
            assert_eq!(forgetful.replayable, index % 2 == 1, "seed {seed}");
            // A source that forgets sends nothing again; every other draw falls alike.
            let mut alike = forgetful.clone();
            alike.replayable = true;
            if forgetful.replayable {
                assert_eq!(&alike, remembering, "seed {seed}");
            } else {
                forgetting += 1;
                assert_eq!(forgetful.replay, None, "seed {seed}");
                alike.replay.clone_from(&remembering.replay);
                assert_eq!(&alike, remembering, "seed {seed}");
            }
        }
    }
    assert!(forgetting >= 500, "{forgetting} forgetting streams");
    let replayed = (0..500).any(|seed| drawn(seed, false).streams[0].replay.is_some());
    assert!(replayed, "a first stream sends changes again where it can");
}

#[test]
fn merge_streams_keep_history_as_drawn_apart_with_whole_events_some_sent_again() {
    let (mut kept, mut echoes) = (0, 0);
    for seed in 0..500 {
        let features = Features::ALL;
        let plain = ChangeWorkload::generate(
            &mut SplitMix64::new(seed),
            features,
            &mut SplitMix64::new(0),
        );
        let drawn = ChangeWorkload::generate(
            &mut SplitMix64::new(seed),
            features,
            &mut SplitMix64::new(seed),
        );
        assert_eq!(plain.streams.len(), drawn.streams.len());
        for (plain, drawn) in plain.streams.iter().zip(&drawn.streams) {
            // Only which streams keep history, and their events, depend on the draw apart.
            let ops = |stream: &ChangeStream| -> Vec<_> {
                stream
                    .events
                    .iter()
                    .map(|event| (event.op, event.key))
                    .collect()
            };
            assert_eq!(ops(plain), ops(drawn), "seed {seed}");
            assert_eq!(
                (plain.write, plain.captured, plain.rounds),
                (drawn.write, drawn.captured, drawn.rounds)
            );
            if !drawn.history {
                continue;
            }
            kept += 1;
            assert_eq!(drawn.write, WriteMode::Merge);
            assert!(
                drawn.events.iter().all(|event| !event.partial),
                "seed {seed}"
            );
            echoes += drawn
                .events
                .iter()
                .enumerate()
                .filter(|(index, event)| {
                    let position = i64::try_from(index + 1).unwrap();
                    event.op != ChangeOp::Truncate
                        && event.op != ChangeOp::Delete
                        && event.n != Some(position)
                })
                .count();
        }
    }
    assert!(kept > 100, "{kept} history streams");
    assert!(echoes > 50, "{echoes} updates sent again");
}

/// An insert or update of `key` setting counter `n`.
fn set(op: ChangeOp, key: i64, n: i64) -> Event {
    Event {
        op,
        key: Some(key),
        value: Some(format!("v{n}")),
        n: Some(n),
        partial: false,
    }
}

/// A delete of `key`, or a truncate where there is none.
fn gone(key: Option<i64>) -> Event {
    let op = key.map_or(ChangeOp::Truncate, |_| ChangeOp::Delete);
    Event {
        op,
        key,
        value: None,
        n: None,
        partial: false,
    }
}

/// A stream of keys 0 and 1, their snapshot's counters 0, reading `events` in its one round
/// through a filter of modulus 2 reading `reads`, written as `write`, deleting as `deletes`.
fn filtered(
    events: Vec<Event>,
    reads: Filtered,
    write: WriteMode,
    deletes: DeleteMode,
) -> ChangeStream {
    let mut stream = drawn(0, false).streams.remove(0);
    stream.keys = 2;
    stream.rounds = [events.len(), events.len()];
    stream.events = events;
    stream.captured = 0;
    stream.replay = None;
    stream.history = false;
    stream.value = Value::Loaded;
    stream.write = write;
    stream.deletes = deletes;
    stream.truncates = OnTruncate::Apply;
    stream.filter = Some(Filter {
        reads,
        modulus: 2,
        cased: false,
    });
    stream
}

#[test]
fn a_filtered_upsert_leaves_a_merge_as_a_delete_and_deletes_and_truncates_always_pass() {
    use DeleteMode::{Hard, Soft};
    use Filtered::{Counter, Key};
    type Case = (
        &'static str,
        Filtered,
        DeleteMode,
        Vec<Event>,
        Vec<(i64, Option<String>, i64, bool)>,
    );
    // A value one over the modulus fails the filter: counter 3 and key 1; counters 0, 2 and 4,
    // and key 0, pass it.
    let updates = || vec![set(ChangeOp::Update, 0, 2), set(ChangeOp::Update, 1, 3)];
    let row =
        |key: i64, value: &str, n: i64, deleted: bool| (key, Some(value.to_owned()), n, deleted);
    let cases: [Case; 5] = [
        (
            "hard",
            Counter,
            Hard,
            updates(),
            vec![row(0, "v2", 2, false)],
        ),
        (
            "soft, keeping the last values",
            Counter,
            Soft,
            updates(),
            vec![row(0, "v2", 2, false), row(1, "s1", 0, true)],
        ),
        (
            "a delete passes",
            Counter,
            Hard,
            vec![gone(Some(0))],
            vec![row(1, "s1", 0, false)],
        ),
        (
            "a truncate passes",
            Counter,
            Hard,
            vec![gone(None)],
            Vec::new(),
        ),
        (
            "a key filter, never keeping its key",
            Key,
            Hard,
            vec![set(ChangeOp::Update, 0, 2), set(ChangeOp::Update, 1, 4)],
            vec![row(0, "v2", 2, false)],
        ),
    ];
    for (case, reads, deletes, events, expected) in cases {
        let stream = filtered(events, reads, WriteMode::Merge, deletes);
        let merged: Vec<_> = stream
            .merged(0)
            .into_iter()
            .map(|(key, row)| (key, row.value, row.n, row.deleted))
            .collect();
        assert_eq!(merged, expected, "{case}");
    }
}

#[test]
fn a_filtered_upsert_closes_its_version_and_leaves_no_log_row_while_deletes_and_truncates_do() {
    let updates = vec![set(ChangeOp::Update, 0, 2), set(ChangeOp::Update, 1, 3)];
    let mut stream = filtered(
        updates,
        Filtered::Counter,
        WriteMode::Merge,
        DeleteMode::Hard,
    );
    stream.history = true;
    let current: Vec<i64> = stream
        .history(0)
        .iter()
        .filter(|version| version.5)
        .map(|version| version.0)
        .collect();
    assert_eq!(current, [0]);
    let events = vec![
        set(ChangeOp::Update, 0, 2),
        set(ChangeOp::Update, 1, 3),
        gone(Some(1)),
        gone(None),
    ];
    let stream = filtered(
        events,
        Filtered::Counter,
        WriteMode::Append,
        DeleteMode::Hard,
    );
    let mut logged: Vec<(i8, Option<i64>)> = stream
        .log(0)
        .into_iter()
        .map(|row| (row.0, row.3))
        .collect();
    logged.sort_unstable();
    let (insert, update) = (ChangeOp::Insert.code(), ChangeOp::Update.code());
    let (delete, truncate) = (ChangeOp::Delete.code(), ChangeOp::Truncate.code());
    let mut expected = vec![
        (insert, Some(0)),
        (insert, Some(0)),
        (update, Some(2)),
        (delete, None),
        (truncate, None),
    ];
    expected.sort_unstable();
    assert_eq!(logged, expected);
    assert_eq!(stream.filtered_in(0), 1);
}

#[test]
fn change_streams_filter_and_list_their_columns_only_where_drawn_to_and_never_where_deletes_are_ignored()
 {
    let (mut filtering, mut unlisted, mut on_key, mut cased) = (0, 0, 0, 0);
    for seed in 0..500 {
        let features = Features {
            transform: true,
            ..Features::ALL
        };
        let transforming = ChangeWorkload::generate(
            &mut SplitMix64::new(seed),
            features,
            &mut SplitMix64::new(!seed),
        );
        let plain = ChangeWorkload::generate(
            &mut SplitMix64::new(seed),
            Features::ALL,
            &mut SplitMix64::new(!seed),
        );
        for (drawn, plain) in transforming.streams.iter().zip(&plain.streams) {
            let ignored = drawn.write == WriteMode::Merge && drawn.deletes == DeleteMode::Ignore;
            assert!(drawn.filter.is_none() || !ignored, "seed {seed}");
            filtering += usize::from(drawn.filter.is_some());
            if let Some(filter) = drawn.filter {
                // A merge whose updates leave a column unchanged filters on its key alone.
                let partial = drawn.events.iter().any(|event| event.partial);
                let keyed = drawn.write == WriteMode::Merge && partial;
                assert_eq!(filter.reads == Filtered::Key, keyed, "seed {seed}");
                on_key += usize::from(keyed);
                cased += usize::from(filter.cased);
            }
            unlisted += usize::from(drawn.value == Value::Unlisted);
            assert!(
                drawn.value != Value::Unlisted || !drawn.history,
                "seed {seed}"
            );
            // Nothing else a transforming seed draws differs.
            let mut alike = drawn.clone();
            alike.filter = None;
            if alike.value == Value::Unlisted {
                alike.value = Value::Loaded;
            }
            assert_eq!(&alike, plain, "seed {seed}");
        }
    }
    assert!(filtering > 100, "{filtering} streams filter");
    assert!(on_key > 20, "{on_key} streams filter on their key");
    assert!(cased > 50, "{cased} streams answer with null");
    assert!(unlisted > 50, "{unlisted} streams list their columns");
}
