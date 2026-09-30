use super::ChangeWorkload;
use crate::rng::SplitMix64;
use crate::swarm::Features;

/// The workload `seed` draws, with pipelines keeping write-ahead logs or not.
fn drawn(seed: u64, wal: bool) -> ChangeWorkload {
    let features = Features {
        wal,
        ..Features::ALL
    };
    ChangeWorkload::generate(
        &mut SplitMix64::new(seed),
        features,
        &mut SplitMix64::new(!seed),
    )
}

#[test]
fn every_other_change_stream_forgets_only_where_pipelines_keep_logs() {
    let mut forgetting = 0;
    for seed in 0..500 {
        let unlogged = drawn(seed, false);
        let logged = drawn(seed, true);
        assert!(unlogged.streams.iter().all(|stream| stream.replayable));
        assert_eq!(logged.streams.len(), unlogged.streams.len());
        for (index, (logged, unlogged)) in logged.streams.iter().zip(&unlogged.streams).enumerate()
        {
            assert_eq!(logged.replayable, index % 2 == 1, "seed {seed}");
            // A source that forgets sends nothing again; every other draw falls alike.
            let mut alike = logged.clone();
            alike.replayable = true;
            if logged.replayable {
                assert_eq!(&alike, unlogged, "seed {seed}");
            } else {
                forgetting += 1;
                assert_eq!(logged.replay, None, "seed {seed}");
                alike.replay.clone_from(&unlogged.replay);
                assert_eq!(&alike, unlogged, "seed {seed}");
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
            let ops = |stream: &super::ChangeStream| -> Vec<_> {
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
            assert_eq!(drawn.write, rdlt_engine::WriteMode::Merge);
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
                    event.op != rdlt_connector::ChangeOp::Truncate
                        && event.op != rdlt_connector::ChangeOp::Delete
                        && event.n != Some(position)
                })
                .count();
        }
    }
    assert!(kept > 100, "{kept} history streams");
    assert!(echoes > 50, "{echoes} updates sent again");
}
