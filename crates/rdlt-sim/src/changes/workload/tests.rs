use super::ChangeWorkload;
use crate::rng::SplitMix64;
use crate::swarm::Features;

/// The workload `seed` draws, with pipelines keeping write-ahead logs or not.
fn drawn(seed: u64, wal: bool) -> ChangeWorkload {
    let features = Features {
        wal,
        ..Features::ALL
    };
    ChangeWorkload::generate(&mut SplitMix64::new(seed), features)
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
