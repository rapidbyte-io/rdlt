use rdlt_connector::ReadMode;
use rdlt_engine::{Code, codes};

use super::refusal;
use crate::rng::SplitMix64;
use crate::swarm::Features;
use crate::workload::{Relaxed, SimStream, Workload};

/// A drawn stream whose first partition holds rows in the first phase, and more in the next.
fn growing() -> SimStream {
    (0..1_000)
        .flat_map(|seed| Workload::generate(&mut SplitMix64::new(seed), Features::ALL).streams)
        .find(|stream| {
            let [first, second] = stream.partitions[0];
            stream.reads[1] == ReadMode::Incremental && first > 0 && second > first
        })
        .expect("some drawn stream grows")
}

#[test]
fn only_an_incremental_phase_of_endless_partitions_whose_reads_end_on_rows_is_refused() {
    use ReadMode::{Full, Incremental};
    const CURSOR: Option<Code> = Some(codes::CURSOR_MISSING);
    const CHECKPOINT: Option<Code> = Some(codes::CHECKPOINT_MISSING);
    // How each phase reads it, whether its partitions never end, whether its reads end on a
    // checkpoint, whether an operator mended its source, and what each phase is refused with.
    #[rustfmt::skip]
    let cases = [
        ([Full, Incremental],        true,  false, false, [None, CURSOR]),
        ([Full, Incremental],        true,  true,  false, [None, None]),
        ([Full, Incremental],        false, false, false, [None, None]),
        ([Incremental, Incremental], true,  false, false, [CHECKPOINT, CHECKPOINT]),
        ([Incremental, Incremental], true,  false, true,  [None, None]),
        ([Incremental, Incremental], false, false, false, [None, None]),
        ([Incremental, Full],        true,  false, false, [CHECKPOINT, None]),
        ([Full, Full],               true,  false, false, [None, None]),
    ];
    for (reads, unbounded, final_checkpoint, mended, refused) in cases {
        let mut stream = growing();
        (stream.reads, stream.unbounded, stream.final_checkpoint) =
            (reads, unbounded, final_checkpoint);
        let relaxed = Relaxed {
            mended,
            ..Relaxed::default()
        };
        for (phase, expected) in refused.into_iter().enumerate() {
            assert_eq!(
                refusal(&stream, relaxed, phase),
                expected,
                "{reads:?} unbounded {unbounded} final {final_checkpoint} mended {mended} phase \
                 {phase}"
            );
        }
    }
}
