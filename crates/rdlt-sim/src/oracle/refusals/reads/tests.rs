use rdlt_connector::{Checkpointing, ReadMode};
use rdlt_engine::{Code, codes};

use super::{Refusal, refusal};
use crate::rng::SplitMix64;
use crate::swarm::Features;
use crate::workload::{Relaxed, SimStream, Workload};

/// A drawn stream whose first partition holds three rows or more in the first phase, and more in
/// the next.
fn growing() -> SimStream {
    (0..1_000)
        .flat_map(|seed| Workload::generate(&mut SplitMix64::new(seed), Features::ALL).streams)
        .find(|stream| {
            let [first, second] = stream.partitions[0];
            stream.reads[1] == ReadMode::Incremental && first > 2 && second > first
        })
        .expect("some drawn stream grows")
}

/// The refusal coded `code`, which every run meets where `must`.
fn refused(code: Code, must: bool) -> Refusal {
    Refusal { code, must }
}

#[test]
fn only_an_incremental_phase_of_endless_partitions_whose_reads_end_on_rows_is_refused() {
    use ReadMode::{Full, Incremental};
    let cursor = || Some(refused(codes::CURSOR_MISSING, true));
    let checkpoint = || Some(refused(codes::CHECKPOINT_MISSING, true));
    let left = Relaxed::default();
    let reset = Relaxed {
        reset: Some(1),
        ..left
    };
    let mended = Relaxed {
        mended: Some(0),
        ..left
    };
    let (changed, kept) = ([Full, Incremental], [Incremental; 2]);
    // How each phase reads it, whether its partitions never end, whether its reads end on a
    // checkpoint, how an operator left it, and what each phase is refused with, of a stream that
    // checkpoints on demand, so that its incremental reads of them checkpoint nowhere.
    let cases = [
        (changed, true, false, left, [None, cursor()]),
        (changed, true, false, reset, [None, checkpoint()]),
        (changed, true, true, left, [None, None]),
        (changed, false, false, left, [None, None]),
        (kept, true, false, left, [checkpoint(), checkpoint()]),
        (kept, true, false, mended, [None, None]),
        (kept, false, false, left, [None, None]),
        ([Incremental, Full], true, false, left, [checkpoint(), None]),
        ([Full, Full], true, false, left, [None, None]),
    ];
    for (reads, unbounded, final_checkpoint, relaxed, expected) in cases {
        let mut stream = growing();
        stream.checkpointing = Checkpointing::OnDemand;
        (stream.reads, stream.unbounded, stream.final_checkpoint) =
            (reads, unbounded, final_checkpoint);
        for (phase, expected) in expected.into_iter().enumerate() {
            assert_eq!(
                refusal(&stream, relaxed, phase, None),
                expected,
                "{reads:?} unbounded {unbounded} final {final_checkpoint} {relaxed:?} phase {phase}"
            );
        }
    }
}

#[test]
fn a_natural_read_is_refused_only_where_it_pushes_one_checkpoint_group_or_less() {
    let mut stream = growing();
    (stream.reads, stream.unbounded, stream.final_checkpoint) =
        ([ReadMode::Incremental; 2], true, false);
    (stream.checkpointing, stream.batch_rows) = (Checkpointing::Natural, 1);
    stream.partitions.truncate(1);
    let rows = u64::try_from(stream.rows(0, 0).len()).unwrap();
    // A first read pushing more than a group seals all but its last rows, whose next read may be
    // refused; one pushing a group or less seals none of them.
    stream.checkpoint_every = rows - 1;
    let may = Some(refused(codes::CHECKPOINT_MISSING, false));
    assert_eq!(refusal(&stream, Relaxed::default(), 0, None), may);
    stream.checkpoint_every = rows;
    let must = Some(refused(codes::CHECKPOINT_MISSING, true));
    assert_eq!(refusal(&stream, Relaxed::default(), 0, None), must);
    // Read from where its last commit left it, a read of more than a group meets none, and one of
    // a group or less meets it every time.
    stream.checkpoint_every = rows - 1;
    let left = Relaxed::default();
    assert_eq!(refusal(&stream, left, 0, Some(&[0])), None);
    assert_eq!(refusal(&stream, left, 0, Some(&[1])), must);
}
