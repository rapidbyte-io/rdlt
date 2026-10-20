#![expect(
    clippy::disallowed_methods,
    reason = "a test bounds its load by tokio's paused clock, which no Env drives here"
)]

//! A segment its partition keeps open across many commits, while others commit and settle: the
//! log never holds more on disk than its bound, a relief carries the open frames out of the
//! chunks the settled ones share once room runs short, and the load ends with each frame once.
//!
//! The open segment holds at most about half the log; loads at three quarters of it are the room
//! tests'.

use std::collections::BTreeSet;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Duration;

use rdlt_connector::{CommitSeq, SegmentId};

use super::room::{Load, stored};
use super::{load, logged, pipeline, receipt, sealed_at};
use crate::budget::MemoryBudget;
use crate::report::Tally;
use crate::table::testing::view;
use crate::wal::load::{LoadLog, Owner};
use crate::wal::memory::MemoryWal;
use crate::wal::scan::scan;
use crate::wal::store::WalStore;

/// Commits the other partitions make while the open segment stays open.
const COMMITS: u64 = 120;

/// Commits between two batches of the open segment.
const EVERY: u64 = 6;

/// Longer than the load takes: past it, the load waits for room nothing frees.
const FOR_EVER: Duration = Duration::from_hours(4);

/// Seals `segment` and commits it as `seq` of `load`, as a coordinator does.
async fn sealed(
    log: &LoadLog,
    budget: &MemoryBudget,
    load: &Load,
    (segment, seq): (u64, CommitSeq),
) -> Result<(), crate::Error> {
    log.checkpointed();
    let committing = log.committing();
    log.took(1);
    let commit = load.commit(&[segment], seq);
    log.commit(budget, vec![sealed_at(segment)], Vec::new(), &commit, 0)
        .await?;
    drop(committing);
    Ok(())
}

/// Logs segment 0's batches a few commits apart, never sealing it until the end, while each
/// other segment logs one, is committed, and has its receipt at once, `bounded` checking the log
/// after every step: the batches of segment 0 logged.
async fn open_beside_commits(
    log: LoadLog,
    budget: &MemoryBudget,
    shape: &Load,
    bounded: impl Fn(&LoadLog),
) -> Result<u64, crate::Error> {
    let orders = view("orders");
    let (mut seq, mut from, mut open) = (CommitSeq::FIRST, 0, 0_u64);
    for commit in 0..COMMITS {
        if commit % EVERY == 0 {
            logged(&log, budget, 0, &orders, SegmentId(0), &shape.batch(from)).await?;
            (from, open) = (from + 1_000, open + 1);
            bounded(&log);
        }
        let segment = commit + 1;
        logged(
            &log,
            budget,
            0,
            &orders,
            SegmentId(segment),
            &shape.batch(from),
        )
        .await?;
        from += 1_000;
        bounded(&log);
        sealed(&log, budget, shape, (segment, seq)).await?;
        log.committed(&receipt(seq)).await?;
        bounded(&log);
        seq = seq.next();
    }
    // Segment 0 is sealed and committed at last; the destination never answers.
    sealed(&log, budget, shape, (0, seq)).await?;
    bounded(&log);
    Ok(open)
}

#[tokio::test(start_paused = true)]
async fn a_segment_open_across_many_commits_keeps_the_log_within_its_bound() {
    let shape = Load::new(&[1]);
    let limit = 40 * shape.frame();
    let store = Arc::new(MemoryWal::default());
    store.open(&pipeline(), load());
    let owner = Owner {
        pipeline: pipeline(),
        load: load(),
        epoch: rdlt_connector::Epoch(1),
        opened: None,
        origin: load(),
    };
    let tally = Arc::new(Tally::default());
    let (log, task) = LoadLog::start(
        Arc::clone(&store) as Arc<dyn WalStore>,
        owner,
        NonZeroU64::new(limit).expect("a limit"),
        (None, Arc::clone(&tally)),
    );
    let budget = crate::budget::budget(64 << 20);
    let bounded = |log: &LoadLog| {
        assert!(log.held() <= limit, "{} counted of {limit}", log.held());
        let stored = stored(&store);
        assert!(stored <= limit, "{stored} stored of {limit}");
        assert!(
            stored <= log.held(),
            "{stored} stored, {} counted",
            log.held()
        );
    };
    let written = open_beside_commits(log, &budget, &shape, bounded);
    let (ended, open) = tokio::time::timeout(FOR_EVER, async { tokio::join!(task, written) })
        .await
        .expect("the load ends");
    ended.expect("the writer ends");
    let open = open.expect("the load fits its log");
    let counters = tally.counters().log;
    assert!(counters.reliefs > 0, "no relief came");
    assert!(counters.carried > 0, "no relief carried");
    // A replay repeats the open segment's commit alone, with each of its batches once.
    let scanned = scan(store.as_ref(), &pipeline(), load(), 1 << 28)
        .await
        .expect("the log reads");
    let pending: Vec<_> = scanned.commits.iter().collect();
    assert_eq!(pending.len(), 1);
    assert!(pending[0].meta.segments.contains(SegmentId(0)));
    let ordinals: BTreeSet<u64> = scanned.batches[&SegmentId(0)]
        .iter()
        .map(|located| located.ordinal)
        .collect();
    assert_eq!(scanned.batches[&SegmentId(0)].len(), ordinals.len());
    assert_eq!(u64::try_from(ordinals.len()).expect("few"), open);
}
