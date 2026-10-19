//! Proofs that a commit's horizon is the earliest commit a replay may repeat and only moves
//! forward, for every input within each proof's bound.

use std::time::{Duration, UNIX_EPOCH};

use rdlt_connector::{CommitSeq, Horizon, LoadId};

use super::earliest;

/// The most loads a listing finds beside the load's own.
const LOADS: usize = 3;

/// Sequence numbers a load's commits are drawn from, the first up to this one.
const SEQS: u8 = 4;

/// Any load whose id's time is one of the first 256 milliseconds: ids order by time first, so
/// these order, tie and differ as any ids do.
fn any_load() -> LoadId {
    let millis: u8 = kani::any();
    LoadId::from_parts(UNIX_EPOCH + Duration::from_millis(u64::from(millis)), 0)
}

/// Any of the first `SEQS` commits of a load.
fn any_seq() -> CommitSeq {
    let after = kani::any_where(|after: &u8| *after < SEQS);
    (0..after).fold(CommitSeq::FIRST, |seq, _| seq.next())
}

/// The first commit of `load`, which a replay of its log may repeat.
fn first(load: LoadId) -> Horizon {
    Horizon {
        load_id: load,
        commit_seq: CommitSeq::FIRST,
    }
}

/// The horizon is the load's own oldest commit or the first commit of another load listed,
/// whichever is earlier, and no later than any of them.
///
/// Bound: up to `LOADS` loads listed, any of them the load itself, each of any id, and the
/// load's own oldest commit among its first `SEQS`.
#[kani::proof]
#[kani::unwind(17)]
fn the_horizon_is_the_earliest_commit_any_listed_log_may_repeat() {
    let (load, oldest) = (any_load(), any_seq());
    let listed: [LoadId; LOADS] = [any_load(), any_load(), any_load()];
    let loads = &listed[..kani::any_where(|count: &usize| *count <= LOADS)];
    let horizon = earliest(load, oldest, loads);
    let own = Horizon {
        load_id: load,
        commit_seq: oldest,
    };
    assert!(horizon <= own);
    for other in loads.iter().filter(|other| **other != load) {
        assert!(horizon <= first(*other));
    }
    assert!(
        horizon == own
            || loads
                .iter()
                .any(|other| *other != load && horizon == first(*other))
    );
}

/// A later commit's horizon is never earlier than an earlier one's: the load's oldest commit
/// only grows, and a log listed later was listed before or is a newer load's.
///
/// Bound: two listings of up to `LOADS` loads, each of any id, the later holding only loads the
/// earlier did or loads newer than the load itself, and two oldest commits among the first
/// `SEQS`, the later no earlier.
#[kani::proof]
#[kani::unwind(17)]
fn the_horizon_only_moves_forward() {
    let load = any_load();
    let (oldest, later) = (any_seq(), any_seq());
    kani::assume(oldest <= later);
    let (mut before, mut after) = ([load; LOADS], [load; LOADS]);
    let (mut listed_before, mut listed_after) = (0, 0);
    for other in [any_load(), any_load(), any_load()] {
        let (listed, still) = (kani::any::<bool>(), kani::any::<bool>());
        kani::assume(!still || listed || other > load);
        if listed {
            before[listed_before] = other;
            listed_before += 1;
        }
        if still {
            after[listed_after] = other;
            listed_after += 1;
        }
    }
    assert!(
        earliest(load, later, &after[..listed_after])
            >= earliest(load, oldest, &before[..listed_before])
    );
}
