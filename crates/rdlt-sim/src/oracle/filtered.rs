//! What the runs' reports count of a stream's filter, against the model: the rows it dropped and
//! the inserts and updates it turned into deletes, within the least and most they may be.

use rdlt_engine::{FilteredRows, Report, WriteMode};

use crate::changes::ChangeStream;
use crate::seed::Seed;
use crate::workload::{Row, SimStream};

/// What `reports` count of the filter of stream `name`.
fn counted(reports: &[Report], name: &str) -> FilteredRows {
    let mut counted = FilteredRows::default();
    for counts in reports.iter().filter_map(|report| report.streams.get(name)) {
        counted.add(counts.filtered);
    }
    counted
}

/// Whether `counted` is `expected`, where the reports are `complete`, and else never more; where
/// changes are sent `again`, each is counted as often as it is sent, so only the least holds.
fn within(counted: FilteredRows, expected: FilteredRows, complete: bool, again: bool) -> bool {
    let fits = |counted: u64, expected: u64| {
        (again || counted <= expected) && (!complete || counted >= expected)
    };
    fits(counted.dropped, expected.dropped) && fits(counted.deleted, expected.deleted)
}

/// Checks what `reports` count of `stream`'s filter in `phase`, `read` being the rows its reads
/// committed, against the rows the model's filter drops; it turns none into deletes.
pub(super) fn check_rows(
    stream: &SimStream,
    read: &[Row],
    reports: &[Report],
    complete: bool,
    (phase, seed): (usize, Seed),
) {
    let dropped = read.iter().filter(|row| !stream.reshaping.keeps(row));
    let expected = FilteredRows {
        dropped: u64::try_from(dropped.count()).unwrap_or(u64::MAX),
        deleted: 0,
    };
    let counted = counted(reports, &stream.name);
    assert!(
        within(counted, expected, complete, false),
        "seed {seed}: stream {} in phase {phase} counted {counted:?} filtered; the model counts \
         {expected:?}, which reports that are {} must {}",
        stream.name,
        if complete { "complete" } else { "incomplete" },
        if complete { "equal" } else { "not exceed" },
    );
}

/// Checks what `reports` count of `stream`'s filter in `round` against the changes first read
/// then that the model's filter does not keep: dropped from a log, deleted from a merge.
pub(super) fn check_changes(
    stream: &ChangeStream,
    round: usize,
    reports: &[Report],
    complete: bool,
    seed: Seed,
) {
    let failing = stream.filtered_in(round);
    let expected = if stream.write == WriteMode::Append {
        FilteredRows {
            dropped: failing,
            deleted: 0,
        }
    } else {
        FilteredRows {
            dropped: 0,
            deleted: failing,
        }
    };
    let counted = counted(reports, &stream.name);
    let again = stream.replay.is_some();
    assert!(
        within(counted, expected, complete, again),
        "seed {seed}: round {round}: stream {} counted {counted:?} filtered; the model counts \
         {expected:?}, which reports that are {} must {}",
        stream.name,
        if complete { "complete" } else { "incomplete" },
        if again { "reach" } else { "not exceed" },
    );
}
