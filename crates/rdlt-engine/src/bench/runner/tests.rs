use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};

use super::{Runner, stream};
use crate::Cores;
use crate::bench::{Replayed, null_sink, replay};

fn batches() -> Replayed {
    let batches = (0..6)
        .map(|batch| {
            let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(batch * 10..batch * 10 + 10));
            RecordBatch::try_from_iter([("id", ids)]).unwrap()
        })
        .collect();
    Replayed::Batches(batches)
}

#[test]
fn a_runner_commits_every_so_many_rows_or_once_at_the_end() {
    let cores = Cores::new(NonZeroUsize::new(2).unwrap());
    for (commit, least) in [(None, 1), (NonZeroU64::new(20), 3)] {
        let runner = Runner::try_new(cores, "runner", stream(), commit).unwrap();
        let (source, destination) = runner.block_on(async {
            (
                replay("runner", Arc::new(batches())).await,
                null_sink().await,
            )
        });
        let report = runner.load(source, destination);
        assert_eq!(report.rows, 60);
        assert!(report.commits >= least, "{commit:?}: {}", report.commits);
        if commit.is_none() {
            assert_eq!(report.commits, 1);
        }
    }
}

#[test]
fn a_runner_s_runtime_has_the_workers_its_cores_suggest() {
    for count in [1, 3, 8] {
        let cores = Cores::new(NonZeroUsize::new(count).unwrap());
        let runner = Runner::try_new(cores, "runner", stream(), None).unwrap();
        let workers =
            runner.block_on(async { tokio::runtime::Handle::current().metrics().num_workers() });
        assert_eq!(workers, cores.runtime_workers().get());
    }
}
