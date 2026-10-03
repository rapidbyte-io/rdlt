use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use parking_lot::Mutex;
use rdlt_connector::{
    BoxFuture, DestinationWriter, GenerationId, Result, SchemaVersion, SegmentId, TablePath,
    TableRef, WriteStats,
};

use super::Staged;
use crate::budget::MemoryBudget;
use crate::error::Error;

type Log = Arc<Mutex<Vec<String>>>;

/// A writer that logs what it is asked to do, named after its table and version.
struct Recording {
    name: String,
    log: Log,
}

impl DestinationWriter for Recording {
    fn write(&mut self, _segment: SegmentId, _batch: RecordBatch) -> BoxFuture<'_, Result<()>> {
        self.log.lock().push(format!("write {}", self.name));
        Box::pin(async { Ok(()) })
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<WriteStats>> {
        self.log.lock().push(format!("flush {}", self.name));
        Box::pin(async { Ok(WriteStats::default()) })
    }
}

impl Drop for Recording {
    fn drop(&mut self) {
        self.log.lock().push(format!("close {}", self.name));
    }
}

fn table(name: &str, version: u32, generation: Option<u64>) -> TableRef {
    TableRef {
        path: TablePath::new([name]).expect("a valid path"),
        name: name.into(),
        version: SchemaVersion(version),
        generation: generation.map(GenerationId),
        merge: None,
    }
}

fn rows() -> RecordBatch {
    RecordBatch::try_from_iter([(
        "id",
        Arc::new(Int64Array::from(vec![1])) as Arc<dyn arrow_array::Array>,
    )])
    .expect("a valid batch")
}

/// Writes one batch of each of `tables` in order through writers a [`Staged`] of `limit` holds,
/// then flushes them; what the writers were asked to do.
async fn staged(limit: usize, tables: &[TableRef]) -> Vec<String> {
    let log = staged_within(limit, &MemoryBudget::new(1 << 20), 100, tables).await;
    log.into_iter()
        .filter(|line| !line.starts_with("reserved"))
        .collect()
}

/// As [`staged`], each batch holding `held` bytes of `budget`.
async fn staged_within(
    limit: usize,
    budget: &MemoryBudget,
    held: u64,
    tables: &[TableRef],
) -> Vec<String> {
    let log = Log::default();
    let mut staged = Staged::new(NonZeroUsize::new(limit).expect("a limit"), budget.clone());
    for table in tables {
        let name = match table.generation {
            Some(generation) => format!("{}@{}v{}", table.name, generation.0, table.version.0),
            None => format!("{}v{}", table.name, table.version.0),
        };
        let held = staged.reserve(held).await.expect("the budget has room");
        log.lock().push(format!("reserved {}", budget.reserved()));
        let log = Arc::clone(&log);
        let open = || async move {
            log.lock().push(format!("open {name}"));
            Ok::<_, Error>(Box::new(Recording { name, log }) as Box<dyn DestinationWriter>)
        };
        staged
            .write(table, open, (SegmentId(1), rows()), held)
            .await
            .expect("the batch is staged");
    }
    staged.flush().await.expect("the writers flush");
    log.lock().clone()
}

#[tokio::test]
async fn a_newer_version_closes_its_tables_older_writers_once_flushed() {
    let tables = [
        table("a", 1, None),
        table("b", 1, None),
        table("a", 2, None),
        table("a", 2, Some(7)),
        table("b", 1, None),
    ];
    let log = staged(8, &tables).await;
    let expected = [
        "open av1",
        "write av1",
        "open bv1",
        "write bv1",
        // Another table's writer, and another generation's, stay open.
        "flush av1",
        "close av1",
        "open av2",
        "write av2",
        "open a@7v2",
        "write a@7v2",
        "write bv1",
        "flush av2",
        "close av2",
        "flush a@7v2",
        "close a@7v2",
        "flush bv1",
        "close bv1",
    ];
    assert_eq!(log, expected);
}

#[tokio::test]
async fn beyond_its_limit_the_writer_written_longest_ago_closes_once_flushed() {
    let tables = [
        table("a", 1, None),
        table("b", 1, None),
        table("a", 1, None),
        table("c", 1, None),
        table("b", 1, None),
    ];
    let log = staged(2, &tables).await;
    let expected = [
        "open av1",
        "write av1",
        "open bv1",
        "write bv1",
        "write av1",
        "flush bv1",
        "close bv1",
        "open cv1",
        "write cv1",
        "flush av1",
        "close av1",
        "open bv1",
        "write bv1",
        "flush bv1",
        "close bv1",
        "flush cv1",
        "close cv1",
    ];
    assert_eq!(log, expected);
}

#[tokio::test]
async fn what_staged_batches_hold_stays_charged_until_their_writers_flush_and_never_passes_it() {
    // Of 64 MiB the data's share is 37 MiB: two batches of a request's 16 MiB fit, and the third
    // makes every writer flush first.
    let budget = MemoryBudget::new(64 << 20);
    let tables = [
        table("a", 1, None),
        table("b", 1, None),
        table("c", 1, None),
    ];
    let log = staged_within(8, &budget, 16 << 20, &tables).await;
    let reserved = |mib: u64| format!("reserved {}", mib << 20);
    let expected = [
        reserved(16),
        "open av1".to_owned(),
        "write av1".to_owned(),
        reserved(32),
        "open bv1".to_owned(),
        "write bv1".to_owned(),
        "flush av1".to_owned(),
        "close av1".to_owned(),
        "flush bv1".to_owned(),
        "close bv1".to_owned(),
        reserved(16),
        "open cv1".to_owned(),
        "write cv1".to_owned(),
        "flush cv1".to_owned(),
        "close cv1".to_owned(),
    ];
    assert_eq!(log, expected);
    assert_eq!(budget.reserved(), 0, "every batch released once flushed");
    assert_eq!(budget.peak(), 32 << 20);
}

#[tokio::test]
async fn a_logged_batch_beyond_what_a_request_may_take_is_refused() {
    let budget = MemoryBudget::new(1 << 20);
    let mut staged = Staged::new(NonZeroUsize::MIN, budget.clone());
    let error = staged
        .reserve((1 << 20) / 4 + 1)
        .await
        .expect_err("beyond a request");
    assert_eq!(error.code(), Some("replay_exceeds_budget"));
    drop(
        staged
            .reserve((1 << 20) / 4)
            .await
            .expect("a request's worth"),
    );
}
