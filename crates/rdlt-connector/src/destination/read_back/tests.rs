use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Int64Array, RecordBatch};

use super::{PublishedReader, PublishedRows};
use crate::destination::TableRef;
use crate::error::{ConnectorError, ConnectorErrorKind, Result};
use crate::id::{SchemaVersion, TablePath};
use crate::spec::BoxFuture;

fn table() -> TableRef {
    TableRef {
        path: TablePath::new(["rows"]).expect("a valid path"),
        name: "rows".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    }
}

fn batch(id: i64) -> RecordBatch {
    RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![id])) as _)])
        .expect("a valid batch")
}

fn id(batch: &RecordBatch) -> i64 {
    batch.column(0).as_primitive::<Int64Type>().value(0)
}

/// Reads back `batches` batches, counting each sent, then fails where `fails`.
struct Counted {
    batches: i64,
    fails: bool,
    sent: Arc<AtomicUsize>,
}

impl PublishedReader for Counted {
    fn published<'a>(&'a self, _: &'a TableRef, rows: PublishedRows) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            for id in 0..self.batches {
                rows.send(batch(id)).await?;
                self.sent.fetch_add(1, Ordering::SeqCst);
            }
            if self.fails {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::Transient,
                    "the table went away",
                ));
            }
            Ok(())
        })
    }
}

fn counted(batches: i64, fails: bool) -> (Counted, Arc<AtomicUsize>) {
    let sent = Arc::new(AtomicUsize::new(0));
    let reader = Counted {
        batches,
        fails,
        sent: Arc::clone(&sent),
    };
    (reader, sent)
}

#[tokio::test]
async fn gathered_rows_are_every_batch_in_the_order_sent() {
    let (reader, sent) = counted(100, false);
    let gathered = PublishedRows::gather(&reader, &table()).await;
    let ids: Vec<i64> = gathered
        .expect("the table reads back")
        .iter()
        .map(id)
        .collect();
    assert_eq!(ids, (0..100).collect::<Vec<_>>());
    assert_eq!(sent.load(Ordering::SeqCst), 100);
}

#[tokio::test]
async fn a_read_back_that_fails_fails_the_gathering() {
    let (reader, _) = counted(3, true);
    let refused = PublishedRows::gather(&reader, &table()).await;
    let refused = refused.expect_err("the read-back failed");
    assert_eq!(refused.kind(), ConnectorErrorKind::Transient);
}

#[tokio::test(start_paused = true)]
async fn a_batch_is_sent_once_the_batch_before_was_taken() {
    let (reader, sent) = counted(1000, false);
    let (rows, mut batches) = PublishedRows::channel();
    let table = table();
    let reading = reader.published(&table, rows);
    tokio::pin!(reading);
    // Nothing takes a batch: the read-back sends the batch the channel holds, and waits.
    let waited = tokio::time::timeout(std::time::Duration::from_secs(60), &mut reading).await;
    assert!(waited.is_err(), "the read-back ran ahead of its reader");
    assert_eq!(sent.load(Ordering::SeqCst), 1);
    // Each batch taken lets the next be sent, and no more.
    for taken in 0..10 {
        let taking = async { batches.recv().await.map(|batch| id(&batch)) };
        let (_, took) = tokio::join!(
            tokio::time::timeout(std::time::Duration::from_secs(1), &mut reading),
            taking
        );
        assert_eq!(took, Some(taken));
        assert_eq!(
            sent.load(Ordering::SeqCst),
            usize::try_from(taken).expect("small") + 2
        );
    }
    // A reader that leaves ends the read-back, as stopped.
    drop(batches);
    let ended = reading.await.expect_err("the reader left");
    assert_eq!(ended.kind(), ConnectorErrorKind::Stopped);
    assert_eq!(sent.load(Ordering::SeqCst), 11);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_blocking_thread_sends_as_a_task_does_and_ends_once_its_reader_left() {
    let (rows, mut batches) = PublishedRows::channel();
    let sending = tokio::task::spawn_blocking(move || {
        let mut sent = 0;
        loop {
            match rows.blocking_send(batch(sent)) {
                Ok(()) => sent += 1,
                Err(error) => return (sent, error.kind()),
            }
        }
    });
    for expected in 0..5 {
        assert_eq!(batches.recv().await.as_ref().map(id), Some(expected));
    }
    drop(batches);
    let (sent, ended) = sending.await.expect("the thread ends");
    assert!((5..=6).contains(&sent), "{sent} batches were sent");
    assert_eq!(ended, ConnectorErrorKind::Stopped);
}
