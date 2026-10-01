//! A destination that takes only as much as a kill clause loads of a source: beyond it, the
//! load fails, and the clause is left unobserved.

#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use arrow_array::RecordBatch;
use rdlt_connector::{
    BoxFuture, Capabilities, CommitMeta, ConnectorError, Destination, DestinationSession,
    DestinationWriter, OpenContext, OpenedSession, Receipt, Result, SegmentId, TableChange,
    TableRef, WriteStats,
};

use crate::limits::{LOADED_BYTES, LOADED_ROWS};

/// The code of the error a write beyond what a load takes fails with.
const BEYOND: &str = "certify_load";

/// `destination`, taking at most [`LOADED_ROWS`] rows and [`LOADED_BYTES`] bytes, all its
/// writes together.
pub(crate) struct Bounded {
    destination: Arc<dyn Destination>,
    load: Arc<Load>,
}

/// What a load may still write, and whether it wrote beyond it.
struct Load {
    rows: AtomicUsize,
    bytes: AtomicUsize,
    beyond: AtomicBool,
}

impl Bounded {
    pub(crate) fn new(destination: Arc<dyn Destination>) -> Self {
        let load = Load {
            rows: AtomicUsize::new(LOADED_ROWS),
            bytes: AtomicUsize::new(LOADED_BYTES),
            beyond: AtomicBool::new(false),
        };
        Self {
            destination,
            load: Arc::new(load),
        }
    }

    /// What tells whether the load wrote beyond what it takes.
    pub(crate) fn witness(&self) -> Beyond {
        Beyond(Arc::clone(&self.load))
    }
}

/// Whether a load wrote beyond what a kill clause takes.
pub(crate) struct Beyond(Arc<Load>);

impl Beyond {
    /// Why the clause is not observed, when the load wrote beyond what it takes.
    pub(crate) fn unobserved(&self) -> Option<String> {
        self.0.beyond.load(Ordering::SeqCst).then(|| {
            format!(
                "the source holds more than the {LOADED_ROWS} rows and {LOADED_BYTES} bytes a \
                 kill clause loads: certify it with less data"
            )
        })
    }
}

impl Load {
    /// Charges `batch`; an error no load retries once it is beyond what the load takes.
    fn charge(&self, batch: &RecordBatch) -> Result<()> {
        let rows = spend(&self.rows, batch.num_rows());
        let bytes = spend(&self.bytes, batch.get_array_memory_size());
        if rows && bytes {
            return Ok(());
        }
        self.beyond.store(true, Ordering::SeqCst);
        let message = "the load writes more than a kill clause takes";
        Err(ConnectorError::data(message).with_code(BEYOND))
    }
}

/// Takes `spent` from what `left` holds; whether that much was left, none being left after.
fn spend(left: &AtomicUsize, spent: usize) -> bool {
    let took = left.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
        Some(left.saturating_sub(spent))
    });
    took.is_ok_and(|left| left >= spent)
}

impl Destination for Bounded {
    fn capabilities(&self) -> &Capabilities {
        self.destination.capabilities()
    }

    fn check(&self) -> BoxFuture<'_, Result<()>> {
        self.destination.check()
    }

    fn open<'a>(&'a self, context: &'a OpenContext) -> BoxFuture<'a, Result<OpenedSession>> {
        Box::pin(async move {
            let opened = self.destination.open(context).await?;
            Ok(OpenedSession {
                session: Box::new(Session {
                    session: opened.session,
                    load: Arc::clone(&self.load),
                }),
                ..opened
            })
        })
    }
}

struct Session {
    session: Box<dyn DestinationSession>,
    load: Arc<Load>,
}

impl DestinationSession for Session {
    fn apply_schema<'a>(&'a mut self, change: &'a TableChange) -> BoxFuture<'a, Result<()>> {
        self.session.apply_schema(change)
    }

    fn writer<'a>(
        &'a mut self,
        table: &'a TableRef,
    ) -> BoxFuture<'a, Result<Box<dyn DestinationWriter>>> {
        Box::pin(async move {
            let writer = self.session.writer(table).await?;
            Ok(Box::new(Writer {
                writer,
                load: Arc::clone(&self.load),
            }) as Box<dyn DestinationWriter>)
        })
    }

    fn commit<'a>(&'a mut self, meta: &'a CommitMeta) -> BoxFuture<'a, Result<Receipt>> {
        self.session.commit(meta)
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, Result<()>> {
        self.session.close()
    }
}

struct Writer {
    writer: Box<dyn DestinationWriter>,
    load: Arc<Load>,
}

impl DestinationWriter for Writer {
    fn write(&mut self, segment: SegmentId, batch: RecordBatch) -> BoxFuture<'_, Result<()>> {
        match self.load.charge(&batch) {
            Ok(()) => self.writer.write(segment, batch),
            Err(beyond) => Box::pin(async move { Err(beyond) }),
        }
    }

    fn flush(&mut self) -> BoxFuture<'_, Result<WriteStats>> {
        self.writer.flush()
    }
}
