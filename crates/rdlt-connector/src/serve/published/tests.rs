use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use rdlt_wire::Limits;
use rdlt_wire::stream::{Call, Outgoing};
use tokio::sync::Notify;

use super::pump;
use crate::destination::{PublishedReader, PublishedRows, TableRef};
use crate::error::Result;
use crate::id::{SchemaVersion, TablePath};
use crate::serve::live::{Live, gone};
use crate::serve::stream::Frames;
use crate::spec::BoxFuture;
use crate::wire::v1;

/// Bounds every wait of a test: a read-back that hangs fails its test.
const WAIT: Duration = Duration::from_secs(60);

/// How long a test lets a task that should end at once go on before it fails.
const HOUR: Duration = Duration::from_mins(60);

/// A reader that waits for ever once it has begun, its rows kept or let go first, counted in
/// `live` while it reads.
struct Stuck {
    keeps_rows: bool,
    begun: Notify,
    live: Arc<AtomicUsize>,
}

impl PublishedReader for Stuck {
    fn published<'a>(&'a self, _: &'a TableRef, rows: PublishedRows) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let _live = Live::new(&self.live);
            // A reader that let its rows go may still clean up before it returns.
            let _rows = self.keeps_rows.then_some(rows);
            self.begun.notify_one();
            std::future::pending::<()>().await;
            Ok(())
        })
    }
}

fn table() -> TableRef {
    TableRef {
        path: TablePath::new(["rows"]).expect("a valid path"),
        name: "rows".into(),
        version: SchemaVersion(1),
        generation: None,
        merge: None,
    }
}

#[tokio::test(start_paused = true)]
async fn a_read_back_waiting_for_its_reader_ends_once_its_host_goes() {
    for keeps_rows in [true, false] {
        let live = Arc::new(AtomicUsize::new(0));
        let reader = Arc::new(Stuck {
            keeps_rows,
            begun: Notify::new(),
            live: Arc::clone(&live),
        });
        let limits = Limits::default();
        let (mut host, connector) = tokio::io::duplex(64 * 1024);
        let start = v1::ReadPublishedRequest::default();
        let mut opening =
            Outgoing::message(Some(Call::ReadPublished), &start).expect("a start fits a head");
        opening.write(&mut host).await.expect("the opening is sent");
        let (read, write) = tokio::io::split(connector);
        let window = rdlt_wire::bounded::Window::new(limits.largest());
        let mut frames = Frames::new(read, limits, window);
        tokio::time::timeout(WAIT, frames.opening())
            .await
            .expect("the opening arrives")
            .expect("the opening reads")
            .expect("the host opened a call");
        let begun = reader.begun.notified();
        let pumping = tokio::spawn(pump(
            Arc::clone(&reader) as _,
            table(),
            limits,
            frames,
            write,
        ));
        tokio::time::timeout(WAIT, begun)
            .await
            .expect("the reader begins");
        drop(host);
        tokio::time::timeout(HOUR, pumping)
            .await
            .expect("the read-back ends once its host goes")
            .expect("the read-back does not panic");
        tokio::time::timeout(HOUR, gone(&live))
            .await
            .unwrap_or_else(|_| {
                panic!("the reader is dropped once its host goes, rows kept: {keeps_rows}")
            });
    }
}
