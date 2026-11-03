//! A served read's frames arrive in the allocation a frame before them left once nothing holds
//! it, where whoever their decoding is charged to keeps it, and what is kept never passes one
//! frame's allocation.

use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::{BinaryArray, RecordBatch};
use parking_lot::Mutex;
use rdlt_connector::prelude::*;
use rdlt_connector::serve::Served;
use rdlt_connector::{Push, ReadRequest, Role, Source as _, SourceEvent, partition_channel};
use rdlt_host::{Connection, Options, RemoteSource};
use rdlt_wire::aligned::Spare;
use rdlt_wire::bounded::{Charge, Charging, Held};
use rdlt_wire::limits::Class;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use crate::HEAP;
use crate::support::served;

/// Frames a read sends.
const FRAMES: usize = 8;

/// Bytes: the value each frame's batch holds, its only one.
const VALUE: usize = 1 << 20;

/// Batches [`Paced`] may send beyond its first, one for each its host has dropped.
static PACE: Semaphore = Semaphore::const_new(0);

/// Configuration of [`Paced`]: none.
#[derive(Debug, Default, Deserialize, JsonSchema)]
struct PacedConfig {}

/// A source of one stream, `paced`, of [`FRAMES`] batches of one [`VALUE`]-byte value each, each
/// after the first sent once one of [`PACE`]'s permits is given.
#[derive(Debug)]
struct Paced;

#[source(id = "test.paced")]
impl SourceConnector for Paced {
    type Config = PacedConfig;

    async fn connect(_config: PacedConfig, _context: &ConnectContext) -> Result<Self> {
        Ok(Self)
    }

    async fn check(&self) -> Result<()> {
        Ok(())
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new().with(PacedStream)
    }
}

struct PacedStream;

/// Batches sent so far.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
struct Sent {
    batches: usize,
}

impl ReadStream<Paced> for PacedStream {
    type Cursor = Sent;

    fn spec(&self) -> StreamSpec {
        StreamSpec::new(StreamName::new("paced").expect("a valid name"))
    }

    async fn read(
        &self,
        _source: &Paced,
        _partition: &Partition,
        _cursor: Sent,
        out: &mut Emitter<Sent>,
    ) -> Result<()> {
        for batch in 0..FRAMES {
            if batch > 0 {
                PACE.acquire().await.expect("never closed").forget();
            }
            let value = BinaryArray::from_iter_values([vec![7_u8; VALUE]]);
            let columns = [("value", Arc::new(value) as _)];
            out.batch(RecordBatch::try_from_iter(columns).expect("a batch"))
                .await?;
        }
        out.checkpoint(&Sent { batches: FRAMES }).await
    }
}

/// Charges nothing, keeps the largest spare it is given, as a budget does, and gives it to a
/// frame it fits, counting the frames of a value's room that took one.
#[derive(Default)]
struct Keeper {
    kept: Mutex<Option<Spare>>,
    taken: Mutex<usize>,
}

impl Charge for Keeper {
    fn charge(&self, _class: Class, _bytes: usize) -> Charging {
        Box::pin(async { Ok(Box::new(()) as Held) })
    }

    fn keep_spare(&self, spare: Spare) {
        let mut kept = self.kept.lock();
        if kept
            .as_ref()
            .is_none_or(|kept| kept.bytes() < spare.bytes())
        {
            *kept = Some(spare);
        }
    }

    fn take_spare(&self, room: usize) -> Option<Spare> {
        let mut kept = self.kept.lock();
        let spare = kept.take_if(|spare| spare.fits(room))?;
        if room >= VALUE {
            *self.taken.lock() += 1;
        }
        Some(spare)
    }
}

/// Reads [`Paced`]'s stream charged to `charge`, dropping each batch as it arrives and letting the
/// next go once it is dropped; returns the heap's peak meanwhile and what it holds once the read is
/// done, each beyond what it held before.
async fn read(charge: Arc<dyn Charge>) -> (usize, usize) {
    let before = HEAP.current_usage();
    HEAP.reset_peak_usage();
    let io = served(Served::new().with_source(source_factory::<Paced>()));
    let config = serde_json::json!({});
    let connection = Connection::connect(io, Role::Source, &config, Options::default())
        .await
        .expect("the source handshakes");
    let source = RemoteSource::new(connection);
    let (sink, mut feed) = partition_channel(NonZeroUsize::MIN);
    let request = ReadRequest::new(
        StreamName::new("paced").expect("a valid name"),
        Partition::single(),
        None,
    );
    let read = tokio::spawn(rdlt_wire::bounded::charging(charge, async move {
        source.read(request, sink).await
    }));
    let mut batches = 0;
    while let Some(event) = feed.recv().await {
        if let SourceEvent::Push(Push::Arrow(batch)) = event {
            drop(batch);
            batches += 1;
            if batches < FRAMES {
                PACE.add_permits(1);
            }
        }
    }
    read.await.expect("the read runs").expect("the read ends");
    assert_eq!(batches, FRAMES);
    let peak = HEAP.peak_usage().saturating_sub(before);
    (peak, HEAP.current_usage().saturating_sub(before))
}

#[tokio::test(flavor = "multi_thread")]
async fn each_frame_of_a_paced_read_arrives_in_the_allocation_the_frame_before_left() {
    let keeper = Arc::new(Keeper::default());
    read(Arc::clone(&keeper) as Arc<dyn Charge>).await;
    assert_eq!(
        *keeper.taken.lock(),
        FRAMES - 1,
        "every frame after the first took the spare its batch's frame before left"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn what_a_read_keeps_never_passes_one_frame_s_allocation() {
    /// Keeps nothing.
    struct Bare;

    impl Charge for Bare {
        fn charge(&self, _class: Class, _bytes: usize) -> Charging {
            Box::pin(async { Ok(Box::new(()) as Held) })
        }
    }

    // The first read leaves what the runtime and the connection allocate once.
    read(Arc::new(Bare)).await;
    let (bare_peak, bare_left) = read(Arc::new(Bare)).await;
    let keeper = Arc::new(Keeper::default());
    let (kept_peak, kept_left) = read(Arc::clone(&keeper) as Arc<dyn Charge>).await;
    let spare = keeper.kept.lock().as_ref().map(Spare::bytes);
    let spare = spare.expect("a frame's allocation was kept");
    assert!(spare >= VALUE, "the largest frame's: {spare} bytes");
    // What the runtime and the connection's tasks leave from one read to the next moves by a few
    // kilobytes; a second frame's allocation kept would be a mebibyte.
    let noise = VALUE / 16;
    assert!(
        kept_left <= bare_left + spare + noise,
        "left {kept_left} bytes, beyond {bare_left} and the {spare} kept"
    );
    assert!(
        kept_peak <= bare_peak + spare + noise,
        "held {kept_peak} bytes at the peak, beyond {bare_peak} and the {spare} kept"
    );
}
