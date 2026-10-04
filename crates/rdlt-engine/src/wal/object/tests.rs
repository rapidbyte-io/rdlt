mod contract;
mod costs;
mod heads;
mod names;
mod parts;
mod probe;
mod requests;

use std::io;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use object_store::memory::InMemory;
use rdlt_connector::{LoadId, PipelineId};
use rdlt_testkit::objects::{Call, Fault, Faulty, Plan, faultless};

use super::{ObjectStoreOptions, ObjectStoreWal};
use crate::env::SystemClock;
use crate::error::Error;
use crate::wal::Chunk;

/// An in-memory store whose requests `plan` faults.
fn objects(plan: Plan) -> Arc<Faulty<InMemory>> {
    Arc::new(Faulty::new(InMemory::new(), plan))
}

/// The options the tests use: parts of `part` bytes, and the default tries.
fn options(part: usize) -> ObjectStoreOptions {
    let part = NonZeroUsize::new(part).expect("not zero");
    ObjectStoreOptions::default().with_part_bytes(part)
}

/// A log in `objects` beneath `logs`, with `options`.
async fn opened(objects: &Arc<Faulty<InMemory>>, options: ObjectStoreOptions) -> ObjectStoreWal {
    let objects = Arc::clone(objects);
    ObjectStoreWal::open(objects, "logs", Arc::new(SystemClock), options)
        .await
        .expect("the store is probed")
}

/// A plan faulting every `nth` request that `matches` with `fault`, and none other.
fn every(nth: u32, fault: Fault, matches: fn(&Call) -> bool) -> Plan {
    let mut seen = 0_u32;
    Box::new(move |call| {
        if !matches(call) {
            return Fault::None;
        }
        seen += 1;
        if seen.is_multiple_of(nth) {
            fault
        } else {
            Fault::None
        }
    })
}

/// A plan faulting every request that `matches` with `fault`.
fn always(fault: Fault, matches: fn(&Call) -> bool) -> Plan {
    Box::new(move |call| if matches(call) { fault } else { Fault::None })
}

fn any(_: &Call) -> bool {
    true
}

fn pipeline(name: &str) -> PipelineId {
    PipelineId::parse(name).expect("a valid pipeline")
}

fn chunk(load: u128, number: u64) -> Chunk {
    Chunk {
        load: LoadId::from_parts(UNIX_EPOCH, load),
        number,
    }
}

/// The code the engine gives `error`, from the store, and whether it retries a run it failed.
fn judged(error: io::Error) -> (Option<String>, bool) {
    let error = Error::from_wal(error);
    (error.code().map(str::to_owned), error.is_retryable())
}

/// Options of `attempts` tries a request, each given `deadline`.
fn tries(attempts: u32, deadline: Duration) -> ObjectStoreOptions {
    let attempts = NonZeroU32::new(attempts).expect("not zero");
    options(1 << 20)
        .with_attempts(attempts)
        .with_deadline(deadline, Duration::ZERO)
}

/// Every object `objects` holds, by key.
async fn keys(objects: &Faulty<InMemory>) -> Vec<String> {
    use futures_util::TryStreamExt as _;
    use object_store::ObjectStore as _;
    let listed: Vec<_> = objects
        .inner()
        .list(None)
        .try_collect()
        .await
        .expect("lists");
    listed
        .into_iter()
        .map(|meta| meta.location.to_string())
        .collect()
}

#[tokio::test]
async fn a_log_in_an_object_store_keeps_the_store_s_contract() {
    let objects = objects(faultless());
    crate::conformance::conforms(&opened(&objects, options(1 << 20)).await).await;
}
