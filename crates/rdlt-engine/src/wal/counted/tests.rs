use std::sync::Arc;
use std::time::UNIX_EPOCH;

use bytes::Bytes;
use rdlt_connector::{LoadId, PipelineId};

use super::CountedStore;
use crate::report::{LogCounters, StoreRequests, Tally};
use crate::wal::memory::MemoryWal;
use crate::wal::{Chunk, WalStore};

#[tokio::test]
async fn each_request_is_counted_by_operation_with_the_bytes_appended_and_read() {
    let tally = Arc::new(Tally::default());
    let store = CountedStore::new(Arc::new(MemoryWal::default()), Arc::clone(&tally));
    let pipeline = PipelineId::parse("orders").unwrap();
    let load = LoadId::from_parts(UNIX_EPOCH, 1);
    let chunk = Chunk { load, number: 0 };
    store.open_log(&pipeline, load).await.unwrap();
    let mut staged = store.stage(&pipeline, chunk).await.unwrap();
    staged
        .append(Bytes::from_static(b"ten bytes!"))
        .await
        .unwrap();
    staged.append(Bytes::from_static(b"four")).await.unwrap();
    staged.publish().await.unwrap();
    let read = store.read(&pipeline, chunk, 3, 6).await.unwrap();
    assert_eq!(read, Bytes::from_static(b" bytes"));
    store.chunks(&pipeline, load).await.unwrap();
    store.remove_log(&pipeline, load).await.unwrap();
    let requests = StoreRequests {
        open_log: 1,
        stage: 1,
        append: 2,
        publish: 1,
        read: 1,
        chunks: 1,
        remove_log: 1,
        ..StoreRequests::default()
    };
    let log = LogCounters {
        appended: 14,
        read: 6,
        requests,
        ..LogCounters::default()
    };
    assert_eq!(tally.counters().log, log);
}
