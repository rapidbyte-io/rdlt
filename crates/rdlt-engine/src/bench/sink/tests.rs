use std::io::Cursor;
use std::num::NonZeroU16;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_ipc::reader::StreamReader;
use rdlt_connector::{ConnectContext, SegmentId, TableWriter, WriteStats};

use super::{SinkWriter, Sinking, sink_factory};

#[tokio::test]
async fn a_sink_declares_the_writers_its_configuration_names_and_one_otherwise() {
    let factory = sink_factory();
    let one = factory
        .connect(Sinking::Ipc.config(), ConnectContext::new())
        .await
        .unwrap();
    assert_eq!(one.capabilities().max_parallel_writers, NonZeroU16::MIN);
    let sixteen = NonZeroU16::new(16).unwrap();
    let config = Sinking::Ipc.writing(sixteen);
    let many = factory
        .connect(config, ConnectContext::new())
        .await
        .unwrap();
    assert_eq!(many.capabilities().max_parallel_writers, sixteen);
}

#[tokio::test]
async fn the_sink_encodes_each_batch_and_reports_what_it_staged() {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..10));
    let batch = RecordBatch::try_from_iter([("id", ids)]).unwrap();
    let mut writer = SinkWriter::new(Arc::default(), Sinking::Ipc);
    let encoded = writer.encode(&batch).unwrap().to_vec();
    let decoded = StreamReader::try_new(Cursor::new(&encoded), None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(decoded, std::slice::from_ref(&batch));
    writer.write(SegmentId(1), batch.clone()).await.unwrap();
    writer.write(SegmentId(1), batch).await.unwrap();
    let staged = WriteStats {
        rows: 20,
        bytes: 2 * encoded.len() as u64,
    };
    assert_eq!(writer.flush().await.unwrap(), staged);
    assert_eq!(writer.flush().await.unwrap(), WriteStats::default());
}

#[tokio::test]
async fn the_null_sink_counts_what_it_stages_and_encodes_nothing() {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..10));
    let batch = RecordBatch::try_from_iter([("id", ids)]).unwrap();
    let staged = Arc::default();
    let mut writer = SinkWriter::new(Arc::clone(&staged), Sinking::Discard);
    writer.write(SegmentId(1), batch.clone()).await.unwrap();
    writer.write(SegmentId(2), batch).await.unwrap();
    let flushed = WriteStats { rows: 20, bytes: 0 };
    assert_eq!(writer.flush().await.unwrap(), flushed);
    assert_eq!(
        *staged.lock(),
        [(SegmentId(1), 10), (SegmentId(2), 10)]
            .into_iter()
            .collect()
    );
}
