//! The unit of work flowing shred → load, and how the byte budget prices it.

use rdlt_connector::arrow::RecordBatch;

use rdlt_connector::channel::{ByteSized, arrow_batch_footprint};
use rdlt_core::commit::WriteMode;
use rdlt_core::cursor::Cursor;
use rdlt_core::id::{StreamName, TableName};
use rdlt_core::schema::{self, TableSchema};

/// One unit of work flowing shred → load. Per-table order within the channel is the
/// ordering guarantee (delta before first batch at the new version).
#[derive(Debug)]
pub(crate) enum LoadItem {
    Delta {
        schema: TableSchema,
        delta: schema::Delta,
        mode: WriteMode,
    },
    Batch {
        table: TableName,
        batch: RecordBatch,
        /// The batch's footprint, computed ONCE at construction
        /// ([`LoadItem::batch`]) — the channel gate, the loader's
        /// counters, and the accumulate threshold all read this number
        /// (round-5 fix: the walk used to re-run at every seam).
        bytes: usize,
    },
    /// A source checkpoint: rows pushed before this are complete up to `cursor`.
    Checkpoint { stream: StreamName, cursor: Cursor },
    /// Policy-driven discards — counted, never silent.
    Discarded {
        table: TableName,
        rows: u64,
        values: u64,
    },
}

impl LoadItem {
    /// A batch item carrying its footprint, computed once here — the
    /// footprint walk (never `get_array_memory_size`: a batch decoded
    /// from a remote connector's IPC stream passes through unchanged,
    /// and capacity-summing charges its one message body once per
    /// buffer, ≈10-17x, collapsing the stage window).
    pub(crate) fn batch(table: TableName, batch: RecordBatch) -> Self {
        let bytes = arrow_batch_footprint(&batch);
        LoadItem::Batch {
            table,
            batch,
            bytes,
        }
    }
}

impl ByteSized for LoadItem {
    fn byte_size(&self) -> usize {
        match self {
            LoadItem::Batch { bytes, .. } => *bytes,
            LoadItem::Delta { .. } | LoadItem::Checkpoint { .. } | LoadItem::Discarded { .. } => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use rdlt_connector::channel::{Permitted, bytes};
    use rdlt_core::id::StreamName;

    use super::*;
    use crate::runtime::STAGE_MSG_CAPACITY;

    fn batch_of(rows: usize) -> RecordBatch {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        let array = Int64Array::from((0..rows as i64).collect::<Vec<_>>());
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
            vec![Arc::new(array)],
        )
        .expect("batch")
    }

    /// `LoadItem::byte_size` has exactly ONE consumer — the stage channel's
    /// permit request — so its only observable consequence is backpressure.
    /// Nothing about the run report can detect a wrong answer: `table.bytes` is
    /// read straight off the batch in `process`, not through this trait. A
    /// constant 0 therefore removes backpressure entirely (peak memory would
    /// scale with input instead of staying capped) while every counter stays
    /// correct, which is why the mutant survived a suite that asserts counters.
    #[tokio::test]
    async fn byte_size_is_what_makes_backpressure_real() {
        let batch = batch_of(64);
        let size = arrow_batch_footprint(&batch);
        assert!(size > 0, "a real batch occupies memory");

        let (tx, mut rx) = bytes::<LoadItem>(size, STAGE_MSG_CAPACITY);
        tx.send(LoadItem::batch(TableName::new("t"), batch.clone()))
            .await
            .expect("a batch at exactly the budget sends");

        // Budget exhausted: the next batch MUST park. Under `byte_size → 0` it
        // sails through; under `→ 1` it also sails through (1 of `size` used).
        let parked = tokio::time::timeout(
            Duration::from_millis(100),
            tx.send(LoadItem::batch(TableName::new("t"), batch.clone())),
        )
        .await;
        assert!(
            parked.is_err(),
            "the byte budget is exhausted — the producer must park, which IS the backpressure"
        );

        // Receiving the first item releases its permit, and the parked send completes.
        let received = rx
            .recv()
            .await
            .map(Permitted::into_value)
            .expect("first item");
        assert!(matches!(received, LoadItem::Batch { .. }));
        tokio::time::timeout(
            Duration::from_secs(5),
            tx.send(LoadItem::batch(TableName::new("t"), batch)),
        )
        .await
        .expect("recv released the budget, so this send must complete")
        .expect("channel still open");
    }

    /// A batch decoded from an Arrow IPC stream (what a remote connector's
    /// wire hands the engine, passed through unchanged) meters near the ONE
    /// message body its buffers are slices of — never once per buffer.
    /// Capacity-summing charges that body per buffer (≈6x here, with six
    /// buffers), which collapsed the stage window and fired byte commit
    /// policies early; the connector channel's footprint walk is the one
    /// meter both seams share.
    #[test]
    fn byte_size_of_an_ipc_decoded_batch_is_near_the_body_it_decodes_from() {
        let (stream_len, decoded, row_payload) =
            rdlt_testkit::fixtures::ipc_fixture::ipc_round_trip();
        let metered = LoadItem::batch(TableName::new("t"), decoded).byte_size();
        assert!(
            metered >= row_payload,
            "an honest footprint cannot undercut the raw row payload: \
             metered {metered}, payload {row_payload}"
        );
        assert!(
            metered <= 2 * stream_len,
            "a decoded batch holds slices of ONE body allocation; metering \
             {metered} against a {stream_len}-byte stream means the body was \
             charged once per buffer, not once"
        );
    }

    /// The same meter stays sane for an ordinary builder-built batch:
    /// between its raw row payload and double it.
    #[test]
    fn byte_size_of_a_builder_built_batch_stays_between_payload_and_double() {
        let (batch, row_payload) = rdlt_testkit::fixtures::ipc_fixture::wide_batch();
        let metered = LoadItem::batch(TableName::new("t"), batch).byte_size();
        assert!(
            metered >= row_payload && metered <= 2 * row_payload,
            "a batch built from exact-length buffers meters near its payload: \
             metered {metered}, payload {row_payload}"
        );
    }

    /// Markers carry no rows and must never be gated by the byte budget: a
    /// checkpoint that could not enqueue would stall committing forever. This is
    /// the other half of the `byte_size` pin — under a constant 1 this send
    /// never completes on a zero budget.
    #[tokio::test]
    async fn zero_sized_markers_pass_a_zero_budget() {
        let (tx, mut rx) = bytes::<LoadItem>(0, STAGE_MSG_CAPACITY);
        for item in [
            LoadItem::Checkpoint {
                stream: StreamName::new("s"),
                cursor: Cursor::new(serde_json::json!({"b": 1})),
            },
            LoadItem::Discarded {
                table: TableName::new("t"),
                rows: 1,
                values: 2,
            },
        ] {
            tokio::time::timeout(Duration::from_secs(5), tx.send(item))
                .await
                .expect("a zero-byte marker must not wait on the byte budget")
                .expect("channel still open");
        }
        assert!(matches!(
            rx.recv().await.map(Permitted::into_value),
            Some(LoadItem::Checkpoint { .. })
        ));
        assert!(matches!(
            rx.recv().await.map(Permitted::into_value),
            Some(LoadItem::Discarded { .. })
        ));
    }
}
