//! The unit of work flowing shred → load, and how the byte budget prices it.

use rdlt_connector::{RecordBatch, channel::ByteSized};
use rdlt_core::{Cursor, SchemaDelta, StreamName, TableName, TableSchema, WriteMode};

/// One unit of work flowing shred → load. Per-table order within the channel is the
/// ordering guarantee (delta before first batch at the new version).
#[derive(Debug)]
pub(crate) enum LoadItem {
    Delta {
        schema: TableSchema,
        delta: SchemaDelta,
        mode: WriteMode,
    },
    Batch {
        table: TableName,
        batch: RecordBatch,
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

impl ByteSized for LoadItem {
    fn byte_size(&self) -> usize {
        match self {
            LoadItem::Batch { batch, .. } => batch.get_array_memory_size(),
            LoadItem::Delta { .. } | LoadItem::Checkpoint { .. } | LoadItem::Discarded { .. } => 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use rdlt_connector::channel::{Permitted, byte_channel};
    use rdlt_core::StreamName;

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
        let size = batch.get_array_memory_size();
        assert!(size > 0, "a real batch occupies memory");

        let (tx, mut rx) = byte_channel::<LoadItem>(size, STAGE_MSG_CAPACITY);
        tx.send(LoadItem::Batch {
            table: TableName::new("t"),
            batch: batch.clone(),
        })
        .await
        .expect("a batch at exactly the budget sends");

        // Budget exhausted: the next batch MUST park. Under `byte_size → 0` it
        // sails through; under `→ 1` it also sails through (1 of `size` used).
        let parked = tokio::time::timeout(
            Duration::from_millis(100),
            tx.send(LoadItem::Batch {
                table: TableName::new("t"),
                batch: batch.clone(),
            }),
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
            tx.send(LoadItem::Batch {
                table: TableName::new("t"),
                batch,
            }),
        )
        .await
        .expect("recv released the budget, so this send must complete")
        .expect("channel still open");
    }

    /// Markers carry no rows and must never be gated by the byte budget: a
    /// checkpoint that could not enqueue would stall committing forever. This is
    /// the other half of the `byte_size` pin — under a constant 1 this send
    /// never completes on a zero budget.
    #[tokio::test]
    async fn zero_sized_markers_pass_a_zero_budget() {
        let (tx, mut rx) = byte_channel::<LoadItem>(0, STAGE_MSG_CAPACITY);
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
