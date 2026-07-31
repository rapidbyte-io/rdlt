//! The per-batch write half of the load session: preparing a target before
//! its FIRST row (the direct-path clear, hoisted to write time because by
//! commit time the rows are already there), and the binary-COPY streaming
//! loop that lands a batch. The transaction boundaries and the publish
//! protocol stay in `commit` — one file decides WHEN, this one lands BYTES.

use std::collections::BTreeSet;

use arrow_array::RecordBatch;
use bytes::{BufMut, Bytes, BytesMut};
use futures::SinkExt;
use rdlt_connector::core::crash_point;
use rdlt_connector::{DestinationError, WriteMode, core::TableName};
use rdlt_connector_sqlcore::{MergeDialect, Step, prepare_target};

use super::commit::{PgSession, stage_name};
use super::dialect::PgDialect;
use super::{classify_stmt, encode, fatal, quote, transient};

/// The 11-byte binary-COPY file signature. The `\r\n` / `\n` pair and the
/// high 0xff byte are a deliberate corruption detector: a stream mangled by
/// a text-mode or non-8-bit-clean transport no longer matches, so the server
/// rejects it up front instead of misreading the first tuple.
const COPY_BINARY_SIGNATURE: &[u8] = b"PGCOPY\n\xff\r\n\0";

impl PgSession {
    /// Everything `write` must do to a target before its first row lands:
    /// open the unit, then run whatever the planner says has to precede the
    /// data (for Replace, exactly one `TRUNCATE` per load).
    pub(super) async fn prepare_for_write(
        &mut self,
        table: &TableName,
        load_id: &str,
    ) -> Result<(), DestinationError> {
        self.begin_unit(load_id).await?;
        // Only Replace has anything to prepare. Checked before building the
        // union below, so the common path (Append, and every merge table)
        // allocates nothing.
        if !matches!(self.tables.get(table), Some((_, WriteMode::Replace))) {
            return Ok(());
        }
        // Seed the guard from the DURABLE record before asking the planner.
        // A crash-recovery session has fresh memory and would otherwise
        // re-clear a target an earlier unit of this same load already cleared
        // and filled.
        if !self.cleared_targets.contains(table)
            && !self
                .unit
                .as_ref()
                .expect("unit open")
                .cleared
                .contains(table)
            && self.target_cleared_durably(load_id, table).await?
        {
            self.cleared_targets.insert(table.clone());
        }
        let cleared: BTreeSet<TableName> = self
            .cleared_targets
            .union(&self.unit.as_ref().expect("unit open").cleared)
            .cloned()
            .collect();
        let steps = prepare_target(
            &self.tables,
            &self.ctx(false, &BTreeSet::new(), &cleared),
            table,
        );
        for step in steps {
            let Step::ClearTarget { table } = step else {
                // `prepare_target` emits nothing else; anything else would be
                // a planner change that this executor has not been taught.
                return Err(fatal(
                    "internal: unexpected step planned before a direct write",
                ));
            };
            crash_point!(
                "pg.target.clear",
                Err(DestinationError::fatal("injected crash at pg.target.clear"))
            );
            self.client
                .batch_execute(&PgDialect.clear_table(&quote(table.as_str())))
                .await
                .map_err(classify_stmt)?;
            // Recorded in the SAME transaction as the TRUNCATE it describes,
            // so the two cannot disagree: a rolled-back clear leaves no record
            // claiming it happened, and a durable record always has the empty
            // target behind it.
            self.client
                .execute(
                    &format!(
                        "INSERT INTO {} VALUES ($1, $2) ON CONFLICT DO NOTHING",
                        rdlt_connector_sqlcore::names::CLEARED_TABLE
                    ),
                    &[&load_id, &table.as_str()],
                )
                .await
                .map_err(classify_stmt)?;
            self.unit.as_mut().expect("unit open").cleared.insert(table);
        }
        Ok(())
    }

    /// Whether a COMMITTED unit of this load already cleared this target.
    ///
    /// The durable half of the once-per-(load, target) guard. It replaces the
    /// old per-LOAD test, which answered the wrong question: a Replace table
    /// registered in one unit and first written in a later one found the load
    /// already committed and skipped its clear entirely.
    async fn target_cleared_durably(
        &self,
        load_id: &str,
        table: &TableName,
    ) -> Result<bool, DestinationError> {
        let cleared: bool = self
            .client
            .query_one(
                &format!(
                    "SELECT EXISTS (SELECT 1 FROM {} WHERE load_id = $1 AND table_name = $2)",
                    rdlt_connector_sqlcore::names::CLEARED_TABLE
                ),
                &[&load_id, &table.as_str()],
            )
            .await
            .map_err(transient)?
            .get(0);
        Ok(cleared)
    }

    /// Where this table's rows land: its stage if it merges, the target itself
    /// otherwise.
    fn write_target(&self, table: &TableName) -> String {
        match self.tables.get(table) {
            Some((_, WriteMode::Merge { .. })) => stage_name(&self.pipeline, table),
            // A table nothing ensured cannot merge, so the target is right.
            _ => table.as_str().to_owned(),
        }
    }
    pub(super) async fn write_inner(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<(), DestinationError> {
        let load_id = self.load_id.as_str().to_owned();
        self.prepare_for_write(table, &load_id).await?;
        crash_point!(
            "pg.unit.write",
            Err(DestinationError::fatal("injected crash at pg.unit.write"))
        );
        let stage = self.write_target(table);
        let arrow_schema = batch.schema();
        let column_names = arrow_schema
            .fields()
            .iter()
            .map(|f| quote(f.name()))
            .collect::<Vec<_>>()
            .join(", ");
        // Wire decisions come from the ENSURED schema's logical types;
        // arrow representation only fills in where the schema is silent.
        let table_schema = self.tables.get(table).map(|(s, _)| s);
        let wires: Vec<encode::ColumnWire> = arrow_schema
            .fields()
            .iter()
            .map(|f| {
                let logical = table_schema
                    .and_then(|s| s.column(f.name()))
                    .map(|c| &c.column_type);
                encode::column_wire(logical, f.data_type())
            })
            .collect::<Result<_, _>>()?;

        // Downcast ONCE per column, not once per cell.
        let encoders: Vec<encode::ColumnEncoder<'_>> = arrow_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, f)| {
                encode::ColumnEncoder::new(wires[idx], batch.column(idx).as_ref(), f.name())
            })
            .collect::<Result<_, _>>()?;
        let names: Vec<&str> = arrow_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        let field_count = i16::try_from(batch.num_columns())
            .map_err(|_| fatal("a COPY tuple cannot carry more than 32767 columns"))?;

        let sink = self
            .client
            .copy_in::<_, Bytes>(&format!(
                "COPY {} ({column_names}) FROM STDIN BINARY",
                quote(&stage)
            ))
            .await
            .map_err(classify_stmt)?;
        futures::pin_mut!(sink);

        // One reused buffer for the whole batch, handed to the sink in ~64 KiB
        // slices. The driver's own `BinaryCopyInWriter` flushes every 4096
        // bytes into an `mpsc::channel(1)`, so each flush is a wakeup and a
        // task handoff — the mechanism behind the six-figure voluntary
        // context-switch count on a 1M-row load.
        //
        // 64 KiB is the measured knee, not a guess. Swept on pg-to-pg-1m
        // (medians of 5, same binary, flush size the only variable):
        //
        //     flush     cpu s    vol ctxsw
        //       4 KiB    0.61        68497
        //      16 KiB    0.53        42774
        //      64 KiB    0.49        27013
        //     256 KiB    0.48        22467
        //       1 MiB    0.48        21836
        //
        // CPU is flat from 64 KiB on, so everything past it buys only context
        // switches, and buys them badly: 4x the buffer for 17% fewer, then 4x
        // again for 3% fewer. Wall was flat across the whole sweep — this
        // cell is not CPU-bound, so the win here is headroom, not seconds.
        const FLUSH_AT: usize = 64 * 1024;
        let mut buf = BytesMut::with_capacity(FLUSH_AT * 2);
        buf.put_slice(COPY_BINARY_SIGNATURE);
        buf.put_i32(0); // flags: no OIDs
        buf.put_i32(0); // header extension area: empty

        for row in 0..batch.num_rows() {
            buf.put_i16(field_count);
            for (col, encoder) in encoders.iter().enumerate() {
                // On error the sink is dropped WITHOUT `finish()`, which is
                // tokio-postgres' abort protocol: the server sees CopyFail and
                // the staged rows never become visible. There is deliberately
                // no cleanup path here that would finish the copy instead.
                encoder.encode_field(row, names[col], &mut buf)?;
            }
            if buf.len() >= FLUSH_AT {
                let chunk = buf.split().freeze();
                sink.as_mut().feed(chunk).await.map_err(classify_stmt)?;
                // `split` hands the used capacity to the chunk, so without
                // this the buffer is near-empty from the third flush on and
                // every later chunk is rebuilt by repeated growth inside the
                // per-cell loop — the opposite of reusing one buffer.
                buf.reserve(FLUSH_AT);
            }
        }
        buf.put_i16(-1); // file trailer
        sink.as_mut()
            .feed(buf.split().freeze())
            .await
            .map_err(classify_stmt)?;
        sink.as_mut().flush().await.map_err(classify_stmt)?;
        sink.finish().await.map_err(classify_stmt)?;
        Ok(())
    }
}
