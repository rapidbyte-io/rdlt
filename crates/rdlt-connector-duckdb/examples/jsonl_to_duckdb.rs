//! The rdlt side of the jsonl → DuckDB baseline comparison (benches/run-e2e.sh).
//!
//! A minimal file source streaming ~8 MB slabs of NDJSON through the raw-bytes perf
//! path into the DuckDB destination. Usage:
//!
//! ```sh
//! cargo run --release -p rdlt-connector-duckdb --example jsonl_to_duckdb -- rows.jsonl out.duckdb
//! ```

use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use async_trait::async_trait;
use bytes::Bytes;
use rdlt_connector::{ConnectorSpec, ReadRequest, Source, SourceError, StreamSpec};
use rdlt_connector_duckdb::dest::DuckDb;
use rdlt_engine::{Engine, EngineConfig};

struct JsonlSource {
    path: PathBuf,
}

const SLAB_BYTES: usize = 8 << 20;

#[async_trait]
impl Source for JsonlSource {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("jsonl-bench", "0.0.0")
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        Ok(vec![StreamSpec::new("events")])
    }

    async fn read(&self, mut req: ReadRequest) -> Result<(), SourceError> {
        let file = std::fs::File::open(&self.path).map_err(SourceError::fatal)?;
        let mut reader = BufReader::with_capacity(SLAB_BYTES, file);
        let mut slab: Vec<u8> = Vec::with_capacity(SLAB_BYTES);
        loop {
            slab.clear();
            // Fill a slab with complete lines.
            loop {
                let buf = reader.fill_buf().map_err(SourceError::fatal)?;
                if buf.is_empty() {
                    break;
                }
                match buf.iter().rposition(|&b| b == b'\n') {
                    Some(newline) if slab.len() + newline < SLAB_BYTES => {
                        slab.extend_from_slice(&buf[..=newline]);
                        let consumed = newline + 1;
                        reader.consume(consumed);
                        if slab.len() >= SLAB_BYTES / 2 {
                            break;
                        }
                    }
                    _ => {
                        // Long tail without newline in view: byte-wise fallback.
                        let mut line = String::new();
                        let n = reader.read_line(&mut line).map_err(SourceError::fatal)?;
                        if n == 0 {
                            break;
                        }
                        slab.extend_from_slice(line.as_bytes());
                        break;
                    }
                }
            }
            if slab.is_empty() {
                break;
            }
            if req
                .out
                .raw_json(Bytes::copy_from_slice(&slab))
                .await
                .is_err()
            {
                return Ok(());
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let input = PathBuf::from(
        args.next()
            .expect("usage: jsonl_to_duckdb <in.jsonl> <out.duckdb>"),
    );
    let output = PathBuf::from(
        args.next()
            .expect("usage: jsonl_to_duckdb <in.jsonl> <out.duckdb>"),
    );
    let _ = std::fs::remove_file(&output);

    let started = std::time::Instant::now();
    let dest = DuckDb::open(&output).expect("open duckdb");
    let report = Engine::new(
        EngineConfig::new("jsonl-bench"),
        JsonlSource { path: input },
        dest,
    )
    .run()
    .await
    .expect("run");
    let elapsed = started.elapsed().as_secs_f64();

    println!(
        "{{\"rows\": {}, \"seconds\": {:.3}, \"rows_per_s\": {:.0}, \"tables\": {}}}",
        report.total_rows(),
        elapsed,
        report.total_rows() as f64 / elapsed,
        report.tables.len(),
    );
}
