//! CSV as a RECORD format: rows convert to NDJSON and ride the record path —
//! primary_key, dedup, merge, drift rules — like jsonl.
//! Whole-file incremental units (quoted newlines make byte-offset resume
//! unsafe); two passes over the local file: infer, then convert.
//!
//! Inference lattice per column over the WHOLE file: bool → int64 →
//! float64 → utf8; empty cells are null. `type_hints` override per column;
//! a value that cannot satisfy a DECLARED hint is a typed error naming
//! file, row, and column.

use std::collections::BTreeMap;

use bytes::Bytes;
use rdlt_connector::{RecordsOut, SourceError};

use super::{Codec, CsvOptions, open_decoded};
use crate::source::config::HintType;
use crate::source::cursor::{FileCursor, FileProgress, FileTask};

const SLAB_BYTES: usize = 8 << 20;

/// The lattice: `Empty` is bottom (a column that never saw a value);
/// int widens to float; bool is DISJOINT from the numeric chain — any
/// mix involving bool (or anything else) joins to text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CellKind {
    Empty,
    Bool,
    Int,
    Float,
    Text,
}

fn kind_of(value: &str) -> CellKind {
    match value {
        "true" | "false" => CellKind::Bool,
        _ if value.parse::<i64>().is_ok() => CellKind::Int,
        _ if value.parse::<f64>().is_ok() => CellKind::Float,
        _ => CellKind::Text,
    }
}

/// The JOIN — total and commutative; never trusts ordering tricks.
fn join(a: CellKind, b: CellKind) -> CellKind {
    use CellKind::*;
    match (a, b) {
        (Empty, x) | (x, Empty) => x,
        (x, y) if x == y => x,
        (Int, Float) | (Float, Int) => Float,
        _ => Text, // bool×numeric, anything×text
    }
}

fn reader_for(
    path: &str,
    codec: Codec,
    options: &CsvOptions,
) -> Result<csv::Reader<Box<dyn std::io::Read + Send>>, SourceError> {
    Ok(csv::ReaderBuilder::new()
        .delimiter(options.delimiter as u8)
        .quote(options.quote as u8)
        .has_headers(options.header)
        .flexible(false)
        .from_reader(open_decoded(path, codec)?))
}

/// How one column converts (after hints override inference).
#[derive(Debug, Clone, Copy, PartialEq)]
enum Conversion {
    Inferred(CellKind),
    /// A DECLARED hint: violations are typed (file, row, column).
    Declared(HintType),
}

/// Read one CSV file task: infer (pass 1), convert + push (pass 2),
/// checkpoint once at completion (whole-file unit).
pub(crate) async fn read_task(
    task: &FileTask,
    options: &CsvOptions,
    hints: &BTreeMap<String, HintType>,
    cursor: &mut FileCursor,
    out: &mut RecordsOut,
) -> Result<bool, SourceError> {
    let read_path = task.read_path.as_deref().unwrap_or(&task.path);
    let codec = super::codec_of(&task.path);

    // Pass 1: headers + per-column lattice kinds.
    let mut reader = reader_for(read_path, codec, options)?;
    let headers: Vec<String> = if options.header {
        reader
            .headers()
            .map_err(|e| SourceError::fatal(format!("reading CSV header of `{}`: {e}", task.path)))?
            .iter()
            .map(str::to_owned)
            .collect()
    } else {
        Vec::new() // named after width below
    };
    let mut kinds: Vec<CellKind> = Vec::new();
    let mut record = csv::StringRecord::new();
    let mut row = if options.header { 1u64 } else { 0u64 };
    loop {
        match reader.read_record(&mut record) {
            Ok(false) => break,
            Ok(true) => {
                row += 1;
                if kinds.len() < record.len() {
                    kinds.resize(record.len(), CellKind::Empty);
                }
                for (i, cell) in record.iter().enumerate() {
                    if !cell.is_empty() {
                        kinds[i] = join(kinds[i], kind_of(cell));
                    }
                }
            }
            Err(e) => {
                return Err(SourceError::fatal(format!(
                    "malformed CSV in `{}` at row {}: {e}",
                    task.path,
                    row + 1
                )));
            }
        }
    }
    let names: Vec<String> = if options.header {
        headers
    } else {
        (0..kinds.len()).map(|i| format!("c{i}")).collect()
    };
    let conversions: Vec<Conversion> = names
        .iter()
        .enumerate()
        .map(|(i, name)| match hints.get(name) {
            Some(hint) => Conversion::Declared(*hint),
            None => Conversion::Inferred(match kinds.get(i).copied() {
                Some(CellKind::Empty) | None => CellKind::Text,
                Some(kind) => kind,
            }),
        })
        .collect();

    // Pass 2: convert to NDJSON slabs.
    let mut reader = reader_for(read_path, codec, options)?;
    let mut slab: Vec<u8> = Vec::with_capacity(SLAB_BYTES);
    let mut record = csv::StringRecord::new();
    let mut row = if options.header { 1u64 } else { 0u64 };
    loop {
        let more = reader
            .read_record(&mut record)
            .map_err(|e| SourceError::fatal(format!("malformed CSV in `{}`: {e}", task.path)))?;
        if !more {
            break;
        }
        row += 1;
        write_row(&mut slab, &names, &conversions, &record, &task.path, row)?;
        if slab.len() >= SLAB_BYTES {
            if out
                .raw_json(Bytes::from(std::mem::take(&mut slab)))
                .await
                .is_err()
            {
                return Ok(false); // closed channel = cancellation
            }
            slab = Vec::with_capacity(SLAB_BYTES);
        }
    }
    if !slab.is_empty() && out.raw_json(Bytes::from(slab)).await.is_err() {
        return Ok(false);
    }

    // Whole-file unit: ONE completion checkpoint (crash re-delivers the
    // file; exactly-once under keyed merge/dedup — documented).
    cursor.record(
        &task.path,
        FileProgress {
            done: task.size,
            size: task.size,
            eol: true,
            mtime_ms: task.mtime_ms,
            etag: task.etag.clone(),
            tail_hash: None, // whole-file units never tail-resume
        },
    );
    if out.checkpoint(cursor.encode()).await.is_err() {
        return Ok(false);
    }
    Ok(true)
}

fn write_row(
    slab: &mut Vec<u8>,
    names: &[String],
    conversions: &[Conversion],
    record: &csv::StringRecord,
    path: &str,
    row: u64,
) -> Result<(), SourceError> {
    let mut object = serde_json::Map::with_capacity(names.len());
    for (i, cell) in record.iter().enumerate() {
        let name = names
            .get(i)
            .ok_or_else(|| {
                SourceError::fatal(format!(
                    "malformed CSV in `{path}` at row {row}: {} fields, {} columns",
                    record.len(),
                    names.len()
                ))
            })?
            .clone();
        let value = if cell.is_empty() {
            serde_json::Value::Null
        } else {
            convert_cell(cell, conversions[i], path, row, &name)?
        };
        object.insert(name, value);
    }
    serde_json::to_writer(&mut *slab, &serde_json::Value::Object(object))
        .map_err(|e| SourceError::fatal(e.to_string()))?;
    slab.push(b'\n');
    Ok(())
}

fn convert_cell(
    cell: &str,
    conversion: Conversion,
    path: &str,
    row: u64,
    column: &str,
) -> Result<serde_json::Value, SourceError> {
    let violation = |expected: &str| {
        SourceError::fatal(format!(
            "`{path}` row {row} column `{column}`: value does not satisfy the \
             declared {expected} hint"
        ))
    };
    // Pass-2 parse failures are TYPED: the two passes read the file twice,
    // and a file modified in between must fail loudly, never panic.
    let two_pass = |expected: &str| {
        SourceError::fatal(format!(
            "`{path}` row {row} column `{column}`: value no longer parses as the \
             inferred {expected} — the file changed between the inference and \
             conversion passes; retry the run"
        ))
    };
    Ok(match conversion {
        Conversion::Inferred(CellKind::Bool) => serde_json::Value::Bool(cell == "true"),
        Conversion::Inferred(CellKind::Int) => {
            serde_json::Value::Number(cell.parse::<i64>().map_err(|_| two_pass("int64"))?.into())
        }
        Conversion::Inferred(CellKind::Float) => {
            let parsed = cell.parse::<f64>().map_err(|_| two_pass("float64"))?;
            serde_json::Number::from_f64(parsed)
                .map(serde_json::Value::Number)
                .ok_or_else(|| {
                    SourceError::fatal(format!(
                        "`{path}` row {row} column `{column}`: non-finite value \
                         `{cell}` has no JSON representation — declare a utf8 \
                         type hint to load it as a string"
                    ))
                })?
        }
        Conversion::Inferred(CellKind::Empty | CellKind::Text) => {
            serde_json::Value::String(cell.to_owned())
        }
        Conversion::Declared(HintType::Bool) => match cell {
            "true" => serde_json::Value::Bool(true),
            "false" => serde_json::Value::Bool(false),
            _ => return Err(violation("bool")),
        },
        Conversion::Declared(HintType::Int64) => {
            serde_json::Value::Number(cell.parse::<i64>().map_err(|_| violation("int64"))?.into())
        }
        Conversion::Declared(HintType::Float64) => {
            serde_json::Number::from_f64(cell.parse::<f64>().map_err(|_| violation("float64"))?)
                .map(serde_json::Value::Number)
                .ok_or_else(|| violation("float64"))?
        }
        Conversion::Declared(HintType::Json) => {
            serde_json::from_str(cell).map_err(|_| violation("json"))?
        }
        // String-shaped logical types (utf8/timestamp_tz/date/time/uuid):
        // emitted as strings; the stream spec's hint types them downstream.
        Conversion::Declared(_) => serde_json::Value::String(cell.to_owned()),
    })
}
