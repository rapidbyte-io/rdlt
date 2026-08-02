//! CSV as a RECORD format: rows become NDJSON and ride the record
//! path — primary_key, dedup, merge, drift — exactly like jsonl.
//!
//! Whole-file units (quoted newlines make byte resume unsafe), TWO
//! passes over the file: infer, then convert. The inference lattice
//! joins per column over the whole file; declared hints override it,
//! and a value that cannot satisfy a DECLARED hint is typed naming
//! file, row, and column. Pass-2 disagreements with pass 1 are typed
//! too — a file changed between the passes must fail loudly, never
//! coerce.

use std::collections::BTreeMap;
use std::ops::ControlFlow;

use bytes::Bytes;
use rdlt_connector_sdk::source::Feed;
use rdlt_connector_sdk::spi::SourceError;

use super::open_decoded;
use crate::format::{Codec, SLAB_BYTES, codec_of};
use crate::source::config::{CsvOptions, HintType};
use crate::source::cursor::{FileCursor, FileProgress, FileTask};

/// The lattice: `Empty` is bottom (a column that never saw a value —
/// it lands as Text); Int widens to Float; Bool is DISJOINT from the
/// numeric chain, so any mix involving it — or anything with text —
/// joins to Text.
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

/// The JOIN — total and commutative; ordering can play no tricks.
fn join(a: CellKind, b: CellKind) -> CellKind {
    use CellKind::*;
    match (a, b) {
        (Empty, x) | (x, Empty) => x,
        (x, y) if x == y => x,
        (Int, Float) | (Float, Int) => Float,
        _ => Text,
    }
}

/// How one column converts, after hints override inference.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Conversion {
    Inferred(CellKind),
    Declared(HintType),
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

/// Read one CSV task: infer, convert into NDJSON slabs, one completion
/// checkpoint (a mid-file crash re-delivers the file; exactly-once is
/// the keyed merge/dedup layer's job). Ok(false) = host cancellation.
pub(crate) async fn read_task(
    task: &FileTask,
    options: &CsvOptions,
    hints: &BTreeMap<String, HintType>,
    cursor: &mut FileCursor,
    feed: &mut Feed,
) -> Result<bool, SourceError> {
    let read_path = task.read_path.as_deref().unwrap_or(&task.path);
    let codec = codec_of(&task.path);

    // Pass 1: headers and the per-column joined kinds.
    let mut reader = reader_for(read_path, codec, options)?;
    let headers: Vec<String> = if options.header {
        reader
            .headers()
            .map_err(|e| SourceError::fatal(format!("reading CSV header of `{}`: {e}", task.path)))?
            .iter()
            .map(str::to_owned)
            .collect()
    } else {
        Vec::new()
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

    // Pass 2: convert and push.
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
            if feed.raw_json(Bytes::from(std::mem::take(&mut slab))).await == ControlFlow::Break(())
            {
                return Ok(false);
            }
            slab = Vec::with_capacity(SLAB_BYTES);
        }
    }
    if !slab.is_empty() && feed.raw_json(Bytes::from(slab)).await == ControlFlow::Break(()) {
        return Ok(false);
    }

    cursor.record(
        &task.path,
        FileProgress {
            done_units: task.size_units,
            size_units: task.size_units,
            ended_at_record_boundary: true,
            mtime_ms: task.mtime_ms,
            etag: task.etag.clone(),
            tail_hash: None, // whole-file units never tail-resume
            row_groups_hash: None,
        },
    );
    if feed.checkpoint(cursor.encode()).await == ControlFlow::Break(()) {
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
            "`{path}` row {row} column `{column}`: value does not satisfy the declared \
             {expected} hint"
        ))
    };
    // A pass-2 value pass 1 could not have inferred means the file
    // changed BETWEEN the passes — typed, never coerced: `cell ==
    // "true"` would silently answer false for "yes" or "1".
    let two_pass = |expected: &str| {
        SourceError::fatal(format!(
            "`{path}` row {row} column `{column}`: value no longer parses as the inferred \
             {expected} — the file changed between the inference and conversion passes; retry \
             the run"
        ))
    };
    Ok(match conversion {
        Conversion::Inferred(CellKind::Bool) => match cell {
            "true" => serde_json::Value::Bool(true),
            "false" => serde_json::Value::Bool(false),
            _ => return Err(two_pass("bool")),
        },
        Conversion::Inferred(CellKind::Int) => {
            serde_json::Value::Number(cell.parse::<i64>().map_err(|_| two_pass("int64"))?.into())
        }
        Conversion::Inferred(CellKind::Float) => {
            let parsed = cell.parse::<f64>().map_err(|_| two_pass("float64"))?;
            serde_json::Number::from_f64(parsed)
                .map(serde_json::Value::Number)
                .ok_or_else(|| {
                    SourceError::fatal(format!(
                        "`{path}` row {row} column `{column}`: non-finite value `{cell}` has no \
                         JSON representation — declare a utf8 type hint to load it as a string"
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
        // String-shaped logical types emit as strings; the stream
        // spec's hint types them downstream. EXHAUSTIVE deliberately —
        // a future hint variant must fail to compile here until its
        // CSV conversion is decided, never coerce to a string.
        Conversion::Declared(
            HintType::Utf8
            | HintType::TimestampTz
            | HintType::Date
            | HintType::Time
            | HintType::Uuid,
        ) => serde_json::Value::String(cell.to_owned()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The lattice, exhaustively: bottom, idempotence, the numeric
    /// widening, bool's disjointness, and commutativity everywhere.
    #[test]
    fn the_join_lattice_is_total_and_commutative() {
        use CellKind::*;
        for kind in [Empty, Bool, Int, Float, Text] {
            assert_eq!(join(Empty, kind), kind, "Empty is bottom");
            assert_eq!(join(kind, kind), kind, "idempotent");
        }
        assert_eq!(join(Int, Float), Float, "int widens");
        assert_eq!(join(Bool, Int), Text, "bool is disjoint from numbers");
        assert_eq!(join(Float, Text), Text);
        for a in [Empty, Bool, Int, Float, Text] {
            for b in [Empty, Bool, Int, Float, Text] {
                assert_eq!(join(a, b), join(b, a), "{a:?} {b:?}");
            }
        }
    }

    /// kind_of: the exact spellings that make a bool, the numeric
    /// parses, and everything else as text.
    #[test]
    fn cell_kinds_come_from_the_exact_rules() {
        assert_eq!(kind_of("true"), CellKind::Bool);
        assert_eq!(kind_of("false"), CellKind::Bool);
        assert_eq!(kind_of("TRUE"), CellKind::Text, "case matters");
        assert_eq!(kind_of("42"), CellKind::Int);
        assert_eq!(kind_of("4.5"), CellKind::Float);
        assert_eq!(kind_of("4e2"), CellKind::Float);
        assert_eq!(kind_of("x"), CellKind::Text);
    }

    /// An inferred bool that no longer parses is the TYPED two-pass
    /// failure — never a silent false.
    #[test]
    fn a_changed_file_between_passes_fails_typed() {
        for changed in ["yes", "1", "TRUE"] {
            let err = convert_cell(
                changed,
                Conversion::Inferred(CellKind::Bool),
                "f.csv",
                7,
                "flag",
            )
            .expect_err("typed");
            assert!(
                format!("{err}").contains("changed between the inference and conversion passes"),
                "{err}"
            );
        }
    }

    /// Declared-hint violations name file, row, and column; non-finite
    /// floats point at the utf8 escape hatch.
    #[test]
    fn hint_violations_and_non_finite_floats_are_typed() {
        let err = convert_cell("x", Conversion::Declared(HintType::Int64), "f.csv", 3, "n")
            .expect_err("violation");
        assert!(
            format!("{err}")
                .contains("`f.csv` row 3 column `n`: value does not satisfy the declared int64"),
            "{err}"
        );
        let err = convert_cell(
            "NaN",
            Conversion::Inferred(CellKind::Float),
            "f.csv",
            4,
            "v",
        )
        .expect_err("non-finite");
        assert!(
            format!("{err}").contains("declare a utf8 type hint to load it as a string"),
            "{err}"
        );
    }
}
