//! Binary COPY stream → Arrow batches (research R1/R4 — THE hot path).
//!
//! Parses the PostgreSQL binary COPY format (19-byte header; per tuple an
//! i16 field count then length-prefixed fields, NULL = -1; i16 -1 trailer)
//! incrementally — chunk boundaries never align with rows — appending
//! straight into Arrow builders. Batches cut at `batch_target_bytes` /
//! `batch_max_rows`, only ever on row boundaries.
//!
//! Total decoder: every failure is a typed [`DecodeError`]; no panics, no
//! silent coercion (fuzz target: `pg_copy_decode`).

use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder, Int64Builder,
    StringBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};

use crate::types::Decode;

/// µs from the PG epoch (2000-01-01) to the Unix epoch.
const PG_EPOCH_US: i64 = 946_684_800_000_000;
/// Days from the PG epoch to the Unix epoch.
const PG_EPOCH_DAYS: i32 = 10_957;
const HEADER_SIGNATURE: &[u8; 11] = b"PGCOPY\n\xff\r\n\0";

#[derive(Debug, thiserror::Error)]
#[error("binary COPY decode: {0}")]
pub(crate) struct DecodeError(pub String);

fn err<T>(detail: impl Into<String>) -> Result<T, DecodeError> {
    Err(DecodeError(detail.into()))
}

/// Per-column plan the reader assembles from reflection.
#[derive(Debug, Clone)]
pub(crate) struct FieldPlan {
    pub name: String,
    pub decode: Decode,
    pub arrow: DataType,
    pub not_null: bool,
}

#[derive(Debug)]
enum ColumnBuilder {
    Bool(BooleanBuilder),
    Int64(Int64Builder),
    Float64(Float64Builder),
    Decimal(Decimal128Builder),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
    Timestamp(TimestampMicrosecondBuilder),
    Date(Date32Builder),
    Time(Time64MicrosecondBuilder),
}

impl ColumnBuilder {
    fn for_plan(plan: &FieldPlan) -> Self {
        match plan.decode {
            Decode::Bool => Self::Bool(BooleanBuilder::new()),
            Decode::Int2 | Decode::Int4 | Decode::Int8 => Self::Int64(Int64Builder::new()),
            Decode::Float4 | Decode::Float8 => Self::Float64(Float64Builder::new()),
            Decode::Decimal { precision, scale } => Self::Decimal(
                Decimal128Builder::new()
                    .with_precision_and_scale(precision, scale as i8)
                    .expect("reflection produced a valid decimal shape"),
            ),
            Decode::Utf8 | Decode::JsonbText | Decode::UuidText => Self::Utf8(StringBuilder::new()),
            Decode::Bytea => Self::Binary(BinaryBuilder::new()),
            Decode::Timestamp { tz } => {
                let b = TimestampMicrosecondBuilder::new();
                Self::Timestamp(if tz { b.with_timezone("UTC") } else { b })
            }
            Decode::Date => Self::Date(Date32Builder::new()),
            Decode::Time => Self::Time(Time64MicrosecondBuilder::new()),
        }
    }

    fn append_null(&mut self) {
        match self {
            Self::Bool(b) => b.append_null(),
            Self::Int64(b) => b.append_null(),
            Self::Float64(b) => b.append_null(),
            Self::Decimal(b) => b.append_null(),
            Self::Utf8(b) => b.append_null(),
            Self::Binary(b) => b.append_null(),
            Self::Timestamp(b) => b.append_null(),
            Self::Date(b) => b.append_null(),
            Self::Time(b) => b.append_null(),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Bool(b) => Arc::new(b.finish()),
            Self::Int64(b) => Arc::new(b.finish()),
            Self::Float64(b) => Arc::new(b.finish()),
            Self::Decimal(b) => Arc::new(b.finish()),
            Self::Utf8(b) => Arc::new(b.finish()),
            Self::Binary(b) => Arc::new(b.finish()),
            Self::Timestamp(b) => Arc::new(b.finish()),
            Self::Date(b) => Arc::new(b.finish()),
            Self::Time(b) => Arc::new(b.finish()),
        }
    }
}

fn be_i16(bytes: &[u8]) -> Result<i16, DecodeError> {
    match bytes.try_into() {
        Ok(arr) => Ok(i16::from_be_bytes(arr)),
        Err(_) => err("short i16"),
    }
}

fn be_i32(bytes: &[u8]) -> Result<i32, DecodeError> {
    match bytes.try_into() {
        Ok(arr) => Ok(i32::from_be_bytes(arr)),
        Err(_) => err("short i32"),
    }
}

fn be_i64(bytes: &[u8]) -> Result<i64, DecodeError> {
    match bytes.try_into() {
        Ok(arr) => Ok(i64::from_be_bytes(arr)),
        Err(_) => err("short i64"),
    }
}

fn exact<const N: usize>(field: &[u8], what: &str) -> Result<[u8; N], DecodeError> {
    field
        .try_into()
        .map_err(|_| DecodeError(format!("{what}: expected {N} bytes, got {}", field.len())))
}

/// PG binary numeric → i128 at the declared scale (contract: NBASE-10000
/// digits; NaN/±Inf are decode errors under `Decimal`).
fn decode_numeric(field: &[u8], scale: u8) -> Result<i128, DecodeError> {
    if field.len() < 8 {
        return err("numeric: header short");
    }
    let ndigits = be_i16(&field[0..2])? as i32;
    let weight = be_i16(&field[2..4])? as i32;
    let sign = u16::from_be_bytes([field[4], field[5]]);
    let negative = match sign {
        0x0000 => false,
        0x4000 => true,
        0xC000 => {
            return err("numeric NaN is not representable as Decimal (contract: typed error)");
        }
        0xD000 | 0xF000 => {
            return err(
                "numeric ±Infinity is not representable as Decimal (contract: typed error)",
            );
        }
        other => return err(format!("numeric: unknown sign word {other:#06x}")),
    };
    if !(0..=0x4000).contains(&(ndigits as i64)) || field.len() < 8 + (ndigits as usize) * 2 {
        return err("numeric: digit count exceeds field");
    }
    let mut acc: i128 = 0;
    for i in 0..ndigits as usize {
        let d = u16::from_be_bytes([field[8 + i * 2], field[9 + i * 2]]) as i128;
        if d >= 10_000 {
            return err("numeric: base-10000 digit out of range");
        }
        acc = acc
            .checked_mul(10_000)
            .and_then(|a| a.checked_add(d))
            .ok_or_else(|| DecodeError("numeric: overflow while accumulating digits".into()))?;
    }
    // The accumulated integer is value × 10000^(ndigits-1-weight); rescale to
    // the declared scale exactly or report drift.
    let exp10 = 4 * (weight - ndigits + 1) + scale as i32;
    let mut value = acc;
    if exp10 >= 0 {
        for _ in 0..exp10 {
            value = value
                .checked_mul(10)
                .ok_or_else(|| DecodeError("numeric: overflow while rescaling".into()))?;
        }
    } else {
        for _ in 0..(-exp10) {
            if value % 10 != 0 {
                return err(
                    "numeric: more fractional digits on the wire than the declared scale \
                     (schema drift)",
                );
            }
            value /= 10;
        }
    }
    Ok(if negative { -value } else { value })
}

/// i64 µs since PG epoch → Unix µs; ±infinity sentinels saturate (contract
/// "Special values": extremes stay visible, never NULL, never an error).
fn rebase_timestamp(v: i64) -> i64 {
    if v == i64::MAX || v == i64::MIN {
        return v;
    }
    v.checked_add(PG_EPOCH_US)
        .unwrap_or(if v > 0 { i64::MAX } else { i64::MIN })
}

fn rebase_date(v: i32) -> i32 {
    if v == i32::MAX || v == i32::MIN {
        return v;
    }
    v.checked_add(PG_EPOCH_DAYS)
        .unwrap_or(if v > 0 { i32::MAX } else { i32::MIN })
}

fn uuid_text(bytes: &[u8; 16]) -> String {
    let mut s = String::with_capacity(36);
    for (i, b) in bytes.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            s.push('-');
        }
        let hex = b"0123456789abcdef";
        s.push(hex[(b >> 4) as usize] as char);
        s.push(hex[(b & 0xf) as usize] as char);
    }
    s
}

#[derive(Debug, PartialEq, Eq)]
enum State {
    Header,
    Tuples,
    Done,
}

#[derive(Debug)]
pub(crate) struct CopyDecoder {
    plans: Vec<FieldPlan>,
    schema: Arc<Schema>,
    builders: Vec<ColumnBuilder>,
    state: State,
    /// Unparsed carry-over between `feed` calls (chunks split mid-tuple).
    buf: Vec<u8>,
    rows_in_batch: usize,
    bytes_in_batch: usize,
    rows_total: u64,
    target_bytes: usize,
    max_rows: usize,
}

impl CopyDecoder {
    pub fn new(plans: Vec<FieldPlan>, target_bytes: usize, max_rows: usize) -> Self {
        let schema = Arc::new(Schema::new(
            plans
                .iter()
                .map(|p| Field::new(&p.name, p.arrow.clone(), !p.not_null))
                .collect::<Vec<_>>(),
        ));
        let builders = plans.iter().map(ColumnBuilder::for_plan).collect();
        Self {
            plans,
            schema,
            builders,
            state: State::Header,
            buf: Vec::new(),
            rows_in_batch: 0,
            bytes_in_batch: 0,
            rows_total: 0,
            target_bytes: target_bytes.max(1),
            max_rows: max_rows.max(1),
        }
    }

    pub fn rows_decoded(&self) -> u64 {
        self.rows_total
    }

    /// A zero-row batch carrying the declared schema — pushed for empty
    /// tables so destinations still materialize them with correct types.
    pub fn empty_batch(&self) -> RecordBatch {
        RecordBatch::new_empty(Arc::clone(&self.schema))
    }

    /// Feed one COPY data chunk; returns every batch completed by it.
    pub fn feed(&mut self, chunk: &[u8]) -> Result<Vec<RecordBatch>, DecodeError> {
        if self.state == State::Done {
            if chunk.is_empty() {
                return Ok(Vec::new());
            }
            return err("data after COPY trailer");
        }
        self.buf.extend_from_slice(chunk);
        let mut pos = 0usize;
        let mut batches = Vec::new();

        if self.state == State::Header {
            match self.try_consume_header(pos)? {
                Some(next) => {
                    pos = next;
                    self.state = State::Tuples;
                }
                None => return Ok(batches), // need more bytes
            }
        }

        while self.state == State::Tuples {
            match self.try_consume_tuple(pos)? {
                Consumed::NeedMore => break,
                Consumed::Trailer(next) => {
                    pos = next;
                    self.state = State::Done;
                    if self.buf.len() > pos {
                        return err("data after COPY trailer");
                    }
                }
                Consumed::Row(next) => {
                    pos = next;
                    if self.rows_in_batch >= self.max_rows
                        || self.bytes_in_batch >= self.target_bytes
                    {
                        batches.push(self.cut_batch()?);
                    }
                }
            }
        }
        self.buf.drain(..pos);
        Ok(batches)
    }

    /// Stream end: the trailer must have been seen; flush the tail batch.
    pub fn finish(&mut self) -> Result<Option<RecordBatch>, DecodeError> {
        if self.state != State::Done {
            return err("COPY stream ended before the binary trailer (truncated stream)");
        }
        if !self.buf.is_empty() {
            return err("trailing bytes after COPY trailer");
        }
        if self.rows_in_batch == 0 {
            return Ok(None);
        }
        Ok(Some(self.cut_batch()?))
    }

    fn cut_batch(&mut self) -> Result<RecordBatch, DecodeError> {
        let arrays: Vec<ArrayRef> = self
            .builders
            .iter_mut()
            .map(ColumnBuilder::finish)
            .collect();
        self.rows_in_batch = 0;
        self.bytes_in_batch = 0;
        RecordBatch::try_new(Arc::clone(&self.schema), arrays)
            .map_err(|e| DecodeError(format!("assembling RecordBatch: {e}")))
    }

    /// Header: signature + flags(4) + extension length(4) + extension.
    fn try_consume_header(&self, pos: usize) -> Result<Option<usize>, DecodeError> {
        let buf = &self.buf[pos..];
        if buf.len() < 19 {
            return Ok(None);
        }
        if &buf[..11] != HEADER_SIGNATURE {
            return err("bad COPY binary signature");
        }
        let ext_len = be_i32(&buf[15..19])?;
        if ext_len < 0 {
            return err("negative header extension length");
        }
        let total = 19usize + ext_len as usize;
        if buf.len() < total {
            return Ok(None);
        }
        Ok(Some(pos + total))
    }

    fn try_consume_tuple(&mut self, pos: usize) -> Result<Consumed, DecodeError> {
        // Split borrows: parse positions from an immutable window while
        // builders mutate — compute field ranges first, then append.
        let buf_len = self.buf.len();
        if buf_len - pos < 2 {
            return Ok(Consumed::NeedMore);
        }
        let field_count = be_i16(&self.buf[pos..pos + 2])?;
        if field_count == -1 {
            return Ok(Consumed::Trailer(pos + 2));
        }
        if field_count as usize != self.plans.len() {
            return err(format!(
                "tuple has {field_count} fields, schema has {} (drift)",
                self.plans.len()
            ));
        }
        // First pass: locate every field; bail NeedMore on a partial tuple.
        let mut ranges: Vec<Option<(usize, usize)>> = Vec::with_capacity(self.plans.len());
        let mut cursor = pos + 2;
        let mut tuple_bytes = 2usize;
        for _ in 0..self.plans.len() {
            if buf_len - cursor < 4 {
                return Ok(Consumed::NeedMore);
            }
            let len = be_i32(&self.buf[cursor..cursor + 4])?;
            cursor += 4;
            tuple_bytes += 4;
            if len == -1 {
                ranges.push(None);
                continue;
            }
            if len < 0 {
                return err(format!("negative field length {len}"));
            }
            let len = len as usize;
            if buf_len - cursor < len {
                return Ok(Consumed::NeedMore);
            }
            ranges.push(Some((cursor, cursor + len)));
            cursor += len;
            tuple_bytes += len;
        }
        // Second pass: whole tuple is buffered — decode and append.
        for (idx, range) in ranges.iter().enumerate() {
            match range {
                None => {
                    if self.plans[idx].not_null {
                        return err(format!(
                            "NULL on NOT NULL column `{}` (schema drift)",
                            self.plans[idx].name
                        ));
                    }
                    self.builders[idx].append_null();
                }
                Some((start, end)) => {
                    let field: &[u8] = &self.buf[*start..*end];
                    let plan = &self.plans[idx];
                    let column = plan.name.as_str();
                    let fail = |detail: String| DecodeError(format!("column `{column}`: {detail}"));
                    match (&mut self.builders[idx], plan.decode) {
                        (ColumnBuilder::Bool(b), Decode::Bool) => {
                            let [v] = exact::<1>(field, "bool").map_err(|e| fail(e.0))?;
                            b.append_value(v != 0);
                        }
                        (ColumnBuilder::Int64(b), Decode::Int2) => {
                            b.append_value(be_i16(field).map_err(|e| fail(e.0))? as i64);
                        }
                        (ColumnBuilder::Int64(b), Decode::Int4) => {
                            b.append_value(be_i32(field).map_err(|e| fail(e.0))? as i64);
                        }
                        (ColumnBuilder::Int64(b), Decode::Int8) => {
                            b.append_value(be_i64(field).map_err(|e| fail(e.0))?);
                        }
                        (ColumnBuilder::Float64(b), Decode::Float4) => {
                            let arr = exact::<4>(field, "float4").map_err(|e| fail(e.0))?;
                            b.append_value(f32::from_be_bytes(arr) as f64);
                        }
                        (ColumnBuilder::Float64(b), Decode::Float8) => {
                            let arr = exact::<8>(field, "float8").map_err(|e| fail(e.0))?;
                            b.append_value(f64::from_be_bytes(arr));
                        }
                        (ColumnBuilder::Decimal(b), Decode::Decimal { scale, .. }) => {
                            b.append_value(decode_numeric(field, scale).map_err(|e| fail(e.0))?);
                        }
                        (ColumnBuilder::Utf8(b), Decode::Utf8) => {
                            let s = std::str::from_utf8(field)
                                .map_err(|_| fail("invalid UTF-8 in text field".into()))?;
                            b.append_value(s);
                        }
                        (ColumnBuilder::Utf8(b), Decode::JsonbText) => {
                            let (&version, rest) = field
                                .split_first()
                                .ok_or_else(|| fail("empty jsonb field".into()))?;
                            if version != 1 {
                                return Err(fail(format!("unknown jsonb version {version}")));
                            }
                            let s = std::str::from_utf8(rest)
                                .map_err(|_| fail("invalid UTF-8 in jsonb".into()))?;
                            b.append_value(s);
                        }
                        (ColumnBuilder::Utf8(b), Decode::UuidText) => {
                            let arr = exact::<16>(field, "uuid").map_err(|e| fail(e.0))?;
                            b.append_value(uuid_text(&arr));
                        }
                        (ColumnBuilder::Binary(b), Decode::Bytea) => b.append_value(field),
                        (ColumnBuilder::Timestamp(b), Decode::Timestamp { .. }) => {
                            let v = be_i64(field).map_err(|e| fail(e.0))?;
                            b.append_value(rebase_timestamp(v));
                        }
                        (ColumnBuilder::Date(b), Decode::Date) => {
                            let v = be_i32(field).map_err(|e| fail(e.0))?;
                            b.append_value(rebase_date(v));
                        }
                        (ColumnBuilder::Time(b), Decode::Time) => {
                            b.append_value(be_i64(field).map_err(|e| fail(e.0))?);
                        }
                        _ => return Err(fail("builder/decode mismatch (internal)".into())),
                    }
                }
            }
        }
        self.rows_in_batch += 1;
        self.bytes_in_batch += tuple_bytes;
        self.rows_total += 1;
        Ok(Consumed::Row(cursor))
    }
}

enum Consumed {
    NeedMore,
    Row(usize),
    Trailer(usize),
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Decimal128Array, Int64Array, StringArray, TimestampMicrosecondArray};

    fn plan(name: &str, decode: Decode, arrow: DataType, not_null: bool) -> FieldPlan {
        FieldPlan {
            name: name.into(),
            decode,
            arrow,
            not_null,
        }
    }

    fn header() -> Vec<u8> {
        let mut v = HEADER_SIGNATURE.to_vec();
        v.extend_from_slice(&0i32.to_be_bytes()); // flags
        v.extend_from_slice(&0i32.to_be_bytes()); // extension length
        v
    }

    fn tuple(fields: &[Option<Vec<u8>>]) -> Vec<u8> {
        let mut v = (fields.len() as i16).to_be_bytes().to_vec();
        for f in fields {
            match f {
                None => v.extend_from_slice(&(-1i32).to_be_bytes()),
                Some(bytes) => {
                    v.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                    v.extend_from_slice(bytes);
                }
            }
        }
        v
    }

    fn trailer() -> Vec<u8> {
        (-1i16).to_be_bytes().to_vec()
    }

    fn numeric_wire(ndigits: i16, weight: i16, sign: u16, dscale: u16, digits: &[u16]) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(&ndigits.to_be_bytes());
        v.extend_from_slice(&weight.to_be_bytes());
        v.extend_from_slice(&sign.to_be_bytes());
        v.extend_from_slice(&dscale.to_be_bytes());
        for d in digits {
            v.extend_from_slice(&d.to_be_bytes());
        }
        v
    }

    fn two_col_decoder() -> CopyDecoder {
        CopyDecoder::new(
            vec![
                plan("id", Decode::Int8, DataType::Int64, true),
                plan("name", Decode::Utf8, DataType::Utf8, false),
            ],
            usize::MAX >> 1,
            usize::MAX >> 1,
        )
    }

    #[test]
    fn decodes_rows_across_chunk_splits() {
        let mut wire = header();
        wire.extend(tuple(&[
            Some(7i64.to_be_bytes().to_vec()),
            Some(b"seven".to_vec()),
        ]));
        wire.extend(tuple(&[Some(8i64.to_be_bytes().to_vec()), None]));
        wire.extend(trailer());

        // Feed byte-by-byte: worst-case chunk misalignment.
        let mut decoder = two_col_decoder();
        let mut batches = Vec::new();
        for b in &wire {
            batches.extend(decoder.feed(std::slice::from_ref(b)).expect("feed"));
        }
        let tail = decoder.finish().expect("finish").expect("batch");
        batches.push(tail);
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
        assert_eq!(decoder.rows_decoded(), 2);
        let batch = &batches[0];
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 7);
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "seven");
        assert!(names.is_null(1));
    }

    #[test]
    fn batch_cuts_on_max_rows() {
        let mut decoder = CopyDecoder::new(
            vec![plan("id", Decode::Int8, DataType::Int64, true)],
            usize::MAX >> 1,
            2, // cut every 2 rows
        );
        let mut wire = header();
        for i in 0..5i64 {
            wire.extend(tuple(&[Some(i.to_be_bytes().to_vec())]));
        }
        wire.extend(trailer());
        let mut batches = decoder.feed(&wire).expect("feed");
        if let Some(tail) = decoder.finish().expect("finish") {
            batches.push(tail);
        }
        assert_eq!(
            batches.iter().map(|b| b.num_rows()).collect::<Vec<_>>(),
            [2, 2, 1]
        );
    }

    #[test]
    fn numeric_decodes_at_declared_scale() {
        // 1.25 as numeric(10,2): digits [1, 2500], weight 0, dscale 2.
        let mut decoder = CopyDecoder::new(
            vec![plan(
                "n",
                Decode::Decimal {
                    precision: 10,
                    scale: 2,
                },
                DataType::Decimal128(10, 2),
                false,
            )],
            usize::MAX >> 1,
            usize::MAX >> 1,
        );
        let mut wire = header();
        wire.extend(tuple(&[Some(numeric_wire(2, 0, 0x0000, 2, &[1, 2500]))]));
        // -0.05: digits [500], weight -1, sign negative.
        wire.extend(tuple(&[Some(numeric_wire(1, -1, 0x4000, 2, &[500]))]));
        // 12345678.99: digits [1234, 5678, 9900], weight 1.
        wire.extend(tuple(&[Some(numeric_wire(
            3,
            1,
            0x0000,
            2,
            &[1234, 5678, 9900],
        ))]));
        wire.extend(trailer());
        let mut batches = decoder.feed(&wire).expect("feed");
        if let Some(tail) = decoder.finish().expect("finish") {
            batches.push(tail);
        }
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(col.value(0), 125); // 1.25 @ scale 2
        assert_eq!(col.value(1), -5); // -0.05
        assert_eq!(col.value(2), 1_234_567_899); // 12345678.99
    }

    #[test]
    fn numeric_nan_and_infinity_are_typed_errors() {
        for sign in [0xC000u16, 0xD000, 0xF000] {
            let e = decode_numeric(&numeric_wire(0, 0, sign, 0, &[]), 2).unwrap_err();
            assert!(e.0.contains("not representable"), "{sign:#06x}: {e}");
        }
    }

    #[test]
    fn timestamp_rebase_and_infinity_saturation() {
        assert_eq!(rebase_timestamp(0), PG_EPOCH_US);
        assert_eq!(rebase_timestamp(i64::MAX), i64::MAX);
        assert_eq!(rebase_timestamp(i64::MIN), i64::MIN);
        assert_eq!(rebase_date(0), PG_EPOCH_DAYS);
        assert_eq!(rebase_date(i32::MAX), i32::MAX);
        assert_eq!(rebase_date(i32::MIN), i32::MIN);
    }

    #[test]
    fn timestamps_land_as_utc_microseconds() {
        let mut decoder = CopyDecoder::new(
            vec![plan(
                "ts",
                Decode::Timestamp { tz: true },
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
                false,
            )],
            usize::MAX >> 1,
            usize::MAX >> 1,
        );
        let mut wire = header();
        wire.extend(tuple(&[Some(0i64.to_be_bytes().to_vec())])); // PG epoch
        wire.extend(trailer());
        let mut batches = decoder.feed(&wire).expect("feed");
        if let Some(tail) = decoder.finish().expect("finish") {
            batches.push(tail);
        }
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(col.value(0), PG_EPOCH_US);
    }

    #[test]
    fn uuid_renders_canonical_lowercase() {
        let bytes: [u8; 16] = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        assert_eq!(uuid_text(&bytes), "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn drift_and_truncation_are_typed_errors() {
        // NULL on NOT NULL column.
        let mut decoder = two_col_decoder();
        let mut wire = header();
        wire.extend(tuple(&[None, Some(b"x".to_vec())]));
        let e = decoder.feed(&wire).unwrap_err();
        assert!(e.0.contains("NOT NULL"), "{e}");

        // Wrong field count.
        let mut decoder = two_col_decoder();
        let mut wire = header();
        wire.extend(tuple(&[Some(1i64.to_be_bytes().to_vec())]));
        let e = decoder.feed(&wire).unwrap_err();
        assert!(e.0.contains("drift"), "{e}");

        // Truncated stream: no trailer before finish.
        let mut decoder = two_col_decoder();
        decoder.feed(&header()).expect("header only");
        let e = decoder.finish().unwrap_err();
        assert!(e.0.contains("truncated"), "{e}");

        // Bad signature.
        let mut decoder = two_col_decoder();
        let e = decoder.feed(&[0u8; 19]).unwrap_err();
        assert!(e.0.contains("signature"), "{e}");

        // Data after trailer.
        let mut decoder = two_col_decoder();
        let mut wire = header();
        wire.extend(trailer());
        wire.push(0);
        let e = decoder.feed(&wire).unwrap_err();
        assert!(e.0.contains("after COPY trailer"), "{e}");
    }

    #[test]
    fn jsonb_version_byte_stripped() {
        let mut decoder = CopyDecoder::new(
            vec![plan("j", Decode::JsonbText, DataType::Utf8, false)],
            usize::MAX >> 1,
            usize::MAX >> 1,
        );
        let mut wire = header();
        let mut payload = vec![1u8];
        payload.extend_from_slice(br#"{"a":1}"#);
        wire.extend(tuple(&[Some(payload)]));
        wire.extend(trailer());
        let mut batches = decoder.feed(&wire).expect("feed");
        if let Some(tail) = decoder.finish().expect("finish") {
            batches.push(tail);
        }
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(col.value(0), r#"{"a":1}"#);
    }
}
