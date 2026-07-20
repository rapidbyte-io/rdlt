//! Cursor state (research R5, data-model §3): one engine `Cursor` (JSON) per
//! stream — `{watermark: <tagged typed scalar>, boundary_keys: [...]}`.
//! Watermarks are TYPE-TAGGED so state round-trips losslessly
//! (`decode(encode(v)) == v`, property-tested) and renders back into
//! injection-safe SQL literals (COPY accepts no binds).

use rdlt_connector::{Cursor, SourceError};
use serde::{Deserialize, Serialize};

use crate::errors::{self, Phase};
use crate::types::Decode;

/// A typed watermark value. Ordering is the cursor ordering (same-variant
/// comparisons only — the column type is fixed per stream).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "t", content = "v", rename_all = "snake_case")]
pub(crate) enum Watermark {
    /// int2/int4/int8 cursors.
    Int(i64),
    /// numeric(p≤38,s) cursors: the scale-preserved scaled integer (i128,
    /// serialized as text — it exceeds JSON number range) + the scale.
    Decimal {
        #[serde(with = "i128_text")]
        scaled: i128,
        scale: u8,
    },
    /// text family + uuid-as-text cursors.
    Text(String),
    /// µs since Unix epoch, UTC.
    TimestampTz(i64),
    /// µs since Unix epoch, no zone.
    TimestampNaive(i64),
    /// days since Unix epoch.
    Date(i32),
    /// µs since midnight.
    Time(i64),
}

impl Watermark {
    /// The injection-safe SQL literal (typed, exact): integer µs/day
    /// arithmetic against 'epoch' anchors — never float round-trips.
    pub fn to_sql_literal(&self) -> String {
        match self {
            Watermark::Int(v) => format!("{v}::int8"),
            Watermark::Decimal { scaled, scale } => {
                format!("'{}'::numeric", decimal_text(*scaled, *scale))
            }
            Watermark::Text(s) => format!("'{}'::text", s.replace('\'', "''")),
            Watermark::TimestampTz(us) => {
                format!("(TIMESTAMPTZ 'epoch' + {us}::int8 * INTERVAL '1 microsecond')")
            }
            Watermark::TimestampNaive(us) => {
                format!("(TIMESTAMP 'epoch' + {us}::int8 * INTERVAL '1 microsecond')")
            }
            Watermark::Date(days) => {
                format!("(DATE 'epoch' + {days}::int4)")
            }
            Watermark::Time(us) => {
                format!("(TIME '00:00' + {us}::int8 * INTERVAL '1 microsecond')")
            }
        }
    }

    /// Parse a config-provided literal (`initial_value`/`end_value`) for the
    /// cursor column's decode kind. Formats are the canonical text forms the
    /// type-mapping contract documents.
    pub fn parse_config_literal(decode: Decode, text: &str, table: &str) -> Result<Self, SourceError> {
        let bad = |detail: String| errors::fatal(Phase::Reflect, Some(table), detail);
        match decode {
            Decode::Int2 | Decode::Int4 | Decode::Int8 => text
                .parse::<i64>()
                .map(Watermark::Int)
                .map_err(|e| bad(format!("cursor literal `{text}` is not an integer: {e}"))),
            Decode::Decimal { scale, .. } => parse_decimal(text, scale)
                .map(|scaled| Watermark::Decimal { scaled, scale })
                .ok_or_else(|| bad(format!("cursor literal `{text}` is not a numeric(…,{scale})"))),
            Decode::Utf8 | Decode::UuidText => Ok(Watermark::Text(text.to_owned())),
            Decode::Timestamp { tz } => {
                let parsed = chrono_parse_us(text)
                    .ok_or_else(|| bad(format!("cursor literal `{text}` is not an RFC3339/ISO timestamp")))?;
                Ok(if tz { Watermark::TimestampTz(parsed) } else { Watermark::TimestampNaive(parsed) })
            }
            Decode::Date => parse_date_days(text)
                .map(Watermark::Date)
                .ok_or_else(|| bad(format!("cursor literal `{text}` is not a YYYY-MM-DD date"))),
            Decode::Time => parse_time_us(text)
                .map(Watermark::Time)
                .ok_or_else(|| bad(format!("cursor literal `{text}` is not a HH:MM[:SS[.ffffff]] time"))),
            _ => Err(bad("cursor column type is not cursor-capable".into())),
        }
    }
}

/// i128 (JSON can't carry it) ↔ decimal-string serde for the Decimal scaled
/// field.
mod i128_text {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &i128, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(&v.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<i128, D::Error> {
        let text = String::deserialize(de)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

fn decimal_text(scaled: i128, scale: u8) -> String {
    let sign = if scaled < 0 { "-" } else { "" };
    let digits = scaled.unsigned_abs().to_string();
    let s = scale as usize;
    if s == 0 {
        return format!("{sign}{digits}");
    }
    let padded = format!("{digits:0>width$}", width = s + 1);
    let (int, frac) = padded.split_at(padded.len() - s);
    format!("{sign}{int}.{frac}")
}

fn parse_decimal(text: &str, scale: u8) -> Option<i128> {
    let (sign, body) = match text.strip_prefix('-') {
        Some(rest) => (-1i128, rest),
        None => (1, text),
    };
    let (int_part, frac_part) = match body.split_once('.') {
        Some((i, f)) => (i, f),
        None => (body, ""),
    };
    if frac_part.len() > scale as usize {
        return None; // more precision than the column carries
    }
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    let mut digits = String::with_capacity(int_part.len() + scale as usize);
    digits.push_str(int_part);
    digits.push_str(frac_part);
    for _ in frac_part.len()..scale as usize {
        digits.push('0');
    }
    digits.parse::<i128>().ok().map(|v| sign * v)
}

fn chrono_parse_us(text: &str) -> Option<i64> {
    // RFC3339 first ("2026-01-02T03:04:05.123456Z"), then the space form.
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(text) {
        return Some(dt.timestamp_micros());
    }
    let naive = chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S%.f"))
        .ok()?;
    Some(naive.and_utc().timestamp_micros())
}

fn parse_date_days(text: &str) -> Option<i32> {
    let date = chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d").ok()?;
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?;
    i32::try_from((date - epoch).num_days()).ok()
}

fn parse_time_us(text: &str) -> Option<i64> {
    let time = chrono::NaiveTime::parse_from_str(text, "%H:%M:%S%.f")
        .or_else(|_| chrono::NaiveTime::parse_from_str(text, "%H:%M"))
        .ok()?;
    use chrono::Timelike;
    Some(time.num_seconds_from_midnight() as i64 * 1_000_000 + (time.nanosecond() / 1_000) as i64)
}


/// Per-stream incremental tracking (research R5): boundary dedup on resume,
/// watermark/prev-distinct tracking for mid-stream checkpoints, and the
/// final boundary-key run. Depends on cursor-ordered streams (NULLS FIRST,
/// direction-aligned) — the sqlgen ORDER BY guarantees it.
pub(crate) struct Tracker {
    cursor_idx: usize,
    decode: Decode,
    direction_max: bool,
    /// Stored state we resumed from (monotonic guard + dedup source).
    stored: Option<CursorState>,
    /// Column indices forming the row key; None ⇒ hash every column.
    key_columns: Option<Vec<usize>>,
    /// True until a row strictly beyond the stored watermark is seen.
    in_boundary: bool,
    last_value: Option<Watermark>,
    prev_distinct: Option<Watermark>,
    last_emitted: Option<Watermark>,
    run_keys: Vec<String>,
    pub deduped_rows: u64,
}

impl Tracker {
    pub fn new(
        cursor_idx: usize,
        decode: Decode,
        direction_max: bool,
        stored: Option<CursorState>,
        key_columns: Option<Vec<usize>>,
    ) -> Self {
        Self {
            cursor_idx,
            decode,
            direction_max,
            stored,
            key_columns,
            in_boundary: true,
            last_value: None,
            prev_distinct: None,
            last_emitted: None,
            run_keys: Vec::new(),
            deduped_rows: 0,
        }
    }

    fn beyond(&self, candidate: &Watermark, reference: &Watermark) -> bool {
        if self.direction_max {
            candidate > reference
        } else {
            candidate < reference
        }
    }

    fn row_key(&self, batch: &arrow_array::RecordBatch, row: usize) -> String {
        use arrow_cast::display::array_value_to_string;
        match &self.key_columns {
            Some(indices) => indices
                .iter()
                .map(|&i| {
                    if batch.column(i).is_null(row) {
                        "\u{2205}".to_owned()
                    } else {
                        array_value_to_string(batch.column(i), row).unwrap_or_default()
                    }
                })
                .collect::<Vec<_>>()
                .join("|"),
            None => {
                // PK-less table: hash the whole row's canonical rendering.
                let mut hasher = blake3::Hasher::new();
                for column in batch.columns() {
                    if column.is_null(row) {
                        hasher.update(b"\xff\x00");
                    } else {
                        hasher.update(
                            array_value_to_string(column, row).unwrap_or_default().as_bytes(),
                        );
                        hasher.update(b"\x00");
                    }
                }
                hasher.finalize().to_hex().to_string()
            }
        }
    }

    /// Process one decoded batch BEFORE pushing: dedup re-fetched boundary
    /// rows and update tracking. Returns the (possibly filtered) batch —
    /// `None` when every row was a duplicate — plus an intermediate
    /// checkpoint to emit AFTER the batch is pushed (S2: rows covered by it
    /// are then all pushed).
    pub fn process(
        &mut self,
        batch: arrow_array::RecordBatch,
    ) -> (Option<arrow_array::RecordBatch>, Option<CursorState>) {
        use arrow_array::builder::BooleanBuilder;

        let cursor_col = batch.column(self.cursor_idx).clone();
        let mut keep = BooleanBuilder::with_capacity(batch.num_rows());
        let mut any_dropped = false;
        let mut kept_rows: Vec<usize> = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let value = watermark_at(&cursor_col, self.decode, row);
            let mut drop = false;
            if self.in_boundary
                && let (Some(v), Some(stored)) = (&value, &self.stored)
            {
                if self.beyond(v, &stored.watermark) {
                    self.in_boundary = false;
                } else if *v == stored.watermark
                    && stored.boundary_keys.contains(&self.row_key(&batch, row))
                {
                    drop = true;
                }
            }
            keep.append_value(!drop);
            if drop {
                self.deduped_rows += 1;
                any_dropped = true;
            } else {
                kept_rows.push(row);
            }
        }
        // Track kept rows only (dropped rows belong to the PRIOR run).
        for &row in &kept_rows {
            let Some(value) = watermark_at(&cursor_col, self.decode, row) else {
                continue; // NULL cursors are re-fetched every run, never tracked
            };
            match &self.last_value {
                Some(last) if *last == value => {
                    let key = self.row_key(&batch, row);
                    self.run_keys.push(key);
                }
                Some(last) => {
                    debug_assert!(
                        self.beyond(&value, last),
                        "cursor stream not ordered — sqlgen ORDER BY violated"
                    );
                    self.prev_distinct = self.last_value.take();
                    self.last_value = Some(value);
                    self.run_keys = vec![self.row_key(&batch, row)];
                }
                None => {
                    self.last_value = Some(value);
                    self.run_keys = vec![self.row_key(&batch, row)];
                }
            }
        }
        let out_batch = if any_dropped {
            let mask = keep.finish();
            let filtered = arrow_select::filter::filter_record_batch(&batch, &mask)
                .expect("filter mask length matches");
            (filtered.num_rows() > 0).then_some(filtered)
        } else {
            Some(batch)
        };
        // Intermediate checkpoint: the last COMPLETED distinct value (a
        // greater one has appeared), if it advances past stored state and
        // was not already emitted. Empty keys ⇒ strict `>` resume.
        let checkpoint = match (&self.prev_distinct, &self.stored) {
            (Some(candidate), stored)
                if stored
                    .as_ref()
                    .is_none_or(|s| self.beyond(candidate, &s.watermark))
                    && self.last_emitted.as_ref() != Some(candidate) =>
            {
                self.last_emitted = Some(candidate.clone());
                Some(CursorState {
                    watermark: candidate.clone(),
                    boundary_keys: Vec::new(),
                })
            }
            _ => None,
        };
        (out_batch, checkpoint)
    }

    /// The final checkpoint at stream end: watermark = last value seen, with
    /// the boundary-key run under a closed boundary (`keep_keys`). `None`
    /// when nothing advanced past the stored state (regressing-clock guard:
    /// the watermark NEVER moves backward).
    pub fn final_state(&self, keep_keys: bool) -> Option<CursorState> {
        let last = self.last_value.as_ref()?;
        if let Some(stored) = &self.stored {
            if !self.beyond(last, &stored.watermark) && *last != stored.watermark {
                return None; // regressed — keep the stored watermark
            }
            if *last == stored.watermark && self.run_keys.is_empty() {
                return None; // nothing new at the boundary
            }
        }
        let mut boundary_keys = if keep_keys { self.run_keys.clone() } else { Vec::new() };
        // Rows equal to an UNCHANGED stored watermark merge with its old keys
        // (they were deduped against it and remain committed).
        if let Some(stored) = &self.stored
            && *last == stored.watermark
            && keep_keys
        {
            boundary_keys.extend(stored.boundary_keys.iter().cloned());
            boundary_keys.sort();
            boundary_keys.dedup();
        }
        Some(CursorState {
            watermark: last.clone(),
            boundary_keys,
        })
    }
}

/// Extract a typed watermark from a decoded column at `row` (None = NULL).
pub(crate) fn watermark_at(
    column: &arrow_array::ArrayRef,
    decode: Decode,
    row: usize,
) -> Option<Watermark> {
    use arrow_array::{
        Array, Date32Array, Decimal128Array, Int64Array, StringArray,
        Time64MicrosecondArray, TimestampMicrosecondArray,
    };
    if column.is_null(row) {
        return None;
    }
    let any = column.as_any();
    match decode {
        Decode::Int2 | Decode::Int4 | Decode::Int8 => {
            Some(Watermark::Int(any.downcast_ref::<Int64Array>()?.value(row)))
        }
        Decode::Decimal { scale, .. } => Some(Watermark::Decimal {
            scaled: any.downcast_ref::<Decimal128Array>()?.value(row),
            scale,
        }),
        Decode::Utf8 | Decode::UuidText => Some(Watermark::Text(
            any.downcast_ref::<StringArray>()?.value(row).to_owned(),
        )),
        Decode::Timestamp { tz } => {
            let v = any.downcast_ref::<TimestampMicrosecondArray>()?.value(row);
            Some(if tz { Watermark::TimestampTz(v) } else { Watermark::TimestampNaive(v) })
        }
        Decode::Date => Some(Watermark::Date(any.downcast_ref::<Date32Array>()?.value(row))),
        Decode::Time => Some(Watermark::Time(
            any.downcast_ref::<Time64MicrosecondArray>()?.value(row),
        )),
        _ => None,
    }
}

/// The persisted per-stream state (data-model §3).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct CursorState {
    pub watermark: Watermark,
    /// Keys of rows whose cursor equals the watermark (PK canonical text,
    /// else row hash). Empty ⇒ strict `>` resume; non-empty ⇒ `>=` + dedup.
    #[serde(default)]
    pub boundary_keys: Vec<String>,
}

impl CursorState {
    pub fn encode(&self) -> Cursor {
        Cursor::new(serde_json::to_value(self).expect("cursor state serializes"))
    }

    pub fn decode(cursor: &Cursor, table: &str) -> Result<Self, SourceError> {
        serde_json::from_value(cursor.as_value().clone()).map_err(|e| {
            errors::fatal(
                Phase::Reflect,
                Some(table),
                format!("stored cursor state does not decode (state corruption?): {e}"),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn watermark_strategy() -> impl Strategy<Value = Watermark> {
        prop_oneof![
            any::<i64>().prop_map(Watermark::Int),
            (any::<i64>(), 0u8..=8).prop_map(|(v, scale)| Watermark::Decimal {
                scaled: v as i128,
                scale
            }),
            "[ -~π🦀']{0,24}".prop_map(Watermark::Text),
            any::<i64>().prop_map(Watermark::TimestampTz),
            any::<i64>().prop_map(Watermark::TimestampNaive),
            any::<i32>().prop_map(Watermark::Date),
            (0i64..86_400_000_000).prop_map(Watermark::Time),
        ]
    }

    proptest! {
        // Data-model validation rule: decode(encode(state)) == state.
        #[test]
        fn state_round_trips(w in watermark_strategy(), keys in proptest::collection::vec("[a-z0-9|]{0,12}", 0..4)) {
            let state = CursorState { watermark: w, boundary_keys: keys };
            let decoded = CursorState::decode(&state.encode(), "t").expect("decode");
            prop_assert_eq!(state, decoded);
        }
    }

    #[test]
    fn literals_are_typed_and_escaped() {
        assert_eq!(Watermark::Int(42).to_sql_literal(), "42::int8");
        assert_eq!(
            Watermark::Text("O'Brien'; DROP--".into()).to_sql_literal(),
            "'O''Brien''; DROP--'::text"
        );
        assert_eq!(
            Watermark::Decimal { scaled: -12345, scale: 2 }.to_sql_literal(),
            "'-123.45'::numeric"
        );
        assert_eq!(
            Watermark::Decimal { scaled: 5, scale: 4 }.to_sql_literal(),
            "'0.0005'::numeric"
        );
        assert_eq!(
            Watermark::TimestampTz(1_000_000).to_sql_literal(),
            "(TIMESTAMPTZ 'epoch' + 1000000::int8 * INTERVAL '1 microsecond')"
        );
        assert_eq!(Watermark::Date(20_000).to_sql_literal(), "(DATE 'epoch' + 20000::int4)");
    }

    #[test]
    fn config_literals_parse_per_type() {
        use crate::types::Decode;
        assert_eq!(
            Watermark::parse_config_literal(Decode::Int8, "17", "t").expect("int"),
            Watermark::Int(17)
        );
        assert_eq!(
            Watermark::parse_config_literal(Decode::Decimal { precision: 10, scale: 2 }, "1.5", "t")
                .expect("decimal"),
            Watermark::Decimal { scaled: 150, scale: 2 }
        );
        assert_eq!(
            Watermark::parse_config_literal(Decode::Timestamp { tz: true }, "2026-01-01T00:00:00Z", "t")
                .expect("ts"),
            Watermark::TimestampTz(1_767_225_600_000_000)
        );
        assert_eq!(
            Watermark::parse_config_literal(Decode::Date, "1970-01-02", "t").expect("date"),
            Watermark::Date(1)
        );
        assert!(Watermark::parse_config_literal(Decode::Int8, "abc", "t").is_err());
        assert!(
            Watermark::parse_config_literal(Decode::Decimal { precision: 10, scale: 2 }, "1.234", "t")
                .is_err(),
            "more precision than the column carries"
        );
    }
}
