//! Cursor state: one engine `Cursor` (JSON) per
//! stream — `{watermark: <tagged typed scalar>, boundary_keys: [...]}`.
//! Watermarks are TYPE-TAGGED so state round-trips losslessly
//! (`decode(encode(v)) == v`, property-tested) and renders back into
//! injection-safe SQL literals (COPY accepts no binds).

use rdlt_connector::{Cursor, SourceError};
use serde::{Deserialize, Serialize};

use crate::source::config::{self, CursorConfig, TableConfig};
use crate::source::errors::{self, Phase};
use crate::source::reflect::{ReflectedColumn, ReflectedTable};
use crate::source::sqlgen;
use crate::source::types::Decode;

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
    /// text family cursors.
    Text(String),
    /// uuid cursors (canonical lowercase-hex text; byte order == PG uuid
    /// order, and the SQL literal is `::uuid`-typed — `uuid >= text` has no
    /// operator, so an untyped literal would fail).
    Uuid(String),
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
            Watermark::Uuid(s) => format!("'{}'::uuid", s.replace('\'', "''")),
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
    /// cursor column's decode kind. Formats are the canonical text forms of
    /// the type mapping.
    pub fn parse_config_literal(
        decode: Decode,
        text: &str,
        table: &str,
    ) -> Result<Self, SourceError> {
        let bad = |detail: String| errors::fatal(Phase::Reflect, Some(table), detail);
        match decode {
            Decode::Int2 | Decode::Int4 | Decode::Int8 => text
                .parse::<i64>()
                .map(Watermark::Int)
                .map_err(|e| bad(format!("cursor literal `{text}` is not an integer: {e}"))),
            Decode::Decimal { scale, .. } => parse_decimal(text, scale)
                .map(|scaled| Watermark::Decimal { scaled, scale })
                .ok_or_else(|| {
                    bad(format!(
                        "cursor literal `{text}` is not a numeric(…,{scale})"
                    ))
                }),
            Decode::Utf8 => Ok(Watermark::Text(text.to_owned())),
            Decode::UuidText => Ok(Watermark::Uuid(text.to_ascii_lowercase())),
            Decode::Timestamp { tz } => {
                let parsed = chrono_parse_us(text).ok_or_else(|| {
                    bad(format!(
                        "cursor literal `{text}` is not an RFC3339/ISO timestamp"
                    ))
                })?;
                Ok(if tz {
                    Watermark::TimestampTz(parsed)
                } else {
                    Watermark::TimestampNaive(parsed)
                })
            }
            Decode::Date => parse_date_days(text)
                .map(Watermark::Date)
                .ok_or_else(|| bad(format!("cursor literal `{text}` is not a YYYY-MM-DD date"))),
            Decode::Time => parse_time_us(text).map(Watermark::Time).ok_or_else(|| {
                bad(format!(
                    "cursor literal `{text}` is not a HH:MM[:SS[.ffffff]] time"
                ))
            }),
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

/// Per-stream incremental tracking: boundary dedup on resume,
/// watermark/prev-distinct tracking for mid-stream checkpoints, and the
/// final boundary-key run. Depends on cursor-ordered streams (NULLS FIRST,
/// direction-aligned) — the sqlgen ORDER BY guarantees it.
pub(crate) struct Tracker {
    cursor_idx: usize,
    decode: Decode,
    direction_max: bool,
    /// Stream name and cursor column, named in the typed error raised when
    /// the stream breaks its cursor ordering or a row key cannot be rendered
    /// (always present — distinct from the policy-gated `null_error`).
    stream: String,
    cursor_column: String,
    /// Stored state we resumed from (monotonic guard + dedup source).
    stored: Option<CursorState>,
    /// Column indices forming the row key; None ⇒ hash every column.
    key_columns: Option<Vec<usize>>,
    /// `NullPolicy::Error` context: (stream, column) when configured.
    null_error: Option<(String, String)>,
    /// True until a row strictly beyond the stored watermark is seen.
    in_boundary: bool,
    last_value: Option<Watermark>,
    /// Keys of rows sharing `last_value` (the current boundary run).
    run_keys: Vec<String>,
    /// Rows-at-watermark count at the last emitted checkpoint (dedup of
    /// identical consecutive checkpoints).
    emitted: Option<(Watermark, usize)>,
    pub deduped_rows: u64,
}

impl Tracker {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cursor_idx: usize,
        decode: Decode,
        direction_max: bool,
        stored: Option<CursorState>,
        key_columns: Option<Vec<usize>>,
        // `NullPolicy::Error` context: (stream, column) — a NULL cursor
        // value fails `process` with a typed error naming both (N1).
        null_error: Option<(String, String)>,
        stream: String,
        cursor_column: String,
    ) -> Self {
        Self {
            cursor_idx,
            null_error,
            decode,
            direction_max,
            stream,
            cursor_column,
            stored,
            key_columns,
            in_boundary: true,
            last_value: None,
            run_keys: Vec::new(),
            emitted: None,
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

    fn row_key(&self, batch: &arrow_array::RecordBatch, row: usize) -> Result<String, SourceError> {
        match &self.key_columns {
            Some(indices) => {
                let mut parts = Vec::with_capacity(indices.len());
                for &i in indices {
                    let column = batch.column(i);
                    if column.is_null(row) {
                        parts.push("\u{2205}".to_owned());
                    } else {
                        parts.push(self.render_cell(column, row)?);
                    }
                }
                Ok(parts.join("|"))
            }
            None => {
                // PK-less table: hash the whole row's canonical rendering.
                let mut hasher = blake3::Hasher::new();
                for column in batch.columns() {
                    if column.is_null(row) {
                        hasher.update(b"\xff\x00");
                    } else {
                        hasher.update(self.render_cell(column, row)?.as_bytes());
                        hasher.update(b"\x00");
                    }
                }
                Ok(hasher.finalize().to_hex().to_string())
            }
        }
    }

    /// Render one non-NULL cell to its canonical resume-key text. A rendering
    /// failure is Fatal: defaulting it to an empty component would silently
    /// collide distinct rows and corrupt boundary dedup on resume.
    ///
    /// Zoned timestamps are re-labeled naive first: arrow's display needs a
    /// timezone database for NAMED zones (`"UTC"`), and without it every
    /// timestamptz cursor key would fail to render. The stored instant is
    /// unchanged (UTC micros), so the naive rendering is deterministic and
    /// collision-free — exactly what a resume key needs.
    fn render_cell(
        &self,
        column: &arrow_array::ArrayRef,
        row: usize,
    ) -> Result<String, SourceError> {
        use arrow_schema::DataType;
        let zoned;
        let column = match column.data_type() {
            DataType::Timestamp(unit, Some(_)) => {
                zoned =
                    arrow_cast::cast(column, &DataType::Timestamp(*unit, None)).map_err(|e| {
                        errors::fatal(
                            Phase::Copy,
                            Some(&self.stream),
                            format!(
                                "row key for cursor column `{}`: re-labeling zoned \
                                 timestamp failed: {e}",
                                self.cursor_column
                            ),
                        )
                    })?;
                &zoned
            }
            _ => column,
        };
        arrow_cast::display::array_value_to_string(column, row).map_err(|e| {
            errors::fatal(
                Phase::Copy,
                Some(&self.stream),
                format!(
                    "row key for cursor column `{}` could not be rendered to text: {e}",
                    self.cursor_column
                ),
            )
        })
    }

    /// Process one decoded batch BEFORE pushing: dedup re-fetched boundary
    /// rows and update tracking. Returns the (possibly filtered) batch —
    /// `None` when every row was a duplicate — plus an intermediate
    /// checkpoint to emit AFTER the batch is pushed (rows covered by it are
    /// then all pushed).
    pub fn process(
        &mut self,
        batch: arrow_array::RecordBatch,
    ) -> Result<(Option<arrow_array::RecordBatch>, Option<CursorState>), SourceError> {
        use arrow_array::builder::BooleanBuilder;

        let cursor_col = batch.column(self.cursor_idx).clone();
        // NullPolicy::Error (N1): zero cost when clean — one null_count()
        // check per batch, typed Fatal on the first violation.
        if let Some((stream, column)) = &self.null_error
            && cursor_col.null_count() > 0
        {
            return Err(crate::source::errors::fatal(
                crate::source::Phase::Copy,
                Some(stream),
                format!(
                    "cursor column `{column}` contains a NULL value and \
                     `nulls: error` is configured — NULL cursors are a data-contract \
                     violation under this policy (exclude/include accept them)"
                ),
            ));
        }
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
                    && stored.boundary_keys.contains(&self.row_key(&batch, row)?)
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
                    let key = self.row_key(&batch, row)?;
                    self.run_keys.push(key);
                }
                Some(last) => {
                    if !self.beyond(&value, last) {
                        return Err(errors::fatal(
                            Phase::Copy,
                            Some(&self.stream),
                            format!(
                                "cursor column `{}` produced an out-of-order row: the read \
                                 stream broke the ordering its resume watermark depends on",
                                self.cursor_column
                            ),
                        ));
                    }
                    self.last_value = Some(value);
                    self.run_keys = vec![self.row_key(&batch, row)?];
                }
                None => {
                    self.last_value = Some(value);
                    self.run_keys = vec![self.row_key(&batch, row)?];
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
        // Intermediate checkpoint after EVERY batch: watermark = last value
        // seen, keys = its boundary run. This COVERS every pushed row:
        // an engine commit at this checkpoint can resume `>= watermark` with
        // key dedup and neither lose nor double-apply the boundary run.
        // (The earlier prev-distinct design under-covered pushed rows at the
        // current value — caught by the armed crash sweep as a duplicate.)
        let checkpoint = self.current_state().filter(|state| {
            let signature = (state.watermark.clone(), state.boundary_keys.len());
            if self.emitted.as_ref() == Some(&signature) {
                false
            } else {
                self.emitted = Some(signature);
                true
            }
        });
        Ok((out_batch, checkpoint))
    }

    /// The current resume-safe state: watermark = last value seen, keys =
    /// its boundary run (merged with the stored keys when the watermark has
    /// not advanced past an equal stored one). `None` when nothing advanced
    /// (regressing-clock guard: the watermark NEVER moves backward).
    fn current_state(&self) -> Option<CursorState> {
        let last = self.last_value.as_ref()?;
        let mut boundary_keys = self.run_keys.clone();
        if let Some(stored) = &self.stored {
            if !self.beyond(last, &stored.watermark) && *last != stored.watermark {
                return None; // regressed — keep the stored watermark
            }
            if *last == stored.watermark {
                if self.run_keys.is_empty() {
                    return None; // nothing new at the boundary
                }
                // New rows at an unchanged watermark merge with its old keys
                // (those remain committed and deduped-against).
                boundary_keys.extend(stored.boundary_keys.iter().cloned());
                boundary_keys.sort();
                boundary_keys.dedup();
            }
        }
        Some(CursorState {
            watermark: last.clone(),
            boundary_keys,
        })
    }

    /// The final checkpoint at stream end. Under `boundary: open` the keys
    /// are stripped (strictly-monotonic cursors resume `>` across runs);
    /// intermediate checkpoints ALWAYS keep keys — in-run resume correctness
    /// is not a user-visible semantic choice.
    pub fn final_state(&self, keep_keys: bool) -> Option<CursorState> {
        let mut state = self.current_state()?;
        if !keep_keys {
            state.boundary_keys = Vec::new();
        }
        Some(state)
    }
}

/// Extract a typed watermark from a decoded column at `row` (None = NULL).
pub(crate) fn watermark_at(
    column: &arrow_array::ArrayRef,
    decode: Decode,
    row: usize,
) -> Option<Watermark> {
    use arrow_array::{
        Array, Date32Array, Decimal128Array, Int64Array, StringArray, Time64MicrosecondArray,
        TimestampMicrosecondArray,
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
        Decode::Utf8 => Some(Watermark::Text(
            any.downcast_ref::<StringArray>()?.value(row).to_owned(),
        )),
        Decode::UuidText => Some(Watermark::Uuid(
            any.downcast_ref::<StringArray>()?.value(row).to_owned(),
        )),
        Decode::Timestamp { tz } => {
            let v = any.downcast_ref::<TimestampMicrosecondArray>()?.value(row);
            Some(if tz {
                Watermark::TimestampTz(v)
            } else {
                Watermark::TimestampNaive(v)
            })
        }
        Decode::Date => Some(Watermark::Date(
            any.downcast_ref::<Date32Array>()?.value(row),
        )),
        Decode::Time => Some(Watermark::Time(
            any.downcast_ref::<Time64MicrosecondArray>()?.value(row),
        )),
        _ => None,
    }
}

/// The persisted per-stream state.
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

/// The resolved incremental read plan for a cursor stream: the boundary/order
/// SQL that shapes the COPY subselect, plus the tracker that dedups re-fetched
/// boundary rows and emits resume checkpoints. Built once per `read()`.
pub(crate) struct IncrementalPlan {
    pub tracker: Tracker,
    pub cursor: CursorConfig,
    pub where_sql: String,
    pub order_sql: String,
}

impl IncrementalPlan {
    /// Resolve the full dlt-parity incremental plan: validate the cursor column
    /// is selected + cursor-capable, decode the resume/config watermarks, apply
    /// the lag window, render the boundary matrix, and build the dedup tracker.
    /// Every failure is typed and early — before any data moves.
    pub(crate) fn prepare(
        cc: &CursorConfig,
        columns: &[&ReflectedColumn],
        table: &ReflectedTable,
        table_config: Option<&TableConfig>,
        since: Option<&Cursor>,
        name: &str,
    ) -> Result<Self, SourceError> {
        let reflected_cursor = columns
            .iter()
            .find(|c| c.name == cc.column)
            .filter(|c| c.mapped.cursor_capable)
            .ok_or_else(|| {
                errors::fatal(
                    Phase::Reflect,
                    Some(name),
                    format!(
                        "cursor column `{}` missing from selection or not \
                         cursor-capable after type mapping/hints",
                        cc.column
                    ),
                )
            })?;
        let cursor_decode = reflected_cursor.mapped.decode;
        let cursor_idx = columns
            .iter()
            .position(|c| c.name == cc.column)
            .ok_or_else(|| {
                errors::fatal(
                    Phase::Reflect,
                    Some(name),
                    format!(
                        "cursor column `{}` is excluded by the column selection",
                        cc.column
                    ),
                )
            })?;
        let direction_max = cc.direction == config::Direction::Max;
        let stored = match since {
            Some(since) => Some(CursorState::decode(since, name)?),
            None => None,
        };
        // Lower bound: stored state — closed (>= + dedup) iff it carries
        // boundary keys, which every checkpoint does except an open-boundary
        // final; else the configured initial_value under the configured
        // boundary.
        let closed_default = cc.boundary == config::Boundary::Closed;
        let lower: Option<(Watermark, bool)> = match &stored {
            Some(state) => Some((state.watermark.clone(), !state.boundary_keys.is_empty())),
            None => match &cc.initial_value {
                Some(text) => Some((
                    Watermark::parse_config_literal(cursor_decode, text, name)?,
                    closed_default,
                )),
                None => None,
            },
        };
        let upper: Option<Watermark> = match &cc.end_value {
            Some(text) => Some(Watermark::parse_config_literal(cursor_decode, text, name)?),
            None => None,
        };
        // Lag: widen the RESUMED window only — run 1 (no watermark) is
        // unaffected, and the saved watermark is never lowered. The window
        // re-read forces closed semantics regardless of how the stored state's
        // boundary flag landed.
        let lag_delta: Option<String> = match (&cc.lag, &stored) {
            (Some(lag), Some(_)) => Some(lag.sql_delta(cursor_decode).map_err(|d| {
                errors::fatal(
                    Phase::Reflect,
                    Some(name),
                    format!("cursor lag on `{}`: {d}", cc.column),
                )
            })?),
            _ => None,
        };
        let lower = match (lower, &lag_delta) {
            (Some((w, _)), Some(_)) => Some((w, true)),
            (other, _) => other,
        };
        let clauses = sqlgen::incremental_clauses(
            &cc.column,
            direction_max,
            lower.as_ref().map(|(w, closed)| (w, *closed)),
            lag_delta.as_deref(),
            upper
                .as_ref()
                .map(|w| (w, cc.end_bound == config::EndBound::Inclusive)),
            // `error` keeps NULL rows IN the read (like include) so the tracker
            // can raise on them (N1).
            cc.nulls != config::NullPolicy::Exclude,
            matches!(cursor_decode, Decode::Utf8),
        );
        // Row keys: configured/reflected PK columns present in the selection;
        // otherwise whole-row hashing.
        let pk_names: Vec<String> = table.effective_pk(table_config);
        let key_columns: Option<Vec<usize>> = if pk_names.is_empty() {
            None
        } else {
            pk_names
                .iter()
                .map(|k| columns.iter().position(|c| &c.name == k))
                .collect()
        };
        let tracker = Tracker::new(
            cursor_idx,
            cursor_decode,
            direction_max,
            stored,
            key_columns,
            (cc.nulls == config::NullPolicy::Error).then(|| (name.to_owned(), cc.column.clone())),
            name.to_owned(),
            cc.column.clone(),
        );
        Ok(Self {
            tracker,
            cursor: cc.clone(),
            where_sql: clauses.where_sql,
            order_sql: clauses.order_sql,
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
        // State round-trips losslessly: decode(encode(state)) == state.
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
            Watermark::Decimal {
                scaled: -12345,
                scale: 2
            }
            .to_sql_literal(),
            "'-123.45'::numeric"
        );
        assert_eq!(
            Watermark::Decimal {
                scaled: 5,
                scale: 4
            }
            .to_sql_literal(),
            "'0.0005'::numeric"
        );
        assert_eq!(
            Watermark::TimestampTz(1_000_000).to_sql_literal(),
            "(TIMESTAMPTZ 'epoch' + 1000000::int8 * INTERVAL '1 microsecond')"
        );
        assert_eq!(
            Watermark::Date(20_000).to_sql_literal(),
            "(DATE 'epoch' + 20000::int4)"
        );
    }

    #[test]
    fn out_of_order_arrival_fails_in_all_profiles() {
        use crate::source::types::Decode;
        use arrow_array::{Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;

        // A MAX cursor whose stream regresses (1, 3, then back to 2). The
        // resume watermark is only sound when the stream is cursor-ordered;
        // a silently-accepted out-of-order row corrupts it.
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64, 3, 2]))])
                .expect("batch builds");
        let mut tracker = Tracker::new(
            0,
            Decode::Int8,
            true,
            None,
            None,
            None,
            "public.events".to_owned(),
            "id".to_owned(),
        );
        let err = tracker
            .process(batch)
            .expect_err("an out-of-order row must be a typed error, not silently accepted");
        let msg = err.to_string();
        assert!(msg.contains("out-of-order"), "names the failure: {msg}");
        assert!(msg.contains("`id`"), "names the cursor column: {msg}");
        assert!(msg.contains("events"), "names the stream: {msg}");
    }

    #[test]
    fn row_key_threads_and_composes() {
        use crate::source::types::Decode;
        use arrow_array::{Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::Arc;

        // Composite key over two columns, one NULL. Key rendering is fallible
        // now; the happy path must still yield the exact composite key (NULL
        // sentinel + rendered value), never a silently defaulted component.
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Int64, true),
            Field::new("id", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![None::<i64>])),
                Arc::new(Int64Array::from(vec![7_i64])),
            ],
        )
        .expect("batch builds");
        let tracker = Tracker::new(
            1,
            Decode::Int8,
            true,
            None,
            Some(vec![0, 1]),
            None,
            "public.events".to_owned(),
            "id".to_owned(),
        );
        let key = tracker
            .row_key(&batch, 0)
            .expect("renderable cells return Ok");
        assert_eq!(key, "\u{2205}|7");
    }

    /// Zoned timestamps (postgres timestamptz decodes as
    /// `Timestamp(Microsecond, Some("UTC"))`) must render DISTINCT,
    /// NON-EMPTY key components. Arrow's display cannot render named
    /// zones without a timezone database, so before key rendering became
    /// fallible this silently produced an empty component for EVERY
    /// timestamptz boundary row — all boundary rows colliding on one key.
    #[test]
    fn zoned_timestamp_keys_are_distinct_and_nonempty() {
        use crate::source::types::Decode;
        use arrow_array::{RecordBatch, TimestampMicrosecondArray};
        use arrow_schema::{DataType, Field, Schema, TimeUnit};
        use std::sync::Arc;

        let ty = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let schema = Arc::new(Schema::new(vec![Field::new("ts", ty.clone(), false)]));
        let values =
            TimestampMicrosecondArray::from(vec![1_700_000_000_000_000_i64, 1_700_000_000_000_001])
                .with_timezone("UTC");
        let batch = RecordBatch::try_new(schema, vec![Arc::new(values)]).expect("batch builds");
        let tracker = Tracker::new(
            1,
            Decode::Timestamp { tz: true },
            true,
            None,
            Some(vec![0]),
            None,
            "public.events".to_owned(),
            "ts".to_owned(),
        );
        let first = tracker.row_key(&batch, 0).expect("zoned key renders");
        let second = tracker.row_key(&batch, 1).expect("zoned key renders");
        assert!(!first.is_empty() && !second.is_empty());
        assert_ne!(first, second, "distinct instants must never share a key");
    }

    #[test]
    fn config_literals_parse_per_type() {
        use crate::source::types::Decode;
        assert_eq!(
            Watermark::parse_config_literal(Decode::Int8, "17", "t").expect("int"),
            Watermark::Int(17)
        );
        assert_eq!(
            Watermark::parse_config_literal(
                Decode::Decimal {
                    precision: 10,
                    scale: 2
                },
                "1.5",
                "t"
            )
            .expect("decimal"),
            Watermark::Decimal {
                scaled: 150,
                scale: 2
            }
        );
        assert_eq!(
            Watermark::parse_config_literal(
                Decode::Timestamp { tz: true },
                "2026-01-01T00:00:00Z",
                "t"
            )
            .expect("ts"),
            Watermark::TimestampTz(1_767_225_600_000_000)
        );
        assert_eq!(
            Watermark::parse_config_literal(Decode::Date, "1970-01-02", "t").expect("date"),
            Watermark::Date(1)
        );
        assert!(Watermark::parse_config_literal(Decode::Int8, "abc", "t").is_err());
        assert!(
            Watermark::parse_config_literal(
                Decode::Decimal {
                    precision: 10,
                    scale: 2
                },
                "1.234",
                "t"
            )
            .is_err(),
            "more precision than the column carries"
        );
    }
}
