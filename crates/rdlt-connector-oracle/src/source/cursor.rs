//! The watermark cursor: a persisted v1 wire format, one watermark
//! per stream, never lowered.

use std::collections::BTreeMap;

use rdlt_connector_sdk::spi::SourceError;
use rdlt_connector_sdk::spi::core::Cursor;

/// The persisted format version this crate writes.
const CURSOR_FORMAT_VERSION: u32 = 1;

/// The whole persisted cursor.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct OracleCursor {
    #[serde(default = "default_version")]
    pub format_version: u32,
    /// Per-stream watermarks: the RENDERED maximum of the cursor
    /// column, compared SQL-side on resume.
    #[serde(default)]
    pub streams: BTreeMap<String, StreamCursor>,
}

fn default_version() -> u32 {
    CURSOR_FORMAT_VERSION
}

// Manual: serde default fns apply only at deserialize (the 030 wire
// lesson — a derived Default would mint version-0 cursors).
impl Default for OracleCursor {
    fn default() -> Self {
        Self {
            format_version: CURSOR_FORMAT_VERSION,
            streams: BTreeMap::new(),
        }
    }
}

/// One stream's progress.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StreamCursor {
    /// The rendered high-water mark of the cursor column.
    pub watermark: String,
}

impl OracleCursor {
    /// Decode a persisted cursor; absent means empty; unreadable and
    /// FUTURE formats are typed (state is precious — silently starting
    /// over duplicates; a future shape would decode EMPTY through the
    /// serde defaults).
    pub fn decode(cursor: Option<&Cursor>) -> Result<Self, SourceError> {
        let Some(cursor) = cursor else {
            return Ok(Self::default());
        };
        let decoded: Self = serde_json::from_value(cursor.as_value().clone())
            .map_err(|e| SourceError::fatal(format!("unreadable oracle cursor: {e}")))?;
        if decoded.format_version > CURSOR_FORMAT_VERSION {
            return Err(SourceError::fatal(format!(
                "oracle cursor format v{} is newer than this build supports \
                 (v{CURSOR_FORMAT_VERSION}); upgrade rdlt instead of clearing state",
                decoded.format_version
            )));
        }
        Ok(decoded)
    }

    /// Encode for the checkpoint channel.
    pub fn encode(&self) -> Cursor {
        Cursor::new(serde_json::to_value(self).expect("cursor serialization"))
    }

    /// Raise (never lower) one stream's watermark.
    pub fn advance(&mut self, stream: &str, candidate: &str) {
        match self.streams.get_mut(stream) {
            Some(existing) if !watermark_less(&existing.watermark, candidate) => {}
            Some(existing) => existing.watermark = candidate.to_owned(),
            None => {
                self.streams.insert(
                    stream.to_owned(),
                    StreamCursor {
                        watermark: candidate.to_owned(),
                    },
                );
            }
        }
    }
}

/// Order two rendered watermarks: numerically when both parse, else
/// lexically (ISO timestamps order lexically by construction).
fn watermark_less(a: &str, b: &str) -> bool {
    match (a.parse::<f64>(), b.parse::<f64>()) {
        (Ok(x), Ok(y)) => x < y,
        _ => a < b,
    }
}

/// A watermark travels back into SQL as a LITERAL, so its shape is
/// REFUSED unless it is unambiguously a number or an ISO-like
/// timestamp — never quoted free text (the injection gate; the
/// cursor document is operator-editable).
pub fn checked_watermark_literal(value: &str) -> Result<String, SourceError> {
    let numeric = !value.is_empty()
        && value
            .chars()
            .all(|c| c.is_ascii_digit() || matches!(c, '.' | '-' | '+'));
    let timestampish = !value.is_empty()
        && value
            .chars()
            .all(|c| c.is_ascii_digit() || matches!(c, '-' | ':' | 'T' | 'Z' | '.' | '+' | ' '));
    if numeric {
        Ok(value.to_owned())
    } else if timestampish {
        Ok(format!(
            "TO_TIMESTAMP_TZ('{value}', 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6TZH:TZM')"
        ))
    } else {
        Err(SourceError::fatal(format!(
            "cursor watermark `{value}` is neither numeric nor timestamp-shaped — \
             refusing to interpolate it into SQL; clear the pipeline state"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The literal v1 wire shape.
    #[test]
    fn the_wire_shape_is_frozen() {
        let mut cursor = OracleCursor::default();
        cursor.advance("events", "42");
        assert_eq!(
            serde_json::to_value(&cursor).expect("json"),
            serde_json::json!({
                "format_version": 1,
                "streams": {"events": {"watermark": "42"}}
            })
        );
    }

    /// Watermarks only rise — numerically for numbers ("9" < "10"),
    /// lexically for timestamps.
    #[test]
    fn watermarks_never_lower() {
        let mut cursor = OracleCursor::default();
        cursor.advance("s", "9");
        cursor.advance("s", "10");
        assert_eq!(cursor.streams["s"].watermark, "10");
        cursor.advance("s", "2");
        assert_eq!(cursor.streams["s"].watermark, "10", "never lowered");
        cursor.advance("t", "2026-01-02T00:00:00.000000Z");
        cursor.advance("t", "2026-01-01T00:00:00.000000Z");
        assert!(cursor.streams["t"].watermark.starts_with("2026-01-02"));
    }

    /// A future cursor format refuses as an upgrade prompt.
    #[test]
    fn a_future_format_refuses_upgrade_not_reset() {
        let value = Cursor::new(serde_json::json!({"format_version": 9}));
        let err = OracleCursor::decode(Some(&value)).expect_err("refused").to_string();
        assert!(err.contains("v9 is newer than this build supports (v1)"), "{err}");
    }

    /// The injection gate on the watermark literal.
    #[test]
    fn watermark_literals_are_shape_checked() {
        assert_eq!(checked_watermark_literal("42.5").expect("num"), "42.5");
        assert!(checked_watermark_literal("2026-01-01T00:00:00.000000Z")
            .expect("ts")
            .starts_with("TO_TIMESTAMP_TZ"));
        let err = checked_watermark_literal("1; DROP TABLE x --")
            .expect_err("refused")
            .to_string();
        assert!(err.contains("refusing to interpolate"), "{err}");
    }
}
