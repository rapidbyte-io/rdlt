//! Canonical renderings (design doc §5.2): the single definition of "what bytes
//! represent this value" — used for `Utf8` widening output AND for `_rdlt_id` hashing,
//! so identity stays stable across type widenings of other columns.

use serde_json::Value;

/// Canonical text of a JSON scalar. `None` for null and for non-scalars.
/// - strings: verbatim
/// - integers: decimal digits
/// - floats: Rust's shortest round-trip rendering
/// - bools: `true` / `false`
pub(crate) fn render_scalar(value: &Value) -> Option<String> {
    match value {
        Value::Null | Value::Object(_) | Value::Array(_) => None,
        Value::String(s) => Some(s.clone()),
        Value::Bool(b) => Some(if *b { "true" } else { "false" }.to_owned()),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(i.to_string())
            } else if let Some(u) = n.as_u64() {
                Some(u.to_string())
            } else {
                n.as_f64().map(|f| f.to_string())
            }
        }
    }
}

/// Canonical JSON bytes: object keys sorted recursively, no whitespace. Used for
/// content hashing (`_rdlt_id` keyless) — two semantically identical rows always hash
/// identically regardless of key order in the source payload.
pub(crate) fn canonical_json_bytes(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Object(map) => {
            out.push(b'{');
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable();
            for (i, key) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                // Key serialization via serde_json for correct escaping.
                serde_json::to_writer(&mut *out, key).expect("string serialization");
                out.push(b':');
                canonical_json_bytes(&map[*key], out);
            }
            out.push(b'}');
        }
        Value::Array(items) => {
            out.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                canonical_json_bytes(item, out);
            }
            out.push(b']');
        }
        scalar => serde_json::to_writer(out, scalar).expect("scalar serialization"),
    }
}

/// Strict timestamp detection: RFC 3339 / ISO-8601 **with explicit offset** only
/// (design doc §5.2 — unambiguous, timezone-carrying strings; everything else stays
/// text).
pub(crate) fn parse_timestamp_tz(s: &str) -> Option<chrono::DateTime<chrono::FixedOffset>> {
    chrono::DateTime::parse_from_rfc3339(s).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn canonical_json_sorts_keys_recursively() {
        let a = json!({"b": {"y": 1, "x": 2}, "a": 3});
        let b = json!({"a": 3, "b": {"x": 2, "y": 1}});
        let (mut ba, mut bb) = (Vec::new(), Vec::new());
        canonical_json_bytes(&a, &mut ba);
        canonical_json_bytes(&b, &mut bb);
        assert_eq!(ba, bb);
        assert_eq!(
            std::str::from_utf8(&ba).unwrap(),
            r#"{"a":3,"b":{"x":2,"y":1}}"#
        );
    }

    #[test]
    fn timestamp_detection_requires_offset() {
        assert!(parse_timestamp_tz("2026-07-19T10:00:00Z").is_some());
        assert!(parse_timestamp_tz("2026-07-19T10:00:00+02:00").is_some());
        assert!(
            parse_timestamp_tz("2026-07-19T10:00:00").is_none(),
            "no offset → text"
        );
        assert!(
            parse_timestamp_tz("2026-07-19").is_none(),
            "date-only → text"
        );
        assert!(parse_timestamp_tz("not a time").is_none());
    }

    #[test]
    fn renderings_are_shortest_round_trip() {
        assert_eq!(render_scalar(&json!(10)).unwrap(), "10");
        assert_eq!(render_scalar(&json!(10.5)).unwrap(), "10.5");
        assert_eq!(
            render_scalar(&json!(9007199254740993i64)).unwrap(),
            "9007199254740993"
        );
        assert_eq!(render_scalar(&json!(true)).unwrap(), "true");
        assert_eq!(render_scalar(&json!(null)), None);
    }
}
