//! Canonical renderings: the single definition of "what bytes
//! represent this value" — used for `Utf8` widening output AND for `_rdlt_id` hashing,
//! so identity stays stable across type widenings of other columns.
//!
//! Generic over [`JsonView`]: the tree and streaming paths
//! render through the SAME functions — identical bytes, identical hashes.
//!
//! Two float renderings coexist ON PURPOSE, exactly as they always have:
//! `render_scalar` uses Rust's `to_string` (Utf8 widening output) while
//! `canonical_json_bytes` serializes through serde_json (ryu — `_rdlt_id`
//! hashing). Changing either changes persisted ids/values.

use super::view::{JsonView, Kind};

/// Canonical text of a JSON scalar. `None` for null and for non-scalars.
/// - strings: verbatim
/// - integers: decimal digits
/// - floats: Rust's shortest round-trip rendering
/// - bools: `true` / `false`
pub(crate) fn render_scalar<'a, V: JsonView<'a>>(value: V) -> Option<String> {
    match value.kind() {
        Kind::Null | Kind::Object | Kind::Array => None,
        Kind::Str(s) => Some(s.to_owned()),
        Kind::Bool(b) => Some(if b { "true" } else { "false" }.to_owned()),
        Kind::Int(i) => Some(i.to_string()),
        Kind::UInt(u) => Some(u.to_string()),
        Kind::Float(f) => Some(f.to_string()),
    }
}

/// Canonical JSON bytes: object keys sorted recursively, no whitespace. Used for
/// content hashing (`_rdlt_id` keyless) — two semantically identical rows always hash
/// identically regardless of key order in the source payload.
///
/// Entry order from the view is native (insertion) order — the sort happens
/// HERE, explicitly, exactly as it always has.
pub(crate) fn canonical_json_bytes<'a, V: JsonView<'a>>(value: V, out: &mut Vec<u8>) {
    match value.kind() {
        Kind::Object => {
            out.push(b'{');
            let mut entries: Vec<(&str, V)> = value.obj_entries().collect();
            entries.sort_unstable_by_key(|(key, _)| *key);
            for (i, (key, item)) in entries.into_iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                // Key serialization via serde_json for correct escaping.
                serde_json::to_writer(&mut *out, key).expect("string serialization");
                out.push(b':');
                canonical_json_bytes(item, out);
            }
            out.push(b'}');
        }
        Kind::Array => {
            out.push(b'[');
            for (i, item) in value.arr_items().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                canonical_json_bytes(item, out);
            }
            out.push(b']');
        }
        Kind::Null => out.extend_from_slice(b"null"),
        Kind::Bool(b) => out.extend_from_slice(if b { b"true" } else { b"false" }),
        Kind::Str(s) => serde_json::to_writer(&mut *out, s).expect("string serialization"),
        // Numbers reconstruct the exact serde_json::Number a Value would hold, so
        // the serialized bytes match the tree path digit-for-digit (itoa/ryu).
        Kind::Int(i) => serde_json::to_writer(&mut *out, &serde_json::Number::from(i))
            .expect("number serialization"),
        Kind::UInt(u) => serde_json::to_writer(&mut *out, &serde_json::Number::from(u))
            .expect("number serialization"),
        Kind::Float(f) => serde_json::to_writer(
            &mut *out,
            &serde_json::Number::from_f64(f).expect("finite by JSON grammar"),
        )
        .expect("number serialization"),
    }
}

/// Strict timestamp detection: RFC 3339 / ISO-8601 **with explicit offset** only
/// (unambiguous, timezone-carrying strings; everything else stays text).
///
/// The pre-filter encodes only what the RFC 3339 grammar REQUIRES (a
/// `YYYY-MM-DD` prefix and the minimum 20-byte length with an offset), so it
/// rejects nothing chrono would accept — it just makes the overwhelmingly
/// common "ordinary string" case cost a few instructions instead of a full
/// chrono parse attempt (this runs on EVERY observed string and would
/// otherwise dominate the shred profile).
pub(crate) fn parse_timestamp_tz(s: &str) -> Option<chrono::DateTime<chrono::FixedOffset>> {
    let b = s.as_bytes();
    if b.len() < 20
        || !b[0].is_ascii_digit()
        || !b[1].is_ascii_digit()
        || !b[2].is_ascii_digit()
        || !b[3].is_ascii_digit()
        || b[4] != b'-'
        || b[7] != b'-'
    {
        return None;
    }
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
