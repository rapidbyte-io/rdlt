//! Canonical renderings: the single definition of "what bytes
//! represent this value" — used for `Utf8` widening output AND for `_rdlt_id` hashing,
//! so identity stays stable across type widenings of other columns.
//!
//! Generic over [`JsonView`]: the tree and streaming paths
//! render through the SAME functions — identical bytes, identical hashes.
//!
//! Two float renderings coexist ON PURPOSE, exactly as they always have:
//! `render_scalar` uses Rust's `to_string` while `canonical_json_bytes`
//! serializes through serde_json's own float formatter. They DISAGREE, and
//! that disagreement is persisted: `1.0` renders as `1` here and `1.0` there;
//! `1e16` as `10000000000000000` here and `1e16` there. Changing either — to
//! `{:?}`, to a faster float crate, or to the other one — rewrites `_rdlt_id`
//! values that already exist in users' warehouses.

use std::borrow::Cow;

use super::view::{JsonView, Kind};

/// Canonical text of a JSON scalar. `None` for null and for non-scalars.
/// - strings: verbatim
/// - integers: decimal digits
/// - floats: Rust's shortest round-trip rendering
/// - bools: `true` / `false`
///
/// Borrowed for strings, whose canonical text IS the view's own bytes, and
/// owned for everything else. The borrow is confined to that one arm on
/// purpose: it is the only case where "render" and "the bytes already there"
/// are provably the same, and it is the common one — primary keys are usually
/// strings. Widening the borrow to any other kind would mean deciding a
/// rendering somewhere new, which is how persisted ids move.
///
/// `None` covers null, objects and arrays alike. The caller turns that into
/// `FieldValue::Null`, so those four inputs share a keyed identity while an
/// empty string does not — pinned in `tests/shred_identity_pin.rs`.
pub(crate) fn render_scalar<'a, V: JsonView<'a>>(value: V) -> Option<Cow<'a, str>> {
    match value.kind() {
        Kind::Null | Kind::Object | Kind::Array => None,
        Kind::Str(s) => Some(Cow::Borrowed(s)),
        Kind::Bool(b) => Some(Cow::Borrowed(if b { "true" } else { "false" })),
        Kind::Int(i) => Some(Cow::Owned(i.to_string())),
        Kind::UInt(u) => Some(Cow::Owned(u.to_string())),
        Kind::Float(f) => Some(Cow::Owned(f.to_string())),
    }
}

/// Canonical JSON bytes: object keys sorted recursively, no whitespace. Used for
/// content hashing (`_rdlt_id` keyless) — two semantically identical rows always hash
/// identically regardless of key order in the source payload.
///
/// Entry order from the view is native (insertion) order — the sort happens
/// HERE, explicitly, exactly as it always has.
///
/// The sibling `build::write_compact_json` shares every rule below EXCEPT this
/// key sort (it preserves native order for the stored `Json` column). They are
/// kept as two functions on purpose: unifying them would put persisted `_rdlt_id`
/// bytes one flag away from a silent change. Mirror any shared-rule edit in both.
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
        // Floats where Rust's `Display` (used here) and `Debug` (or any
        // JSON-shaped formatter) DISAGREE. `10.5` above renders identically
        // under both and so cannot catch a swapped formatter; these can.
        assert_eq!(render_scalar(&json!(1.0)).unwrap(), "1");
        assert_eq!(render_scalar(&json!(-0.0)).unwrap(), "-0");
        assert_eq!(render_scalar(&json!(1e16)).unwrap(), "10000000000000000");
        assert_eq!(render_scalar(&json!(1e-6)).unwrap(), "0.000001");
        // …and the same values through the OTHER renderer, so the two stay
        // provably distinct rather than drifting into agreement.
        let canon = |v: &serde_json::Value| {
            let mut out = Vec::new();
            canonical_json_bytes(v, &mut out);
            String::from_utf8(out).expect("utf8")
        };
        assert_eq!(canon(&json!(1.0)), "1.0");
        assert_eq!(canon(&json!(1e16)), "1e+16");
        assert_eq!(canon(&json!(1e-6)), "1e-6");
        assert_eq!(
            render_scalar(&json!(9007199254740993i64)).unwrap(),
            "9007199254740993"
        );
        assert_eq!(render_scalar(&json!(true)).unwrap(), "true");
        assert_eq!(render_scalar(&json!(null)), None);
    }
}
