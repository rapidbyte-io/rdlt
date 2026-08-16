//! The trust boundary: every gate a connector-authored byte passes
//! before becoming host vocabulary, held in one module so a new seat
//! can never misquote the rule.
//!
//! Three dispositions, by what the text is: identifiers REFUSE (a name
//! is either clean or refused), display text ESCAPES (a diagnostic
//! should survive its own bad bytes rather than vanish), and opaque
//! data documents get neither — their one render path, JSON
//! serialization, already spells control characters inert. The
//! character inventory itself lives in
//! `rdlt_connector_protocol::sanitize`, shared with the serving side,
//! so the two ends of the wire cannot drift.

use std::borrow::Cow;
use std::time::Duration;

use rdlt_connector_protocol::proto::{self, Classification};
use rdlt_connector_protocol::sanitize;

/// The most bytes a wire identifier may carry — the SPI's constant,
/// re-exported so seats read it beside the gate they pair it with.
pub(crate) use rdlt_connector::MAX_WIRE_IDENTIFIER_BYTES;

/// Refuse an identifier that is oversized or carries control or
/// invisible characters. Length is checked first so the escaped
/// refusal stays bounded; the content predicate admits ZWNJ/ZWJ —
/// load-bearing orthography in Persian, Malayalam and Devanagari
/// names — and refuses everything else invisible.
pub(crate) fn identifier(seat: &str, value: &str) -> Result<(), String> {
    if value.len() > MAX_WIRE_IDENTIFIER_BYTES {
        return Err(format!(
            "the connector sent a {seat} of {} bytes — over the \
             {MAX_WIRE_IDENTIFIER_BYTES}-byte identifier ceiling",
            value.len()
        ));
    }
    if value
        .chars()
        .any(sanitize::is_control_or_invisible_in_identifier)
    {
        return Err(format!(
            "the connector sent a {seat} of `{}` — control or invisible \
             characters in identifiers are refused at the wire boundary",
            escape(value)
        ));
    }
    Ok(())
}

/// Refuse a collection larger than its seat's cap: the content gates
/// bound each value, this bounds how many of them one frame may carry.
/// `seat` is spelled plural ("primary-key fields").
pub(crate) fn count(seat: &str, n: usize, cap: usize) -> Result<(), String> {
    if n > cap {
        return Err(format!(
            "the connector sent {n} {seat} — over the {cap} ceiling"
        ));
    }
    Ok(())
}

/// The ceiling on raw bytes about to be parsed into an untyped
/// `serde_json::Value` — a compact frame would otherwise materialize
/// hundreds of MB of `Value` before any semantic check could refuse.
/// Delegated to the SPI's one implementation so this crate and the
/// serving side cannot drift.
pub(crate) fn document(field: &str, bytes: &[u8]) -> Result<(), String> {
    rdlt_connector::json::refuse_oversized_document(field, bytes)
}

/// Serialize a cursor and refuse it when the SERIALIZED form exceeds
/// the cursor contract — that is the form the WAL persists, and
/// serde_json's own number rendering can inflate a wire-legal cursor
/// (`1e15` re-serializes as `1000000000000000.0`). Returns the
/// serialized bytes so a sending seat forwards exactly what was
/// measured.
pub(crate) fn cursor(value: &serde_json::Value) -> Result<Vec<u8>, String> {
    let bytes =
        serde_json::to_vec(value).expect("a serde_json::Value serializes to JSON infallibly");
    if bytes.len() as u64 > rdlt_connector::MAX_CURSOR_BYTES {
        return Err(format!(
            "a cursor serializes to {} bytes, over the {}-byte cursor contract — the connector \
             must summarize its state (a high-water mark, an offset, a resume token) rather \
             than embed the data",
            bytes.len(),
            rdlt_connector::MAX_CURSOR_BYTES
        ));
    }
    Ok(bytes)
}

/// Render text inert for display: every control or invisible character
/// — the full inventory, joiners included — becomes its spelled-out
/// escape, while everything else (quotes, non-ASCII text, which are
/// data) passes byte-identical. Borrowed unchanged when there is
/// nothing to escape, which is every honest message.
pub(crate) fn escape(text: &str) -> Cow<'_, str> {
    if !text.chars().any(sanitize::is_control_or_invisible) {
        return Cow::Borrowed(text);
    }
    let mut escaped = String::with_capacity(text.len() + 8);
    for character in text.chars() {
        if sanitize::is_control_or_invisible(character) {
            // `escape_debug` passes "printable" characters through, and
            // the inventory's two Hangul fillers are category Lo —
            // printable to it while rendering as blank glyphs — so
            // anything it hands back unchanged is spelled out by hand.
            let mut debug = character.escape_debug();
            if debug.len() == 1 && debug.next() == Some(character) {
                use std::fmt::Write as _;
                let _ = write!(escaped, "\\u{{{:x}}}", character as u32);
            } else {
                escaped.extend(character.escape_debug());
            }
        } else {
            escaped.push(character);
        }
    }
    Cow::Owned(escaped)
}

/// Decode a frame's raw classification safe-loud: `Unspecified`
/// (proto3's zero value) and any value this build does not know both
/// normalize to `Fatal` — retrying an unclassified failure could loop
/// a run forever on something permanent, while aborting a retryable
/// one merely costs a re-run.
pub(crate) fn classification(raw: i32) -> Classification {
    match Classification::try_from(raw) {
        Ok(Classification::Unspecified) | Err(_) => Classification::Fatal,
        Ok(classification) => classification,
    }
}

/// The ceiling on a connector-supplied retry hint: one minute. The
/// engine sleeps the hinted duration directly in place of its own
/// backoff, so an unclamped rogue hint would park a run indefinitely;
/// a minute is generous to every honest Retry-After.
pub(crate) const MAX_RETRY_AFTER: Duration = Duration::from_secs(60);

/// A frame's wait hint, clamped to [`MAX_RETRY_AFTER`].
pub(crate) fn retry_after(frame: &proto::ErrorFrame) -> Option<Duration> {
    frame
        .retry_after_ms
        .map(|ms| Duration::from_millis(ms.min(MAX_RETRY_AFTER.as_millis() as u64)))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The content gate's edges: C0, DEL, and C1 are control; the
    /// shared inventory's format characters (the Arabic number signs
    /// and the tag block included) refuse; ordinary text — non-ASCII
    /// included — passes.
    #[test]
    fn the_identifier_gate_covers_controls_and_invisible_formatting() {
        for hostile in [
            "a\u{1b}b",
            "a\nb",
            "a\tb",
            "a\u{7f}b",
            "a\u{85}b",
            "a\u{200b}b",
            "a\u{2028}b",
            "a\u{202e}b",
            "a\u{0600}b",
            "a\u{e0041}b",
            "a\u{3164}b",
        ] {
            assert!(
                identifier("stream name", hostile).is_err(),
                "{hostile:?} is control"
            );
        }
        for clean in ["orders", "Événements", "naïve — text", ""] {
            assert!(
                identifier("stream name", clean).is_ok(),
                "{clean:?} is data"
            );
        }
    }

    /// The joiners are orthography, not reordering controls: a Persian
    /// name spelled with ZWNJ passes the identifier gate, while display
    /// escaping still spells the joiner out so rendered names cannot
    /// hide it.
    #[test]
    fn the_joiners_pass_identifiers_but_still_escape_in_display() {
        for name in ["می\u{200c}خواهم", "a\u{200d}b"] {
            assert!(
                identifier("stream name", name).is_ok(),
                "{name:?} is a legal identifier"
            );
        }
        assert_eq!(
            escape("a\u{200c}b"),
            "a\\u{200c}b",
            "display text spells the joiner out"
        );
    }

    /// Escaping spells control bytes out and touches nothing else —
    /// quotes and backslashes included, which `escape_debug` over the
    /// WHOLE string would mangle.
    #[test]
    fn escaping_spells_control_bytes_and_leaves_data_alone() {
        assert_eq!(
            escape("\u{1b}]52;\u{7}\nx \"quoted\" \\slash é"),
            "\\u{1b}]52;\\u{7}\\nx \"quoted\" \\slash é"
        );
        assert!(matches!(
            escape("clean \"text\""),
            Cow::Borrowed("clean \"text\"")
        ));
    }

    /// The inventory's two Hangul fillers are category `Lo` — printable
    /// to `escape_debug`, which hands them back unchanged while they
    /// render as blank glyphs. The display seat's invariant is that
    /// rendered text cannot hide an inventory character, so the
    /// fallback spells them out.
    #[test]
    fn the_hangul_fillers_escaped_raw_get_spelled_out() {
        assert_eq!(escape("a\u{3164}b"), "a\\u{3164}b");
        assert_eq!(escape("a\u{ffa0}b"), "a\\u{ffa0}b");
    }

    /// The invariant at the refusal seats too: every inventory
    /// character renders spelled-out through the shared escape, so a
    /// refusal quoting a hostile value cannot carry the bytes it
    /// refuses. (Mechanical sweep across the whole table, not a
    /// sample.)
    #[test]
    fn no_inventory_character_survives_the_escape_raw() {
        let inventory: Vec<char> = ('\u{0}'..='\u{10FFFF}')
            .filter(|c| sanitize::is_control_or_invisible(*c))
            .collect();
        assert!(inventory.len() > 100, "the sweep covers the inventory");
        for character in inventory {
            let input = character.to_string();
            let escaped = escape(&input);
            assert_ne!(
                escaped, input,
                "U+{:04X} must not survive the escape raw",
                character as u32
            );
        }
    }

    /// The length half of the identifier gate, exact at its boundary: a
    /// name of exactly the ceiling passes, one byte over refuses naming
    /// the ceiling.
    #[test]
    fn the_identifier_length_boundary_is_exact() {
        let at = "a".repeat(MAX_WIRE_IDENTIFIER_BYTES);
        assert!(identifier("stream name", &at).is_ok());
        let over = "a".repeat(MAX_WIRE_IDENTIFIER_BYTES + 1);
        let error =
            identifier("stream name", &over).expect_err("one byte over the ceiling refuses");
        assert!(
            error.contains("identifier ceiling"),
            "the refusal names the ceiling: {error}"
        );
    }

    /// The cursor contract is inclusive at its boundary — a cursor
    /// serializing to EXACTLY the ceiling passes; one byte over refuses
    /// naming the contract.
    #[test]
    fn the_cursor_ceiling_is_inclusive_at_the_boundary() {
        let at_cap = serde_json::Value::String("c".repeat(
            rdlt_connector::MAX_CURSOR_BYTES as usize - 2, // quotes round it to exactly the cap
        ));
        let bytes = cursor(&at_cap).expect("a cursor at the cap passes");
        assert_eq!(bytes.len() as u64, rdlt_connector::MAX_CURSOR_BYTES);
        let over =
            serde_json::Value::String("c".repeat(rdlt_connector::MAX_CURSOR_BYTES as usize - 1));
        let error = cursor(&over).expect_err("one byte over refuses");
        assert!(
            error.contains("cursor contract"),
            "the refusal names the contract: {error}"
        );
    }

    /// The contract measures the RE-SERIALIZED form, because that is
    /// the form the WAL records — serde_json's own number rendering
    /// inflates a compact wire cursor (`1e15` re-serializes as
    /// `1000000000000000.0`), so a wire-legal document can still
    /// violate the contract its persistence must honor.
    #[test]
    fn the_cursor_gate_measures_the_re_serialized_form() {
        // A compact wire spelling whose serialized form is ~4.5× larger.
        let floats = format!("[{}]", vec!["1e15"; 300_000].join(","));
        let parsed: serde_json::Value =
            serde_json::from_str(&floats).expect("compact exponent notation parses");
        assert!(
            floats.len() < rdlt_connector::MAX_CURSOR_BYTES as usize,
            "the wire form is well under the ceiling: {}",
            floats.len()
        );
        let error = cursor(&parsed).expect_err("the inflated serialization must refuse");
        assert!(
            error.contains("cursor contract"),
            "the refusal names the contract: {error}"
        );
        // The same numbers spelled the way serde re-serializes them sit
        // over the ceiling — the measurement is honest, not synthetic.
        assert!(format!("{parsed}").len() > rdlt_connector::MAX_CURSOR_BYTES as usize);
    }

    /// The count gate is inclusive at its cap and its refusal names the
    /// seat.
    #[test]
    fn the_count_gate_is_inclusive_at_the_cap_and_names_the_seat() {
        assert!(count("primary-key fields", 64, 64).is_ok());
        let error = count("primary-key fields", 65, 64).expect_err("one over the cap refuses");
        assert!(
            error.contains("primary-key fields") && error.contains("over the 64 ceiling"),
            "the refusal names the seat and the cap: {error}"
        );
    }

    /// The safe-loud decode: `Unspecified` (a server that never set the
    /// field) and a value this build does not know both normalize to
    /// `Fatal`; known values pass through untouched.
    #[test]
    fn classification_normalizes_unspecified_and_unknown_to_fatal() {
        assert_eq!(
            classification(Classification::Unspecified as i32),
            Classification::Fatal
        );
        assert_eq!(classification(42), Classification::Fatal);
        assert_eq!(
            classification(Classification::Transient as i32),
            Classification::Transient
        );
        assert_eq!(
            classification(Classification::RateLimited as i32),
            Classification::RateLimited
        );
        assert_eq!(
            classification(Classification::Fatal as i32),
            Classification::Fatal
        );
    }

    /// The clamp on the wait hint: a rogue `u64::MAX` arrives as
    /// [`MAX_RETRY_AFTER`], a hint AT the cap passes exactly (the clamp
    /// bounds, it does not distort), and no hint stays no hint.
    #[test]
    fn the_retry_hint_clamps_at_the_ceiling_and_forwards_honest_values() {
        let frame = |retry_after_ms| proto::ErrorFrame {
            classification: Classification::RateLimited as i32,
            message: "the cause".to_string(),
            retry_after_ms,
        };
        assert_eq!(
            retry_after(&frame(Some(u64::MAX))),
            Some(MAX_RETRY_AFTER),
            "a rogue hint clamps"
        );
        assert_eq!(
            retry_after(&frame(Some(MAX_RETRY_AFTER.as_millis() as u64))),
            Some(MAX_RETRY_AFTER),
            "a hint at the cap passes exactly"
        );
        assert_eq!(
            retry_after(&frame(None)),
            None,
            "no hint given, none invented"
        );
    }
}
