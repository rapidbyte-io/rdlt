//! The wire edge's ONE control-character rule, held in one place.
//!
//! Connector-authored text crosses into host vocabulary here — events,
//! tracing lines, the CLI's output, filesystem-adjacent identifiers —
//! and control bytes in it are how a hostile connector forges log
//! lines or drives escape sequences (OSC 52 clipboard writes, ANSI
//! resets) through an operator's terminal. The character inventory
//! itself lives in `rdlt_connector_protocol::sanitize` — the protocol
//! crate's handshake gate refuses by the same table, and this crate
//! depends on it, so the two sides of the wire cannot drift. This
//! module owns the DISPOSITIONS: which seats refuse, which escape,
//! and which deliberately do neither.
//!
//! Three dispositions, by what the text IS:
//!
//! - IDENTIFIERS refuse — a stream name, a part event's table, the
//!   handshake's reported id/version and spec name/version become host
//!   names for things, and a name is either clean or refused. Each
//!   seat renders its own typed refusal, quoting the value in its
//!   `{:?}` escaped form so the refusal cannot carry the bytes it
//!   refuses.
//! - DISPLAY TEXT escapes — an error frame's message is a diagnostic,
//!   and a connector's real cause should survive its own bad bytes
//!   rather than vanish behind a refusal: control characters render as
//!   their spelled-out escapes ([`escape_control_characters`]),
//!   everything else byte-identical.
//! - OPAQUE DATA DOCUMENTS (cursor, receipt, state-doc JSON) get
//!   NEITHER, deliberately: control characters inside their string
//!   values are legitimate source data, and their one render path —
//!   JSON serialization — already spells every control character inert
//!   (backslash-u001b and kin), which the cursor seat's pin holds.

use std::borrow::Cow;

use rdlt_connector_protocol::sanitize;

/// Does `text` carry any character an IDENTIFIER refuses? The one
/// predicate every refusal seat asks — the shared inventory's
/// identifier form, which admits the two joiners U+200C/U+200D (ZWNJ/
/// ZWJ are load-bearing orthography in Persian, Malayalam and
/// Devanagari names; the trade-off is recorded on the predicate
/// itself) and refuses everything else invisible.
pub(crate) fn contains_control(text: &str) -> bool {
    text.chars()
        .any(sanitize::is_control_or_invisible_in_identifier)
}

/// The most BYTES a wire identifier may carry (5L5): the content gates
/// refuse hostile CHARACTERS, but nothing priced the LENGTH — a
/// 60 MiB control-free stream name passed every gate within the frame
/// cap (log swelling, plan noise). Real identifiers are bounded by the
/// destinations' own limits (postgres 63, snowflake 255); a KiB is
/// already absurd for a name, and the socket path's 107-byte gate is
/// the in-tree precedent for bounding vocabulary at the wire edge.
pub(crate) const MAX_WIRE_IDENTIFIER_BYTES: usize = 1024;

/// Is `text` longer than a wire identifier may be? See the constant.
pub(crate) fn is_oversized_identifier(text: &str) -> bool {
    text.len() > MAX_WIRE_IDENTIFIER_BYTES
}

/// Render `text` inert for display: each control or invisible
/// character — the FULL inventory, joiners included, so rendered text
/// cannot hide one — becomes its spelled-out escape (`\n`, `\u{1b}`,
/// …) while every other character — quotes, backslashes, non-ASCII
/// text, which are data — passes byte-identical. Borrowed unchanged
/// when there is nothing to escape, which is every honest message.
pub(crate) fn escape_control_characters(text: &str) -> Cow<'_, str> {
    if !text.chars().any(sanitize::is_control_or_invisible) {
        return Cow::Borrowed(text);
    }
    let mut escaped = String::with_capacity(text.len() + 8);
    for character in text.chars() {
        if sanitize::is_control_or_invisible(character) {
            // `escape_debug` escapes the Cc/Cf/Cs/Co/Cn/Z* categories but
            // passes printable characters through unchanged — and the
            // inventory's two Hangul fillers (U+3164, U+FFA0) are category
            // Lo, i.e. "printable" to it while rendering as blank glyphs.
            // An unchanged escape would render the inventory character
            // raw, falsifying this seat's one invariant — rendered text
            // cannot hide an inventory character — so spell those out
            // explicitly (4L2).
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The rule's edges: C0, DEL, and C1 are control; the shared
    /// inventory's format characters (the Arabic number signs and the
    /// tag block included) refuse; ordinary text — non-ASCII included —
    /// is not.
    #[test]
    fn the_predicate_covers_controls_and_invisible_formatting() {
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
            assert!(contains_control(hostile), "{hostile:?} is control");
        }
        for clean in ["orders", "Événements", "naïve — text", ""] {
            assert!(!contains_control(clean), "{clean:?} is data");
        }
    }

    /// The joiners are orthography, not reordering controls: a Persian
    /// name spelled with ZWNJ passes the IDENTIFIER predicate, while
    /// display escaping still spells the joiner out so rendered names
    /// cannot hide it.
    #[test]
    fn the_joiners_pass_identifiers_but_still_escape_in_display() {
        for name in ["می\u{200c}خواهم", "a\u{200d}b"] {
            assert!(!contains_control(name), "{name:?} is a legal identifier");
        }
        assert_eq!(
            escape_control_characters("a\u{200c}b"),
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
            escape_control_characters("\u{1b}]52;\u{7}\nx \"quoted\" \\slash é"),
            "\\u{1b}]52;\\u{7}\\nx \"quoted\" \\slash é"
        );
        assert!(matches!(
            escape_control_characters("clean \"text\""),
            Cow::Borrowed("clean \"text\"")
        ));
    }

    /// 4L2: the inventory's two Hangul fillers are category `Lo` —
    /// printable to `escape_debug`, which hands them back unchanged while
    /// they render as blank glyphs. The display seat's invariant is that
    /// rendered text cannot hide an inventory character, so the fallback
    /// spells them out.
    #[test]
    fn the_hangul_fillers_escaped_raw_get_spelled_out() {
        assert_eq!(escape_control_characters("a\u{3164}b"), "a\\u{3164}b");
        assert_eq!(escape_control_characters("a\u{ffa0}b"), "a\\u{ffa0}b");
    }

    /// 5L4: the invariant at the REFUSAL seats too — every inventory
    /// character renders spelled-out through the shared escape, so a
    /// refusal quoting a hostile value cannot carry the bytes it
    /// refuses. (Mechanical sweep across the whole table, not a sample.)
    #[test]
    fn no_inventory_character_survives_the_escape_raw() {
        let inventory: Vec<char> = ('\u{0}'..='\u{10FFFF}')
            .filter(|c| rdlt_connector_protocol::sanitize::is_control_or_invisible(*c))
            .collect();
        assert!(inventory.len() > 100, "the sweep covers the inventory");
        for character in inventory {
            let input = character.to_string();
            let escaped = escape_control_characters(&input);
            assert_ne!(
                escaped, input,
                "U+{:04X} must not survive the escape raw",
                character as u32
            );
        }
    }
}
