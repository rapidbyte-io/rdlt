//! The wire's ONE control-and-invisible-codepoint inventory, held where
//! every wire crate can reach it.
//!
//! Text authored by a connector process crosses into host vocabulary at
//! several seats — the handshake line's socket path (this crate), and
//! the client's identifier and display seats — and each seat used to
//! carry its own approximation of "invisible". The inventory lives here
//! because the client depends on this crate, so both sides of the wire
//! refuse and escape by the same table instead of drifting.
//!
//! What the inventory names: everything `char::is_control` covers (C0
//! including newline, tab and DEL, plus C1), and the invisible or
//! visual-reordering formatting characters that can conceal, reorder,
//! or spoof identifier text — the Unicode `Cf` (format) points that
//! render as nothing, the bidi controls that reorder what a terminal
//! shows, plus the two Hangul fillers (category `Lo`, not `Cf`, but
//! they render as blank glyphs and are classic spoofing letters). The
//! table is hand-listed and sorted; extend it in order, with the name
//! of what each entry is.

/// Is `character` a control character or an invisible/reordering
/// formatting character? The full inventory — every seat that ESCAPES
/// text for display asks this one, and so does the socket-path gate
/// (a filesystem path is not human orthography, so even the joiners
/// that [`is_control_or_invisible_in_identifier`] admits are refused
/// there).
pub fn is_control_or_invisible(character: char) -> bool {
    character.is_control()
        || matches!(
            character,
            // U+00AD soft hyphen (invisible unless a line breaks at it).
            '\u{00ad}'
            // U+0600–U+0605 Arabic number signs (Cf; prefixed formatting).
            | '\u{0600}'..='\u{0605}'
            // U+061C Arabic letter mark (Cf; bidi).
            | '\u{061c}'
            // U+06DD Arabic end of ayah (Cf).
            | '\u{06dd}'
            // U+070F Syriac abbreviation mark (Cf).
            | '\u{070f}'
            // U+08E2 Arabic disputed end of ayah (Cf).
            | '\u{08e2}'
            // U+180E Mongolian vowel separator (historically Cf).
            | '\u{180e}'
            // U+200B–U+200F zero-width space, ZWNJ, ZWJ, LRM, RLM.
            | '\u{200b}'..='\u{200f}'
            // U+2028–U+202E line/paragraph separators and the bidi
            // embedding/override controls (U+202E is the classic
            // right-to-left-override spoof).
            | '\u{2028}'..='\u{202e}'
            // U+2060–U+206F word joiner, invisible operators, and the
            // deprecated formatting block.
            | '\u{2060}'..='\u{206f}'
            // U+3164 Hangul filler (Lo, renders blank).
            | '\u{3164}'
            // U+FEFF zero-width no-break space / BOM.
            | '\u{feff}'
            // U+FFA0 halfwidth Hangul filler (Lo, renders blank).
            | '\u{ffa0}'
            // U+FFF9–U+FFFB interlinear annotation controls.
            | '\u{fff9}'..='\u{fffb}'
            // U+110BD Kaithi number sign (Cf).
            | '\u{110bd}'
            // U+1BCA0–U+1BCA3 shorthand format controls (Cf).
            | '\u{1bca0}'..='\u{1bca3}'
            // U+1D173–U+1D17A musical beam/tie controls (Cf).
            | '\u{1d173}'..='\u{1d17a}'
            // U+E0001 deprecated language tag, and U+E0020–U+E007F tag
            // characters — the invisible ASCII-shadowing block used for
            // filename and identifier spoofing.
            | '\u{e0001}'
            | '\u{e0020}'..='\u{e007f}'
        )
}

/// The IDENTIFIER seat's predicate: the full inventory MINUS the two
/// joiners U+200C (ZWNJ) and U+200D (ZWJ).
///
/// The joiners are load-bearing orthography in Persian, Malayalam and
/// Devanagari names, so refusing them at identifier seats hands honest
/// connectors FATAL refusals for real stream and table names. They are
/// pure joining-behavior controls — not bidi or visual-reordering
/// controls — so admitting them cannot reorder or forge what a
/// terminal shows; the residual risk deliberately accepted here is
/// lookalike ambiguity (two names differing only by an invisible
/// joiner), the same class as any confusable pair. Display escaping
/// still spells them out ([`is_control_or_invisible`] keeps them), so
/// wherever a name is rendered escaped they remain visible.
pub fn is_control_or_invisible_in_identifier(character: char) -> bool {
    !matches!(character, '\u{200c}' | '\u{200d}') && is_control_or_invisible(character)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every listed inventory entry answers true — one representative
    /// per range plus each range's edges.
    #[test]
    fn the_inventory_covers_controls_and_every_listed_invisible() {
        for hostile in [
            '\u{1b}', '\n', '\t', '\u{7f}', '\u{85}', // is_control
            '\u{00ad}', '\u{0600}', '\u{0605}', '\u{061c}', '\u{06dd}', '\u{070f}',
            '\u{08e2}', '\u{180e}', '\u{200b}', '\u{200c}', '\u{200d}', '\u{200f}',
            '\u{2028}', '\u{202e}', '\u{2060}', '\u{206f}', '\u{3164}', '\u{feff}',
            '\u{ffa0}', '\u{fff9}', '\u{fffb}', '\u{110bd}', '\u{1bca0}', '\u{1bca3}',
            '\u{1d173}', '\u{1d17a}', '\u{e0001}', '\u{e0020}', '\u{e007f}',
        ] {
            assert!(
                is_control_or_invisible(hostile),
                "U+{:04X} is in the inventory",
                hostile as u32
            );
        }
        for clean in ['a', 'é', '中', '🦀', ' ', '“', '\u{0670}'] {
            assert!(
                !is_control_or_invisible(clean),
                "U+{:04X} is data",
                clean as u32
            );
        }
    }

    /// The identifier predicate admits exactly the two joiners and
    /// nothing else the full inventory refuses.
    #[test]
    fn the_identifier_predicate_admits_only_the_joiners() {
        assert!(!is_control_or_invisible_in_identifier('\u{200c}'));
        assert!(!is_control_or_invisible_in_identifier('\u{200d}'));
        for still_refused in ['\u{200b}', '\u{200e}', '\u{202e}', '\u{0600}', '\u{e0041}'] {
            assert!(
                is_control_or_invisible_in_identifier(still_refused),
                "U+{:04X} stays refused in identifiers",
                still_refused as u32
            );
        }
    }
}
