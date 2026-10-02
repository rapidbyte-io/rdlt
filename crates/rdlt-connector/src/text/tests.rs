use proptest::prelude::*;

use super::{CUT, shown};

/// Every character of a general category that hides, reorders, breaks or pads the text
/// around it, or acts on a terminal, and every one a renderer ignores by default: all but
/// the space itself.
fn deceiving() -> regex::Regex {
    let categories = r"\p{Cc}\p{Cf}\p{Zl}\p{Zp}\p{Zs}\p{Co}\p{Default_Ignorable_Code_Point}";
    let noncharacters = r"\p{Noncharacter_Code_Point}";
    regex::Regex::new(&format!("[{categories}{noncharacters}--[ ]]")).expect("a valid class")
}

fn characters() -> impl Iterator<Item = char> {
    (0..=u32::from(char::MAX)).filter_map(char::from_u32)
}

#[test]
fn every_character_that_can_deceive_a_reader_is_shown_as_an_escape_that_names_it() {
    let deceiving = deceiving();
    let mut buffer = [0; 4];
    let mut escaped = 0_u32;
    for c in characters() {
        let raw: &str = c.encode_utf8(&mut buffer);
        if !deceiving.is_match(raw) {
            continue;
        }
        escaped += 1;
        let text = shown(format_args!("a{c}b"), 64);
        let escape: String = c.escape_default().collect();
        assert_eq!(text, format!("a{escape}b"), "U+{:04X}", u32::from(c));
        assert!(text.is_ascii(), "U+{:04X}", u32::from(c));
    }
    // Controls, format characters, separators, spaces, private use and ignorables: none of
    // the categories is empty.
    assert!(escaped > 130_000, "{escaped}");
}

#[test]
fn letters_marks_digits_symbols_and_punctuation_are_shown_as_they_are() {
    let kept = regex::Regex::new(r"^[\p{L}\p{M}\p{N}\p{P}\p{S}]$").expect("a valid class");
    let (deceiving, mut buffer) = (deceiving(), [0; 4]);
    let mut same = 0_u32;
    for c in characters() {
        let raw: &str = c.encode_utf8(&mut buffer);
        if !kept.is_match(raw) || deceiving.is_match(raw) {
            continue;
        }
        // The blank of Braille is a symbol that shows as a space.
        if c == '\u{2800}' {
            continue;
        }
        same += 1;
        assert_eq!(shown(raw, 64), raw, "U+{:04X}", u32::from(c));
    }
    assert!(same > 100_000, "{same}");
}

#[test]
fn a_character_that_shows_as_blank_is_escaped_whatever_its_category() {
    for c in [
        '\u{2800}', '\u{3164}', '\u{115f}', '\u{ffa0}', '\u{a0}', '\u{3000}',
    ] {
        let text = shown(c, 64);
        assert!(text.starts_with("\\u{"), "U+{:04X}: {text}", u32::from(c));
    }
}

#[test]
fn what_a_terminal_would_obey_reads_as_text_on_one_line() {
    let hostile = "\u{1b}[2J\u{1b}]0;title\u{7}\r INFO fake\n  pass S-CHECK\u{9b}2J\u{7f}";
    let text = shown(hostile, 256);
    assert_eq!(
        text,
        r"\u{1b}[2J\u{1b}]0;title\u{7}\r INFO fake\n pass S-CHECK\u{9b}2J\u{7f}"
    );
}

#[test]
fn a_run_of_spaces_is_one_space() {
    for (given, kept) in [
        ("a b", "a b"),
        ("a  b", "a b"),
        ("x)          pass S-RESUME", "x) pass S-RESUME"),
        ("  lead and trail  ", " lead and trail "),
        // Spaces in pieces of a text formatted apart are one run.
        ("a \t b", r"a \t b"),
    ] {
        assert_eq!(shown(given, 64), kept, "{given:?}");
    }
    assert_eq!(shown(format_args!("{}{}", "a ", " b"), 64), "a b");
    let padded = format!("x){}pass", " ".repeat(1 << 16));
    assert_eq!(shown(padded, 64), "x) pass");
}

#[test]
fn a_text_that_fits_is_kept_whole_and_one_that_does_not_is_cut_and_marked() {
    for bytes in [CUT.len(), 7, 16, 64, 2048] {
        for length in 0..=bytes + 3 {
            let given = "x".repeat(length);
            let text = shown(&given, bytes);
            if length <= bytes {
                assert_eq!(text, given, "{length} in {bytes}");
            } else {
                assert_eq!(text.len(), bytes, "{length} in {bytes}");
                let kept = text.strip_suffix(CUT).expect("a cut text is marked");
                assert!(given.starts_with(kept));
            }
        }
    }
}

#[test]
fn a_text_is_cut_between_characters_and_escapes_never_within_one() {
    for wide in ["é", "名", "🦀", "\u{202e}", "\n"] {
        for lead in 0..12 {
            let given = format!("{}{}", "x".repeat(lead), wide.repeat(64));
            let text = shown(&given, 48);
            assert!(text.len() <= 48, "{wide:?} {lead}");
            let kept = text.strip_suffix(CUT).expect("a cut text is marked");
            let whole = shown(&given, usize::MAX);
            assert!(whole.starts_with(kept), "{wide:?} {lead}: {text}");
            // What follows the kept text is a whole character or escape.
            let piece = shown(wide, 64);
            assert_eq!(
                (kept.len() - lead) % piece.len(),
                0,
                "{wide:?} {lead}: {text}"
            );
        }
    }
}

#[test]
fn a_cut_leaves_no_space_before_its_mark() {
    let text = shown("abcdefghi jklmnop", 16);
    assert_eq!(text, format!("abcdefghi{CUT}"));
}

/// Counts how much of it was formatted.
struct Counted<'a>(&'a std::cell::Cell<usize>);

impl std::fmt::Display for Counted<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for _ in 0..1_000_000 {
            self.0.set(self.0.get() + 1);
            formatter.write_str("0123456789")?;
        }
        Ok(())
    }
}

#[test]
fn no_more_of_a_text_is_formatted_than_is_kept() {
    let pieces = std::cell::Cell::new(0);
    let text = shown(Counted(&pieces), 100);
    assert_eq!(text.len(), 100);
    assert!(pieces.get() <= 11, "{}", pieces.get());
    let both = shown(format_args!("{}{}", Counted(&pieces), "tail"), 100);
    assert!(!both.contains("tail"));
}

#[test]
fn a_limit_smaller_than_the_mark_keeps_the_mark_alone() {
    assert_eq!(shown("abcdefgh", 3), CUT);
    assert_eq!(shown("ab", 3), "ab");
}

proptest! {
    #[test]
    fn showing_what_was_shown_changes_nothing(given in any::<String>(), bytes in 6_usize..200) {
        let once = shown(&given, bytes);
        prop_assert_eq!(shown(&once, bytes), once.clone());
        prop_assert_eq!(shown(&once, usize::MAX), once);
    }

    #[test]
    fn a_shown_text_holds_nothing_that_deceives(given in any::<String>()) {
        let text = shown(&given, usize::MAX);
        prop_assert!(!deceiving().is_match(&text), "{:?}", text);
        prop_assert!(!text.contains("  "));
    }

    #[test]
    fn a_shown_text_is_the_same_in_json_and_out(given in any::<String>()) {
        let text = shown(&given, 512);
        let json = serde_json::to_string(&text).expect("a string serializes");
        // Nothing but the quote and the backslash needs JSON's own escapes.
        prop_assert!(json.is_ascii() || !deceiving().is_match(&json));
        let back: String = serde_json::from_str(&json).expect("it parses");
        prop_assert_eq!(back, text);
    }
}
