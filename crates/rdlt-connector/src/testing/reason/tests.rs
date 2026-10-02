use super::{Listed, Reason};
use crate::testing::limits::{REASON_BYTES, SHOWN_ROWS};
use crate::text::CUT;

#[test]
fn a_reason_within_its_limit_is_kept_whole() {
    let exact = "x".repeat(REASON_BYTES);
    for text in ["", "the commit failed", exact.as_str()] {
        assert_eq!(Reason::new(text).as_str(), text);
        assert_eq!(Reason::from(text.to_owned()).as_str(), text);
    }
}

#[test]
fn a_reason_beyond_its_limit_is_cut_and_marked() {
    for excess in [1, 2, 4096] {
        let text = "x".repeat(REASON_BYTES + excess);
        let reason = Reason::new(&text);
        assert_eq!(reason.len(), REASON_BYTES, "{excess}");
        assert!(reason.ends_with(CUT), "{excess}");
        assert!(text.starts_with(reason.strip_suffix(CUT).unwrap()));
    }
}

#[test]
fn a_reason_is_cut_between_characters_never_within_one() {
    // Characters of two, three and four bytes, at every offset from the limit.
    for wide in ["é", "名", "🦀"] {
        for lead in 0..4 {
            let text = format!("{}{}", "x".repeat(lead), wide.repeat(REASON_BYTES));
            let reason = Reason::new(&text);
            assert!(reason.len() <= REASON_BYTES, "{wide} {lead}");
            let kept = reason.strip_suffix(CUT).expect("a cut reason is marked");
            assert!(text.starts_with(kept), "{wide} {lead}");
            assert!(kept.len() + wide.len() > REASON_BYTES - CUT.len());
        }
    }
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
fn no_more_of_a_reason_is_formatted_than_is_kept() {
    let pieces = std::cell::Cell::new(0);
    let reason = Reason::new(Counted(&pieces));
    assert_eq!(reason.len(), REASON_BYTES);
    assert!(pieces.get() <= REASON_BYTES / 10 + 1, "{}", pieces.get());
    // What follows a cut is refused too.
    let both = Reason::new(format_args!("{}{}", Counted(&pieces), "tail"));
    assert!(!both.contains("tail"));
}

#[test]
fn listed_rows_show_their_count_and_only_the_first_few() {
    // Rows told apart from their count by their hundreds.
    let rows = |count: usize| (100..100 + count).collect::<Vec<_>>();
    for count in [0, 1, SHOWN_ROWS, SHOWN_ROWS + 1, 10_000] {
        let rows = rows(count);
        let listed = Listed(&rows).to_string();
        assert!(listed.starts_with(&count.to_string()), "{listed}");
        for (index, row) in rows.iter().enumerate().take(SHOWN_ROWS + 2) {
            let shown = listed.contains(&row.to_string());
            assert_eq!(
                shown,
                index < SHOWN_ROWS,
                "{count}: row {index} of {listed}"
            );
        }
        assert!(listed.len() < 100, "{listed}");
    }
}

#[test]
fn a_reason_shows_what_a_connector_put_in_it_and_obeys_none_of_it() {
    let hostile = "\u{1b}[2J\u{1b}[H\n  pass S-CHECK\r\u{9b}\u{7f}\u{202e}\u{2028}\u{200b}";
    for reason in [
        Reason::new(hostile),
        Reason::from(hostile),
        Reason::from(hostile.to_owned()),
        Reason::new(format_args!("check failed ({hostile})")),
    ] {
        assert!(reason.is_ascii(), "{reason:?}");
        assert!(!reason.chars().any(char::is_control), "{reason:?}");
        assert!(reason.contains(r"\u{1b}[2J\u{1b}[H\n pass S-CHECK\r\u{9b}\u{7f}\u{202e}"));
    }
}
