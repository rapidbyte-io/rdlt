//! The one stderr sink and the render-boundary escapes. Connector-
//! controlled text (declared stream names, error messages) rides into
//! human lines verbatim by design; this is the boundary that keeps it
//! from driving the operator's terminal.

use rdlt_connector_protocol::inventory::is_control_or_invisible;

/// Best-effort stderr line: `eprintln!` PANICS on a closed stderr,
/// which would turn a finished run into exit 101 — the human channel
/// failing must never change the outcome or the exit code. EVERY line
/// is sanitized here, at the one sink.
pub(crate) fn line(line: &str) {
    use std::io::Write as _;
    let line = sanitize_text(line);
    let mut stderr = std::io::stderr().lock();
    let _ = stderr
        .write_all(line.as_bytes())
        .and_then(|()| stderr.write_all(b"\n"));
}

/// Escape terminal-control and invisible characters for MULTI-LINE
/// display text: C0 (except newline and tab — legitimate formatting in
/// multi-line error text), DEL, C1, and the shared inventory's
/// invisible/formatting characters, each rendered as its visible
/// `\u{..}` escape. Unescaped, an ESC or C1 byte is how a hostile
/// connector forges log lines, moves the cursor, or drives OSC sequences
/// (title, clipboard) through an operator's terminal; an admitted joiner
/// (ZWNJ/ZWJ) would render two names differing only by it as one.
/// Applied at the render boundary — never to the data itself.
pub(crate) fn sanitize_text(text: &str) -> String {
    let hostile = is_control_or_invisible;
    if !text.chars().any(|c| hostile(c) && c != '\n' && c != '\t') {
        return text.to_owned();
    }
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        if hostile(c) && c != '\n' && c != '\t' {
            use std::fmt::Write as _;
            let _ = write!(out, "\\u{{{:x}}}", c as u32);
        } else {
            out.push(c);
        }
    }
    out
}

/// Escape every character unsafe in a one-line identifier seat. Unlike
/// [`sanitize_text`], newlines and tabs are not formatting here:
/// accepting them would let a pipeline/table name mint additional
/// terminal lines. The character inventory is the wire protocol's ONE
/// table, so this defense-in-depth layer can never drift narrower than
/// the gate in front of it.
pub(crate) fn sanitize_identifier(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for character in text.chars() {
        if is_control_or_invisible(character) {
            use std::fmt::Write as _;
            let _ = write!(out, "\\u{{{:x}}}", character as u32);
        } else {
            out.push(character);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The render-boundary escape: terminal controls become visible
    /// escapes — the OSC/CSI and carriage-return shapes a hostile
    /// connector would drive a terminal or forge log lines with — while
    /// plain text, newlines and tabs pass untouched.
    #[test]
    fn sanitize_escapes_terminal_controls_and_keeps_text() {
        assert_eq!(sanitize_text("events: +3 rows"), "events: +3 rows");
        assert_eq!(
            sanitize_text("line\nbreak\tand tab"),
            "line\nbreak\tand tab"
        );
        // ESC-driven OSC (clipboard write) with its BEL terminator.
        assert_eq!(
            sanitize_text("\x1b]52;c;evil\x07"),
            "\\u{1b}]52;c;evil\\u{7}"
        );
        // A C1 CSI, one byte, no ESC needed on many terminals.
        assert_eq!(sanitize_text("\u{9b}31m"), "\\u{9b}31m");
        // A bare carriage return overwrites the line it lands on.
        assert_eq!(sanitize_text("a\rb"), "a\\u{d}b");
    }

    #[test]
    fn identifier_sanitizing_refuses_line_breaks_and_bidi_formatting() {
        assert_eq!(
            sanitize_identifier("pipeline\nFORGED\u{202e}"),
            "pipeline\\u{a}FORGED\\u{202e}"
        );
    }

    /// The display escape covers the FULL inventory — the joiners the
    /// identifier gates deliberately admit (ZWNJ/ZWJ, orthography in
    /// Persian/Malayalam/Devanagari names) must still be SPELLED OUT in
    /// rendered text, or two names differing only by an invisible
    /// joiner display as the same name. Newline and tab stay
    /// (multi-line error formatting).
    #[test]
    fn the_display_escape_spells_the_invisible_inventory() {
        assert_eq!(sanitize_text("a\u{200c}b"), "a\\u{200c}b");
        assert_eq!(sanitize_text("a\u{3164}b"), "a\\u{3164}b");
        assert_eq!(sanitize_text("line\nbreak\ttab"), "line\nbreak\ttab");
        assert_eq!(sanitize_text("Événements"), "Événements", "text is data");
    }
}
