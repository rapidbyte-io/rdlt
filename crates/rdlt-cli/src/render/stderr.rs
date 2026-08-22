//! The one stderr sink and the render-boundary escapes. Connector-
//! controlled text (declared stream names, error messages) rides into
//! human lines verbatim by design; this is the boundary that keeps it
//! from driving the operator's terminal.

use rdlt_core::inventory::is_control_or_invisible;

/// Best-effort stderr line — ONE physical line, whatever the text
/// carries. `eprintln!` PANICS on a closed stderr, which would turn a
/// finished run into exit 101 — the human channel failing must never
/// change the outcome or the exit code. EVERY line is sanitized here,
/// at the one sink, as a one-line record: a newline, carriage return
/// or tab inside the text renders as its visible escape, because the
/// text routinely carries values an operator did not author (a
/// connector's error message, a document's pipeline name, a path from
/// a config), and a line break inside one would mint a forged record
/// — a fake `ok` row, a fake summary — on the operator's terminal. A
/// caller with genuinely multi-line output of its own composes it
/// through [`frame`], whose LINE STRUCTURE is the renderer's and whose
/// data fields it escapes itself.
pub(crate) fn line(line: &str) {
    use std::io::Write as _;
    let line = sanitize_identifier(line);
    let mut stderr = std::io::stderr().lock();
    let _ = stderr
        .write_all(line.as_bytes())
        .and_then(|()| stderr.write_all(b"\n"));
}

/// Best-effort MULTI-LINE frame: the run summary, the `watch`
/// monitor's redraw. The caller's ANSI cursor codes go out RAW (they
/// are the renderer's own constants, never data); the body's line
/// structure is the renderer's own too, and every body line is
/// sanitized at this sink like every other line. What a frame's body
/// must NOT contain raw is a data value — a table name, a stream name,
/// a pipeline name — which the renderer passes through
/// [`sanitize_identifier`] as it composes the frame, so a value cannot
/// add a line the renderer did not draw.
pub(crate) fn frame(header_ansi: &str, body: &str) {
    use std::io::Write as _;
    let mut stderr = std::io::stderr().lock();
    let _ = stderr.write_all(header_ansi.as_bytes());
    for line in body.lines() {
        let _ = stderr.write_all(sanitize_text(line).as_bytes());
        let _ = stderr.write_all(b"\n");
    }
}

/// Escape terminal-control and invisible characters for a line of a
/// MULTI-LINE frame: C0 (except newline and tab — the frame's own
/// formatting, which [`frame`] has already split on), DEL, C1, and the
/// shared inventory's invisible/formatting characters, each rendered
/// as its visible `\u{..}` escape. Unescaped, an ESC or C1 byte is how
/// a hostile connector forges log lines, moves the cursor, or drives
/// OSC sequences (title, clipboard) through an operator's terminal; an
/// admitted joiner (ZWNJ/ZWJ) would render two names differing only by
/// it as one. Applied at the render boundary — never to the data
/// itself.
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

/// Escape every character unsafe in a one-line seat — an identifier
/// interpolated into a frame, or a whole [`line()`]. Unlike
/// [`sanitize_text`], newlines and tabs are not formatting here:
/// accepting them would let a pipeline/table name mint additional
/// terminal lines. The character inventory is the wire protocol's ONE
/// table, so this defense-in-depth layer can never drift narrower than
/// the gate in front of it.
pub(crate) fn sanitize_identifier(text: &str) -> String {
    if !text.chars().any(is_control_or_invisible) {
        return text.to_owned();
    }
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

    /// The one-line record and the frame line differ exactly on the
    /// frame's own formatting: a frame line keeps a tab (the frame
    /// split its newlines already); an identifier, and so a whole
    /// `line`, keeps neither.
    #[test]
    fn the_one_line_escape_keeps_no_line_structure() {
        assert_eq!(sanitize_identifier("a\nb\tc\rd"), "a\\u{a}b\\u{9}c\\u{d}d");
        assert_eq!(sanitize_text("a\nb\tc\rd"), "a\nb\tc\\u{d}d");
        assert_eq!(sanitize_identifier("plain ünïcode"), "plain ünïcode");
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
