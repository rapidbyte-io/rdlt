//! Rendering the event feed. Presentation ONLY — the constitution's
//! line: everything the CLI shows, the library computed.

/// Best-effort stderr line: `eprintln!` PANICS on a closed stderr,
/// which would turn a finished run into exit 101 — the human channel
/// failing must never change the outcome or the exit code. EVERY line
/// is sanitized here, at the one sink: connector-controlled strings
/// (declared stream names, error text) ride into these lines verbatim
/// by design, and this is the boundary that keeps them from driving
/// the terminal.
pub fn stderr_line(line: &str) {
    use std::io::Write as _;
    let line = sanitize_display_text(line);
    let mut stderr = std::io::stderr().lock();
    let _ = stderr
        .write_all(line.as_bytes())
        .and_then(|()| stderr.write_all(b"\n"));
}

/// Escape terminal-control and invisible characters for MULTI-LINE display
/// text: C0 (except newline
/// and tab — legitimate formatting in multi-line error text), DEL, and
/// C1, each rendered as its visible `\u{..}` escape — plus the shared
/// inventory's invisible/formatting characters (5L14: the joiners the
/// identifier gates deliberately ADMIT still render invisibly, so a
/// stream name differing from its twin only by a ZWNJ must not display
/// as its twin). The 038/044 model deliberately runs third-party
/// connector binaries, and a declared stream name or error message is
/// their text: unescaped, an ESC or C1 byte is how a hostile connector
/// forges log lines, moves the cursor, or drives OSC sequences (title,
/// clipboard) through an operator's terminal. Applied at the RENDER
/// boundary — [`stderr_line`] and the pretty renderer's messages —
/// never to the data itself.
pub fn sanitize_display_text(text: &str) -> String {
    use rdlt_connector_protocol::sanitize::is_control_or_invisible as hostile;
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
/// [`sanitize`], newlines and tabs are not formatting here: accepting
/// them would let a pipeline/table name mint additional terminal lines.
///
/// The character inventory is the wire protocol's ONE table (4L3): this
/// seat previously carried its own approximation, which had drifted
/// narrower than the boundary it backs — the wire gates refuse first, but
/// a defense-in-depth layer must not be narrower than the layer in front.
pub fn sanitize_identifier(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for character in text.chars() {
        if rdlt_connector_protocol::sanitize::is_control_or_invisible(character) {
            use std::fmt::Write as _;
            let _ = write!(out, "\\u{{{:x}}}", character as u32);
        } else {
            out.push(character);
        }
    }
    out
}

pub mod format;
pub mod plain;
pub mod pretty;
pub mod summary;

/// Which renderer drives stderr for `run`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RendererKind {
    /// Live in-place redraw — a real terminal, progress wanted.
    Pretty,
    /// Line per event — pipes, CI, `--no-progress`.
    Plain,
    /// Nothing but errors and the report.
    Quiet,
}

/// Resolve the renderer from flags and the terminal. `is_tty` is a
/// parameter so the decision is testable; the caller passes the real
/// stderr's answer. `-v` forces plain: its detail lines are the point
/// of asking, and a redrawing display would swallow them.
pub fn select(quiet: bool, verbose: bool, no_progress: bool, is_tty: bool) -> RendererKind {
    if quiet {
        RendererKind::Quiet
    } else if verbose || no_progress || !is_tty {
        RendererKind::Plain
    } else {
        RendererKind::Pretty
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The render-boundary escape (045 external findings, GROK 10 /
    /// KIMI 1): terminal controls become visible escapes — the OSC/CSI
    /// and carriage-return shapes a hostile connector would drive a
    /// terminal or forge log lines with — while plain text, newlines
    /// and tabs pass untouched.
    #[test]
    fn sanitize_escapes_terminal_controls_and_keeps_text() {
        assert_eq!(sanitize_display_text("events: +3 rows"), "events: +3 rows");
        assert_eq!(
            sanitize_display_text("line\nbreak\tand tab"),
            "line\nbreak\tand tab"
        );
        // ESC-driven OSC (clipboard write) with its BEL terminator.
        assert_eq!(
            sanitize_display_text("\x1b]52;c;evil\x07"),
            "\\u{1b}]52;c;evil\\u{7}"
        );
        // A C1 CSI, one byte, no ESC needed on many terminals.
        assert_eq!(sanitize_display_text("\u{9b}31m"), "\\u{9b}31m");
        // A bare carriage return overwrites the line it lands on.
        assert_eq!(sanitize_display_text("a\rb"), "a\\u{d}b");
    }

    #[test]
    fn identifier_sanitizing_refuses_line_breaks_and_bidi_formatting() {
        assert_eq!(
            sanitize_identifier("pipeline\nFORGED\u{202e}"),
            "pipeline\\u{a}FORGED\\u{202e}"
        );
    }

    /// 5L14: the display escape covers the FULL inventory — the joiners
    /// the identifier gates deliberately admit (ZWNJ/ZWJ, orthography in
    /// Persian/Malayalam/Devanagari names) must still be SPELLED OUT in
    /// rendered text, or two names differing only by an invisible joiner
    /// display as the same name. Newline and tab stay (multi-line error
    /// formatting).
    #[test]
    fn the_display_escape_spells_the_invisible_inventory() {
        assert_eq!(sanitize_display_text("a\u{200c}b"), "a\\u{200c}b");
        assert_eq!(sanitize_display_text("a\u{3164}b"), "a\\u{3164}b");
        assert_eq!(
            sanitize_display_text("line\nbreak\ttab"),
            "line\nbreak\ttab"
        );
        assert_eq!(
            sanitize_display_text("Événements"),
            "Événements",
            "text is data"
        );
    }

    /// The selection ladder: quiet beats everything; -v, a pipe, or
    /// --no-progress force plain; a bare terminal gets the live
    /// display.
    #[test]
    fn renderer_selection_ladder() {
        assert_eq!(select(true, false, false, true), RendererKind::Quiet);
        assert_eq!(select(false, true, false, true), RendererKind::Plain);
        assert_eq!(select(false, false, true, true), RendererKind::Plain);
        assert_eq!(select(false, false, false, false), RendererKind::Plain);
        assert_eq!(select(false, false, false, true), RendererKind::Pretty);
    }
}
