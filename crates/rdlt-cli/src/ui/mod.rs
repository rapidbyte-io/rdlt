//! Rendering the event feed. Presentation ONLY — the constitution's
//! line: everything the CLI shows, the library computed.

/// Best-effort stderr line: `eprintln!` PANICS on a closed stderr,
/// which would turn a finished run into exit 101 — the human channel
/// failing must never change the outcome or the exit code.
pub fn stderr_line(line: &str) {
    use std::io::Write as _;
    let mut stderr = std::io::stderr().lock();
    let _ = stderr
        .write_all(line.as_bytes())
        .and_then(|()| stderr.write_all(b"\n"));
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
