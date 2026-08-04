//! Rendering the event feed. Presentation ONLY — the constitution's
//! line: everything the CLI shows, the library computed.

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
/// stderr's answer.
pub fn select(quiet: bool, no_progress: bool, is_tty: bool) -> RendererKind {
    if quiet {
        RendererKind::Quiet
    } else if no_progress || !is_tty {
        RendererKind::Plain
    } else {
        RendererKind::Pretty
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The selection ladder: quiet beats everything, a pipe forces
    /// plain, a terminal without --no-progress gets the live display.
    #[test]
    fn renderer_selection_ladder() {
        assert_eq!(select(true, false, true), RendererKind::Quiet);
        assert_eq!(select(false, true, true), RendererKind::Plain);
        assert_eq!(select(false, false, false), RendererKind::Plain);
        assert_eq!(select(false, false, true), RendererKind::Pretty);
    }
}
