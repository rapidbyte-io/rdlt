//! Which renderer drives stderr for `run`, resolved from the flags and
//! the terminal.

use crate::args::Output;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    /// Live in-place redraw — a real terminal, progress wanted.
    Pretty,
    /// Line per event — pipes, CI, `--no-progress`, `--output plain`.
    Plain,
    /// Nothing but errors and the report — `-q`, `--output json`.
    Quiet,
}

impl Mode {
    /// The display's terminal is also receiving another stream
    /// (`--events -` with stdout on the terminal): every foreign line
    /// shoves a redrawing display, so Pretty degrades to a line per
    /// event; the other modes are unaffected.
    pub(crate) fn sharing_terminal(self) -> Mode {
        match self {
            Mode::Pretty => Mode::Plain,
            other => other,
        }
    }
}

/// The ladder: `-q` silences everything; `--output json` is machine
/// mode and silences the feed too; `--output plain` forces a line per
/// event; then `-v`, `--no-progress` or a non-terminal stderr force
/// plain (detail lines want scrollback, and a redrawing display would
/// swallow them); a bare terminal gets the live display. `is_tty` is a
/// parameter so the decision is testable.
pub(crate) fn select(
    quiet: bool,
    verbose: bool,
    no_progress: bool,
    output: Output,
    is_tty: bool,
) -> Mode {
    if quiet || output == Output::Json {
        Mode::Quiet
    } else if output == Output::Plain || verbose || no_progress || !is_tty {
        Mode::Plain
    } else {
        Mode::Pretty
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
        assert_eq!(select(true, false, false, Output::Auto, true), Mode::Quiet);
        assert_eq!(select(false, true, false, Output::Auto, true), Mode::Plain);
        assert_eq!(select(false, false, true, Output::Auto, true), Mode::Plain);
        assert_eq!(
            select(false, false, false, Output::Auto, false),
            Mode::Plain
        );
        assert_eq!(
            select(false, false, false, Output::Auto, true),
            Mode::Pretty
        );
    }

    /// `--output` beats the terminal: `plain` forces a line per event on
    /// a TTY, `json` silences the feed on a TTY and ignores `-v`.
    #[test]
    fn the_output_flag_overrides_the_terminal() {
        assert_eq!(
            select(false, false, false, Output::Plain, true),
            Mode::Plain
        );
        assert_eq!(select(false, false, false, Output::Json, true), Mode::Quiet);
        assert_eq!(select(false, true, false, Output::Json, false), Mode::Quiet);
    }

    /// A shared terminal demotes only the redrawing display.
    #[test]
    fn a_shared_terminal_demotes_pretty_to_plain() {
        assert_eq!(Mode::Pretty.sharing_terminal(), Mode::Plain);
        assert_eq!(Mode::Plain.sharing_terminal(), Mode::Plain);
        assert_eq!(Mode::Quiet.sharing_terminal(), Mode::Quiet);
    }
}
