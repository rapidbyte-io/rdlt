use super::*;

type Vars = &'static [(&'static str, &'static str)];

fn found(vars: &[(&str, &str)], terminal: bool) -> Found {
    let get = |name: &str| {
        vars.iter()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| (*value).to_owned())
    };
    Found {
        term: get("TERM"),
        no_color: get("NO_COLOR"),
        clicolor_force: get("CLICOLOR_FORCE"),
        lc_all: get("LC_ALL"),
        lc_ctype: get("LC_CTYPE"),
        lang: get("LANG"),
        rust_log: get("RUST_LOG"),
        collector: get("OTEL_EXPORTER_OTLP_ENDPOINT"),
        out_terminal: terminal,
        err_terminal: terminal,
    }
}

fn asked(color: Color) -> Asked {
    Asked {
        color,
        quiet: false,
        events_on_stdout: false,
        verbose_logs: false,
    }
}

#[test]
fn colour_is_decided_by_terminal_term_no_color_clicolor_force_and_the_flag() {
    let cases: [(Vars, bool, Color, bool); 12] = [
        (&[("TERM", "xterm")], true, Color::Auto, true),
        (&[("TERM", "xterm")], false, Color::Auto, false),
        (&[], true, Color::Auto, true),
        (&[("TERM", "dumb")], true, Color::Auto, false),
        (&[("NO_COLOR", "1")], true, Color::Auto, false),
        (&[("NO_COLOR", "")], true, Color::Auto, true),
        (&[("CLICOLOR_FORCE", "1")], false, Color::Auto, true),
        (&[("CLICOLOR_FORCE", "0")], false, Color::Auto, false),
        (
            &[("CLICOLOR_FORCE", "1"), ("NO_COLOR", "1")],
            false,
            Color::Auto,
            false,
        ),
        (
            &[("TERM", "dumb"), ("CLICOLOR_FORCE", "yes")],
            false,
            Color::Auto,
            true,
        ),
        (
            &[("NO_COLOR", "1"), ("TERM", "dumb")],
            false,
            Color::Always,
            true,
        ),
        (&[("CLICOLOR_FORCE", "1")], true, Color::Never, false),
    ];
    for (vars, terminal, color, expected) in cases {
        let looks = Looks::resolved(asked(color), &found(vars, terminal));
        assert_eq!(
            looks.out.coloured, expected,
            "{vars:?} {terminal} {color:?}"
        );
        assert_eq!(
            looks.err.coloured, expected,
            "{vars:?} {terminal} {color:?}"
        );
    }
}

#[test]
fn colour_is_per_stream_where_only_one_is_a_terminal() {
    let mut mixed = found(&[("TERM", "xterm")], false);
    mixed.err_terminal = true;
    let looks = Looks::resolved(asked(Color::Auto), &mixed);
    assert!(!looks.out.coloured);
    assert!(looks.err.coloured);
    assert_eq!(looks.out(), Brush(false));
    assert_eq!(looks.err(), Brush(true));
}

#[test]
fn animation_needs_a_terminal_on_standard_error_and_no_quiet_and_no_dumb_term() {
    let cases = [
        (&[("TERM", "xterm")][..], true, false, true),
        (&[("TERM", "dumb")][..], true, false, false),
        (&[("TERM", "xterm")][..], false, false, false),
        (&[("TERM", "xterm")][..], true, true, false),
    ];
    for (vars, err_terminal, quiet, expected) in cases {
        let mut environment = found(vars, false);
        environment.err_terminal = err_terminal;
        let asked = Asked {
            quiet,
            ..asked(Color::Auto)
        };
        let looks = Looks::resolved(asked, &environment);
        assert_eq!(looks.animates, expected, "{vars:?} {err_terminal} {quiet}");
    }
    let always = Looks::resolved(asked(Color::Always), &found(&[("TERM", "dumb")], true));
    assert!(
        !always.animates,
        "colour does not make a dumb terminal animate"
    );
}

#[test]
fn another_writer_on_the_terminal_stops_the_animation() {
    let animates = |asked: Asked, vars: Vars, out_terminal: bool| {
        let mut environment = found(vars, true);
        environment.out_terminal = out_terminal;
        Looks::resolved(asked, &environment).animates
    };
    let events = Asked {
        events_on_stdout: true,
        ..asked(Color::Auto)
    };
    assert!(!animates(events, &[], true), "events on a terminal");
    assert!(animates(events, &[], false), "events into a pipe");
    let verbose = Asked {
        verbose_logs: true,
        ..asked(Color::Auto)
    };
    assert!(!animates(verbose, &[], true));
    assert!(!animates(asked(Color::Auto), &[("RUST_LOG", "info")], true));
    assert!(
        animates(asked(Color::Auto), &[("RUST_LOG", "")], true),
        "an empty RUST_LOG is none"
    );
    let collector: Vars = &[("OTEL_EXPORTER_OTLP_ENDPOINT", "http://x")];
    assert!(!animates(asked(Color::Auto), collector, true));
    assert!(animates(asked(Color::Auto), &[], true));
}

#[test]
fn unicode_marks_follow_the_first_of_lc_all_lc_ctype_and_lang_set() {
    let cases: [(Vars, bool); 7] = [
        (&[("LC_ALL", "en_US.UTF-8")], true),
        (&[("LC_ALL", "C"), ("LANG", "en_US.UTF-8")], false),
        (&[("LC_CTYPE", "C.utf8"), ("LANG", "C")], true),
        (&[("LANG", "de_DE.UTF8")], true),
        (&[("LANG", "POSIX")], false),
        (&[("LC_ALL", ""), ("LANG", "en_US.UTF-8")], true),
        (&[], false),
    ];
    for (vars, unicode) in cases {
        let looks = Looks::resolved(asked(Color::Auto), &found(vars, true));
        assert_eq!(looks.marks == &UNICODE, unicode, "{vars:?}");
        assert_eq!(looks.marks == &ASCII, !unicode, "{vars:?}");
    }
}

#[test]
fn a_brush_applies_its_tone_only_where_colour_is_on() {
    assert_eq!(Brush(false).paint(Tone::Good, "x"), "x");
    let painted = Brush(true).paint(Tone::Bad, "x");
    assert!(
        painted.starts_with("\u{1b}[31m") && painted.contains('x'),
        "{painted:?}"
    );
    for (tone, code) in [
        (Tone::Good, "32"),
        (Tone::Bad, "31"),
        (Tone::Warn, "33"),
        (Tone::Name, "36"),
        (Tone::Dim, "2"),
    ] {
        let painted = Brush(true).paint(tone, "x");
        assert!(
            painted.contains(&format!("\u{1b}[{code}m")),
            "{tone:?}: {painted:?}"
        );
    }
}

#[test]
fn counted_keeps_singular_nouns_singular() {
    assert_eq!(counted(0, "row", "rows"), "0 rows");
    assert_eq!(counted(1, "row", "rows"), "1 row");
    assert_eq!(counted(2, "commit", "commits"), "2 commits");
    assert_eq!(counted(1_048_203, "row", "rows"), "1,048,203 rows");
}

#[test]
fn short_id_is_the_last_segment_of_a_connector_s_id() {
    assert_eq!(short_id("io.rapidbyte.postgres"), "postgres");
    assert_eq!(short_id("plain"), "plain");
    assert_eq!(short_id("a.b\u{1b}c"), "b\\u{1b}c", "shown, not obeyed");
}

#[test]
fn fit_cuts_a_line_to_one_less_than_the_width() {
    let marks = &UNICODE;
    assert_eq!(fit("short", 80, marks), "short");
    let fitted = fit("abcdefghij", 5, marks);
    assert_eq!(console::measure_text_width(&fitted), 4);
    assert!(fitted.ends_with('…'), "{fitted}");
    let ascii = fit("abcdefghij", 8, &ASCII);
    assert_eq!(console::measure_text_width(&ascii), 7);
    assert!(ascii.ends_with("..."), "{ascii}");
    let styled = Brush(true).paint(Tone::Good, "abcdefghijklmnop");
    let fitted = fit(&styled, 10, marks);
    assert_eq!(console::measure_text_width(&fitted), 9);
    assert!(fitted.contains('\u{1b}'), "colour is kept: {fitted:?}");
}

#[test]
fn wrapped_breaks_at_spaces_keeps_each_word_and_indents_continuations() {
    assert_eq!(wrapped("one two", 80, "  "), ["one two"]);
    assert_eq!(wrapped("one two three", 7, ""), ["one two", "three"]);
    assert_eq!(wrapped("one two three", 9, "  "), ["one two", "  three"]);
    assert_eq!(wrapped("aaaaa", 5, ""), ["aaaaa"], "as wide as a line");
    let long = "aaaaaaaaaaaaaaa";
    assert_eq!(wrapped(long, 5, ""), ["aaaaa", "aaaaa", "aaaaa"]);
    assert_eq!(
        wrapped(long, 5, "  "),
        ["aaa", "  aaa", "  aaa", "  aaa", "  aaa"]
    );
    for line in wrapped(
        "word another-word yet-another-rather-longer-word end",
        12,
        "  ",
    ) {
        assert!(console::measure_text_width(&line) <= 12, "{line}");
    }
}

#[test]
fn wrapped_leaves_a_painted_word_whole_and_measures_it_by_what_shows() {
    let word = Brush(true).paint(Tone::Name, "orders");
    let text = format!("{word} {word} {word}");
    let lines = wrapped(&text, 13, "");
    assert_eq!(lines.len(), 2, "{lines:?}");
    for line in lines {
        assert!(console::measure_text_width(&line) <= 13, "{line:?}");
    }
    let long = Brush(true).paint(Tone::Name, "a".repeat(40));
    assert_eq!(wrapped(&long, 10, "").len(), 1, "an escape is never cut");
}

#[test]
fn an_empty_text_wraps_to_one_empty_line() {
    assert_eq!(wrapped("", 10, "  "), [""]);
}

#[test]
fn a_word_wider_than_a_line_starts_in_what_is_left_of_the_line_it_comes_to() {
    assert_eq!(
        wrapped("✗ aaaaaaaaaaaaaaa", 5, ""),
        ["✗ aaa", "aaaaa", "aaaaa", "aa"]
    );
    assert_eq!(
        wrapped("✗ aaaaaaaaaaaaaaa", 5, "  "),
        ["✗ a", "  aaa", "  aaa", "  aaa", "  aaa", "  aa"]
    );
    assert_eq!(wrapped("abcd aaaaaaaa", 5, ""), ["abcd", "aaaaa", "aaa"]);
}

#[test]
fn wrapped_keeps_the_leading_spaces_of_its_first_line() {
    assert_eq!(wrapped("  hint: a b", 80, "    "), ["  hint: a b"]);
    assert_eq!(wrapped("  one two", 8, "  "), ["  one", "  two"]);
}

#[test]
fn logs_are_written_through_the_painter_only_where_one_animates() {
    assert!(Ui::for_tests().log_lines().is_none());
    let (ui, _queue) = Ui::queued();
    assert!(ui.log_lines().is_some());
    ui.stop_animating();
    assert!(!ui.animating());
}

/// A process ending a look whose painter lost `lost` log lines, its standard error read by the
/// test below.
fn finishing_having_lost(lost: u64) {
    let (ui, _queue) = Ui::queued_having_lost(lost);
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime")
        .block_on(ui.finished());
}

#[test]
#[ignore = "run by the test below, as the process whose standard error it reads"]
fn stand_in_that_finishes_having_lost_three_log_lines() {
    finishing_having_lost(3);
}

#[test]
#[ignore = "run by the test below, as the process whose standard error it reads"]
fn stand_in_that_finishes_having_lost_no_log_line() {
    finishing_having_lost(0);
}

/// What a stand-in of this file wrote to standard error.
fn said_by_stand_in(test: &str) -> String {
    let mut command = rdlt_testkit::process::stand_in(&format!("ui::tests::{test}"));
    command
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped());
    let child = rdlt_testkit::process::guarded(&mut command).expect("the stand-in runs");
    let output = child.wait_with_output().expect("it ends");
    assert!(output.status.success(), "{output:?}");
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn log_lines_lost_are_said_at_the_end_and_none_lost_is_not_said() {
    assert_eq!(
        said_by_stand_in("stand_in_that_finishes_having_lost_three_log_lines"),
        "3 log lines not shown\n"
    );
    assert_eq!(
        said_by_stand_in("stand_in_that_finishes_having_lost_no_log_line"),
        ""
    );
}
