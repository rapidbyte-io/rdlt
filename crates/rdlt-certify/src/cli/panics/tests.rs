use std::sync::{Arc, Mutex};

use super::report;

/// What `raise` makes this process say as it panics.
fn panicking(raise: impl FnOnce() + std::panic::UnwindSafe) -> String {
    let printed = Arc::new(Mutex::new(Vec::new()));
    let printing = Arc::clone(&printed);
    report(move |said| {
        printing
            .lock()
            .expect("no panic holds it")
            .push(said.to_owned());
    });
    std::panic::catch_unwind(raise).expect_err("it panics");
    drop(std::panic::take_hook());
    let mut printed = printed.lock().expect("no panic holds it");
    assert_eq!(printed.len(), 1, "{printed:?}");
    printed.remove(0)
}

#[test]
fn a_panic_is_printed_as_one_bounded_line_a_terminal_does_not_obey() {
    let hostile = "\u{1b}[2J\n  pass S-CHECK\r\u{202e}".to_owned();
    let said = panicking(move || panic!("{hostile}"));
    assert!(said.starts_with("panicked at "), "{said}");
    assert!(said.contains(file!()), "{said}");
    assert!(
        said.ends_with(r"\u{1b}[2J\n  pass S-CHECK\r\u{202e}"),
        "{said}"
    );
    assert!(!said.chars().any(char::is_control), "{said:?}");
    // A message of any length is cut where a reason is.
    let long = "x".repeat(1 << 20);
    let said = panicking(move || panic!("{long}"));
    assert_eq!(said.len(), rdlt_certify::REASON_BYTES, "{}", said.len());
    // A message that is not text says so.
    let said = panicking(|| std::panic::panic_any(7_u8));
    assert!(said.ends_with(": with no message"), "{said}");
}
