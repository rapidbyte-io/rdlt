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
        said.ends_with(r"\u{1b}[2J\n pass S-CHECK\r\u{202e}"),
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

/// A process that contains its panics, as rdlt-certify does, and panics saying a secret its
/// configuration held.
#[test]
#[ignore = "run by the test below, as the process that panics"]
fn stand_in_that_contains_a_panic_saying_a_secret() {
    super::super::redactions().add("hunter2");
    super::contain();
    panic!("the configuration held hunter2\u{1b}[2J");
}

#[test]
fn a_panic_of_this_process_is_said_on_one_line_scrubbed_of_its_secrets() {
    let mut command = rdlt_testkit::process::stand_in(
        "cli::panics::tests::stand_in_that_contains_a_panic_saying_a_secret",
    );
    command
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped());
    let child = rdlt_testkit::process::guarded(&mut command).expect("the stand-in runs");
    let output = child.wait_with_output().expect("it ends");
    assert!(!output.status.success());
    let said = String::from_utf8_lossy(&output.stderr);
    let line = said
        .lines()
        .find(|line| line.starts_with("rdlt-certify: panicked at "))
        .unwrap_or_else(|| panic!("no contained line: {said}"));
    assert!(
        line.ends_with(r"the configuration held ***\u{1b}[2J"),
        "{line}"
    );
    assert!(
        !said.contains("hunter2") && !said.contains('\u{1b}'),
        "{said}"
    );
}
