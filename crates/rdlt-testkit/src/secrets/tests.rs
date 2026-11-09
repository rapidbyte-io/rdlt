use base64::engine::general_purpose::{STANDARD, URL_SAFE};

use super::*;

const SECRET: &str = "CANARY\"quoted\\by\"JSON-päss\u{1}\u{200b}";

#[test]
fn a_secret_is_found_in_each_of_its_forms() {
    for form in forms(SECRET) {
        let text = format!("before {form} after");
        assert!(
            leaks(&text, [SECRET]).contains(&form),
            "{form} is not found"
        );
    }
}

#[test]
fn the_forms_are_the_escapes_and_encodings_a_text_may_hold() {
    let found = forms("a\"z \u{1}");
    for form in [
        "a\"z \u{1}",
        "a\\\"z \\u0001",
        "a\\\"z \\u{1}",
        "61227a2001",
        "61227A2001",
        "a%22z%20%01",
    ] {
        assert!(found.contains(&form.to_owned()), "{form} in {found:?}");
    }
}

#[test]
fn a_secret_is_found_in_base64_wherever_it_starts_in_what_was_encoded() {
    for before in ["", "u", "us", "user:"] {
        let text = format!("{before}{SECRET}@host");
        for encoded in [STANDARD.encode(&text), URL_SAFE.encode(&text)] {
            assert!(
                !leaks(&encoded, [SECRET]).is_empty(),
                "{before:?}: {encoded}"
            );
        }
    }
}

#[test]
fn a_text_that_holds_no_form_leaks_nothing() {
    let text = "nothing of it, *** in its place";
    assert_eq!(leaks(text, [SECRET]), Vec::<String>::new());
}
