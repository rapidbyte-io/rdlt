//! Finding a secret in text a test captured: in each form a value can leak in.

use std::fmt::Write as _;

use base64::Engine as _;
use base64::engine::GeneralPurpose;
use base64::engine::general_purpose::{STANDARD_NO_PAD, URL_SAFE_NO_PAD};

/// Each form `secret` could leak in: as it is; JSON-escaped, as Rust's `Debug` writes it and as
/// a host shows connector text; in lower- and upper-case hex; percent-encoded; and in base64,
/// standard and URL-safe, at each of the three offsets it can start at within an encoded text.
#[must_use]
pub fn forms(secret: &str) -> Vec<String> {
    let escaped = serde_json::to_string(secret).expect("a string serializes");
    let hex = secret.bytes().fold(String::new(), |mut text, byte| {
        write!(text, "{byte:02x}").expect("a string takes what is written");
        text
    });
    let mut forms = vec![
        secret.to_owned(),
        escaped.trim_matches('"').to_owned(),
        secret.escape_debug().collect(),
        rdlt_connector::text::shown(secret, usize::MAX),
        hex.to_ascii_uppercase(),
        hex,
        percent_encoded(secret),
    ];
    for alphabet in [STANDARD_NO_PAD, URL_SAFE_NO_PAD] {
        forms.extend((0..3).filter_map(|offset| encoded_within(&alphabet, secret, offset)));
    }
    forms.retain(|form| !form.is_empty());
    forms.sort();
    forms.dedup();
    forms
}

/// Each form of each of `secrets` that `text` holds.
#[must_use]
pub fn leaks<'a>(text: &str, secrets: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    let every = secrets.into_iter().flat_map(forms);
    every.filter(|form| text.contains(form.as_str())).collect()
}

/// `secret` as a URL writes it: every byte but a letter, a digit and `-._~` as `%XX`.
fn percent_encoded(secret: &str) -> String {
    secret.bytes().fold(String::new(), |mut text, byte| {
        if byte.is_ascii_alphanumeric() || b"-._~".contains(&byte) {
            text.push(char::from(byte));
        } else {
            write!(text, "%{byte:02X}").expect("a string takes what is written");
        }
        text
    })
}

/// What `secret`, `offset` bytes into a text encoded with `alphabet`, encodes to whatever is
/// around it: the groups of four characters its bytes alone make; none where it fills none.
fn encoded_within(alphabet: &GeneralPurpose, secret: &str, offset: usize) -> Option<String> {
    let mut placed = vec![0; offset];
    placed.extend_from_slice(secret.as_bytes());
    let encoded = alphabet.encode(&placed);
    // The first group holds the bytes before it, where there are any; a last group that is not
    // whole, those after it.
    let first = if offset == 0 { 0 } else { 4 };
    let last = placed.len() / 3 * 4;
    (last > first).then(|| encoded[first..last].to_owned())
}

#[cfg(test)]
mod tests;
