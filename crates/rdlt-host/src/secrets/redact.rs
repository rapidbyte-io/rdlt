//! What a connector was given that no log, error or report may hold.

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use rdlt_connector::text::{CUT, shown};
use zeroize::Zeroizing;

/// What replaces a secret in a text.
const REDACTED: &str = "***";

/// What starts a text whose beginning was dropped.
pub(crate) const DROPPED: &str = "[cut] ";

/// The secret values one connector was sent, shared by everything that receives its text:
/// each is replaced wherever the connector says it back.
///
/// A value is found as it is, and as JSON, Rust's `Debug` and [`shown`] write it; where a
/// text was cut, the part of a value left at the cut is replaced too. A connector that
/// transforms a secret before it says it, by an encoding or a split, is beyond this.
#[derive(Clone, Default)]
pub struct Redactions {
    /// The values' forms, the longest first, so that one holding another is replaced whole.
    secrets: Arc<Mutex<Vec<Zeroizing<String>>>>,
}

impl fmt::Debug for Redactions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let secrets = self.secrets.lock().unwrap_or_else(PoisonError::into_inner);
        write!(formatter, "Redactions({})", secrets.len())
    }
}

impl Redactions {
    /// No secret yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `secret`, and the forms a connector's text may hold it in; an empty one is
    /// nothing to find.
    pub fn add(&self, secret: &str) {
        if secret.is_empty() {
            return;
        }
        let json = serde_json::to_string(secret).unwrap_or_default();
        let json = json.trim_matches('"').to_owned();
        let debug: String = secret.escape_debug().collect();
        let forms = [secret.to_owned(), json, debug, shown(secret, usize::MAX)];
        let mut secrets = self.secrets.lock().unwrap_or_else(PoisonError::into_inner);
        for form in forms.map(Zeroizing::new) {
            if !form.is_empty() && !secrets.contains(&form) {
                secrets.push(form);
            }
        }
        secrets.sort_by_key(|secret| std::cmp::Reverse(secret.len()));
    }

    /// `text` with every secret in it replaced by `***`, and with it the start of a secret
    /// that ends where the text was cut, and the end of one that starts where its beginning
    /// was dropped.
    pub fn scrubbed(&self, text: String) -> String {
        let secrets = self.secrets.lock().unwrap_or_else(PoisonError::into_inner);
        let mut text = text;
        for secret in secrets.iter() {
            if text.contains(secret.as_str()) {
                // What held the secret is wiped, not only freed.
                let held = Zeroizing::new(text);
                text = held.replace(secret.as_str(), REDACTED);
            }
        }
        if text.contains(CUT) || text.contains(DROPPED) {
            let held = Zeroizing::new(text);
            text = at_cuts(&held, &secrets);
        }
        text
    }

    /// `text`, whose beginning was dropped when `dropped`, scrubbed as [`scrubbed`]
    /// scrubs: with the end of a secret it starts with replaced too.
    ///
    /// [`scrubbed`]: Self::scrubbed
    pub(crate) fn scrubbed_end(&self, text: String, dropped: bool) -> String {
        if !dropped {
            return self.scrubbed(text);
        }
        let marked = Zeroizing::new(format!("{DROPPED}{text}"));
        drop(Zeroizing::new(text));
        let scrubbed = self.scrubbed(marked.as_str().to_owned());
        scrubbed
            .strip_prefix(DROPPED)
            .map_or_else(|| scrubbed.clone(), str::to_owned)
    }
}

/// `text` with the part of a secret left before each mark of a cut, and after each mark of a
/// dropped beginning, replaced.
fn at_cuts(text: &str, secrets: &[Zeroizing<String>]) -> String {
    let mut scrubbed = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find(CUT) {
        let before = &rest[..at];
        let left = starts(before, secrets);
        scrubbed.push_str(&before[..before.len() - left]);
        if left > 0 {
            scrubbed.push_str(REDACTED);
        }
        scrubbed.push_str(CUT);
        rest = &rest[at + CUT.len()..];
    }
    scrubbed.push_str(rest);
    let (mut ended, mut rest) = (String::with_capacity(scrubbed.len()), scrubbed.as_str());
    while let Some(at) = rest.find(DROPPED) {
        let after = &rest[at + DROPPED.len()..];
        let left = ends(after, secrets);
        ended.push_str(&rest[..at + DROPPED.len()]);
        if left > 0 {
            ended.push_str(REDACTED);
        }
        rest = &after[left..];
    }
    ended.push_str(rest);
    ended
}

/// How many bytes at the end of `text` are the start of a secret, at most.
fn starts(text: &str, secrets: &[Zeroizing<String>]) -> usize {
    let longest = |secret: &Zeroizing<String>| {
        let starts = secret.char_indices().rev().map(|(at, _)| at);
        starts
            .filter(|length| *length > 0)
            .find(|length| text.ends_with(&secret[..*length]))
    };
    secrets.iter().filter_map(longest).max().unwrap_or(0)
}

/// How many bytes at the start of `text` are the end of a secret, at most.
fn ends(text: &str, secrets: &[Zeroizing<String>]) -> usize {
    let longest = |secret: &Zeroizing<String>| {
        let ends = secret.char_indices().map(|(at, _)| at).skip(1);
        let ended = ends
            .map(|at| &secret[at..])
            .find(|end| text.starts_with(end));
        ended.map(str::len)
    };
    secrets.iter().filter_map(longest).max().unwrap_or(0)
}
