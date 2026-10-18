//! What a connector was given that no log, error or report may hold.

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use rdlt_connector::text::{CUT, shown};
use zeroize::Zeroizing;

/// What replaces a secret in a text.
const REDACTED: &str = "***";

/// The fewest characters other than whitespace a line of a secret needs to be found on its own.
const LEAST_LINE: usize = 8;

/// How many steps a text is scrubbed in at most, the last finding nothing.
///
/// One step replaces every secret said or cut in it; a text needs another only for each `***`
/// that completes a secret with what is around it, and a chain of more than two of those is
/// made, not said by chance.
pub(super) const STEPS: usize = 4;

/// What a text is said as when [`STEPS`] steps leave something of a secret in it.
pub(super) const WITHHELD: &str = "[withheld: it could not be scrubbed]";

/// What starts a text whose beginning was dropped.
pub(crate) const DROPPED: &str = "[cut] ";

/// The secret values one connector was sent, shared by everything that receives its text:
/// each is replaced wherever the connector says it back.
///
/// A value is found as it is, and as JSON, Rust's `Debug` and [`shown`] write it; where a
/// text was cut, the part of a value left at the cut is replaced too. Each line of a value
/// that holds line ends and at least eight characters other than whitespace is found on its
/// own as well, since output is logged a line at a time; a shorter line, such as a lone
/// brace, would garble every text it is in. A form, or a part of one at a cut, of nothing but
/// `*` and whitespace is not found, since the mask would hold it.
/// A connector that transforms a secret before it says it, by an encoding or a split
/// elsewhere, is beyond this.
#[derive(Clone, Default)]
pub struct Redactions {
    /// The values' forms, the longest first, so that one holding another is replaced whole;
    /// a text is scrubbed with the forms there were as it began.
    secrets: Arc<Mutex<Arc<Vec<Zeroizing<String>>>>>,
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

    /// Adds `secret`, each of its lines that is long enough to be found on its own, and the
    /// forms a connector's text may hold them in; one of nothing but `*` and whitespace, an
    /// empty one among them, is nothing to find.
    pub fn add(&self, secret: &str) {
        let long = |line: &&str| line.chars().filter(|c| !c.is_whitespace()).count() >= LEAST_LINE;
        let lines = secret.lines().filter(long);
        let mut shared = self.secrets.lock().unwrap_or_else(PoisonError::into_inner);
        let secrets = Arc::make_mut(&mut shared);
        for value in std::iter::once(secret).chain(lines) {
            let json = serde_json::to_string(value).unwrap_or_default();
            let json = json.trim_matches('"').to_owned();
            let debug: String = value.escape_debug().collect();
            let forms = [value.to_owned(), json, debug, shown(value, usize::MAX)];
            for form in forms.map(Zeroizing::new) {
                if maskable(&form) && !secrets.contains(&form) {
                    secrets.push(form);
                }
            }
        }
        secrets.sort_by_key(|secret| std::cmp::Reverse(secret.len()));
    }

    /// `text` with every secret in it replaced by `***`, and with it the start of a secret
    /// that ends where the text was cut, and the end of one that starts where its beginning
    /// was dropped, until nothing of a secret is left in it; a text that says it was withheld
    /// if the few steps scrubbing is given leave something.
    pub fn scrubbed(&self, mut text: String) -> String {
        let secrets = Arc::clone(&self.secrets.lock().unwrap_or_else(PoisonError::into_inner));
        let cut = text.contains(CUT) || text.contains(DROPPED);
        for _ in 0..STEPS {
            let found;
            (text, found) = step(text, &secrets, cut);
            if !found {
                return text;
            }
        }
        drop(Zeroizing::new(text));
        WITHHELD.to_owned()
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

/// Whether `text` holds a character other than `*` and whitespace: one that holds none may be
/// in any text that holds the mask.
pub(super) fn maskable(text: &str) -> bool {
    text.chars()
        .any(|c| !REDACTED.contains(c) && !c.is_whitespace())
}

/// `text` with every secret in it replaced, and then, where it holds the `cut` marks, the
/// part of one at each, and whether anything was.
fn step(mut text: String, secrets: &[Zeroizing<String>], cut: bool) -> (String, bool) {
    let mut found = false;
    for secret in secrets {
        if text.contains(secret.as_str()) {
            found = true;
            // What held the secret is wiped, not only freed.
            let held = Zeroizing::new(text);
            text = held.replace(secret.as_str(), REDACTED);
        }
    }
    if cut {
        let held = Zeroizing::new(text);
        text = at_cuts(&held, secrets);
        found |= text != *held;
    }
    (text, found)
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

/// How many bytes at the end of `text` are the start of a secret, at most, one the mask would
/// hold not counted.
fn starts(text: &str, secrets: &[Zeroizing<String>]) -> usize {
    let longest = |secret: &Zeroizing<String>| {
        let starts = secret.char_indices().rev().map(|(at, _)| &secret[..at]);
        starts
            .take_while(|start| maskable(start))
            .find(|start| text.ends_with(start))
            .map(str::len)
    };
    secrets.iter().filter_map(longest).max().unwrap_or(0)
}

/// How many bytes at the start of `text` are the end of a secret, at most, one the mask would
/// hold not counted.
fn ends(text: &str, secrets: &[Zeroizing<String>]) -> usize {
    let longest = |secret: &Zeroizing<String>| {
        let ends = secret.char_indices().skip(1).map(|(at, _)| &secret[at..]);
        let ended = ends
            .take_while(|end| maskable(end))
            .find(|end| text.starts_with(end));
        ended.map(str::len)
    };
    secrets.iter().filter_map(longest).max().unwrap_or(0)
}
