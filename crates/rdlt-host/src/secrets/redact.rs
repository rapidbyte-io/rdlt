//! What a connector was given that no log, error or report may hold.

use std::fmt::{self, Write as _};
use std::mem;
use std::ops::Range;
use std::sync::{Arc, OnceLock};

use aho_corasick::{AhoCorasick, AhoCorasickKind};
use parking_lot::Mutex;
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
/// A value is found as it is, as JSON and Rust's `{:?}` write it, and as [`shown`] writes each
/// of those; where a text was cut, the part of a value left at the cut is replaced too. Each
/// line of a value that holds line ends and at least eight characters other than whitespace is
/// found on its own as well, since output is logged a line at a time; a shorter line, such as
/// a lone brace, would garble every text it is in. A form, or a part of one at a cut, of
/// nothing but `*` and whitespace is not found, since the mask would hold it.
/// A connector that transforms a secret before it says it, by an encoding or a split
/// elsewhere, is beyond this.
#[derive(Clone, Default)]
pub struct Redactions {
    /// The values' forms; a text is scrubbed with the forms there were as it began.
    forms: Arc<Mutex<Arc<Forms>>>,
}

impl fmt::Debug for Redactions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let forms = self.forms.lock();
        write!(formatter, "Redactions({})", forms.forms.len())
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
        let mut shared = self.forms.lock();
        let mut added = Vec::new();
        for value in std::iter::once(secret).chain(lines) {
            for form in forms_of(value) {
                if maskable(&form) && shared.position(&form).is_err() {
                    added.push(form);
                }
            }
        }
        if !added.is_empty() {
            let forms = Arc::make_mut(&mut shared);
            forms.forms.append(&mut added);
            // Moved, not copied: a form's text stays where it was written.
            forms
                .forms
                .sort_unstable_by(|one, other| one.as_str().cmp(other.as_str()));
            forms.forms.dedup();
            forms.automaton = OnceLock::new();
        }
    }

    /// `text` with every secret in it replaced by `***`, and with it the start of a secret
    /// that ends where the text was cut, and the end of one that starts where its beginning
    /// was dropped, until nothing of a secret is left in it; a text that says it was withheld
    /// if the few steps scrubbing is given leave something.
    ///
    /// A step is one pass of one automaton over the text, however many forms there are, and,
    /// where the text holds a mark of a cut, one more for each form.
    pub fn scrubbed(&self, mut text: String) -> String {
        let forms = Arc::clone(&self.forms.lock());
        if forms.forms.is_empty() {
            return text;
        }
        let Some(automaton) = forms.automaton() else {
            drop(Zeroizing::new(text));
            return WITHHELD.to_owned();
        };
        let cut = text.contains(CUT) || text.contains(DROPPED);
        for _ in 0..STEPS {
            let found;
            (text, found) = step(text, &forms, automaton, cut);
            if !found {
                return text;
            }
        }
        drop(Zeroizing::new(text));
        WITHHELD.to_owned()
    }

    /// Whether `text`, a name, holds a secret: the host never cuts a name, so a mark of a cut in
    /// one is text like any other.
    pub(crate) fn holds(&self, text: &str) -> bool {
        let forms = Arc::clone(&self.forms.lock());
        if forms.forms.is_empty() {
            return false;
        }
        forms
            .automaton()
            .is_none_or(|automaton| automaton.is_match(text))
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

/// Every form of every value added, and the automaton that finds them all in one pass, built
/// as a text is first scrubbed after a form was added.
#[derive(Clone, Default)]
struct Forms {
    /// Each wiped when dropped, in the order of their text, none twice.
    forms: Vec<Zeroizing<String>>,
    /// `None` where the forms make an automaton too large to build: then no text is shown.
    automaton: OnceLock<Option<AhoCorasick>>,
}

impl Forms {
    fn automaton(&self) -> Option<&AhoCorasick> {
        let built = self.automaton.get_or_init(|| {
            let patterns = self.forms.iter().map(|form| form.as_bytes());
            let mut builder = AhoCorasick::builder();
            builder.kind(Some(AhoCorasickKind::ContiguousNFA));
            builder.build(patterns).ok()
        });
        built.as_ref()
    }

    /// Where `form` is among the forms, or where it would go.
    fn position(&self, form: &str) -> Result<usize, usize> {
        self.forms.binary_search_by(|kept| kept.as_str().cmp(form))
    }
}

/// `value` as it is and inside the quotes JSON and `{:?}` write around it, and each of those as
/// [`shown`] writes it.
fn forms_of(value: &str) -> [Zeroizing<String>; 6] {
    // A byte is escaped in six at most: buffers that large never grow and leave a copy behind.
    let room = value.len().saturating_mul(6).saturating_add(2);
    let mut json = Zeroizing::new(Vec::with_capacity(room));
    // Writing to memory cannot fail.
    serde_json::to_writer(&mut *json, value).ok();
    let mut debug = Zeroizing::new(String::with_capacity(room));
    write!(debug, "{value:?}").ok();
    let unquoted = |quoted: &str| {
        let inside = quoted.get(1..quoted.len().saturating_sub(1));
        Zeroizing::new(inside.unwrap_or_default().to_owned())
    };
    let json = unquoted(std::str::from_utf8(&json).unwrap_or_default());
    let debug = unquoted(&debug);
    let shown = |form: &str| Zeroizing::new(shown(form, usize::MAX));
    [
        shown(value),
        shown(&json),
        shown(&debug),
        Zeroizing::new(value.to_owned()),
        json,
        debug,
    ]
}

/// Whether `text` holds a character other than `*` and whitespace: one that holds none may be
/// in any text that holds the mask.
pub(super) fn maskable(text: &str) -> bool {
    text.chars()
        .any(|c| !REDACTED.contains(c) && !c.is_whitespace())
}

/// `text` with every secret in it replaced, and, where it holds the `cut` marks, the part of
/// one at each, and whether anything was.
///
/// All are found in `text` as it is, and those that overlap are replaced by one mask: a secret
/// within the part of one at a cut, or such a part within a secret, leaves nothing of the other.
fn step(text: String, forms: &Forms, automaton: &AhoCorasick, cut: bool) -> (String, bool) {
    // What held a secret is wiped, not only freed.
    let mut held = Zeroizing::new(text);
    let covered = covered(&held, forms, automaton, cut);
    if covered.is_empty() {
        return (mem::take(&mut *held), false);
    }
    let (mut scrubbed, mut written) = (String::with_capacity(held.len()), 0);
    for range in covered {
        if range.start >= written {
            scrubbed.push_str(&held[written..range.start]);
            scrubbed.push_str(REDACTED);
        }
        written = written.max(range.end);
    }
    scrubbed.push_str(&held[written..]);
    (scrubbed, true)
}

/// The bytes of `text` a form is found at, and the part of one at each mark of a cut where
/// `cut` says to look, in the order they start; where forms found overlap, they are one.
fn covered(text: &str, forms: &Forms, automaton: &AhoCorasick, cut: bool) -> Vec<Range<usize>> {
    let mut covered: Vec<Range<usize>> = Vec::new();
    // Each is found where it ends, so none found later ends sooner.
    for found in automaton.find_overlapping_iter(text) {
        let mut range = found.range();
        while let Some(last) = covered.last()
            && range.start < last.end
        {
            range = range.start.min(last.start)..range.end.max(last.end);
            covered.pop();
        }
        covered.push(range);
    }
    if cut {
        covered.extend(at_cuts(text, forms));
        covered.retain(|range| !range.is_empty());
        covered.sort_by_key(|range| range.start);
    }
    covered
}

/// The bytes of `text` that are the part of a form left before each mark of a cut, and after
/// each mark of a dropped beginning; one the mask would hold not counted.
fn at_cuts(text: &str, forms: &Forms) -> Vec<Range<usize>> {
    let cuts: Vec<usize> = text.match_indices(CUT).map(|(at, _)| at).collect();
    let drops: Vec<usize> = text
        .match_indices(DROPPED)
        .map(|(at, mark)| at + mark.len())
        .collect();
    let (mut starting, mut ending) = (vec![0; cuts.len()], vec![0; drops.len()]);
    // A form's end that starts a text is the start of the form reversed that ends it reversed.
    let backwards = Zeroizing::new(text.bytes().rev().collect::<Vec<u8>>());
    let ends: Vec<usize> = drops.iter().rev().map(|from| text.len() - from).collect();
    for form in &forms.forms {
        let form = form.as_str();
        if !cuts.is_empty() {
            let found = longest_starts(text.as_bytes(), form.as_bytes(), &cuts);
            for (longest, length) in starting.iter_mut().zip(found) {
                if form.get(..length).is_some_and(maskable) {
                    *longest = (*longest).max(length);
                }
            }
        }
        if let Some(&last) = ends.last() {
            // No more of a form than the text holds after its first mark can start there.
            let tail = &form.as_bytes()[form.len().saturating_sub(last)..];
            let reversed = Zeroizing::new(tail.iter().rev().copied().collect::<Vec<u8>>());
            let found = longest_starts(&backwards, &reversed, &ends);
            for (longest, length) in ending.iter_mut().rev().zip(found) {
                if form.get(form.len() - length..).is_some_and(maskable) {
                    *longest = (*longest).max(length);
                }
            }
        }
    }
    let before = cuts
        .iter()
        .zip(starting)
        .map(|(at, length)| at - length..*at);
    let after = drops
        .iter()
        .zip(ending)
        .map(|(from, length)| *from..from + length);
    before.chain(after).collect()
}

/// For each of `marks`, ascending places in `text`, how many bytes before it are the start of
/// `form`, at most: in one pass over the text, as Knuth, Morris and Pratt search.
fn longest_starts(text: &[u8], form: &[u8], marks: &[usize]) -> Vec<usize> {
    let Some(&last) = marks.last() else {
        return Vec::new();
    };
    // No more of a form than the text holds before its last mark can end at one.
    let form = &form[..form.len().min(last)];
    let borders = borders(form);
    let (mut found, mut matched) = (Vec::with_capacity(marks.len()), 0);
    let mut marks = marks.iter().peekable();
    for (at, &byte) in text[..last].iter().enumerate() {
        while marks.next_if(|&&mark| mark == at).is_some() {
            found.push(matched);
        }
        // Falling back leaves the start matched shorter than the form, which is never empty.
        while matched > 0 && (matched == form.len() || form[matched] != byte) {
            matched = borders[matched - 1];
        }
        if form[matched] == byte {
            matched += 1;
        }
    }
    found.resize(found.len() + marks.count(), matched);
    found
}

/// For each start of `form`, how many bytes long its longest start is that also ends it, itself
/// not counted.
fn borders(form: &[u8]) -> Zeroizing<Vec<usize>> {
    let mut borders = Zeroizing::new(vec![0; form.len()]);
    let mut matched = 0;
    for (at, &byte) in form.iter().enumerate().skip(1) {
        while matched > 0 && byte != form[matched] {
            matched = borders[matched - 1];
        }
        if byte == form[matched] {
            matched += 1;
        }
        borders[at] = matched;
    }
    borders
}
