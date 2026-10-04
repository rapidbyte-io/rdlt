//! Names alike under an equality wider than a destination declares: by case, by case
//! folding, by normalization or by compatibility.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

use regex_syntax::hir::{ClassUnicode, ClassUnicodeRange};
use unicase::UniCase;

use super::folded;
use crate::capabilities::{IdentifierChars, IdentifierRules};
use crate::text::deceives;

/// Two names, or two pieces of names.
type Pair = (String, String);

/// Pairs that normalization or compatibility makes alike: a letter composed and decomposed, a
/// sign and the letter it is canonically, and a ligature, a numeral and a wide letter and the
/// letters they stand for.
const NORMALIZED: [(&str, &str); 6] = [
    ("\u{e9}", "e\u{301}"),
    ("\u{c5}", "\u{212b}"),
    ("K", "\u{212a}"),
    ("\u{fb01}", "fi"),
    ("\u{2163}", "IV"),
    ("\u{ff46}", "f"),
];

/// The pairs of names `rules` keep apart that a wider equality makes alike, each at most
/// `longest` bytes: every character Unicode's case mappings or its case folding change beside
/// what they make of it, and the pairs normalization and compatibility make alike.
///
/// Case pairs are gathered into names by the equalities that make them alike, so a destination
/// comparing by any one of them finds a pair of names alike wherever it finds one piece alike;
/// each pair normalization or compatibility makes alike is a pair of names of its own.
pub(super) fn apart(rules: &IdentifierRules, longest: usize) -> Vec<Pair> {
    let [cased, case_folded, normalized] = pieces(rules);
    let mut kinds: BTreeMap<[bool; 4], Vec<Pair>> = BTreeMap::new();
    for pair in cased.into_iter().chain(case_folded) {
        kinds.entry(alike(&pair)).or_default().push(pair);
    }
    let mut names = Vec::new();
    for pairs in kinds.values() {
        let prefix = |place: usize| folded(rules, &format!("c{}_", names.len() + place));
        names.extend(gathered(&prefix, pairs, longest));
    }
    for pair in normalized {
        let prefix = folded(rules, &format!("n{}_", names.len()));
        names.push((format!("{prefix}{}", pair.0), format!("{prefix}{}", pair.1)));
    }
    names
}

/// Which equalities make the two sides of `pair` alike: ASCII case, the lower case the declared
/// rules fold by, Unicode's simple case folding and its full case folding, in that order.
pub(super) fn alike((one, other): &Pair) -> [bool; 4] {
    [
        one.eq_ignore_ascii_case(other),
        one.to_lowercase() == other.to_lowercase(),
        simply_folded(one) == simply_folded(other),
        fully_folded(one) == fully_folded(other),
    ]
}

/// The characters Unicode's simple case folding makes alike with `c`, `c` among them, as
/// `regex-syntax` holds them: the folding of Unicode 16.0, whatever Unicode the toolchain knows.
fn simple_class(c: char) -> impl Iterator<Item = char> {
    let mut class = ClassUnicode::new([ClassUnicodeRange::new(c, c)]);
    class.case_fold_simple();
    let ranges: Vec<(char, char)> = class
        .iter()
        .map(|range| (range.start(), range.end()))
        .collect();
    ranges.into_iter().flat_map(|(start, end)| start..=end)
}

/// `name` under Unicode's simple case folding: each character as the least of those the folding
/// makes alike with it.
pub(in crate::testing) fn simply_folded(name: &str) -> String {
    name.chars()
        .map(|c| simple_class(c).min().unwrap_or(c))
        .collect()
}

/// `name` under Unicode's full case folding, as `unicase` holds it.
fn fully_folded(name: &str) -> String {
    UniCase::unicode(name).to_folded_case()
}

/// The pieces of names `rules` keep apart that a wider equality makes alike, as `rules` fold
/// them, by kind: those case mappings make alike, those case folding does beyond them, and those
/// normalization and compatibility do.
pub(super) fn pieces(rules: &IdentifierRules) -> [Vec<Pair>; 3] {
    let admitted = |text: &str| {
        !text.chars().any(deceives)
            && (rules.chars == IdentifierChars::Any
                || text.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
    };
    // Each side as the rules fold it, which is what the destination is sent.
    let kept = |pairs: Vec<Pair>| -> Vec<Pair> {
        pairs
            .into_iter()
            .filter_map(|(one, other)| {
                let (one, other) = (folded(rules, &one), folded(rules, &other));
                (admitted(&one) && admitted(&other) && one != other).then_some((one, other))
            })
            .collect()
    };
    let (cased, case_folded) = mapped(rules.chars);
    let normalized = NORMALIZED.map(|(one, other)| (one.to_owned(), other.to_owned()));
    [kept(cased), kept(case_folded), kept(normalized.to_vec())]
}

/// Every character Unicode's simple case folding makes alike with another beside each such
/// other, and every character whose full case folding is more than one character beside that
/// folding: among the characters `chars` admits, found once.
fn mapped(chars: IdentifierChars) -> (Vec<Pair>, Vec<Pair>) {
    static ASCII: OnceLock<(Vec<Pair>, Vec<Pair>)> = OnceLock::new();
    static ANY: OnceLock<(Vec<Pair>, Vec<Pair>)> = OnceLock::new();
    match chars {
        IdentifierChars::AsciiWord => ASCII.get_or_init(|| mappings(0x7f)),
        IdentifierChars::Any => ANY.get_or_init(|| mappings(u32::from(char::MAX))),
    }
    .clone()
}

/// What [`mapped`] finds, every character up to `last` looked at.
///
/// A character alike with another under the simple folding folds to something else under the
/// full one, or another folds to it: each such class is met from one that folds.
fn mappings(last: u32) -> (Vec<Pair>, Vec<Pair>) {
    let (mut cased, mut case_folded) = (BTreeSet::new(), BTreeSet::new());
    for c in (0..=last).filter_map(char::from_u32) {
        let own = c.to_string();
        let full = fully_folded(&own);
        if full == own {
            continue;
        }
        // A character alike with itself, or one the rules do not admit, is dropped as kept.
        for other in simple_class(c) {
            cased.insert(ordered(own.clone(), other.to_string()));
        }
        if full.chars().count() > 1 {
            case_folded.insert(ordered(own, full));
        }
    }
    (
        cased.into_iter().collect(),
        case_folded.into_iter().collect(),
    )
}

/// `one` and `other` in order, so a pair met from either side is one pair.
fn ordered(one: String, other: String) -> Pair {
    if one <= other {
        (one, other)
    } else {
        (other, one)
    }
}

/// `pairs`, each side gathered into names of at most `longest` bytes behind the `prefix` of the
/// name's place, so no two names of the clause are alike.
pub(super) fn gathered(
    prefix: &dyn Fn(usize) -> String,
    pairs: &[Pair],
    longest: usize,
) -> Vec<Pair> {
    let mut names = Vec::new();
    let (mut one, mut other) = (prefix(0), prefix(0));
    let mut held = 0;
    for (left, right) in pairs {
        let fits = one.len() + left.len() <= longest && other.len() + right.len() <= longest;
        if !fits && held > 0 {
            names.push((one, other));
            (one, other) = (prefix(names.len()), prefix(names.len()));
            held = 0;
        }
        if one.len() + left.len() <= longest && other.len() + right.len() <= longest {
            one.push_str(left);
            other.push_str(right);
            held += 1;
        }
    }
    if held > 0 {
        names.push((one, other));
    }
    names
}
