//! Names alike under an equality wider than a destination declares: by case, by case
//! folding, by normalization or by compatibility.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

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
    let mut kinds: BTreeMap<u8, Vec<Pair>> = BTreeMap::new();
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

/// Which equalities make the two sides of `pair` alike, a bit each: ASCII case, Unicode's lower
/// case, its simple case folding and its full case folding.
fn alike((one, other): &Pair) -> u8 {
    let full = |text: &str| -> String {
        text.chars()
            .flat_map(char::to_uppercase)
            .flat_map(char::to_lowercase)
            .collect()
    };
    [
        one.eq_ignore_ascii_case(other),
        one.to_lowercase() == other.to_lowercase(),
        simply_folded(one) == simply_folded(other),
        full(one) == full(other),
    ]
    .into_iter()
    .enumerate()
    .fold(0, |bits, (bit, alike)| bits | (u8::from(alike) << bit))
}

/// `name` under Unicode's simple case folding: each character's upper case in lower case, where
/// both are one character.
pub(in crate::testing) fn simply_folded(name: &str) -> String {
    let single = |chars: &mut dyn Iterator<Item = char>| {
        let first = chars.next()?;
        chars.next().is_none().then_some(first)
    };
    name.chars()
        .map(|c| {
            let upper = single(&mut c.to_uppercase());
            let lower = upper.and_then(|upper| single(&mut upper.to_lowercase()));
            lower.unwrap_or(c)
        })
        .collect()
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

/// Every character Unicode maps to another case beside each it maps to, and every character
/// whose case folding, its upper case in lower case, is not its lower case beside that folding:
/// among the characters `chars` admits, found once.
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
fn mappings(last: u32) -> (Vec<Pair>, Vec<Pair>) {
    let (mut cased, mut case_folded) = (BTreeSet::new(), BTreeSet::new());
    for c in (0..=last).filter_map(char::from_u32) {
        let own = c.to_string();
        let lower: String = c.to_lowercase().collect();
        let upper: String = c.to_uppercase().collect();
        let folding: String = upper.chars().flat_map(char::to_lowercase).collect();
        for other in [&lower, &upper] {
            if *other != own {
                cased.insert(ordered(own.clone(), other.clone()));
            }
        }
        if folding != lower && folding != own {
            case_folded.insert(ordered(own, folding));
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
fn gathered(prefix: &dyn Fn(usize) -> String, pairs: &[Pair], longest: usize) -> Vec<Pair> {
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
