//! The functions a SQL transform may call, each with what its output holds at most in terms of
//! what its arguments output.

use std::sync::Arc;

use datafusion_expr::ScalarUDF;
use datafusion_functions::{core, datetime, math, regex, string, unicode};
use rdlt_engine::Bound;

/// A function a SQL transform may call.
pub(crate) struct Listed {
    pub(crate) name: &'static str,
    pub(crate) udf: fn() -> Arc<ScalarUDF>,
    /// The arguments, by place, that must be text literals.
    pub(crate) literals: &'static [usize],
    /// Where its regular expression and that expression's flags sit, where it takes one.
    pub(crate) regex: Option<Pattern>,
    /// What a call outputs at most; none where the call's literals leave it unbounded.
    pub(crate) output: fn(&Call<'_>) -> Option<Bound>,
}

/// Where a function's regular expression and its flags sit among its arguments.
pub(crate) struct Pattern {
    pub(crate) at: usize,
    pub(crate) flags: usize,
    /// Whether the flags may hold `g`, which asks for every match rather than changing what the
    /// expression matches.
    pub(crate) global: bool,
}

/// A call's arguments as a function's bound reads them.
pub(crate) struct Call<'a> {
    /// What each argument outputs at most.
    pub(crate) args: &'a [Bound],
    /// Each argument's text, where it is a text literal.
    pub(crate) texts: &'a [Option<&'a str>],
    /// Bytes a row of the call's output takes, where its type has a fixed width.
    pub(crate) width: Option<u64>,
}

/// Bytes an output array takes beside its rows: its buffers' rounding.
pub(crate) const ARRAY: u64 = 256;

/// Bytes an output array of text takes beside its rows and its text: the first block a builder
/// of views takes, 16 KiB, since it doubles its 8 KiB starting size before it allocates one,
/// which holds a builder of offsets' first 1 KiB, and its buffers' rounding.
pub(crate) const TEXT_ARRAY: u64 = (16 << 10) + ARRAY;

/// Bytes a row of text takes beside its bytes: a view or an offset, and its validity.
pub(crate) const TEXT: u64 = 17;

/// Every function a SQL transform may call, by name, in order.
pub(crate) const LISTED: &[Listed] = &[
    entry("abs", math::abs, fixed),
    entry("btrim", string::btrim, first),
    entry("char_length", unicode::character_length, fixed),
    entry("character_length", unicode::character_length, fixed),
    entry("coalesce", core::coalesce, all),
    entry("concat", string::concat, all),
    entry("concat_ws", string::concat_ws, joined),
    entry("date_part", datetime::date_part, fixed),
    entry("date_trunc", datetime::date_trunc, fixed),
    entry("ends_with", string::ends_with, fixed),
    entry("initcap", unicode::initcap, cased),
    entry("left", unicode::left, first),
    entry("length", unicode::character_length, fixed),
    entry("lower", string::lower, cased),
    entry("ltrim", string::ltrim, first),
    entry("nullif", core::nullif, first),
    entry("octet_length", string::octet_length, fixed),
    Listed {
        name: "regexp_like",
        udf: regex::regexp_like,
        literals: &[1, 2],
        regex: Some(Pattern {
            at: 1,
            flags: 2,
            global: false,
        }),
        output: fixed,
    },
    Listed {
        name: "regexp_replace",
        udf: regex::regexp_replace,
        literals: &[1, 2, 3],
        regex: Some(Pattern {
            at: 1,
            flags: 3,
            global: true,
        }),
        output: regex_replaced,
    },
    Listed {
        name: "replace",
        udf: string::replace,
        literals: &[1, 2],
        regex: None,
        output: replaced,
    },
    entry("reverse", unicode::reverse, first),
    entry("right", unicode::right, first),
    entry("round", math::round, fixed),
    entry("rtrim", string::rtrim, first),
    entry("starts_with", string::starts_with, fixed),
    entry("strpos", unicode::strpos, fixed),
    entry("substr", unicode::substr, first),
    entry("trunc", math::trunc, fixed),
    entry("upper", string::upper, cased),
];

/// A function taking no literal and no regular expression.
const fn entry(
    name: &'static str,
    udf: fn() -> Arc<ScalarUDF>,
    output: fn(&Call<'_>) -> Option<Bound>,
) -> Listed {
    Listed {
        name,
        udf,
        literals: &[],
        regex: None,
        output,
    }
}

/// The function a SQL transform may call by `name`, in lower case.
pub(crate) fn listed(name: &str) -> Option<&'static Listed> {
    LISTED.iter().find(|listed| listed.name == name)
}

/// The names of every function a SQL transform may call, in order.
pub fn function_names() -> impl Iterator<Item = &'static str> {
    LISTED.iter().map(|listed| listed.name)
}

/// Text holding at most `bound`'s bytes.
fn text(bound: Bound) -> Bound {
    bound.plus(&Bound::new(TEXT_ARRAY).with_rows(TEXT))
}

/// A number, a time or a truth: its type's width a row.
fn fixed(call: &Call<'_>) -> Option<Bound> {
    call.width.map(|width| Bound::new(ARRAY).with_rows(width))
}

/// At most what its first argument outputs: a trim, a slice, a reversal.
fn first(call: &Call<'_>) -> Option<Bound> {
    Some(text(call.args.first()?.clone()))
}

/// At most three times its first argument: Unicode's case mappings grow a character to three
/// times its bytes at most.
fn cased(call: &Call<'_>) -> Option<Bound> {
    Some(text(call.args.first()?.clone().times(3)))
}

/// Every argument together.
#[expect(
    clippy::unnecessary_wraps,
    reason = "every listed function's bound has one type, and others have none for some calls"
)]
fn all(call: &Call<'_>) -> Option<Bound> {
    Some(text(call.args.iter().fold(Bound::new(0), Bound::plus)))
}

/// Every argument but the separator, and the separator between each two.
fn joined(call: &Call<'_>) -> Option<Bound> {
    let (separator, rest) = call.args.split_first()?;
    let between = u64::try_from(rest.len().saturating_sub(1)).ok()?;
    let parts = rest
        .iter()
        .fold(separator.clone().times(between), Bound::plus);
    Some(text(parts))
}

/// A replacement of a non-empty literal by a literal: at most one match a byte, each growing by
/// the replacement.
fn replaced(call: &Call<'_>) -> Option<Bound> {
    let from = call.texts.get(1).copied().flatten()?;
    let to = u64::try_from(call.texts.get(2).copied().flatten()?.len()).ok()?;
    if from.is_empty() {
        return None;
    }
    Some(text(call.args.first()?.clone().times(to.saturating_add(1))))
}

/// A regular expression's replacement by a literal naming no group: a match at each byte and
/// at the end at most, each adding the replacement.
fn regex_replaced(call: &Call<'_>) -> Option<Bound> {
    let to = call.texts.get(2).copied().flatten()?;
    if to.contains(['$', '\\']) {
        return None;
    }
    let to = u64::try_from(to.len()).ok()?;
    let matched = call.args.first()?.clone().times(to.saturating_add(1));
    Some(text(matched.plus(&Bound::new(0).with_rows(to))))
}
