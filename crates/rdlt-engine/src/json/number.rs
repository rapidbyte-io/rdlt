//! The canonical text of a JSON number: its value exactly, in one notation that follows from the
//! value alone, as integers, decimals and floats render theirs.

use std::io::Write as _;

use super::{EXPONENT_DIGITS, JsonError};

/// Bytes: the longest a number's canonical text is written in plain notation; a longer one is
/// written in scientific notation.
///
/// Every value a 64-bit float or a decimal holds is plain within it, so such a value's canonical
/// text is the plain one its type renders.
pub(crate) const PLAIN_BYTES: usize = 400;

/// The canonical text of the JSON number `written`: its value exactly, in plain notation (`-12.5`,
/// `0.001`, `1000`) where that takes at most [`PLAIN_BYTES`], in scientific (`1.25e401`) beyond.
///
/// Numbers of one value have one text: `1`, `1.0`, `10e-1` and `0.1e1` are all `1`, and zero
/// has no sign. A value's notation follows from the value alone, so distinct values never
/// share a text.
///
/// # Errors
///
/// Where `written` is not a JSON number, or its exponent is beyond what the limit lets one be.
#[cfg(test)]
pub(crate) fn canonical_number(written: &str) -> Result<String, JsonError> {
    let mut reader = super::Reader::new(written);
    match (reader.next()?, reader.next()?) {
        (Some(super::Token::Number(number)), None) if number.len() == written.len() => {}
        _ => return Err(JsonError::Invalid("a number is due")),
    }
    let mut out = Vec::new();
    write_number(written, &mut out)?;
    String::from_utf8(out).map_err(|_| JsonError::Invalid("a number is due"))
}

/// Appends the canonical text of `written`, a number the reader read, to `out`, as
/// [`canonical_number`] gives it.
///
/// # Errors
///
/// [`JsonError::Exponent`] where the value's place is beyond a 64-bit integer.
pub(crate) fn write_number(written: &str, out: &mut Vec<u8>) -> Result<(), JsonError> {
    let (negative, unsigned) = written
        .strip_prefix('-')
        .map_or((false, written), |rest| (true, rest));
    let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
        Some(at) => (&unsigned[..at], &unsigned[at + 1..]),
        None => (unsigned, "0"),
    };
    let exponent: i64 = exponent
        .trim_start_matches('+')
        .parse()
        .map_err(|_| JsonError::Exponent)?;
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = Digits {
        whole: whole.as_bytes(),
        fraction: fraction.as_bytes(),
    };
    let count = digits.len();
    let Some(first) = (0..count).find(|at| digits.get(*at) != b'0') else {
        out.push(b'0');
        return Ok(());
    };
    let last = (0..count)
        .rev()
        .find(|at| digits.get(*at) != b'0')
        .unwrap_or(first);
    let shifted = |count: usize| i64::try_from(count).map_err(|_| JsonError::Exponent);
    // The value is the digits from `first` to `last` times ten to `place`.
    let place = exponent
        .checked_sub(shifted(fraction.len())?)
        .and_then(|place| place.checked_add(shifted(count - 1 - last).ok()?))
        .ok_or(JsonError::Exponent)?;
    if negative {
        out.push(b'-');
    }
    let significant = (first..=last).map(|at| digits.get(at));
    if plain(significant.clone(), last - first + 1, place, out) {
        return Ok(());
    }
    let mut significant = significant;
    out.extend(significant.next());
    if last > first {
        out.push(b'.');
        out.extend(significant);
    }
    let power = place
        .checked_add(shifted(last - first)?)
        .ok_or(JsonError::Exponent)?;
    write!(out, "e{power}").map_err(|_| JsonError::Exponent)
}

/// Whether the exponent of `written`, a JSON number, has no more than [`EXPONENT_DIGITS`]
/// digits beside its leading zeros: whether its value has a canonical text.
pub(crate) fn exponent_within(written: &str) -> bool {
    let Some(at) = written.find(['e', 'E']) else {
        return true;
    };
    let exponent = written[at + 1..].trim_start_matches(['+', '-']);
    exponent.trim_start_matches('0').len() <= EXPONENT_DIGITS
}

/// Whether `bytes`, JSON text, may hold a number whose exponent has more than
/// [`EXPONENT_DIGITS`] digits beside its leading zeros: a digit, an `e` and such digits, which
/// a string may hold too.
pub(crate) fn may_hold_long_exponent(bytes: &[u8]) -> bool {
    memchr::memchr2_iter(b'e', b'E', bytes).any(|at| {
        let after_digit = at > 0 && bytes[at - 1].is_ascii_digit();
        let rest = &bytes[at + 1..];
        let rest = rest
            .strip_prefix(b"+")
            .or_else(|| rest.strip_prefix(b"-"))
            .unwrap_or(rest);
        let zeros = rest.iter().take_while(|byte| **byte == b'0').count();
        let digits = rest[zeros..]
            .iter()
            .take_while(|byte| byte.is_ascii_digit())
            .count();
        after_digit && digits > EXPONENT_DIGITS
    })
}

/// The canonical text of the float `value`: of the shortest text that reads back as it, a tie
/// going to the even one as JSON writers break it; a float JSON has no number for, its name.
#[cfg(test)]
pub(crate) fn canonical_float(value: f64) -> String {
    let mut out = Vec::new();
    write_float(value, &mut out);
    String::from_utf8(out).unwrap_or_default()
}

/// The canonical text of the 32-bit float `value`, as [`canonical_float`] gives a float's.
#[cfg(test)]
pub(crate) fn canonical_float32(value: f32) -> String {
    let mut out = Vec::new();
    write_float32(value, &mut out);
    String::from_utf8(out).unwrap_or_default()
}

/// Appends the canonical text of the float `value` to `out`, as [`canonical_float`] gives it.
pub(crate) fn write_float(value: f64, out: &mut Vec<u8>) {
    if value.is_finite() {
        written_float(ryu::Buffer::new().format_finite(value), out);
    } else {
        write!(out, "{value}").unwrap_or_default();
    }
}

/// Appends the canonical text of the 32-bit float `value` to `out`.
pub(crate) fn write_float32(value: f32, out: &mut Vec<u8>) {
    if value.is_finite() {
        written_float(ryu::Buffer::new().format_finite(value), out);
    } else {
        write!(out, "{value}").unwrap_or_default();
    }
}

/// Appends the canonical text of `written`, a finite float's shortest text, whose exponent is a
/// few digits.
fn written_float(written: &str, out: &mut Vec<u8>) {
    let start = out.len();
    if write_number(written, out).is_err() {
        out.truncate(start);
        out.extend_from_slice(written.as_bytes());
    }
}

/// The digits of a number's mantissa, read across its point.
#[derive(Clone, Copy)]
struct Digits<'a> {
    whole: &'a [u8],
    fraction: &'a [u8],
}

impl Digits<'_> {
    fn len(self) -> usize {
        self.whole.len() + self.fraction.len()
    }

    fn get(self, at: usize) -> u8 {
        match at.checked_sub(self.whole.len()) {
            None => self.whole[at],
            Some(at) => self.fraction[at],
        }
    }
}

/// Appends `count` `digits`, with no zero at either end, times ten to `place`, in plain notation
/// to `out`; false, appending nothing, where that is longer than [`PLAIN_BYTES`].
fn plain(digits: impl Iterator<Item = u8>, count: usize, place: i64, out: &mut Vec<u8>) -> bool {
    let (Ok(count), Ok(limit)) = (i64::try_from(count), i64::try_from(PLAIN_BYTES)) else {
        return false;
    };
    let zeros = |count: i64| std::iter::repeat_n(b'0', usize::try_from(count).unwrap_or(0));
    if place >= 0 {
        if count.saturating_add(place) > limit {
            return false;
        }
        out.extend(digits.chain(zeros(place)));
        return true;
    }
    // Where the point goes, counted from the first digit.
    let point = count.saturating_add(place);
    if point > 0 {
        if count.saturating_add(1) > limit {
            return false;
        }
        let point = usize::try_from(point).unwrap_or(0);
        for (at, digit) in digits.enumerate() {
            if at == point {
                out.push(b'.');
            }
            out.push(digit);
        }
        return true;
    }
    let leading = point.saturating_neg();
    if leading.saturating_add(count).saturating_add(2) > limit {
        return false;
    }
    out.extend_from_slice(b"0.");
    out.extend(zeros(leading).chain(digits));
    true
}
