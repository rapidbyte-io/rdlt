//! The canonical text of a JSON number: its value exactly, in one notation that follows from the
//! value alone, as integers, decimals and floats render theirs.

use super::{JsonError, Reader, Token};

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
pub(crate) fn canonical_number(written: &str) -> Result<String, JsonError> {
    let mut reader = Reader::new(written);
    match (reader.next()?, reader.next()?) {
        (Some(Token::Number(number)), None) if number.len() == written.len() => {}
        _ => return Err(JsonError::Invalid("a number is due")),
    }
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
    let digits = format!("{whole}{fraction}");
    let significant = digits.trim_start_matches('0');
    let trimmed = significant.trim_end_matches('0');
    if trimmed.is_empty() {
        return Ok("0".to_owned());
    }
    let shifted = |count: usize| i64::try_from(count).map_err(|_| JsonError::Exponent);
    // The value is `trimmed` times ten to `place`.
    let place = exponent
        .checked_sub(shifted(fraction.len())?)
        .and_then(|place| place.checked_add(shifted(significant.len() - trimmed.len()).ok()?))
        .ok_or(JsonError::Exponent)?;
    let sign = if negative { "-" } else { "" };
    if let Some(plain) = plain(trimmed, place) {
        return Ok(format!("{sign}{plain}"));
    }
    let (first, rest) = trimmed.split_at(1);
    let point = if rest.is_empty() { "" } else { "." };
    let power = place
        .checked_add(shifted(rest.len())?)
        .ok_or(JsonError::Exponent)?;
    Ok(format!("{sign}{first}{point}{rest}e{power}"))
}

/// The canonical text of the float `value`: of the shortest text that reads back as it, a tie
/// going to the even one as JSON writers break it; a float JSON has no number for, its name.
pub(crate) fn canonical_float(value: f64) -> String {
    if !value.is_finite() {
        return value.to_string();
    }
    let written = serde_json::to_string(&value).unwrap_or_default();
    canonical_number(&written).unwrap_or(written)
}

/// The canonical text of the 32-bit float `value`, as [`canonical_float`] gives a float's.
pub(crate) fn canonical_float32(value: f32) -> String {
    if !value.is_finite() {
        return value.to_string();
    }
    let written = serde_json::to_string(&value).unwrap_or_default();
    canonical_number(&written).unwrap_or(written)
}

/// `digits`, with no zero at either end, times ten to `place`, in plain notation; `None` where
/// that is longer than [`PLAIN_BYTES`].
fn plain(digits: &str, place: i64) -> Option<String> {
    let count = i64::try_from(digits.len()).ok()?;
    let limit = i64::try_from(PLAIN_BYTES).ok()?;
    if place >= 0 {
        if count.checked_add(place)? > limit {
            return None;
        }
        return Some(format!(
            "{digits}{}",
            "0".repeat(usize::try_from(place).ok()?)
        ));
    }
    // Where the point goes, counted from the first digit.
    let point = count.checked_add(place)?;
    if point > 0 {
        if count.checked_add(1)? > limit {
            return None;
        }
        let (whole, fraction) = digits.split_at(usize::try_from(point).ok()?);
        return Some(format!("{whole}.{fraction}"));
    }
    let zeros = point.checked_neg()?;
    if zeros.checked_add(count)?.checked_add(2)? > limit {
        return None;
    }
    Some(format!(
        "0.{}{digits}",
        "0".repeat(usize::try_from(zeros).ok()?)
    ))
}
