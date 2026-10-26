//! What a history stream's rows carry beside their value and counter: a JSON document and a float,
//! each a value of the row's data alone, the document written in a way the row picks.
//!
//! Its numbers are written in plain or scientific notation, with or without zeros a value does
//! not need, beyond 64 bits and beyond what plain notation keeps short; its names and strings
//! escaped or not, its members in either order, a null member written or left out, and spaces
//! between tokens or none. Every way reads as one value, so a row sending its key's data again,
//! written another way, opens no version.

#[cfg(test)]
mod tests;

use serde_json::Value;

/// The document of a row of `value` and counter `n`, written the way `way` picks.
pub(super) fn document(value: Option<&str>, n: u64, way: u64) -> String {
    let written = |digits: u64, place: i32, negative: bool| number(digits, place, negative, way);
    let escaped = way & 0b1000 != 0;
    let string =
        |text: Option<&str>| text.map_or_else(|| "null".to_owned(), |text| quoted(text, escaped));
    let (colon, comma) = if way & 0b1_0000 == 0 {
        (":", ",")
    } else {
        (" : ", " ,\n\t")
    };
    let list = [
        written(n, 0, false),
        written(n, 0, true),
        "true".to_owned(),
        "null".to_owned(),
        string(value),
    ]
    .join(comma);
    let mut members = vec![
        ("n", written(n, 0, false)),
        ("half", written(n.saturating_mul(5), -1, false)),
        ("big", written(n, 24, false)),
        ("small", written(n, -30, false)),
        ("tiny", written(n, -420, false)),
        ("list", format!("[{list}]")),
        (
            "nested",
            format!("{{{}{colon}{}}}", quoted("value", escaped), string(value)),
        ),
    ];
    if way & 0b10_0000 != 0 {
        members.push(("gone", "null".to_owned()));
    }
    if way & 0b100 != 0 {
        members.reverse();
    }
    let members: Vec<String> = members
        .into_iter()
        .map(|(name, member)| format!("{}{colon}{member}", quoted(name, escaped)))
        .collect();
    format!(" {{{}}}\n", members.join(comma))
}

/// The float of a row of counter `n`: an eighth of it, beyond 2^64, below 2^-32 or with a half,
/// as the counter picks; negative zero for a counter of zero.
pub(super) fn ratio(n: u64) -> f64 {
    #[expect(clippy::cast_precision_loss, reason = "counters are small")]
    let counter = n as f64;
    if n == 0 {
        return -0.0;
    }
    match n % 4 {
        0 => counter / 8.0,
        1 => -counter * 2_f64.powi(70),
        2 => counter * 2_f64.powi(-40),
        _ => counter + 0.5,
    }
}

/// `digits` times ten to `place`, negative where `negative` says, in the notation `way` picks.
fn number(digits: u64, place: i32, negative: bool, way: u64) -> String {
    let sign = if negative { "-" } else { "" };
    let digits = digits.to_string();
    match way % 4 {
        0 => format!("{sign}{digits}e{place}"),
        1 => {
            let (first, rest) = digits.split_at(1);
            let exponent = place + i32::try_from(rest.len()).unwrap_or(0);
            format!("{sign}{first}.{rest}0E{exponent:+}")
        }
        2 => format!("{sign}{}", plain(&digits, place, 2)),
        _ => format!("{sign}{}", plain(&digits, place, 0)),
    }
}

/// `digits` times ten to `place` in plain notation, with `zeros` more after the point.
fn plain(digits: &str, place: i32, zeros: usize) -> String {
    let after = |text: String| match zeros {
        0 => text,
        _ if text.contains('.') => format!("{text}{}", "0".repeat(zeros)),
        _ => format!("{text}.{}", "0".repeat(zeros)),
    };
    if digits == "0" {
        return after("0".to_owned());
    }
    let shift = usize::try_from(place.unsigned_abs()).unwrap_or(usize::MAX);
    if place >= 0 {
        return after(format!("{digits}{}", "0".repeat(shift)));
    }
    match digits.len().checked_sub(shift) {
        Some(whole) if whole > 0 => after(format!("{}.{}", &digits[..whole], &digits[whole..])),
        _ => after(format!("0.{}{digits}", "0".repeat(shift - digits.len()))),
    }
}

/// `text` as a JSON string, every character escaped by its code where `escaped` says.
fn quoted(text: &str, escaped: bool) -> String {
    if !escaped {
        return Value::from(text).to_string();
    }
    let codes: String = text
        .encode_utf16()
        .map(|unit| format!("\\u{unit:04x}"))
        .collect();
    format!("\"{codes}\"")
}
