//! A JSON string's text as it is written: checked where it lies, its escapes read only on demand.

use std::borrow::Cow;

use super::JsonError;

/// A JSON string's text between its quotes, escapes and all, which a reader has checked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Text<'a> {
    raw: &'a str,
    /// Whether the text holds an escape.
    escaped: bool,
}

impl<'a> Text<'a> {
    /// The string whose opening quote is just before `start` in `text`, and the place past its
    /// closing quote: its escapes and control characters checked, nothing copied.
    pub(super) fn read(text: &'a str, start: usize) -> Result<(Self, usize), JsonError> {
        let bytes = text.as_bytes();
        let mut at = start;
        let mut escaped = false;
        loop {
            let Some(run) = memchr::memchr2(b'"', b'\\', &bytes[at..]) else {
                return Err(JsonError::Invalid("a string does not end"));
            };
            no_controls(&bytes[at..at + run])?;
            at += run;
            if bytes[at] == b'"' {
                let raw = &text[start..at];
                return Ok((Self { raw, escaped }, at + 1));
            }
            escaped = true;
            at += escape(bytes, at)?.1;
        }
    }

    /// The string, its escapes read: borrowed where it holds none.
    pub(crate) fn unescaped(&self) -> Cow<'a, str> {
        if !self.escaped {
            return Cow::Borrowed(self.raw);
        }
        let bytes = self.raw.as_bytes();
        // An escape is never shorter than the character it stands for.
        let mut owned = String::with_capacity(self.raw.len());
        let mut at = 0;
        while let Some(run) = memchr::memchr(b'\\', &bytes[at..]) {
            owned.push_str(&self.raw[at..at + run]);
            at += run;
            let (character, taken) =
                escape(bytes, at).expect("a reader read the text, so its escapes are whole");
            owned.push(character);
            at += taken;
        }
        owned.push_str(&self.raw[at..]);
        Cow::Owned(owned)
    }
}

/// The character the escape at `at` in `bytes` stands for, and how many bytes the escape takes: a
/// `\u` surrogate only as half of a pair.
fn escape(bytes: &[u8], at: usize) -> Result<(char, usize), JsonError> {
    let Some(&kind) = bytes.get(at + 1) else {
        return Err(JsonError::Invalid("a string does not end"));
    };
    let short = match kind {
        b'"' => '"',
        b'\\' => '\\',
        b'/' => '/',
        b'b' => '\u{8}',
        b'f' => '\u{c}',
        b'n' => '\n',
        b'r' => '\r',
        b't' => '\t',
        b'u' => return unicode(bytes, at + 2).map(|(character, taken)| (character, 2 + taken)),
        _ => return Err(JsonError::Invalid("a string holds an unknown escape")),
    };
    Ok((short, 2))
}

/// The character of a `\u` escape whose four hex digits are at `at` in `bytes`, and how many bytes
/// follow its `\u`: a surrogate only as half of a pair.
fn unicode(bytes: &[u8], at: usize) -> Result<(char, usize), JsonError> {
    let lone = JsonError::Invalid("a string holds a lone surrogate");
    let high = hex(bytes, at)?;
    let (code, taken) = match high {
        0xd800..=0xdbff => {
            if bytes.get(at + 4..at + 6) != Some(b"\\u") {
                return Err(lone);
            }
            let low = hex(bytes, at + 6)?;
            if !(0xdc00..=0xdfff).contains(&low) {
                return Err(lone);
            }
            (0x10000 + ((high - 0xd800) << 10) + (low - 0xdc00), 10)
        }
        0xdc00..=0xdfff => return Err(lone),
        other => (other, 4),
    };
    let character =
        char::from_u32(code).ok_or(JsonError::Invalid("a string holds no character"))?;
    Ok((character, taken))
}

/// The value of the four hex digits at `at` in `bytes`.
fn hex(bytes: &[u8], at: usize) -> Result<u32, JsonError> {
    bytes
        .get(at..at + 4)
        .and_then(|digits| {
            digits.iter().try_fold(0, |value, byte| {
                char::from(*byte)
                    .to_digit(16)
                    .map(|digit| value << 4 | digit)
            })
        })
        .ok_or(JsonError::Invalid("a string holds a short unicode escape"))
}

/// Refuses `piece`, part of a string between its escapes, where it holds a control character,
/// which JSON writes only escaped.
fn no_controls(piece: &[u8]) -> Result<(), JsonError> {
    if piece.iter().any(|byte| *byte < 0x20) {
        return Err(JsonError::Invalid(
            "a string holds an unescaped control character",
        ));
    }
    Ok(())
}
