//! JSON text the engine holds as values, read a token at a time: no recursion however deep it
//! nests, nesting bounded, and numbers as they are written, so none is read through a float.
//!
//! Columns of JSON hold such text, from a connector's Arrow batches or rendered by the shredder;
//! row identity reads it, and so does the check every such column of a push meets first.

mod check;
#[cfg(test)]
mod tests;

use std::borrow::Cow;

use rdlt_connector::limits::MAX_NESTING_DEPTH;

pub(crate) use check::{NotJson, check_batch, holds_json};

/// Digits: the most a JSON number's exponent may have, beside its leading zeros, so its value's
/// place is a 64-bit integer whatever its digits are.
pub(crate) const EXPONENT_DIGITS: usize = 18;

/// Bytes: the longest a number's canonical text is written in plain notation; a longer one is
/// written in scientific notation.
///
/// Every value a 64-bit float or a decimal holds is plain within it, so such a value's canonical
/// text is the plain one its type renders.
pub(crate) const PLAIN_BYTES: usize = 400;

/// Why JSON text cannot be read.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum JsonError {
    /// The text is not JSON.
    #[error("the text is not JSON: {0}")]
    Invalid(&'static str),
    /// A value nests deeper than the limit.
    #[error("a value nests deeper than {MAX_NESTING_DEPTH} levels")]
    TooDeep,
    /// A number's exponent has more digits than the limit.
    #[error("a number's exponent has more than {EXPONENT_DIGITS} digits")]
    Exponent,
}

impl JsonError {
    /// The machine code of the error.
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::Invalid(_) => "json_invalid",
            Self::TooDeep | Self::Exponent => "limit_exceeded",
        }
    }
}

/// One step through JSON text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Token<'a> {
    Null,
    Bool(bool),
    /// A number, as it is written.
    Number(&'a str),
    /// A string, its escapes read.
    String(Cow<'a, str>),
    /// An object member's key, its escapes read; its value's tokens follow.
    Key(Cow<'a, str>),
    BeginObject,
    EndObject,
    BeginArray,
    EndArray,
}

/// Reads JSON text a token at a time, keeping only which containers it is in.
pub(crate) struct Reader<'a> {
    text: &'a str,
    at: usize,
    /// The containers open, the innermost last: whether each is an object.
    open: Vec<bool>,
    next: Next,
}

/// What the reader reads next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Next {
    /// A value: the text's, an item or a member's.
    Value,
    /// An array's first item, or its end.
    FirstItem,
    /// An object's first key, or its end.
    FirstKey,
    /// A key, after a comma.
    Key,
    /// A comma, or the end of the container open; or the end of the text where none is.
    Separator,
}

impl<'a> Reader<'a> {
    /// A reader of `text`, which holds one JSON value.
    pub(crate) fn new(text: &'a str) -> Self {
        Self {
            text,
            at: 0,
            open: Vec::new(),
            next: Next::Value,
        }
    }

    /// The next token; `None` once the value has ended and nothing but whitespace follows.
    ///
    /// # Errors
    ///
    /// Where the text is not one JSON value, or a value nests deeper than the limit.
    pub(crate) fn next(&mut self) -> Result<Option<Token<'a>>, JsonError> {
        loop {
            self.skip_whitespace();
            let byte = self.peek();
            match self.next {
                Next::Value => return self.value().map(Some),
                Next::Separator if self.open.is_empty() => {
                    return match byte {
                        None => Ok(None),
                        Some(_) => Err(JsonError::Invalid("more follows the value")),
                    };
                }
                Next::Separator => match (byte, self.open.last() == Some(&true)) {
                    (Some(b','), true) => self.advance(Next::Key),
                    (Some(b','), false) => self.advance(Next::Value),
                    (Some(b'}'), true) => return Ok(Some(self.close(Token::EndObject))),
                    (Some(b']'), false) => return Ok(Some(self.close(Token::EndArray))),
                    _ => return Err(JsonError::Invalid("a container is not separated or closed")),
                },
                Next::FirstItem if byte == Some(b']') => {
                    return Ok(Some(self.close(Token::EndArray)));
                }
                Next::FirstItem => self.next = Next::Value,
                Next::FirstKey if byte == Some(b'}') => {
                    return Ok(Some(self.close(Token::EndObject)));
                }
                Next::FirstKey | Next::Key => return self.key().map(Some),
            }
        }
    }

    fn peek(&self) -> Option<u8> {
        self.text.as_bytes().get(self.at).copied()
    }

    /// Steps past one byte, then reads `next`.
    fn advance(&mut self, next: Next) {
        self.at += 1;
        self.next = next;
    }

    fn skip_whitespace(&mut self) {
        let rest = &self.text.as_bytes()[self.at..];
        let blank = rest
            .iter()
            .position(|byte| !matches!(byte, b' ' | b'\t' | b'\n' | b'\r'));
        self.at += blank.unwrap_or(rest.len());
    }

    /// Closes the innermost container, with its closing byte, as `token`.
    fn close(&mut self, token: Token<'a>) -> Token<'a> {
        self.open.pop();
        self.advance(Next::Separator);
        token
    }

    fn value(&mut self) -> Result<Token<'a>, JsonError> {
        let Some(byte) = self.peek() else {
            return Err(JsonError::Invalid("the text ends where a value is due"));
        };
        self.next = Next::Separator;
        match byte {
            b'{' | b'[' => {
                if u64::try_from(self.open.len()).unwrap_or(u64::MAX) >= MAX_NESTING_DEPTH {
                    return Err(JsonError::TooDeep);
                }
                let object = byte == b'{';
                self.open.push(object);
                if object {
                    self.advance(Next::FirstKey);
                    Ok(Token::BeginObject)
                } else {
                    self.advance(Next::FirstItem);
                    Ok(Token::BeginArray)
                }
            }
            b'"' => self.string().map(Token::String),
            b't' => self.literal("true", Token::Bool(true)),
            b'f' => self.literal("false", Token::Bool(false)),
            b'n' => self.literal("null", Token::Null),
            b'-' | b'0'..=b'9' => self.number().map(Token::Number),
            _ => Err(JsonError::Invalid("a value is due")),
        }
    }

    fn key(&mut self) -> Result<Token<'a>, JsonError> {
        if self.peek() != Some(b'"') {
            return Err(JsonError::Invalid("a key is due"));
        }
        let key = self.string()?;
        self.skip_whitespace();
        if self.peek() != Some(b':') {
            return Err(JsonError::Invalid("a key is not followed by a colon"));
        }
        self.advance(Next::Value);
        Ok(Token::Key(key))
    }

    fn literal(&mut self, word: &str, token: Token<'a>) -> Result<Token<'a>, JsonError> {
        if !self.text[self.at..].starts_with(word) {
            return Err(JsonError::Invalid("a literal is misspelt"));
        }
        self.at += word.len();
        Ok(token)
    }

    /// The string whose opening quote is at the reader's place, its escapes read.
    fn string(&mut self) -> Result<Cow<'a, str>, JsonError> {
        let bytes = self.text.as_bytes();
        let start = self.at + 1;
        let Some(end) = memchr::memchr2(b'"', b'\\', &bytes[start..]).map(|end| start + end) else {
            return Err(JsonError::Invalid("a string does not end"));
        };
        if bytes[end] == b'"' {
            unescaped(&bytes[start..end])?;
            self.at = end + 1;
            return Ok(Cow::Borrowed(&self.text[start..end]));
        }
        let mut owned = String::with_capacity(end - start + 16);
        owned.push_str(&self.text[start..end]);
        unescaped(&bytes[start..end])?;
        self.at = end;
        loop {
            match bytes.get(self.at) {
                Some(b'"') => {
                    self.at += 1;
                    return Ok(Cow::Owned(owned));
                }
                Some(b'\\') => self.escape(&mut owned)?,
                Some(_) => {
                    let run = memchr::memchr2(b'"', b'\\', &bytes[self.at..])
                        .ok_or(JsonError::Invalid("a string does not end"))?;
                    let piece = &bytes[self.at..self.at + run];
                    unescaped(piece)?;
                    owned.push_str(&self.text[self.at..self.at + run]);
                    self.at += run;
                }
                None => return Err(JsonError::Invalid("a string does not end")),
            }
        }
    }

    /// Reads the escape at the reader's place onto `owned`.
    fn escape(&mut self, owned: &mut String) -> Result<(), JsonError> {
        let bytes = self.text.as_bytes();
        let Some(&kind) = bytes.get(self.at + 1) else {
            return Err(JsonError::Invalid("a string does not end"));
        };
        self.at += 2;
        let short = match kind {
            b'"' => '"',
            b'\\' => '\\',
            b'/' => '/',
            b'b' => '\u{8}',
            b'f' => '\u{c}',
            b'n' => '\n',
            b'r' => '\r',
            b't' => '\t',
            b'u' => return self.unicode(owned),
            _ => return Err(JsonError::Invalid("a string holds an unknown escape")),
        };
        owned.push(short);
        Ok(())
    }

    /// Reads a `\u` escape, whose four hex digits are at the reader's place, onto `owned`: a
    /// surrogate only as half of a pair.
    fn unicode(&mut self, owned: &mut String) -> Result<(), JsonError> {
        let high = self.hex()?;
        let code = match high {
            0xd800..=0xdbff => {
                let bytes = self.text.as_bytes();
                if bytes.get(self.at..self.at + 2) != Some(b"\\u") {
                    return Err(JsonError::Invalid("a string holds a lone surrogate"));
                }
                self.at += 2;
                let low = self.hex()?;
                if !(0xdc00..=0xdfff).contains(&low) {
                    return Err(JsonError::Invalid("a string holds a lone surrogate"));
                }
                0x10000 + ((high - 0xd800) << 10) + (low - 0xdc00)
            }
            0xdc00..=0xdfff => return Err(JsonError::Invalid("a string holds a lone surrogate")),
            other => other,
        };
        let character =
            char::from_u32(code).ok_or(JsonError::Invalid("a string holds no character"))?;
        owned.push(character);
        Ok(())
    }

    /// The four hex digits at the reader's place.
    fn hex(&mut self) -> Result<u32, JsonError> {
        let digits = self
            .text
            .get(self.at..self.at + 4)
            .filter(|digits| digits.bytes().all(|byte| byte.is_ascii_hexdigit()))
            .ok_or(JsonError::Invalid("a string holds a short unicode escape"))?;
        self.at += 4;
        u32::from_str_radix(digits, 16).map_err(|_| JsonError::Invalid("a unicode escape"))
    }

    /// The number at the reader's place, as it is written.
    fn number(&mut self) -> Result<&'a str, JsonError> {
        let bytes = self.text.as_bytes();
        let start = self.at;
        let digits = |at: usize| {
            bytes[at..]
                .iter()
                .take_while(|b| b.is_ascii_digit())
                .count()
        };
        let mut at = start + usize::from(bytes[start] == b'-');
        match bytes.get(at) {
            Some(b'0') => at += 1,
            Some(b'1'..=b'9') => at += digits(at),
            _ => return Err(JsonError::Invalid("a number has no digits")),
        }
        if bytes.get(at) == Some(&b'.') {
            let fraction = digits(at + 1);
            if fraction == 0 {
                return Err(JsonError::Invalid(
                    "a number's point has no digits after it",
                ));
            }
            at += 1 + fraction;
        }
        if matches!(bytes.get(at), Some(b'e' | b'E')) {
            at += 1;
            if matches!(bytes.get(at), Some(b'+' | b'-')) {
                at += 1;
            }
            let exponent = digits(at);
            if exponent == 0 {
                return Err(JsonError::Invalid("a number's exponent has no digits"));
            }
            let zeros = bytes[at..at + exponent]
                .iter()
                .take_while(|b| **b == b'0')
                .count();
            if exponent - zeros > EXPONENT_DIGITS {
                return Err(JsonError::Exponent);
            }
            at += exponent;
        }
        self.at = at;
        Ok(&self.text[start..at])
    }
}

/// Refuses `piece`, part of a string between its escapes, where it holds a control character,
/// which JSON writes only escaped.
fn unescaped(piece: &[u8]) -> Result<(), JsonError> {
    if piece.iter().any(|byte| *byte < 0x20) {
        return Err(JsonError::Invalid(
            "a string holds an unescaped control character",
        ));
    }
    Ok(())
}

/// Checks that `text` is one JSON value nested no deeper than the limit.
///
/// # Errors
///
/// Why it is not.
pub(crate) fn check(text: &str) -> Result<(), JsonError> {
    let mut reader = Reader::new(text);
    while reader.next()?.is_some() {}
    Ok(())
}

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
