//! JSON text the engine holds as values, read a token at a time: no recursion however deep it
//! nests, nesting bounded, and numbers as they are written, so none is read through a float.
//!
//! Columns of JSON hold such text, from a connector's Arrow batches or rendered by the shredder;
//! row identity reads it, and so does the check every such column of a push meets first.

mod check;
mod number;
#[cfg(test)]
mod tests;

use std::borrow::Cow;

use rdlt_connector::limits::MAX_NESTING_DEPTH;

pub(crate) use check::{NotJson, check_batch, holds_json};
pub(crate) use number::{canonical_float, canonical_float32, canonical_number};

/// Digits: the most a JSON number's exponent may have, beside its leading zeros, so its value's
/// place is a 64-bit integer whatever its digits are.
pub(crate) const EXPONENT_DIGITS: usize = 18;

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

/// Whether the containers of JSON `text` nest deeper than the limit: a linear scan of its
/// brackets outside its strings, for whoever parses it recursively after.
pub(crate) fn too_deep(text: &[u8]) -> bool {
    let (mut depth, mut in_string, mut escaped) = (0_u64, false, false);
    for byte in text {
        if in_string {
            if escaped {
                escaped = false;
            } else if *byte == b'\\' {
                escaped = true;
            } else if *byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'[' | b'{' => {
                depth += 1;
                if depth > MAX_NESTING_DEPTH {
                    return true;
                }
            }
            b']' | b'}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    false
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
