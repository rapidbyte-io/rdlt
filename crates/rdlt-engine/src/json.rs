//! JSON text the engine holds as values, read a token at a time: no recursion however deep it
//! nests, nesting bounded, and numbers as they are written, so none is read through a float.
//!
//! Columns of JSON hold such text, from a connector's Arrow batches or rendered by the shredder;
//! row identity reads it, and so does the check every such column of a push meets first. The
//! shredder's exact parse reads records through it too.

mod check;
mod number;
#[cfg(test)]
mod tests;
mod text;

use rdlt_wire::limits::{NESTING_DEPTH, count};

use crate::error::{Code, codes};

#[cfg(feature = "bench")]
pub(crate) use check::bench_field;
pub(crate) use check::{NotJson, check_batch, held as held_by_check, schema_holds_json};
#[cfg(test)]
pub(crate) use number::{canonical_float, canonical_number};
pub(crate) use number::{long_numbers, may_hold_exponent, write_float, write_number};
pub(crate) use text::Text;

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
    #[error("a value nests deeper than {NESTING_DEPTH} levels")]
    TooDeep,
    /// A number's exponent has more digits than the limit.
    #[error("a number's exponent has more than {EXPONENT_DIGITS} digits")]
    Exponent,
}

impl JsonError {
    /// The engine's code for the error.
    pub(crate) fn code(&self) -> Code {
        match self {
            Self::Invalid(_) => codes::JSON_INVALID,
            Self::TooDeep | Self::Exponent => codes::LIMIT_EXCEEDED,
        }
    }
}

/// One step through JSON text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Token<'a> {
    Null,
    Bool(bool),
    /// A number, as it is written.
    Number(&'a str),
    /// A string as it is written, its escapes checked; [`Text::unescaped`] reads them.
    String(Text<'a>),
    /// An object member's key, as a string is; its value's tokens follow.
    Key(Text<'a>),
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

    /// Reads `text` next, which holds one JSON value, from its start: only the room the reader
    /// grew for its containers is kept.
    pub(crate) fn reset(&mut self, text: &'a str) {
        self.text = text;
        self.at = 0;
        self.open.clear();
        self.next = Next::Value;
    }

    /// The text from the reader's place on: where a token it refused starts.
    pub(crate) fn rest(&self) -> &'a str {
        self.text.get(self.at..).unwrap_or_default()
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
                if count(self.open.len()) >= NESTING_DEPTH {
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

    /// The string whose opening quote is at the reader's place.
    fn string(&mut self) -> Result<Text<'a>, JsonError> {
        let (text, end) = Text::read(self.text, self.at + 1)?;
        self.at = end;
        Ok(text)
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
