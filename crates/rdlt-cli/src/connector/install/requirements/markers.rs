//! A requirement's markers, read in PEP 508's grammar and written again from what was read.
//!
//! An environment's variable is compared with a quoted value, joined by `and` and `or`, in
//! parentheses where they nest. A quoted value holds no whitespace and starts with no `-`, so no
//! word of what is written could be taken by pip for one of its options.

#[cfg(test)]
mod tests;

/// The variables of an environment a marker compares.
const VARIABLES: [&str; 12] = [
    "python_version",
    "python_full_version",
    "os_name",
    "sys_platform",
    "platform_release",
    "platform_system",
    "platform_version",
    "platform_machine",
    "platform_python_implementation",
    "implementation_name",
    "implementation_version",
    "extra",
];

/// The comparisons written as symbols; `in` and `not in` are words.
const OPERATORS: [&str; 8] = ["===", "==", "!=", "<=", ">=", "<", ">", "~="];

/// The characters of a quoted value besides ASCII letters and digits.
const VALUE: &[u8] = b"._+*,!~<>=-";

/// The deepest markers nest in parentheses.
const NESTING: usize = 16;

/// A word of markers as they are read.
#[derive(Clone, Copy, PartialEq)]
enum Token<'a> {
    Open,
    Close,
    Word(&'a str),
    Operator(&'a str),
    Value(&'a str),
}

/// The markers `text` names, written as pip is given them, one space between their words and each
/// value in double quotes; none where `text` is not markers rdlt takes.
pub(super) fn marked(text: &str) -> Option<String> {
    let mut reading = Reading {
        tokens: tokens(text)?,
        at: 0,
        written: Vec::new(),
    };
    reading.any(0)?;
    (reading.at == reading.tokens.len()).then(|| reading.written.join(" "))
}

/// The words of `text`; none where it holds what no marker does.
fn tokens(text: &str) -> Option<Vec<Token<'_>>> {
    let bytes = text.as_bytes();
    let mut tokens = Vec::new();
    let mut at = 0;
    while let Some(&byte) = bytes.get(at) {
        let run =
            |taken: fn(u8) -> bool| bytes[at..].iter().take_while(|byte| taken(**byte)).count();
        let (token, length) = match byte {
            b' ' | b'\t' => {
                at += 1;
                continue;
            }
            b'(' => (Token::Open, 1),
            b')' => (Token::Close, 1),
            b'\'' | b'"' => {
                let value = &text[at + 1..];
                let end = value.find(char::from(byte))?;
                let value = &value[..end];
                let valued = value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || VALUE.contains(&byte));
                if !valued || value.starts_with('-') {
                    return None;
                }
                (Token::Value(value), end + 2)
            }
            b'<' | b'>' | b'=' | b'!' | b'~' => {
                let length = run(|byte| b"<>=!~".contains(&byte));
                (Token::Operator(&text[at..at + length]), length)
            }
            _ if byte.is_ascii_alphanumeric() || byte == b'_' => {
                let length = run(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
                (Token::Word(&text[at..at + length]), length)
            }
            _ => return None,
        };
        tokens.push(token);
        at += length;
    }
    Some(tokens)
}

/// Markers being read, how far, and what is written of them.
struct Reading<'a> {
    tokens: Vec<Token<'a>>,
    at: usize,
    written: Vec<String>,
}

impl Reading<'_> {
    /// Reads `token` where it comes next.
    fn took(&mut self, token: Token<'_>) -> bool {
        let next = self.tokens.get(self.at) == Some(&token);
        self.at += usize::from(next);
        next
    }

    /// Markers joined by `or`, nested `depth` deep.
    fn any(&mut self, depth: usize) -> Option<()> {
        self.all(depth)?;
        while self.took(Token::Word("or")) {
            self.written.push("or".to_owned());
            self.all(depth)?;
        }
        Some(())
    }

    /// Markers joined by `and`, nested `depth` deep.
    fn all(&mut self, depth: usize) -> Option<()> {
        self.one(depth)?;
        while self.took(Token::Word("and")) {
            self.written.push("and".to_owned());
            self.one(depth)?;
        }
        Some(())
    }

    /// One comparison, or markers in parentheses, nested `depth` deep.
    fn one(&mut self, depth: usize) -> Option<()> {
        if self.took(Token::Open) {
            if depth == NESTING {
                return None;
            }
            self.written.push("(".to_owned());
            self.any(depth + 1)?;
            self.took(Token::Close).then_some(())?;
            self.written.push(")".to_owned());
            return Some(());
        }
        self.operand()?;
        self.operator()?;
        self.operand()
    }

    /// A variable of the environment, or a quoted value.
    fn operand(&mut self) -> Option<()> {
        let written = match self.tokens.get(self.at)? {
            Token::Word(word) if VARIABLES.contains(word) => (*word).to_owned(),
            Token::Value(value) => format!("\"{value}\""),
            _ => return None,
        };
        self.at += 1;
        self.written.push(written);
        Some(())
    }

    /// A comparison.
    fn operator(&mut self) -> Option<()> {
        if self.took(Token::Word("not")) {
            self.took(Token::Word("in")).then_some(())?;
            self.written.push("not in".to_owned());
            return Some(());
        }
        let written = match self.tokens.get(self.at)? {
            Token::Word("in") => "in",
            Token::Operator(operator) if OPERATORS.contains(operator) => *operator,
            _ => return None,
        };
        self.at += 1;
        self.written.push(written.to_owned());
        Some(())
    }
}
