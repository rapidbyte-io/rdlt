//! What each read of a text costs its budget.

use datafusion_sql::sqlparser::tokenizer::{Token, TokenWithSpan};

use super::TYPE_WORDS;

/// Whether `token` is a space, which the parser passes over.
pub(super) fn spaced(token: &Token) -> bool {
    matches!(token, Token::Whitespace(_))
}

/// What a read at each of `tokens` costs: one, and every token other than a space that a typed
/// string's type tried there may take, its value among them.
///
/// sqlparser tries each operand as a typed string first: a dotted name, a few more words, a
/// parenthesised part it scans up to its first bracket, a run of `[…]`, and a value. None of that
/// reads an operand, so the read alone would leave it uncounted.
pub(super) fn charges(tokens: &[TokenWithSpan]) -> Vec<usize> {
    let reach = Reach::of(tokens);
    (0..tokens.len()).map(|at| reach.charge(at)).collect()
}

/// Where a typed string's type tried at each token may reach, in tables built once for a text.
struct Reach<'a> {
    tokens: &'a [TokenWithSpan],
    /// The first token at or after each that is not a space.
    next: Vec<usize>,
    /// How many tokens other than spaces come before each.
    counted: Vec<usize>,
    /// Where a dotted name from each ends.
    dotted: Vec<usize>,
    /// The first bracket at or after each.
    opened: Vec<usize>,
    /// The first token at or after each beyond a run of `[`, `]` and numbers.
    bracketed: Vec<usize>,
}

impl<'a> Reach<'a> {
    /// The tables of `tokens`.
    fn of(tokens: &'a [TokenWithSpan]) -> Self {
        let ends = tokens.len();
        let mut reach = Self {
            tokens,
            next: vec![ends; ends + 1],
            counted: vec![0; ends + 1],
            dotted: (0..=ends).collect(),
            opened: (0..=ends).collect(),
            bracketed: (0..=ends).collect(),
        };
        for (at, held) in tokens.iter().enumerate().rev() {
            reach.next[at] = if spaced(&held.token) {
                reach.next[at + 1]
            } else {
                at
            };
        }
        for (at, held) in tokens.iter().enumerate() {
            reach.counted[at + 1] = reach.counted[at] + usize::from(!spaced(&held.token));
        }
        for (at, held) in tokens.iter().enumerate().rev() {
            let then = reach.after(at);
            let named = reach.after(then);
            if reach.word(at) && reach.token(then) == Some(&Token::Period) && reach.word(named) {
                reach.dotted[at] = reach.dotted[named];
            }
            let bracket = matches!(
                held.token,
                Token::LParen
                    | Token::RParen
                    | Token::LBracket
                    | Token::RBracket
                    | Token::LBrace
                    | Token::RBrace
            );
            if !bracket {
                reach.opened[at] = reach.opened[then];
            }
            if matches!(
                held.token,
                Token::LBracket | Token::RBracket | Token::Number(..)
            ) {
                reach.bracketed[at] = reach.bracketed[then];
            }
        }
        reach
    }

    /// The token at `at`, where there is one.
    fn token(&self, at: usize) -> Option<&Token> {
        self.tokens.get(at).map(|token| &token.token)
    }

    /// Whether the token at `at` is a word.
    fn word(&self, at: usize) -> bool {
        matches!(self.token(at), Some(Token::Word(_)))
    }

    /// The first token after `at` that is not a space.
    fn after(&self, at: usize) -> usize {
        self.next.get(at + 1).copied().unwrap_or(self.tokens.len())
    }

    /// Where the words after `end` end, as many as a type may take.
    fn words(&self, mut end: usize) -> usize {
        for _ in 0..TYPE_WORDS {
            match self.after(end) {
                then if self.word(then) => end = then,
                _ => break,
            }
        }
        end
    }

    /// What a read at `at` costs.
    fn charge(&self, at: usize) -> usize {
        let first = self.next[at];
        if !self.word(first) {
            return 1;
        }
        let mut end = self.words(self.dotted[first]);
        if self.token(self.after(end)) == Some(&Token::LParen) {
            end = self.words(self.opened[self.after(self.after(end))]);
        }
        let mut value = self.after(end);
        if self.token(value) == Some(&Token::LBracket) {
            value = self.bracketed[value];
        }
        let value = value.min(self.tokens.len() - 1);
        1 + self.counted[value + 1] - self.counted[first]
    }
}
