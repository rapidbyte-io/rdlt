//! A version as PEP 440 writes one, in the forms pip takes: nothing else reaches pip as a version,
//! where `1.tar.gz` would name a file.

#[cfg(test)]
mod tests;

/// The words that mark a prerelease, the longer first where one starts another.
const PRERELEASES: [&str; 8] = ["alpha", "beta", "preview", "pre", "rc", "a", "b", "c"];

/// The words that mark a release after another.
const POSTS: [&str; 3] = ["post", "rev", "r"];

/// Whether `text` is a version as PEP 440 has it: `[v][N!]N(.N)*`, then a prerelease, a post
/// release and a development release, each where it has one, and a local label after `+`.
pub(super) fn version(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    let mut read = Reading {
        bytes: lower.as_bytes(),
        at: 0,
    };
    read.eat(b'v');
    let start = read.at;
    if !(read.digits() && read.eat(b'!')) {
        read.at = start;
    }
    if !read.digits() {
        return false;
    }
    while read.peek(0) == Some(b'.') && read.peek(1).is_some_and(|byte| byte.is_ascii_digit()) {
        read.at += 1;
        read.digits();
    }
    read.labelled(&PRERELEASES);
    let start = read.at;
    if !(read.eat(b'-') && read.digits()) {
        read.at = start;
        read.labelled(&POSTS);
    }
    read.labelled(&["dev"]);
    if read.eat(b'+') {
        loop {
            if !read.alphanumerics() {
                return false;
            }
            if !read.separator() {
                break;
            }
        }
    }
    read.at == read.bytes.len()
}

/// A version being read, and how far.
struct Reading<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Reading<'_> {
    /// The byte `ahead` of where the reading is.
    fn peek(&self, ahead: usize) -> Option<u8> {
        self.bytes.get(self.at + ahead).copied()
    }

    /// Whether `byte` comes next, read past where it does.
    fn eat(&mut self, byte: u8) -> bool {
        let next = self.peek(0) == Some(byte);
        self.at += usize::from(next);
        next
    }

    /// Whether a separator, `-`, `_` or `.`, comes next, read past where one does.
    fn separator(&mut self) -> bool {
        let next = self.peek(0).is_some_and(|byte| b"-_.".contains(&byte));
        self.at += usize::from(next);
        next
    }

    /// Whether digits come next, read past where they do.
    fn digits(&mut self) -> bool {
        self.run(|byte| byte.is_ascii_digit())
    }

    /// Whether letters or digits come next, read past where they do.
    fn alphanumerics(&mut self) -> bool {
        self.run(|byte| byte.is_ascii_alphanumeric())
    }

    /// Whether bytes `taken` come next, read past where they do.
    fn run(&mut self, taken: impl Fn(u8) -> bool) -> bool {
        let length = self.bytes[self.at..]
            .iter()
            .take_while(|byte| taken(**byte))
            .count();
        self.at += length;
        length > 0
    }

    /// Reads a separator, one of `words` and a separator and digits after it, each but the word
    /// where it has one; reads nothing where none of `words` comes.
    fn labelled(&mut self, words: &[&str]) {
        let start = self.at;
        self.separator();
        let rest = &self.bytes[self.at..];
        match words.iter().find(|word| rest.starts_with(word.as_bytes())) {
            Some(word) => {
                self.at += word.len();
                let before = self.at;
                if !(self.separator() && self.digits()) {
                    self.at = before;
                    self.digits();
                }
            }
            None => self.at = start,
        }
    }
}
