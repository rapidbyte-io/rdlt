//! How pip reads a line of the requirements file rdlt writes for it, so that each line is seen to
//! read as the line rdlt checked before it is written: pip ends a line at its comment, expands
//! `${...}`, splits a requirement's line at its spaces, taking each word from the first that
//! starts with `-` for its options, and splits options as a shell does.

#[cfg(test)]
mod tests;

/// Whether pip reads `line` as it is: printable ASCII, no `${` to expand, and no `#` that would
/// start a comment.
fn plain(line: &str) -> bool {
    let commented = line.starts_with('#') || line.contains(" #") || line.contains("\t#");
    line.bytes().all(|byte| (b' '..=b'~').contains(&byte)) && !line.contains("${") && !commented
}

/// Whether pip reads `line`, a requirement, as one whose options are `hashes`, each a hash.
pub(super) fn reads_requirement(line: &str, hashes: &[&str]) -> bool {
    let words: Vec<&str> = line.split(' ').collect();
    let Some(first) = words.iter().position(|word| word.starts_with('-')) else {
        return false;
    };
    let options = words[first..]
        .iter()
        .map(|word| word.strip_prefix("--hash="));
    plain(line) && options.eq(hashes.iter().map(|hash| Some(*hash)))
}

/// Whether pip reads `line`, an option's, as `option` given `value`.
pub(super) fn reads_option(line: &str, option: &str, value: &str) -> bool {
    plain(line) && shell_words(line).is_some_and(|words| words == [option, value])
}

/// `text` split into words as Python's `shlex.split` splits it, which pip splits options with:
/// at whitespace, outside quotes, `\` keeping the character after it, and within double quotes
/// keeping only a `\` or a `"`; none where a quote is left open.
fn shell_words(text: &str) -> Option<Vec<String>> {
    let (mut words, mut word, mut within) = (Vec::new(), String::new(), false);
    let mut characters = text.chars();
    while let Some(character) = characters.next() {
        match character {
            ' ' | '\t' | '\r' | '\n' => {
                if within {
                    words.push(std::mem::take(&mut word));
                }
                within = false;
                continue;
            }
            '\'' => loop {
                match characters.next()? {
                    '\'' => break,
                    kept => word.push(kept),
                }
            },
            '"' => loop {
                match characters.next()? {
                    '"' => break,
                    '\\' => {
                        let escaped = characters.next()?;
                        if !matches!(escaped, '\\' | '"') {
                            word.push('\\');
                        }
                        word.push(escaped);
                    }
                    kept => word.push(kept),
                }
            },
            '\\' => word.push(characters.next()?),
            kept => word.push(kept),
        }
        within = true;
    }
    if within {
        words.push(word);
    }
    Some(words)
}
