//! The requirements file an install is given, read as rdlt's own grammar and written again as the
//! file pip reads, so that nothing in it can have pip build a package from its source.
//!
//! A line names a wheel by its version, `name[extras]==version`, or by its URL, `name @ URL`
//! ending in `.whl`, with its hash, `--hash=sha256:...` (or `sha384`, `sha512`), and markers where
//! it needs them; or it names where packages are found: `--index-url`, `--extra-index-url`,
//! `--find-links` or `--no-index`. Lines join and comments end as pip has them. The file holds
//! printable ASCII, spaces and tabs alone, its lines ending in `\n` or `\r\n`, so it breaks into
//! lines as pip breaks it. Anything else is refused, naming its line, and no variable is
//! expanded; so are `--trusted-host`, which would have pip trust a host without checking its
//! certificate, and `--pre`. What pip reads is written from the parts read, each in its own
//! grammar, and each line written is seen to read, as pip reads it, as the line checked.

mod markers;
mod pip;
#[cfg(test)]
mod tests;
mod version;

use std::fmt;
use std::path::{Path, PathBuf};

use super::wheel::Wheel;
use super::{InstallError, blocking, io_error};

/// The options that name where packages are found, each with its value, as pip reads them.
const PLACES: [(&str, &str); 5] = [
    ("--index-url", "--index-url"),
    ("-i", "--index-url"),
    ("--extra-index-url", "--extra-index-url"),
    ("--find-links", "--find-links"),
    ("-f", "--find-links"),
];

/// The schemes a wheel's URL may have.
const SCHEMES: [&str; 3] = ["https://", "http://", "file:"];

/// The characters of a URL besides ASCII letters and digits, as RFC 3986 has them.
const URL: &[u8] = b"-._~:/?#[]@!$&'()*+,;=%";

/// Each algorithm a hash may name, and how many hex digits its digest has.
const DIGESTS: [(&str, usize); 3] = [("sha256", 64), ("sha384", 96), ("sha512", 128)];

/// Why a requirements file is refused.
#[derive(Debug, PartialEq)]
pub(crate) enum Refused {
    /// A line the grammar does not take, and why.
    Line(&'static str),
    /// A character no line takes, which pip could read as the end of a line.
    Character(char),
}

impl fmt::Display for Refused {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Line(why) => formatter.write_str(why),
            Self::Character(character) => write!(
                formatter,
                "it holds {character:?}, which is no printable ASCII, space or tab"
            ),
        }
    }
}

/// The requirements file at `path`, each of its lines checked, as the lines pip is given.
pub(super) async fn read(path: &Path) -> Result<Vec<String>, InstallError> {
    let path = path.to_owned();
    blocking(move || read_now(&path)).await
}

/// [`read`], on a thread that may block.
fn read_now(path: &Path) -> Result<Vec<String>, InstallError> {
    let absolute =
        std::path::absolute(path).map_err(io_error(format!("resolving {}", path.display())))?;
    let text = std::fs::read_to_string(&absolute)
        .map_err(io_error(format!("reading {}", path.display())))?;
    let directory = absolute.parent().unwrap_or(Path::new("/"));
    checked(&text, directory).map_err(|(line, why)| InstallError::Requirements {
        path: path.to_owned(),
        line,
        why,
    })
}

/// The line that names `wheel`, a copy of the operator's `named`, to pip by its URL and its hash,
/// written as a checked line is.
pub(super) fn wheel_line(wheel: &Wheel, named: &Path) -> Result<String, InstallError> {
    let hash = format!("--hash=sha256:{}", wheel.sha256);
    requirement(&[wheel.name.as_str(), "@", wheel.uri.as_str(), &hash]).map_err(|why| {
        InstallError::Wheel {
            path: named.to_owned(),
            why: why.to_string(),
        }
    })
}

/// The lines of `text`, a requirements file in `directory`, as pip is given them; the number of
/// the first line refused, and why, otherwise.
fn checked(text: &str, directory: &Path) -> Result<Vec<String>, (usize, Refused)> {
    let text = characters(text)?;
    let mut lines = Vec::new();
    for (number, line) in logical(text) {
        if line.contains("${") {
            let why = "it names a variable, which is not expanded";
            return Err((number, Refused::Line(why)));
        }
        let tokens: Vec<&str> = line.split_whitespace().collect();
        let checked = if tokens[0].starts_with('-') {
            place(&tokens, directory)
        } else {
            requirement(&tokens)
        };
        lines.push(checked.map_err(|why| (number, why))?);
    }
    Ok(lines)
}

/// `text` without the byte-order mark it may start with, once it holds printable ASCII, spaces,
/// tabs and line ends alone, a `\r` only before a `\n`: no other character could end a line for
/// pip and not for rdlt.
fn characters(text: &str) -> Result<&str, (usize, Refused)> {
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let mut line = 1;
    let mut characters = text.chars().peekable();
    while let Some(character) = characters.next() {
        let taken = match character {
            ' '..='~' | '\t' => true,
            '\n' => {
                line += 1;
                true
            }
            '\r' => characters.peek() == Some(&'\n'),
            _ => false,
        };
        if !taken {
            return Err((line, Refused::Character(character)));
        }
    }
    Ok(text)
}

/// The lines of `text` as pip reads them, each with the number of its first line: a line ending in
/// `\` joined with the next, unless it is a comment, comments dropped, and blank lines left out.
fn logical(text: &str) -> Vec<(usize, String)> {
    let (mut lines, mut joined, mut first) = (Vec::new(), String::new(), None);
    for (index, line) in text.lines().enumerate() {
        let comment = line.trim_start().starts_with('#');
        if let Some(head) = line.strip_suffix('\\').filter(|_| !comment) {
            first.get_or_insert(index + 1);
            joined.push_str(head);
            continue;
        }
        joined.push(' ');
        joined.push_str(line);
        let number = first.take().unwrap_or(index + 1);
        let kept = uncommented(&std::mem::take(&mut joined)).trim().to_owned();
        if !kept.is_empty() {
            lines.push((number, kept));
        }
    }
    let kept = uncommented(&joined).trim().to_owned();
    if let Some(number) = first.filter(|_| !kept.is_empty()) {
        lines.push((number, kept));
    }
    lines
}

/// `line` up to its comment: a `#` that starts it or follows whitespace.
fn uncommented(line: &str) -> &str {
    let mut previous = ' ';
    for (at, c) in line.char_indices() {
        if c == '#' && previous.is_whitespace() {
            return &line[..at];
        }
        previous = c;
    }
    line
}

/// An option line naming where packages are found, as pip is given it; a relative path is taken
/// from `directory`, where the file lies.
fn place(tokens: &[&str], directory: &Path) -> Result<String, Refused> {
    let refused = Refused::Line("it gives pip an option an install does not take");
    if tokens == ["--no-index"] {
        return Ok("--no-index".to_owned());
    }
    let named = match tokens {
        [option] => option.split_once('='),
        [option, value] => Some((*option, *value)),
        _ => None,
    };
    let place = named.and_then(|(name, value)| {
        let (_, long) = PLACES.iter().find(|(option, _)| *option == name)?;
        Some((*long, value))
    });
    let Some((long, value)) = place.filter(|(_, value)| !value.is_empty()) else {
        return Err(refused);
    };
    let unreadable = Refused::Line("it names no place pip would read as written");
    let value = if value.contains("://") || value.starts_with("file:") {
        if !value.bytes().all(url) {
            return Err(unreadable);
        }
        value.to_owned()
    } else {
        directory.join(PathBuf::from(value)).display().to_string()
    };
    // pip splits an option's line as a shell does, so the value is quoted.
    let line = format!("{long} '{value}'");
    if !pip::reads_option(&line, long, &value) {
        return Err(unreadable);
    }
    Ok(line)
}

/// A requirement line, a wheel by its version or its URL with its hashes, as pip is given it.
fn requirement(tokens: &[&str]) -> Result<String, Refused> {
    let split = tokens
        .iter()
        .position(|token| token.starts_with('-'))
        .unwrap_or(tokens.len());
    let (named, options) = tokens.split_at(split);
    let mut hashes = Vec::new();
    let mut options = options.iter();
    while let Some(option) = options.next() {
        let hash = match *option {
            "--hash" => options.next().copied(),
            option => option.strip_prefix("--hash="),
        };
        let Some(hash) = hash.filter(|hash| digest(hash)) else {
            let why = "a requirement takes no option but its hash, a SHA-2 digest in hex";
            return Err(Refused::Line(why));
        };
        hashes.push(hash);
    }
    if hashes.is_empty() {
        return Err(Refused::Line("a requirement names no hash"));
    }
    let named = named.join(" ");
    let (head, markers) = match named.split_once(';') {
        Some((head, markers)) => {
            let not_markers = Refused::Line("its markers are none, or not PEP 508's");
            (
                head.trim(),
                Some(markers::marked(markers).ok_or(not_markers)?),
            )
        }
        None => (named.trim(), None),
    };
    let mut line = if let Some((name, url)) = head.split_once('@') {
        let name = with_extras(name.trim())?;
        format!("{name} @ {}", wheel_url(url.trim())?)
    } else {
        let no_version = Refused::Line("it names no wheel by one version");
        let (name, version) = head.split_once("==").ok_or(no_version)?;
        let (name, version) = (with_extras(name.trim())?, version.trim());
        if !version::version(version) {
            return Err(Refused::Line("its version is not PEP 440's"));
        }
        format!("{name}=={version}")
    };
    if let Some(markers) = markers {
        line.push_str(" ; ");
        line.push_str(&markers);
    }
    for hash in &hashes {
        line.push_str(" --hash=");
        line.push_str(hash);
    }
    if !pip::reads_requirement(&line, &hashes) {
        return Err(Refused::Line(
            "pip would read it as other than it was checked",
        ));
    }
    Ok(line)
}

/// `url`, once it is a wheel's: of a scheme pip downloads, in RFC 3986's characters, its path
/// ending in `.whl`.
#[expect(
    clippy::case_sensitive_file_extension_comparisons,
    reason = "pip takes a file for a wheel by its extension in lower case alone"
)]
fn wheel_url(url: &str) -> Result<&str, Refused> {
    let path = url.split(['#', '?']).next().unwrap_or_default();
    let wheel = SCHEMES.iter().any(|scheme| url.starts_with(scheme))
        && path.ends_with(".whl")
        && url.bytes().all(self::url);
    if wheel {
        Ok(url)
    } else {
        let why = "its URL names no wheel, which pip would build from its source";
        Err(Refused::Line(why))
    }
}

/// Whether `byte` may be part of a URL, as RFC 3986 has it.
fn url(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || URL.contains(&byte)
}

/// Whether `hash` is a digest pip checks: an algorithm it takes, and its digest's hex digits.
fn digest(hash: &str) -> bool {
    hash.split_once(':').is_some_and(|(algorithm, hex)| {
        DIGESTS
            .iter()
            .any(|(named, digits)| *named == algorithm && hex.len() == *digits)
            && hex.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

/// `named`, a distribution's name as PEP 508 has it, with its extras where it names them, written
/// again from its parts.
fn with_extras(named: &str) -> Result<String, Refused> {
    let refused = || Refused::Line("it names no distribution");
    let (name, extras): (&str, Vec<&str>) = match named.split_once('[') {
        Some((name, rest)) => {
            let extras = rest.strip_suffix(']').ok_or_else(refused)?;
            (name.trim_end(), extras.split(',').map(str::trim).collect())
        }
        None => (named, Vec::new()),
    };
    let distribution = super::wheel::distribution;
    if !distribution(name) || !extras.iter().all(|extra| distribution(extra)) {
        return Err(refused());
    }
    if extras.is_empty() {
        Ok(name.to_owned())
    } else {
        Ok(format!("{name}[{}]", extras.join(",")))
    }
}
