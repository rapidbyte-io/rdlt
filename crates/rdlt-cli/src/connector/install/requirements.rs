//! The requirements file an install is given, read as rdlt's own grammar and written again as the
//! file pip reads, so that nothing in it can have pip build a package from its source.
//!
//! A line names a wheel by its version, `name[extras]==version`, with its hash,
//! `--hash=sha256:...` (or `sha384`, `sha512`), and markers where it needs them; or it names where
//! wheels are found: `--index-url`, `--extra-index-url`, `--find-links` or `--no-index`. A wheel at
//! a URL is named so, by `--find-links` and its version, never as `name @ URL`, which pip would
//! read as a path or build whatever it is. Lines join and comments end as pip has them. The file
//! holds printable ASCII, spaces and tabs alone, its lines ending in `\n` or `\r\n`, so it breaks
//! into lines as pip breaks it. Anything else is refused, naming its line, and no variable is
//! expanded; so are `--trusted-host`, which would have pip trust a host without checking its
//! certificate, and `--pre`. What pip reads is written from the parts read, each in its own
//! grammar: no requirement holds a `/` or `\`, nor ends as an archive's name does, so pip reads
//! none as a path, and each line splits, as pip splits it, into the options checked.

mod markers;
mod pip;
#[cfg(test)]
mod tests;

use std::fmt;
use std::path::{Path, PathBuf};

use super::{InstallError, blocking, io_error};

/// The options that name where packages are found, each with its value, as pip reads them.
const PLACES: [(&str, &str); 5] = [
    ("--index-url", "--index-url"),
    ("-i", "--index-url"),
    ("--extra-index-url", "--extra-index-url"),
    ("--find-links", "--find-links"),
    ("-f", "--find-links"),
];

/// The characters of a URL besides ASCII letters and digits, as RFC 3986 has them.
const URL: &[u8] = b"-._~:/?#[]@!$&'()*+,;=%";

/// Each algorithm a hash may name, and how many hex digits its digest has.
const DIGESTS: [(&str, usize); 3] = [("sha256", 64), ("sha384", 96), ("sha512", 128)];

/// The extensions by which pip takes a name for an archive's, as its `is_archive_file` reads them.
const ARCHIVES: [&str; 12] = [
    ".zip",
    ".whl",
    ".tar.bz2",
    ".tbz",
    ".tar.gz",
    ".tgz",
    ".tar",
    ".tar.xz",
    ".txz",
    ".tlz",
    ".tar.lz",
    ".tar.lzma",
];

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

/// A line of the file pip reads, as rdlt read it.
#[derive(Debug, PartialEq)]
enum Line {
    /// A wheel by its version.
    Pinned(Pinned),
    /// Where wheels are found: the option, written in full, and its value.
    Place { option: &'static str, value: String },
    /// That no index is asked.
    NoIndex,
}

/// A wheel by its version, as rdlt read it.
#[derive(Debug, PartialEq)]
struct Pinned {
    name: String,
    extras: Vec<String>,
    version: String,
    /// Its markers, written again from their grammar.
    markers: Option<String>,
    /// Each hash, `algorithm:hex`.
    hashes: Vec<String>,
}

impl Line {
    /// The line as pip is given it.
    fn written(&self) -> String {
        match self {
            Self::Pinned(pinned) => pinned.written(),
            // pip splits an option's line as a shell does, so the value is quoted.
            Self::Place { option, value } => format!("{option} '{value}'"),
            Self::NoIndex => "--no-index".to_owned(),
        }
    }
}

impl Pinned {
    /// The requirement before its options, as pip is given it.
    fn requirement(&self) -> String {
        let mut written = self.name.clone();
        if !self.extras.is_empty() {
            written.push('[');
            written.push_str(&self.extras.join(","));
            written.push(']');
        }
        written.push_str("==");
        written.push_str(&self.version);
        if let Some(markers) = &self.markers {
            written.push_str(" ; ");
            written.push_str(markers);
        }
        written
    }

    /// The line as pip is given it.
    fn written(&self) -> String {
        let mut written = self.requirement();
        for hash in &self.hashes {
            written.push_str(" --hash=");
            written.push_str(hash);
        }
        written
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

/// The line that names the wheel to pip, by `reference`, its name and `file:` URL, and its
/// `sha256`, the hash of the operator's `named`: no other requirement holds a `/`.
pub(super) fn wheel_line(
    reference: &str,
    sha256: &str,
    named: &Path,
) -> Result<String, InstallError> {
    let hash = format!("sha256:{sha256}");
    let line = format!("{reference} --hash={hash}");
    if digest(&hash) && pip::reads_requirement(&line, &[hash.as_str()]) {
        Ok(line)
    } else {
        Err(InstallError::Wheel {
            path: named.to_owned(),
            why: "pip would read its line as other than it was checked".to_owned(),
        })
    }
}

/// The lines of `text`, a requirements file in `directory`, as pip is given them; the number of
/// the first line refused, and why, otherwise.
fn checked(text: &str, directory: &Path) -> Result<Vec<String>, (usize, Refused)> {
    Ok(parsed(text, directory)?.iter().map(Line::written).collect())
}

/// The lines of `text`, a requirements file in `directory`, as rdlt read them; the number of the
/// first line refused, and why, otherwise.
fn parsed(text: &str, directory: &Path) -> Result<Vec<Line>, (usize, Refused)> {
    let text = characters(text)?;
    let mut lines = Vec::new();
    for (number, line) in logical(text) {
        if line.contains("${") {
            let why = "it names a variable, which is not expanded";
            return Err((number, Refused::Line(why)));
        }
        let tokens: Vec<&str> = line.split_whitespace().collect();
        let read = if tokens[0].starts_with('-') {
            place(&tokens, directory)
        } else {
            requirement(&tokens).map(Line::Pinned)
        };
        lines.push(read.map_err(|why| (number, why))?);
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
fn place(tokens: &[&str], directory: &Path) -> Result<Line, Refused> {
    let refused = Refused::Line("it gives pip an option an install does not take");
    if tokens == ["--no-index"] {
        return Ok(Line::NoIndex);
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
    let line = Line::Place {
        option: long,
        value: value.clone(),
    };
    if pip::reads_option(&line.written(), long, &value) {
        Ok(line)
    } else {
        Err(unreadable)
    }
}

/// A requirement line, a wheel by its version with its hashes.
fn requirement(tokens: &[&str]) -> Result<Pinned, Refused> {
    let split = tokens
        .iter()
        .position(|token| token.starts_with('-'))
        .unwrap_or(tokens.len());
    let (named, options) = tokens.split_at(split);
    let hashes = hashed(options)?;
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
    if head.contains('@') {
        let why = "it names a wheel by a URL: name the URL's directory or index with \
                   `--find-links` or `--index-url`, and the wheel as `name==version` with its hash";
        return Err(Refused::Line(why));
    }
    let no_version = Refused::Line("it names no wheel by one version");
    let (named, version) = head.split_once("==").ok_or(no_version)?;
    let (name, extras) = with_extras(named.trim())?;
    let version = version.trim();
    if !super::version::version(version) {
        return Err(Refused::Line("its version is not PEP 440's"));
    }
    let pinned = Pinned {
        name,
        extras,
        version: version.to_owned(),
        markers,
        hashes,
    };
    let (requirement, line) = (pinned.requirement(), pinned.written());
    let hashes: Vec<&str> = pinned.hashes.iter().map(String::as_str).collect();
    let pathless = [head, requirement.as_str(), line.as_str()]
        .into_iter()
        .all(|text| !text.contains(['/', '\\']) && !archive(text));
    if !pathless || !pip::reads_requirement(&line, &hashes) {
        return Err(Refused::Line(
            "pip would read it as other than it was checked",
        ));
    }
    Ok(pinned)
}

/// The hashes `options`, a requirement's, name, each `algorithm:hex`; why they are none, or other
/// options too, otherwise.
fn hashed(options: &[&str]) -> Result<Vec<String>, Refused> {
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
        hashes.push(hash.to_owned());
    }
    if hashes.is_empty() {
        return Err(Refused::Line("a requirement names no hash"));
    }
    Ok(hashes)
}

/// Whether pip reads `text` as an archive's name: its extension, as pip's `splitext` takes it, a
/// `.tar` before it included, is an archive's, in any case.
fn archive(text: &str) -> bool {
    let (base, extension) = split_extension(text);
    let extension = if base.to_ascii_lowercase().ends_with(".tar") {
        &text[base.len() - ".tar".len()..]
    } else {
        extension
    };
    ARCHIVES.contains(&extension.to_ascii_lowercase().as_str())
}

/// `path` split before its extension as Python's `posixpath.splitext` splits it: at the last `.`
/// of its last segment, unless only dots come before it there.
fn split_extension(path: &str) -> (&str, &str) {
    let segment = path.rfind('/').map_or(0, |slash| slash + 1);
    match path.rfind('.') {
        Some(dot) if dot >= segment && path[segment..dot].bytes().any(|byte| byte != b'.') => {
            path.split_at(dot)
        }
        _ => (path, ""),
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

/// The name and the extras of `named`, a distribution's name as PEP 508 has it, with its extras
/// where it names them.
fn with_extras(named: &str) -> Result<(String, Vec<String>), Refused> {
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
    Ok((
        name.to_owned(),
        extras.into_iter().map(str::to_owned).collect(),
    ))
}
