//! Holds a crate that catches a panic to a build that unwinds: its root must refuse to compile
//! with `panic = "abort"`, under which a caught panic ends the process instead.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use regex::Regex;

use crate::lexer::Scan;
use crate::rules::{FileRole, Finding, Rule};

static CATCHES_PANIC: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\bcatch_unwind\b").expect("a valid pattern"));
static GUARD: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"#\[\s*cfg\s*\([^\]]*\bpanic\s*=[^\]]*\]\s*compile_error\s*!")
        .expect("a valid pattern")
});

/// What the files read so far say about unwinding, by the crate under `crates` they belong to.
#[derive(Debug, Default)]
pub(crate) struct Unwinding {
    /// Each crate's files outside tests that catch a panic, with the line of the first catch.
    catching: BTreeMap<PathBuf, Vec<(PathBuf, usize)>>,
    /// The crates whose roots require unwinding.
    guarded: BTreeSet<PathBuf>,
}

impl Unwinding {
    /// Reads the file at `path`, relative to the repository root.
    pub(crate) fn read(&mut self, path: &Path, role: FileRole, scan: &Scan) {
        let Some(member) = member(path) else {
            return;
        };
        if let Some(caught) = CATCHES_PANIC.find(&scan.code).filter(|_| !role.test) {
            let line = scan.code[..caught.start()].matches('\n').count() + 1;
            let catching = self.catching.entry(member.clone()).or_default();
            catching.push((path.to_path_buf(), line));
        }
        if path == member.join("src/lib.rs") && requires_unwinding(scan) {
            self.guarded.insert(member);
        }
    }

    /// A finding for each file that catches a panic in a crate whose root does not require
    /// unwinding.
    pub(crate) fn findings(self) -> Vec<(PathBuf, Finding)> {
        let unguarded = self
            .catching
            .into_iter()
            .filter(|(member, _)| !self.guarded.contains(member))
            .flat_map(|(_, caught)| caught);
        let finding = |line| Finding {
            line,
            rule: Rule::UnguardedUnwind,
            message: "a crate that catches a panic refuses to build with `panic = \"abort\"`: add \
                      `#[cfg(panic = \"abort\")] compile_error!(..)` to its root"
                .to_owned(),
        };
        unguarded
            .map(|(path, line)| (path, finding(line)))
            .collect()
    }
}

/// Whether a crate's root refuses to build where panics do not unwind: a `compile_error!` under
/// a `cfg` of the panic strategy, outside every module and item.
fn requires_unwinding(scan: &Scan) -> bool {
    GUARD.find_iter(&scan.code).any(|guard| {
        let before = &scan.code[..guard.start()];
        before.matches('{').count() == before.matches('}').count()
    })
}

/// The directory of the workspace member under `crates` that `path` belongs to.
fn member(path: &Path) -> Option<PathBuf> {
    let mut parts = path.components();
    let crates = parts.next().filter(|part| part.as_os_str() == "crates")?;
    Some(Path::new(&crates).join(parts.next()?))
}
