//! `cargo xtask lint`'s check that the compiler forbids `unsafe` code outside the audited crate.
//!
//! Every root cargo compiles carries `#![forbid(unsafe_code)]`, which the compiler holds through
//! every module, included file and macro of that root. The audited crate cannot carry it, so it
//! is held to its audited files, which may bring in no other.

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Context as _;
use cargo_metadata::MetadataCommand;
use proc_macro2::{Delimiter, TokenStream, TokenTree};
use syn::ext::IdentExt as _;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use walkdir::WalkDir;

use crate::rules::{Finding, Rule};
use crate::workspaces::{self, MANIFESTS};

/// The crate that holds the workspace's `unsafe` code, relative to the repository root.
pub(crate) const AUDITED_CRATE: &str = "crates/rdlt-adopt";

/// Every file of the audited crate, relative to it.
const AUDITED_FILES: &[&str] = &["Cargo.toml", "src/lib.rs", "src/tests.rs"];

/// Macros that compile another file's contents into the file that calls them.
const INCLUDES: &[&str] = &["include", "include_str", "include_bytes"];

/// Every finding about `unsafe` code in the repository at `root`.
pub(crate) fn check(root: &Path) -> anyhow::Result<Vec<(PathBuf, Finding)>> {
    let mut all = check_tree(root, &target_roots(root)?)?;
    // A workspace that is not listed has roots this check never read.
    for lockfile in workspaces::unlisted(&workspaces::lockfiles(root)?) {
        let message = "a workspace xtask's MANIFESTS does not list";
        all.push((lockfile, finding(1, Rule::UnlistedWorkspace, message)));
    }
    Ok(all)
}

/// The root file of every target cargo compiles in the repository's workspaces, relative to
/// `root`.
pub(crate) fn target_roots(root: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut roots = BTreeSet::new();
    for manifest in MANIFESTS {
        let metadata = MetadataCommand::new()
            .manifest_path(root.join(manifest))
            .no_deps()
            .exec()
            .with_context(|| format!("running cargo metadata on {manifest}"))?;
        for target in metadata
            .packages
            .iter()
            .flat_map(|package| &package.targets)
        {
            let path = target.src_path.as_std_path();
            let relative = path
                .strip_prefix(root)
                .with_context(|| format!("{} is outside the repository", path.display()))?;
            roots.insert(relative.to_path_buf());
        }
    }
    Ok(roots.into_iter().collect())
}

/// Every finding in the tree under `root`, whose targets' root files are `roots`.
pub(crate) fn check_tree(
    root: &Path,
    roots: &[PathBuf],
) -> anyhow::Result<Vec<(PathBuf, Finding)>> {
    let audited = Path::new(AUDITED_CRATE);
    let mut all = Vec::new();
    for path in roots.iter().filter(|path| !path.starts_with(audited)) {
        let file = root.join(path);
        let source =
            fs::read_to_string(&file).with_context(|| format!("reading {}", file.display()))?;
        if !forbids_unsafe(&source) {
            let message = "a target's root forbids unsafe code: `#![forbid(unsafe_code)]`";
            all.push((path.clone(), finding(1, Rule::UnforbiddenUnsafe, message)));
        }
    }
    let dir = root.join(audited);
    if !dir.exists() {
        return Ok(all);
    }
    for entry in WalkDir::new(&dir).sort_by_file_name() {
        let entry = entry.with_context(|| format!("walking {}", dir.display()))?;
        if entry.file_type().is_dir() {
            continue;
        }
        let path = entry.path();
        let relative = path.strip_prefix(root).unwrap_or(path).to_path_buf();
        if !AUDITED_FILES.iter().any(|file| path == dir.join(file)) {
            let message = format!("{AUDITED_CRATE} holds only its audited files");
            all.push((relative, finding(1, Rule::UnauditedFile, message)));
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            let source =
                fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
            let found = inclusions(&source);
            all.extend(found.into_iter().map(|found| (relative.clone(), found)));
        }
    }
    Ok(all)
}

fn finding(line: usize, rule: Rule, message: impl Into<String>) -> Finding {
    Finding {
        line,
        rule,
        message: message.into(),
    }
}

/// The attributes of a crate: the inner attributes its root file opens with.
struct CrateAttributes(Vec<syn::Attribute>);

impl Parse for CrateAttributes {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let attributes = input.call(syn::Attribute::parse_inner)?;
        input.parse::<TokenStream>()?;
        Ok(Self(attributes))
    }
}

/// Whether the crate whose root file is `source` forbids unsafe code, as the compiler reads it.
fn forbids_unsafe(source: &str) -> bool {
    let Ok(CrateAttributes(attributes)) = syn::parse_str(source) else {
        return false;
    };
    attributes.iter().any(|attribute| {
        let lints = Punctuated::<syn::Path, syn::Token![,]>::parse_terminated;
        attribute.path().is_ident("forbid")
            && attribute
                .parse_args_with(lints)
                .is_ok_and(|lints| lints.iter().any(|lint| lint.is_ident("unsafe_code")))
    })
}

/// Every place the audited file `source` would compile another file's contents.
fn inclusions(source: &str) -> Vec<Finding> {
    let mut findings = Vec::new();
    match source.parse::<TokenStream>() {
        Ok(tokens) => find_inclusions(tokens, false, &mut findings),
        Err(error) => {
            let message = format!("an audited file is Rust source: {error}");
            findings.push(finding(1, Rule::IncludedCode, message));
        }
    }
    findings
}

fn find_inclusions(tokens: TokenStream, in_attribute: bool, findings: &mut Vec<Finding>) {
    let mut after_hash = false;
    let mut tokens = tokens.into_iter().peekable();
    while let Some(token) = tokens.next() {
        let hash = punct(Some(&token), '#') || (after_hash && punct(Some(&token), '!'));
        match token {
            TokenTree::Group(group) => {
                let attribute = after_hash && group.delimiter() == Delimiter::Bracket;
                find_inclusions(group.stream(), in_attribute || attribute, findings);
            }
            TokenTree::Ident(ident) => {
                let name = ident.unraw();
                let included = INCLUDES.iter().any(|include| name == include);
                let path = in_attribute && name == "path" && punct(tokens.peek(), '=');
                if included || path {
                    let line = ident.span().start().line;
                    let message =
                        "audited code is in its audited files: no `include!`, no `#[path]`";
                    findings.push(finding(line, Rule::IncludedCode, message));
                }
            }
            TokenTree::Punct(_) | TokenTree::Literal(_) => {}
        }
        after_hash = hash;
    }
}

/// Whether `token` is the punctuation character `c`.
fn punct(token: Option<&TokenTree>, c: char) -> bool {
    matches!(token, Some(TokenTree::Punct(punct)) if punct.as_char() == c)
}
