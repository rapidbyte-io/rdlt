//! `cargo xtask lint`'s check that the compiler forbids `unsafe` code outside the audited crate.
//!
//! Every root cargo compiles carries `#![forbid(unsafe_code)]`, which the compiler holds through
//! every module, included file and macro of that root. The audited crate cannot carry it, so it
//! is held to its audited files, which may bring in no other.

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;
use std::fs;
use std::path::{Component, Path, PathBuf};

use anyhow::Context as _;
use cargo_metadata::MetadataCommand;
use proc_macro2::{Delimiter, TokenStream, TokenTree};
use syn::ext::IdentExt as _;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;

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

/// Every finding in the tokens of the Rust file at `path`, relative to the repository root.
///
/// Outside the audited crate, `unsafe` is reported wherever it is a token, a macro's body
/// included, and so is code brought in from a file the lint does not read. In the audited crate,
/// any file brought in is.
pub(crate) fn check_file(path: &Path, source: &str) -> Vec<Finding> {
    let audited = path.starts_with(AUDITED_CRATE);
    let mut findings = Vec::new();
    match source.parse::<TokenStream>() {
        Ok(tokens) => {
            let held = Held {
                audited,
                in_attribute: false,
            };
            find_tokens(tokens, held, &mut findings);
        }
        Err(error) => {
            let rule = if audited {
                Rule::IncludedCode
            } else {
                Rule::Unsafe
            };
            findings.push(finding(
                1,
                rule,
                format!("a linted file is Rust source: {error}"),
            ));
        }
    }
    findings
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
    for path in workspaces::tracked(root, &[AUDITED_CRATE])? {
        if !AUDITED_FILES.iter().any(|file| path == audited.join(file)) {
            let message = format!("{AUDITED_CRATE} holds only its audited files");
            all.push((path, finding(1, Rule::UnauditedFile, message)));
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

/// What a file's tokens are held to.
#[derive(Clone, Copy)]
struct Held {
    /// The file is the audited crate's: it may hold `unsafe` code, and bring in no other file.
    audited: bool,
    /// The tokens are inside an attribute.
    in_attribute: bool,
}

fn find_tokens(tokens: TokenStream, held: Held, findings: &mut Vec<Finding>) {
    let mut after_hash = false;
    let mut tokens = tokens.into_iter().peekable();
    while let Some(token) = tokens.next() {
        let hash = punct(Some(&token), '#') || (after_hash && punct(Some(&token), '!'));
        match token {
            TokenTree::Group(group) => {
                let attribute = after_hash && group.delimiter() == Delimiter::Bracket;
                let in_attribute = held.in_attribute || attribute;
                find_tokens(
                    group.stream(),
                    Held {
                        in_attribute,
                        ..held
                    },
                    findings,
                );
            }
            TokenTree::Ident(ident) => {
                let name = ident.unraw();
                let line = ident.span().start().line;
                if name == "unsafe" && !held.audited {
                    let message = format!("`unsafe` code lives only in {AUDITED_CRATE}");
                    findings.push(finding(line, Rule::Unsafe, message));
                }
                let included = if held.audited {
                    INCLUDES.iter().any(|banned| name == banned)
                } else {
                    name == "include"
                };
                let assigned = held.in_attribute && name == "path" && punct(tokens.peek(), '=');
                let path = assigned && {
                    tokens.next();
                    held.audited || !tokens.peek().is_some_and(beneath_the_crate)
                };
                if included || path {
                    let message = "code comes from linted Rust files: no `include!`, and \
                                   `#[path]` only to a `.rs` file beneath the crate";
                    findings.push(finding(line, Rule::IncludedCode, message));
                }
            }
            TokenTree::Punct(_) | TokenTree::Literal(_) => {}
        }
        after_hash = hash;
    }
}

/// Whether `token` is a string literal naming a Rust file at or beneath the directory it is
/// resolved from.
fn beneath_the_crate(token: &TokenTree) -> bool {
    let TokenTree::Literal(literal) = token else {
        return false;
    };
    let syn::Lit::Str(path) = syn::Lit::new(literal.clone()) else {
        return false;
    };
    let path = path.value();
    let path = Path::new(&path);
    path.extension().is_some_and(|extension| extension == "rs")
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

/// Whether `token` is the punctuation character `c`.
fn punct(token: Option<&TokenTree>, c: char) -> bool {
    matches!(token, Some(TokenTree::Punct(punct)) if punct.as_char() == c)
}
