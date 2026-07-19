//! Identifier normalization and collision-safe naming (design doc §5.3).
//!
//! The invariant these functions defend: **distinct source names never silently merge**.
//! When normalization or flattening maps two different source names to the same
//! destination identifier, the later one gets a deterministic hash suffix derived from
//! its *source* name — so the outcome is stable across runs and independent of input
//! order for the first-seen winner.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Destination identifier constraints (a projection of `DestCapabilities` ident rules).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentRules {
    /// Maximum identifier byte length (e.g. 63 for Postgres).
    pub max_len: usize,
}

impl Default for IdentRules {
    fn default() -> Self {
        // Postgres' 63-byte limit is the tightest among v1 destinations; using it
        // everywhere keeps names portable.
        Self { max_len: 63 }
    }
}

/// Suffix length: `_` + 8 hex chars of the source-name hash.
const SUFFIX_LEN: usize = 9;

/// Normalize a source name into a destination-safe identifier: lowercase,
/// `[a-z0-9_]` charset, no leading digit, truncated (with a disambiguating hash
/// suffix) to `rules.max_len`.
pub fn normalize_ident(source: &str, rules: IdentRules) -> String {
    let mut out = String::with_capacity(source.len());
    for c in source.chars() {
        match c {
            'a'..='z' | '0'..='9' | '_' => out.push(c),
            'A'..='Z' => out.push(c.to_ascii_lowercase()),
            _ => out.push('_'),
        }
    }
    if out.is_empty() {
        out.push('_');
    }
    if out.as_bytes()[0].is_ascii_digit() {
        out.insert(0, '_');
    }
    if out.len() > rules.max_len {
        // Truncation can merge distinct names; the hash of the *source* keeps them apart.
        out.truncate(rules.max_len.saturating_sub(SUFFIX_LEN));
        out.push('_');
        out.push_str(&short_hash(source));
    }
    out
}

/// The child-table name for a list-of-objects field.
pub fn child_table_name(parent: &str, field: &str, rules: IdentRules) -> String {
    normalize_ident(&format!("{parent}__{field}"), rules)
}

/// The flattened column name for a nested object path (destinations without struct
/// support).
pub fn flattened_column_name(path: &[&str], rules: IdentRules) -> String {
    normalize_ident(&path.join("__"), rules)
}

/// Assigns each distinct source name a unique normalized identifier, appending a
/// deterministic hash suffix on collision. First occurrence keeps the plain name;
/// later colliding names get `_<hash8(source)>`.
#[derive(Debug, Default)]
pub struct UniqueNamer {
    rules: IdentRules,
    /// normalized name -> source name that owns it
    taken: HashMap<String, String>,
}

/// Owner marker for reserved names: contains a NUL so no real source name can
/// ever equal it (source names are normalized text).
const RESERVED_OWNER: &str = "\0reserved";

impl UniqueNamer {
    pub fn new(rules: IdentRules) -> Self {
        Self {
            rules,
            taken: HashMap::new(),
        }
    }

    /// Claim `name` so that NO source column may take it verbatim — even a source
    /// column literally named `name` gets a hash suffix. Used for system columns
    /// (`_rdlt_*`): without this, an input field named `_rdlt_id` would alias the
    /// lineage column instead of being suffixed.
    pub fn reserve(&mut self, name: &str) {
        self.taken
            .insert(normalize_ident(name, self.rules), RESERVED_OWNER.to_owned());
    }

    pub fn name_for(&mut self, source: &str) -> String {
        let base = normalize_ident(source, self.rules);
        match self.taken.get(&base) {
            None => {
                self.taken.insert(base.clone(), source.to_owned());
                base
            }
            Some(owner) if owner == source => base,
            Some(_) => {
                let mut candidate = suffixed(&base, source, self.rules);
                // Suffixes are derived from the source name, so a repeat collision can
                // only come from a (astronomically unlikely) short-hash clash; extend
                // deterministically rather than loop forever.
                while let Some(owner) = self.taken.get(&candidate) {
                    if owner == source {
                        return candidate;
                    }
                    candidate = suffixed(&candidate, source, self.rules);
                }
                self.taken.insert(candidate.clone(), source.to_owned());
                candidate
            }
        }
    }
}

fn suffixed(base: &str, source: &str, rules: IdentRules) -> String {
    let mut out = base.to_owned();
    out.truncate(rules.max_len.saturating_sub(SUFFIX_LEN).min(out.len()));
    out.push('_');
    out.push_str(&short_hash(source));
    out
}

fn short_hash(source: &str) -> String {
    ident_hash(source, 8)
}

/// Deterministic short hex digest for building bounded-length identifiers (e.g.
/// destination staging-table names). Stable across runs and machines.
pub fn ident_hash(input: &str, hex_len: usize) -> String {
    blake3::hash(input.as_bytes()).to_hex()[..hex_len.clamp(4, 64)].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distinct_sources_colliding_after_normalize_stay_distinct() {
        let mut namer = UniqueNamer::new(IdentRules::default());
        let a = namer.name_for("user-name");
        let b = namer.name_for("user name");
        let c = namer.name_for("user_name");
        assert_eq!(a, "user_name");
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_ne!(b, c);
    }

    #[test]
    fn same_source_is_stable() {
        let mut namer = UniqueNamer::new(IdentRules::default());
        let first = namer.name_for("User Name");
        let again = namer.name_for("User Name");
        assert_eq!(first, again);
    }

    #[test]
    fn long_names_truncate_within_limit_and_stay_distinct() {
        let rules = IdentRules { max_len: 20 };
        let long_a = "a_very_long_column_name_variant_one";
        let long_b = "a_very_long_column_name_variant_two";
        let a = normalize_ident(long_a, rules);
        let b = normalize_ident(long_b, rules);
        assert!(a.len() <= 20 && b.len() <= 20);
        assert_ne!(a, b);
    }
}
