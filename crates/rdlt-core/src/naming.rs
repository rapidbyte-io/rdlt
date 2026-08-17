//! Identifier normalization and collision-safe naming.
//!
//! The invariant these functions defend: **distinct source names never silently merge**.
//! When normalization or flattening maps two different source names to the same
//! destination identifier, the later one gets a deterministic hash suffix derived from
//! its *source* name — so the outcome is stable across runs and independent of input
//! order for the first-seen winner.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Destination identifier constraints (a projection of `destination::Capabilities` ident rules).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentRules {
    /// Maximum identifier byte length (e.g. 63 for Postgres).
    pub max_len: usize,
}

/// The legal range for [`IdentRules::max_len`] (GLM round-5, 5M6/6.3).
/// Below the FLOOR the collision-suffix machinery degenerates: every
/// over-long name normalizes to the same prefix, the candidate space is
/// `16^(max_len-1)`, and the probe loop in `UniqueNamer::name_for` either
/// false-positives or exhausts — a connector-declared `max_len` is
/// untrusted wire input (SECURITY.md), so a tiny one must never reach the
/// namer. Above the CEILING the length is pure bloat (real destinations
/// top out at 255) and per-name work scales with it. 8 admits 16⁷ ≈
/// 2.7×10⁸ candidate suffixes — effectively inexhaustible; 4096 is
/// arbitrary but far past every real engine.
pub const MIN_IDENT_MAX_LEN: usize = 8;
/// See [`MIN_IDENT_MAX_LEN`].
pub const MAX_IDENT_MAX_LEN: usize = 4096;

impl IdentRules {
    /// Validate a rules value that arrived over a trust boundary (the
    /// handshake's `capabilities_json`, the WAL's rules sidecar): the
    /// field is a plain `usize` because serde fills it, and anything
    /// outside [`MIN_IDENT_MAX_LEN`]..=[`MAX_IDENT_MAX_LEN`] is refused
    /// typed rather than allowed to drive the naming probe loop.
    pub fn validate(&self) -> Result<(), String> {
        if !(MIN_IDENT_MAX_LEN..=MAX_IDENT_MAX_LEN).contains(&self.max_len) {
            return Err(format!(
                "identifier max_len {} is outside [{MIN_IDENT_MAX_LEN}, {MAX_IDENT_MAX_LEN}] \
                 — below the floor the collision-suffix space is exhaustible (a host panic \
                 path), above the ceiling is pure per-name bloat; no real destination's \
                 limit leaves this range (postgres 63, snowflake 255)",
                self.max_len
            ));
        }
        Ok(())
    }
}

impl Default for IdentRules {
    fn default() -> Self {
        // Postgres' 63-byte limit is the tightest among v1 destinations; using it
        // everywhere keeps names portable.
        Self { max_len: 63 }
    }
}

/// Hex characters of source hash appended to a truncated identifier.
const HASH_LEN: usize = 8;

/// The hash-suffix sizing for a length bound, spelled ONCE (6.10 — the
/// two callers drifted apart once already, 4L9). The bound is clamped to
/// ≥ 1 first: a zero `max_len` is degenerate (5L13), and "never exceeds
/// the bound" then holds against the clamp, the smallest legal identifier.
fn hash_len_for(max_len: usize) -> usize {
    HASH_LEN.min(max_len.max(1).saturating_sub(1))
}

/// Normalize a source name into a destination-safe identifier: lowercase,
/// `[a-z0-9_]` charset, no leading digit, truncated (with a disambiguating hash
/// suffix) to `rules.max_len` (clamped to ≥ 1 — the smallest legal identifier).
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
    let bound = rules.max_len.max(1);
    if out.len() > bound {
        // Truncation can merge distinct names; the hash of the *source* keeps them apart.
        //
        // The suffix is SIZED TO THE BOUND, not fixed: with a `max_len` below the
        // full suffix, a fixed one produces a name LONGER than the bound the
        // function documents. `max_len` is a public field, so that is reachable
        // by any embedder — the hash simply gets shorter, and collision
        // resistance degrades gracefully instead of the contract breaking.
        // Sliced locally rather than asking `ident_hash` for a shorter digest:
        // that helper clamps to 4..64 for its own callers and must keep doing
        // so, and the clamp is exactly what would push this past the bound.
        let hash_len = hash_len_for(bound);
        out.truncate(bound.saturating_sub(hash_len + 1));
        out.push('_');
        out.push_str(&short_hash(source)[..hash_len]);
    }
    out
}

/// The child-table name for a list-of-objects field.
pub fn child_table_name(parent: &str, field: &str, rules: IdentRules) -> String {
    normalize_ident(&format!("{parent}__{field}"), rules)
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
    /// A namer with nothing claimed yet, normalizing under these rules.
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

    /// The destination name for a source name, normalized and made unique.
    ///
    /// Stable and append-only: the same sequence of source names always yields
    /// the same destination names, which is what lets a schema be rebuilt
    /// identically on a later run. Two distinct source names that normalize to
    /// the same base never merge — the loser gets a hash suffix.
    pub fn name_for(&mut self, source: &str) -> String {
        let base = normalize_ident(source, self.rules);
        match self.taken.get(&base) {
            None => {
                self.taken.insert(base.clone(), source.to_owned());
                base
            }
            Some(owner) if owner == source => base,
            Some(_) => {
                // A repeat collision can only come from an (astronomically
                // unlikely) short-hash clash, and each probe therefore hashes a
                // DIFFERENT input so the candidate genuinely changes.
                //
                // The previous version re-suffixed the CANDIDATE and claimed
                // that "extends deterministically". It does not: `suffixed`
                // truncates to `max_len - SUFFIX_LEN` BEFORE appending, so for
                // any base already at that bound `suffixed(suffixed(x))` equals
                // `suffixed(x)` — the candidate stops changing and the loop
                // spins forever, with no diagnostic and no bound. The trigger
                // is rare enough never to have been observed, which is exactly
                // what makes a silent hang the wrong failure mode.
                //
                // `\u{1}` separates the probe counter from the source name: it
                // cannot occur in a normalized identifier, so probe inputs can
                // never alias a real source name.
                let mut candidate = suffixed(&base, source, self.rules);
                let mut probe = 0usize;
                while let Some(owner) = self.taken.get(&candidate) {
                    if owner == source {
                        return candidate;
                    }
                    probe += 1;
                    // At most `taken.len()` names are occupied, so a correct
                    // probe sequence finds a free one within that many steps.
                    // Exceeding it means the probes stopped being distinct —
                    // a loud failure beats the unbounded spin it replaced.
                    assert!(
                        probe <= self.taken.len() + 1,
                        "identifier probe exhausted for `{source}`: candidates stopped changing"
                    );
                    candidate = suffixed(&base, &format!("{source}\u{1}{probe}"), self.rules);
                }
                self.taken.insert(candidate.clone(), source.to_owned());
                candidate
            }
        }
    }
}

fn suffixed(base: &str, source: &str, rules: IdentRules) -> String {
    // The hash slice is SIZED TO THE BOUND, exactly like `normalize_ident`'s
    // (4L9 — it used to be fixed at `_` + 8 hex, producing 9+ character
    // names for any `max_len` ≤ 9, breaking the documented contract at the
    // small bounds only an embedder can set). Degradation is graceful: a
    // shorter hash buys weaker collision resistance, never an over-bound
    // name. The sizing is [`hash_len_for`]'s, shared so the two cannot
    // drift apart again (6.10).
    let bound = rules.max_len.max(1);
    let hash_len = hash_len_for(bound);
    let mut out = base.to_owned();
    out.truncate(bound.saturating_sub(hash_len + 1).min(out.len()));
    out.push('_');
    out.push_str(&short_hash(source)[..hash_len]);
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

    /// Colliding sources must each get a DISTINCT name, and getting there must
    /// terminate.
    ///
    /// Three sources are the minimum that exercises the probe loop: the first
    /// takes the base, the second takes the suffixed form, and the third finds
    /// the suffixed form already owned by someone else and has to probe. A
    /// `suffixed` that returns a constant makes every probe identical — which
    /// used to spin forever and now trips the probe bound instead.
    #[test]
    fn colliding_sources_get_distinct_names_and_terminate() {
        let rules = IdentRules { max_len: 20 };
        let mut namer = UniqueNamer::new(rules);
        // All three normalize to the same base.
        let names: Vec<String> = ["a b", "a-b", "a.b"]
            .iter()
            .map(|s| namer.name_for(s))
            .collect();

        let unique: std::collections::BTreeSet<&String> = names.iter().collect();
        assert_eq!(
            unique.len(),
            3,
            "distinct sources must not merge: {names:?}"
        );
        for name in &names {
            assert!(name.len() <= rules.max_len, "within the bound: {name}");
            assert!(!name.is_empty(), "a name is never empty");
        }
        // Stable: asking again for the same source returns the same name.
        assert_eq!(namer.name_for("a-b"), names[1], "append-only and stable");
    }

    /// The same, at a bound tight enough that every base is TRUNCATED — the
    /// regime where the old probe was idempotent and looped forever.
    #[test]
    fn truncating_bound_still_terminates_and_stays_distinct() {
        let rules = IdentRules { max_len: 12 };
        let mut namer = UniqueNamer::new(rules);
        let sources: Vec<String> = (0..24)
            .map(|i| format!("a very long column name {i}"))
            .collect();
        let names: Vec<String> = sources.iter().map(|s| namer.name_for(s)).collect();

        let unique: std::collections::BTreeSet<&String> = names.iter().collect();
        assert_eq!(unique.len(), sources.len(), "no two sources share a name");
        for name in &names {
            assert!(name.len() <= rules.max_len, "{name} exceeds max_len");
        }
    }

    /// At a bound roomy enough for the full hash, `suffixed` fills the
    /// bound exactly: an underscore plus `HASH_LEN` hex characters.
    #[test]
    fn a_roomy_bound_gets_the_full_suffix() {
        let rules = IdentRules { max_len: 32 };
        let out = suffixed("a_base_name_long_enough_to_truncate", "src", rules);
        assert_eq!(
            out.len(),
            rules.max_len,
            "a truncated name fills the bound exactly"
        );
        let (_, hash) = out.rsplit_once('_').expect("suffix separator");
        assert_eq!(hash.len(), HASH_LEN, "the hash is HASH_LEN hex chars");
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

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
        // Mutation-report closure: assert the VALUE, not just stability —
        // uppercase must lowercase (not fall through to `_`).
        assert_eq!(first, "user_name");
    }

    #[test]
    fn truncation_boundary_is_exact_max_len() {
        // Mutation-report closure: exactly max_len stays untouched; one over
        // truncates with a hash suffix.
        let rules = IdentRules { max_len: 10 };
        assert_eq!(normalize_ident("abcdefghij", rules), "abcdefghij");
        let over = normalize_ident("abcdefghijk", rules);
        assert_ne!(over, "abcdefghijk");
        assert!(over.len() <= 10);
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

    /// The documented bound is the bound. `max_len` is a public field, so an
    /// embedder can set one smaller than the hash suffix — where a fixed-width
    /// suffix produced a name LONGER than the limit it was enforcing.
    #[test]
    fn a_normalized_identifier_never_exceeds_its_stated_bound() {
        for max_len in 1..=24usize {
            let rules = IdentRules { max_len };
            for source in [
                "a_very_long_source_column_name_indeed",
                "Ünïcødé Column!! With Spaces",
                "9starts_with_a_digit_and_is_long",
            ] {
                let out = normalize_ident(source, rules);
                assert!(
                    out.len() <= max_len,
                    "`{source}` at max_len={max_len} produced `{out}` ({} chars)",
                    out.len()
                );
                assert!(!out.is_empty(), "a name is never empty");
            }
        }
    }

    /// 4L9: the COLLISION suffix honors the same bound — `suffixed` used
    /// to write a fixed `_` + 8-hex suffix, producing 9+ character names
    /// for any `max_len` ≤ 9. The hash slice is sized to the bound, like
    /// `normalize_ident`'s. (The loop starts at 2: at `max_len` 1 two
    /// colliding sources have exactly one legal name between them, and
    /// the probe bound answers that loudly.)
    #[test]
    fn a_suffixed_identifier_never_exceeds_its_stated_bound() {
        for max_len in 2..=24usize {
            let rules = IdentRules { max_len };
            let mut namer = UniqueNamer::new(rules);
            // Two sources normalizing to one over-long base force the
            // suffix path.
            let first = namer.name_for("a very long colliding source name");
            let second = namer.name_for("a-very-long-colliding-source-name");
            for name in [&first, &second] {
                assert!(
                    name.len() <= max_len,
                    "max_len={max_len} produced `{name}` ({} chars)",
                    name.len()
                );
                assert!(!name.is_empty(), "a name is never empty");
            }
            assert_ne!(first, second, "distinct sources stay distinct");
        }
    }

    /// 5M6: the rules range gate, at its edges — the floor admits 16⁷
    /// candidate suffixes (effectively inexhaustible), the ceiling is
    /// arbitrary but past every real destination.
    #[test]
    fn ident_rules_validate_at_their_range_edges() {
        assert!(IdentRules { max_len: 0 }.validate().is_err());
        assert!(
            IdentRules {
                max_len: MIN_IDENT_MAX_LEN - 1
            }
            .validate()
            .is_err()
        );
        assert!(
            IdentRules {
                max_len: MIN_IDENT_MAX_LEN
            }
            .validate()
            .is_ok()
        );
        assert!(IdentRules { max_len: 63 }.validate().is_ok());
        assert!(
            IdentRules {
                max_len: MAX_IDENT_MAX_LEN
            }
            .validate()
            .is_ok()
        );
        assert!(
            IdentRules {
                max_len: MAX_IDENT_MAX_LEN + 1
            }
            .validate()
            .is_err()
        );
        assert!(
            IdentRules {
                max_len: usize::MAX
            }
            .validate()
            .is_err()
        );
    }

    /// 5L13: a zero `max_len` is degenerate, and both writers clamp it to
    /// the smallest legal identifier rather than producing a name LONGER
    /// than the stated bound.
    #[test]
    fn a_zero_max_len_clamps_to_the_smallest_legal_identifier() {
        let rules = IdentRules { max_len: 0 };
        let out = normalize_ident("anything at all", rules);
        assert_eq!(out, "_", "the clamped bound is 1");
        let mut namer = UniqueNamer::new(rules);
        let first = namer.name_for("a long colliding source name");
        assert_eq!(first, "_");
    }

    /// 6.11: the exhaustion regime directly — tight bounds with many
    /// colliding sources stay distinct AND in-bound, and terminate (the
    /// probe bound is the loud answer when the space genuinely runs out,
    /// which these bounds never reach).
    #[test]
    fn tight_bounds_keep_colliding_sources_distinct_and_terminating() {
        for max_len in 8..=12usize {
            let rules = IdentRules { max_len };
            let mut namer = UniqueNamer::new(rules);
            let sources: Vec<String> = (0..24)
                .map(|i| format!("a very long colliding column name {i}"))
                .collect();
            let names: Vec<String> = sources.iter().map(|s| namer.name_for(s)).collect();
            let unique: std::collections::BTreeSet<&String> = names.iter().collect();
            assert_eq!(unique.len(), sources.len(), "max_len={max_len}: no merges");
            for name in &names {
                assert!(name.len() <= max_len, "max_len={max_len} produced `{name}`");
            }
        }
    }

    /// 6L5: the probe bound's OWN answer, pinned. At `max_len = 2` every
    /// long source truncates to the empty base and the suffixed
    /// candidates are `_` + one hex digit — sixteen of them. The
    /// seventeenth distinct colliding source exhausts the space, and the
    /// loud failure IS the assert (release-active, embedder-reachable
    /// only: the wire seats validate `max_len ≥ 8`, where the space is
    /// 16⁷ and every in-tree namer feeds at most ~4,100 names). This
    /// pin is the "regime pinned" the wave-6 commit claimed.
    #[test]
    #[should_panic(expected = "identifier probe exhausted")]
    fn an_exhausted_candidate_space_panics_loudly_rather_than_spinning() {
        let rules = IdentRules { max_len: 2 };
        let mut namer = UniqueNamer::new(rules);
        for i in 0..18u32 {
            let _ = namer.name_for(&format!("distinct long colliding source {i}"));
        }
        unreachable!("the sixteenth collision must exhaust the one-hex space first");
    }
}
