//! The crash-point registry scanner: proof that a declared registry and
//! the sites armed in the sources agree. Three earlier designs were
//! overturned by measurement and each wrong one FAILS OPEN — do not
//! simplify this scanner (set equality, occurrence counting, and
//! one-assertion-per-registry are the recorded wrong designs; the docs
//! below carry the evidence).

/// The arming spellings a crash point can be written with.
///
/// Two exist in this workspace, and recognising only the first is one of
/// two traps this scanner had to survive. `crash_point!("name")` is the
/// common form; `crash_at("name")` is a local helper one destination uses
/// at its unit edges, because a crash there has cleanup to do before it
/// propagates — a bare early return would leave a session holding an open
/// transaction and the test would blame the protocol rather than the
/// injection.
///
/// Adding a third spelling means adding it HERE. The vacuity guard in
/// [`assert_registry_matches_sources`] is what makes a missing spelling
/// surface as a failure rather than as agreement.
const ARMING_PATTERNS: &[&str] = &["crash_point!(", "crash_at("];

/// Assert that a crash-point registry and the sites armed in its own
/// sources agree, in both the directions a source scan can actually
/// establish.
///
/// `src_dir` is the directory to scan; `registries` are ALL the declared
/// lists that directory's sites belong to, compared against their union.
///
/// The union rather than one list at a time, because several crates
/// declare more than one registry over the same sources — one destination
/// crate has three. Checking a single registry against a scope containing
/// others would report every sibling's points as undeclared, and the
/// shape of that false failure invites exactly the wrong fix: widening
/// the registry being checked.
///
/// # What this checks, and what it deliberately cannot
///
/// Two assertions, each catching a different mistake:
///
/// 1. **Every name armed next to an arming call is declared.** Catches a
///    crash point added to the code and forgotten in the registry — an
///    unswept boundary.
/// 2. **Every declared name appears as a literal somewhere in the
///    sources, outside the declaration.** Catches a registry entry that
///    arms nothing. The exclusion of declaration blocks is what breaks
///    the circularity: comparing a constant against itself always agrees.
///
/// **It cannot catch a point deleted from the code AND the registry
/// together.** After such a deletion neither side mentions it, and both
/// assertions above still hold. That case is caught by an
/// independently-recorded site COUNT — see [`armed_crash_points`] and the
/// committed per-crate counts that use it. Saying so here matters: a
/// reader could otherwise believe this function alone closes the
/// silent-shrink hole, and it does not.
///
/// # Why two directions rather than set equality
///
/// Set equality was the first design and it does not survive this
/// workspace. Two crash points are armed INDIRECTLY: the `crash_point!`
/// invocation takes a variable, and the name's literal lives at the
/// constructor that supplies it. A scanner demanding the literal sit
/// beside the macro finds fewer names than the registry declares, and the
/// plausible reading of that gap is "the registry is too big" — shrinking
/// it, and silently removing points from a sweep matrix while every
/// assertion passed. The two-direction form accommodates indirect arming
/// without weakening either direction.
///
/// # Limitation, stated because it is load-bearing
///
/// This reads TEXT, so a commented-out arming call counts as a site. That
/// is deliberate: the alternative is parsing Rust to decide what is live,
/// and a scanner that guessed wrong would fail open. A commented-out
/// arming call must be deleted rather than left, and this assertion is
/// what forces that.
pub fn assert_registry_matches_sources(src_dir: &std::path::Path, registries: &[&[&str]]) {
    let registry: Vec<&str> = registries.iter().flat_map(|r| r.iter().copied()).collect();
    let armed = armed_crash_points(src_dir);

    // Vacuity guard: scanning nothing must never read as agreement.
    // Without it a mistyped path, a moved module, or an unrecognised
    // arming spelling produces an empty set that only agrees with an
    // empty registry.
    assert!(
        !armed.is_empty() || registry.is_empty(),
        "no crash-point sites found under {} while the registry declares {} — \
         either the scan path is wrong or an arming spelling is unrecognised; \
         recognised spellings are {ARMING_PATTERNS:?}",
        src_dir.display(),
        registry.len()
    );

    // Direction 1: armed ⊆ declared.
    let undeclared: Vec<&String> = armed
        .iter()
        .filter(|a| !registry.contains(&a.as_str()))
        .collect();
    assert!(
        undeclared.is_empty(),
        "armed in {} but absent from the registry: {undeclared:?} — an instrumented \
         boundary the sweep will never visit",
        src_dir.display()
    );

    // Direction 2: every declared name is a literal somewhere outside its
    // own declaration. Indirect arming puts that literal at the
    // constructor rather than beside the macro, which is why this is a
    // text search over the crate rather than a lookup in `armed`.
    // A name is ARMED if its literal appears somewhere that is not a
    // registry DECLARATION. Declaration blocks are located and excluded
    // rather than counted, because a registry may be declared inside the
    // scanned tree (every connector) or outside it (the engine declares
    // its list in its test), and a count-based rule silently means
    // different things in those two cases.
    let armed_literals = literals_outside_declarations(src_dir);
    let dead: Vec<&&str> = registry
        .iter()
        .filter(|name| !armed_literals.contains(**name))
        .collect();
    assert!(
        dead.is_empty(),
        "declared in the registry but armed nowhere in {}: {dead:?} — a name the \
         sweep iterates that no code can ever fire. Each declared name must appear \
         as a literal at least twice: once in the declaration, once where it arms.",
        src_dir.display()
    );
}

/// Every string literal under `dir` that is NOT inside a crash-point
/// registry declaration.
///
/// This is what breaks the circularity. A registry compared against
/// itself always agrees; a registry whose every entry must also appear
/// somewhere that is not a declaration cannot. Declaration blocks are
/// recognised by their SHAPE — the marker below matches
/// `pub const NAME: &[&str] = &[ … ];` as rustfmt renders it — and their
/// contents removed from consideration, so a name that exists only in a
/// list arms nothing and says so.
fn literals_outside_declarations(dir: &std::path::Path) -> std::collections::BTreeSet<String> {
    // SHAPE-SENSITIVE: this marker is the declaration's rustfmt spelling.
    // A reformat that breaks `= &[` onto its own line (or a declaration
    // written through a type alias) would stop matching here, the
    // declaration's own literals would then count as arming sites, and
    // direction 2 would degrade to always-true for that registry — a
    // FAIL-OPEN, not a false failure. The scanner-selfcheck's committed
    // per-crate counts are the net that catches the degradation.
    const DECL: &str = ": &[&str] = &[";
    let mut out = std::collections::BTreeSet::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        let entries =
            std::fs::read_dir(&next).unwrap_or_else(|e| panic!("scanning {}: {e}", next.display()));
        for entry in entries {
            let path = entry.expect("directory entry").path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().is_none_or(|e| e != "rs") {
                continue;
            }
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
            // Blank out every declaration block, then harvest what
            // remains.
            let mut remaining = String::with_capacity(text.len());
            let mut rest = text.as_str();
            while let Some(at) = rest.find(DECL) {
                remaining.push_str(&rest[..at]);
                let after = &rest[at + DECL.len()..];
                // The block ends at the first `];` — registry declarations
                // are flat slices of literals, so no nesting can hide the
                // terminator.
                match after.find("];") {
                    Some(end) => rest = &after[end + 2..],
                    None => {
                        rest = "";
                        break;
                    }
                }
            }
            remaining.push_str(rest);
            let mut chars = remaining.as_str();
            while let Some(open) = chars.find('"') {
                let after = &chars[open + 1..];
                let Some(close) = after.find('"') else { break };
                out.insert(after[..close].to_owned());
                chars = &after[close + 1..];
            }
        }
    }
    out
}

/// Every crash-point name armed beside an arming call under `dir`, sorted
/// and deduplicated. Sites whose name is a VARIABLE contribute nothing
/// here, because their literal lives elsewhere — see
/// [`assert_registry_matches_sources`].
///
/// Exposed separately from the assertion so the scanner's own count can
/// be checked against an independently-derived one. A scanner is the
/// piece of this machinery that fails OPEN when wrong: it finds fewer
/// sites while every assertion still passes, so its count is committed
/// and compared.
pub fn armed_crash_points(dir: &std::path::Path) -> Vec<String> {
    let mut found: Vec<String> = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        let entries = std::fs::read_dir(&next)
            .unwrap_or_else(|e| panic!("scanning {} for crash points: {e}", next.display()));
        for entry in entries {
            let path = entry.expect("directory entry").path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().is_none_or(|e| e != "rs") {
                continue;
            }
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
            for pattern in ARMING_PATTERNS {
                let mut rest = text.as_str();
                while let Some(at) = rest.find(pattern) {
                    rest = &rest[at + pattern.len()..];
                    // The name is the first token after the call opens.
                    // When it is a string literal we can read it; when it
                    // is a variable the site is armed indirectly and its
                    // literal lives at the constructor that supplies it,
                    // so nothing is recorded here.
                    let head: String = rest
                        .chars()
                        .take_while(|c| *c != ',' && *c != ')')
                        .collect();
                    let Some(open) = head.find('"') else { continue };
                    let after = &head[open + 1..];
                    let Some(close) = after.find('"') else {
                        continue;
                    };
                    found.push(after[..close].to_owned());
                }
            }
        }
    }
    found.sort_unstable();
    found.dedup();
    found
}
