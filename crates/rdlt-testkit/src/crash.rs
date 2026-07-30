//! Crash-injection tools: named fault points that kill a run at a
//! precise stage. A "crash" in tests = the run aborts; durable state (the shared
//! memory destination and the on-disk WAL) survives into the next run — exactly the
//! process-death model, minus the process.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use rdlt_connector::{
    CommitMeta, CommitReceipt, ConnectorSpec, Destination, DestinationCapabilities,
    DestinationError, LoadSession, OpenCtx, PipelineId, RecordBatch, StateDoc, TableName,
    TableSchema, WriteMode,
};

/// Where to inject the fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultPoint {
    /// Fail the Nth `write` call (1-based) before the batch reaches the inner
    /// destination. Models a crash where the WAL has recorded the batch but the
    /// destination never received it; recovery must replay it from the WAL.
    BeforeWrite(u64),
    /// Fail the Nth `commit` call before the inner destination commits. The
    /// earlier `write` calls have all reached the inner destination, so this
    /// models a crash where the batches are staged but not yet published;
    /// recovery must re-drive the commit.
    BeforeCommit(u64),
    /// Let the Nth `commit` succeed inside, then fail — the receipt is lost.
    /// Models a crash after the destination published but before the caller
    /// learned; recovery must hit idempotence to avoid double-publishing.
    AfterCommit(u64),
}

/// Wraps any destination and injects a single deterministic fault: it fails exactly
/// once at the configured fault point, then behaves like the inner destination. Clone
/// shares the trigger, so "restart" = build a new engine over the same wrapper
/// (already-fired faults don't fire again).
#[derive(Debug, Clone)]
pub struct CrashDestination<D> {
    inner: D,
    fault: FaultPoint,
    writes: Arc<AtomicU64>,
    commits: Arc<AtomicU64>,
    fired: Arc<AtomicU64>,
}

impl<D> CrashDestination<D> {
    pub fn new(inner: D, fault: FaultPoint) -> Self {
        Self {
            inner,
            fault,
            writes: Arc::new(AtomicU64::new(0)),
            commits: Arc::new(AtomicU64::new(0)),
            fired: Arc::new(AtomicU64::new(0)),
        }
    }

    /// How many times the fault actually fired (each fault fires at most once).
    pub fn fired(&self) -> u64 {
        self.fired.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl<D: Destination + Clone> Destination for CrashDestination<D> {
    fn spec(&self) -> ConnectorSpec {
        self.inner.spec()
    }

    fn capabilities(&self) -> DestinationCapabilities {
        self.inner.capabilities()
    }

    async fn open(&self, ctx: OpenCtx) -> Result<Box<dyn LoadSession>, DestinationError> {
        let session = self.inner.open(ctx).await?;
        Ok(Box::new(CrashSession {
            inner: session,
            fault: self.fault,
            writes: Arc::clone(&self.writes),
            commits: Arc::clone(&self.commits),
            fired: Arc::clone(&self.fired),
        }))
    }
}

struct CrashSession {
    inner: Box<dyn LoadSession>,
    fault: FaultPoint,
    writes: Arc<AtomicU64>,
    commits: Arc<AtomicU64>,
    fired: Arc<AtomicU64>,
}

impl CrashSession {
    fn crash(&self, what: &str) -> DestinationError {
        self.fired.fetch_add(1, Ordering::SeqCst);
        DestinationError::fatal(format!("injected crash: {what}"))
    }
}

#[async_trait]
impl LoadSession for CrashSession {
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestinationError> {
        self.inner.ensure_table(schema, mode).await
    }

    async fn write(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<(), DestinationError> {
        let n = self.writes.fetch_add(1, Ordering::SeqCst) + 1;
        if let FaultPoint::BeforeWrite(at) = self.fault
            && n == at
            && self.fired.load(Ordering::SeqCst) == 0
        {
            return Err(self.crash("before write"));
        }
        self.inner.write(table, batch).await
    }

    async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        let n = self.commits.fetch_add(1, Ordering::SeqCst) + 1;
        let unfired = self.fired.load(Ordering::SeqCst) == 0;
        match self.fault {
            FaultPoint::BeforeCommit(at) if n == at && unfired => Err(self.crash("before commit")),
            FaultPoint::AfterCommit(at) if n == at && unfired => {
                // The inner commit SUCCEEDS; the caller never learns.
                self.inner.commit(meta).await?;
                Err(self.crash("after commit (receipt lost)"))
            }
            _ => self.inner.commit(meta).await,
        }
    }

    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError> {
        self.inner.read_state(pipeline).await
    }
}

/// The arming spellings a crash point can be written with.
///
/// Two exist in this workspace, and recognising only the first is one of two
/// traps this scanner had to survive. `crash_point!("name")` is the common form;
/// `crash_at("name")` is a local helper one destination uses at its unit edges,
/// because a crash there has cleanup to do before it propagates — a bare early
/// return would leave a session holding an open transaction and the test would
/// blame the protocol rather than the injection.
///
/// Adding a third spelling means adding it HERE. The vacuity guard in
/// [`assert_registry_is_armed`] is what makes a missing spelling surface as a
/// failure rather than as agreement.
const ARMING_PATTERNS: &[&str] = &["crash_point!(", "crash_at("];

/// Assert that a crash-point registry and the sites armed in its own sources
/// agree, in both the directions a source scan can actually establish.
///
/// `src_dir` is the directory to scan; `registries` are ALL the declared lists
/// that directory's sites belong to, compared against their union.
///
/// The union rather than one list at a time, because several crates declare more
/// than one registry over the same sources — one destination crate has three.
/// Checking a single registry against a scope containing others would report
/// every sibling's points as undeclared, and the shape of that false failure
/// invites exactly the wrong fix: widening the registry being checked.
///
/// # What this checks, and what it deliberately cannot
///
/// Two assertions, each catching a different mistake:
///
/// 1. **Every name armed next to an arming call is declared.** Catches a crash
///    point added to the code and forgotten in the registry — an unswept
///    boundary.
/// 2. **Every declared name appears as a literal somewhere in the sources,
///    outside the declaration.** Catches a registry entry that arms nothing.
///    The exclusion of `decl_file`'s declaration is what breaks the circularity:
///    comparing a constant against itself always agrees.
///
/// **It cannot catch a point deleted from the code AND the registry together.**
/// After such a deletion neither side mentions it, and both assertions above
/// still hold. That case is caught by an independently-recorded site COUNT — see
/// `scan_arming_sites` and the committed per-crate counts that use it. Saying so
/// here matters: a reader could otherwise believe this function alone closes the
/// silent-shrink hole, and it does not.
///
/// # Why two directions rather than set equality
///
/// Set equality was the first design and it does not survive this workspace. Two
/// crash points are armed INDIRECTLY: the `crash_point!` invocation takes a
/// variable, and the name's literal lives at the constructor that supplies it.
/// A scanner demanding the literal sit beside the macro finds fewer names than
/// the registry declares, and the plausible reading of that gap is "the registry
/// is too big" — shrinking it, and silently removing points from a sweep matrix
/// while every assertion passed. The two-direction form accommodates indirect
/// arming without weakening either direction.
///
/// # Limitation, stated because it is load-bearing
///
/// This reads TEXT, so a commented-out arming call counts as a site. That is
/// deliberate: the alternative is parsing Rust to decide what is live, and a
/// scanner that guessed wrong would fail open. A commented-out arming call must
/// be deleted rather than left, and this assertion is what forces that.
pub fn assert_registry_is_armed(src_dir: &std::path::Path, registries: &[&[&str]]) {
    let registry: Vec<&str> = registries.iter().flat_map(|r| r.iter().copied()).collect();
    let armed = scan_arming_sites(src_dir);

    // Vacuity guard: scanning nothing must never read as agreement. Without it a
    // mistyped path, a moved module, or an unrecognised arming spelling produces
    // an empty set that only agrees with an empty registry.
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

    // Direction 2: every declared name is a literal somewhere outside its own
    // declaration. Indirect arming puts that literal at the constructor rather
    // than beside the macro, which is why this is a text search over the crate
    // rather than a lookup in `armed`.
    // A name is ARMED if its literal appears somewhere that is not a registry
    // DECLARATION. Declaration blocks are located and excluded rather than
    // counted, because a registry may be declared inside the scanned tree (every
    // connector) or outside it (the engine declares its list in its test), and a
    // count-based rule silently means different things in those two cases.
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

/// Every string literal under `dir` that is NOT inside a crash-point registry
/// declaration.
///
/// This is what breaks the circularity. A registry compared against itself always
/// agrees; a registry whose every entry must also appear somewhere that is not a
/// declaration cannot. Declaration blocks are recognised by their shape —
/// `pub const NAME: &[&str] = &[ … ];` — and their contents removed from
/// consideration, so a name that exists only in a list arms nothing and says so.
fn literals_outside_declarations(dir: &std::path::Path) -> std::collections::BTreeSet<String> {
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
            // Blank out every declaration block, then harvest what remains.
            let mut remaining = String::with_capacity(text.len());
            let mut rest = text.as_str();
            while let Some(at) = rest.find(DECL) {
                remaining.push_str(&rest[..at]);
                let after = &rest[at + DECL.len()..];
                // The block ends at the first `];` — registry declarations are flat
                // slices of literals, so no nesting can hide the terminator.
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

/// Every crash-point name armed beside an arming call under `dir`, sorted and
/// deduplicated. Sites whose name is a VARIABLE contribute nothing here, because
/// their literal lives elsewhere — see [`assert_registry_is_armed`].
///
/// Exposed separately from the assertion so the scanner's own count can be
/// checked against an independently-derived one. A scanner is the piece of this
/// machinery that fails OPEN when wrong: it finds fewer sites while every
/// assertion still passes, so its count is committed and compared.
pub fn scan_arming_sites(dir: &std::path::Path) -> Vec<String> {
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
                    // The name is the first token after the call opens. When it
                    // is a string literal we can read it; when it is a variable
                    // the site is armed indirectly and its literal lives at the
                    // constructor that supplies it, so nothing is recorded here.
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
