# Security Analysis 14 — rdlt workspace (fourteenth deep review)

**Date:** 2026-08-19
**Scope:** the full workspace at **`421b477b` on `main`** — five new commits (`2a5fb2b1..HEAD`, +756/−171) on top of the round-13 review HEAD: the honest-check conformance clause (certify `clause/c.rs` + testkit `verify_check_refusal` for both roles + census/bin/README wiring + a python hostile-config fixture in `make test`), the loader's un-parenting refusal and re-spelled re-parent refusal, the serve-gate family refactor (`serve/gate.rs`), FIFO-pin hardening, and the ADR's third amendment. Round-13's report file was deleted by the owner, so every round-13 finding is restated here self-contained.
**Method:** six parallel comprehensive subsystem reviews (client/protocol; WAL+lineage+loader; engine data-path; SDK serve+runtime; reference/certify/testkit; facade/CLI/docs). Each lane verified its assigned round-13 findings at the seat and hunted fresh. Full workspace gate: **1006/1006 serial, green**. The load-bearing statuses below were re-verified by hand at their seats.

**Severity scale (unchanged):** High — the stated adversary can crash, hang, or abuse the host trivially, **or a legal input shape silently loses, duplicates, or alters data**. Medium — narrower preconditions or reduced impact; contract violations short of corruption. Low — defense-in-depth, hygiene, pin gaps, availability-only edges. Info — observations and recorded postures.

---

## 1. Executive summary — read this one to the end

**The wave delivered the documentation/conformance half of round 13, and did that half well:** the D7/S5 honest-check clause is thorough (clause, census, bin flag, testkit helpers for both roles, in-process lying-double pins, live-wire pass AND can-fail pins, python wiring into `make test`); the ADR's `validate` record is amendment-closed; the refusal spellings are clean and join-pinned; `serve/gate.rs` is a verified semantics-preserving refactor. 13L4 was **declined with an accurate documented rationale** (all-scalar `Capabilities`, ~1× materialization — re-evaluate on shape change) — a legitimate closure-by-decision.

**But the owner's "all findings fixed" is wrong on the code.** Five of round-13's six code Mediums are **byte-identical at their seats** — `reference/source/connector.rs:93` still does the ungated whole-file `tokio::fs::read` (13M1); `append_receipt` still writes past its own read ceiling (13M2); `shred/types.rs:118` still admits the 64th struct level the lowering walk refuses (13M3); the shred's quadratic seats are untouched (13M5 — plus a **third sibling seat** found this round in `infer.rs`); the cursor term is still named-not-bounded (13M6) and the streams emit still ungated (13M7). The loader corner (13M4) got its two *easy* halves (re-parent spelling, Some→none refusal) while the shape that actually loses rows — **none→Some after memoization** — is still open, and `lineage.rs`'s "ENFORCED, not merely relied on" claim is now false as written (2 of 3 mutation shapes guarded). The new conformance layer does not reach any of the open code defects: **it drives `check()` only — no clause anywhere drives a Read against a hostile config**, which is exactly why every automated gate stays green while 13M1 stands.

**Eighth consecutive zero-High round** under the standing calibration (same-UID adversary ⇒ SIGKILL-equivalent impact is not escalatory). One new Medium this round (**14M1**, the Write seat's uncapped Arrow column count — the Arrow twin of 13L2, ~300–500 MB transient per rogue frame), six new Lows, and the sharpest arms of the carryovers sharpened further: 13M1's FIFO shape now carries a **process-wide wedge** (512-thread blocking pool vs 1024-read admission), and 13M3's wedge has a second delivery path (the crash-loop shape when the deep table has no segments).

**Nothing found this round is new-in-kind — every open item was named by round 13 at the same seat.** The fix list is therefore fully specified already; what round 14 adds is verification that it has not been executed, plus the conformance-arm shape that would keep it executed.

---

## 2. Round-13 fix verification (self-contained restatements)

| Round-13 | Status | Evidence (current tree, `421b477b`) |
|---|---|---|
| **13M1** reference source reads config-authored path whole (`/dev/zero` → unbounded `tokio::fs::read` → OOM; FIFO → blocking-thread stall; `check()`'s `is_file` never on the read path) | **NOT-FIXED** — top carryover | `source/connector.rs:93-95` byte-identical; config gate still non-empty+has-stem (`config.rs:32-45`); served Read path still never calls check. **Sharpened:** the FIFO arm is process-wide — 512 concurrent parked reads exhaust tokio's default blocking pool, queuing every `tokio::fs` op including `check()` itself; and an honest multi-GiB jsonl self-crashes the same way (legal input) |
| **13M2** receipts store writes what it refuses to read (no append-side size check; read side refuses >8 MiB at `find_receipt` AND `truncate_torn_tail`, which heads every append → all commits refuse FATAL permanently; ~120k honest commits) | **NOT-FIXED** | `store.rs:117-138` unchanged — `writeln!` appends unconditionally; both read-side refusals confirmed at :158 and :278 |
| **13M3** depth doors off by one (Arrow door admits 64 struct levels, lowering walk refuses above 63 → honest depth-64 schema into structs-off destination: WAL-recorded, `Error::internal`, never cleared) | **NOT-FIXED** | `types.rs:117-123` unchanged (`depth > 64` from 0); `lower.rs` diff was doc-only. **Sharpened:** second delivery path — with no segments for the table, recovery succeeds and clears, but re-extraction re-delivers the admitted record → permanent crash loop; boundary still unpinned anywhere |
| **13M4** first-parent-after-memoization (parentless→parented delta passes the guard silently; stale memo; silent-row-loss recovery trace) | **PARTIAL — the losing shape still open** | `loader.rs:241-266` now refuses Some(a)→Some(b) (re-spelled, join-pinned) and Some→none (new) — but both consult `parents.get()` only; the none→Some-after-memo shape inserts with no branch fired and no `has_memo` probe exists on `Chain`. Loss trace re-verified end-to-end (scan.rs:669-682 coverage join vs stale memo). `lineage.rs:37-39` "ENFORCED" claim now false as written |
| **13M5** quadratic shred (observe-time `column_index` linear scan per key per row; build-time `obj_get` linear find per column per row; ~34G compares/seat/push at W=4096, minutes uninterruptible) | **NOT-FIXED — plus a third seat** | `table.rs:236-239` and `slab.rs:148-156` unchanged; no HashMap beside the columns Vec; no scatter at build. **New sibling:** `infer.rs:170-184` — struct-field observation is a linear `fields.iter_mut().find` per key per row; one struct column of 4095 fields buys the same ~68G through seats round 13 did not name. Fold into one remediation |
| **13M6** retained cursor term (4 MiB × ~12× adversarial Value expansion × 1024 reads ≈ 48 GiB, all client-authored) | **NOT-FIXED — declined with a non-sequitur** | Seat unchanged (`serve/source.rs:451-475`); the pool doc's "no count cap can apply to an opaque value" misses the actual lever: a **post-parse node-count walk** bounds the memory dimension without touching cursor semantics. The "~3-5×" claim understates (`[0,0,…]` ≈ 16×) |
| **13M7** streams emit ungated (legal 1024×4.3 MiB declaration → ~4.4 GiB blob built before the 64 MiB encode cap refuses; no admission cap on Streams) | **NOT-FIXED** | `serve/source.rs:376-383` byte-for-byte pre-refactor code |
| 13L1 `_staged-` prefix window (248–255-byte names pass the 255 gate then ENAMETOOLONG, transient class) | **NOT-FIXED** | `part.rs:53-61` unchanged; `store.rs:98` still stages `+8 B` |
| 13L2 Ensure column count uncapped | **NOT-FIXED — sibling added** | No `schema.columns.len()` check (`serve/destination.rs:491-530`); **merge-key count** is the same frame's cheapest expander (~8 MiB → ~2M keys ≈ 80 MB typed); and **14M1** below is the Arrow twin at the Write seat |
| 13L3 `gate_commit_meta` sub-map counts uncapped | **NOT-FIXED — aggravator found** | Moved verbatim to `serve/gate.rs:56-70`, still length-only; ~1M tiny cursor keys ≈ 130 MB transient — and each Publish **plants** ~12× expanded cursor Values into backend-retained state, compounding via fresh keys (feeds the ReadState emit, 14L4) |
| 13L4 capabilities_json raw-byte gate | **DECLINED — documented, accurate** | `handshake.rs:281-286` records the rationale (all-scalar, ~1×, 18 MiB-capped; re-evaluate on shape change); verified factual; also in protocol README:436-441 |
| 13L5 JSONL hostile-shape pins (trailing newline, `\r\n`, empty lines) | **NOT-FIXED** | No pins landed; code-traced again: trailing newline and empty lines refuse typed; `\r\n` lines are ACCEPTED (trailing `\r` is JSON whitespace) — the one behavior that deserves a deliberate pin |
| 13L6 state-document write side unbounded (direct driver) | **NOT-FIXED** | `session.rs:227-232` unchanged |
| 13L7 certify gaps on v1 (version-skew probe; streams-blob framing) | **PARTIAL** | The check clause landed (see 13L11); **no** skew probe and **no** blob-framing arm anywhere (`rogue.rs:250-260` emits only well-formed joins) |
| 13L8 protocol lib.rs "stays 0" crate doc | **NOT-FIXED — now self-contradictory** | `lib.rs:12-16` vs `:76` (`PROTOCOL_VERSION = 1`) in one crate doc |
| 13L9 protocol README "can never skew" decode-cap claim | **NOT-FIXED** | `README.md:384-386` vs `wire.rs:158,167-169` (18 MiB Connector-service cap) |
| 13L10 ADR `validate` spelling | **FIXED** | Third amendment (`docs/adr/0001:558-562`) retires `validate` → `check`, dated; the historical line stands by the ADR's own convention |
| 13L11 / 12L8 D7 check conformance clause | **FIXED — thorough** | `certify/clause/c.rs` (hostile-config second spawn; lying probe FAILs, Fatal PASSES, handshake refusal PASSES, other spawn errors fail); census S5/D7 rows pinned both directions; bin `--hostile-config` with honest skip; testkit helpers both roles; reference in-process + live-wire pins; python fixture in `make test`. Two gaps → 14L2, and the suite still drives `check()` only → 14L1 |

---

## 3. Open Mediums (carried, restated with this round's sharpenings)

**13M1 — the reference source's ungated whole-file read.** `crates/rdlt-connector-reference/src/source/connector.rs:93-95`. One handshake (`{"path":"/dev/zero"}`) + one Read from the stated rogue same-UID client → unbounded buffer growth → OOM of the served connector; a FIFO parks a blocking-pool thread per read and **512 parked reads wedge the entire process's fs layer** (pool 512 < admission 1024); an honest multi-GiB jsonl self-crashes identically. The template every third-party source copies. *Fix:* metadata `is_file` + length ceiling before the read (the store's `gate_store_read` shape), or a bounded streaming reader — plus the conformance arm of 14L1 so the class cannot regress silently. *Severity note:* walks the High line; Medium under the standing same-UID/SIGKILL calibration; High the day connectors run with any privilege separation.

**13M2 — the receipts store self-wedges on honest growth.** `store.rs:117-138` vs `:158`/`:278`/`:228`. ~120k commits (days at 1/s; ~7.6k worst case) cross the 8 MiB read ceiling; `truncate_torn_tail` heads every append, so every later publish refuses FATAL — no adversary needed, no compaction story. *Fix:* refuse the append that would cross, or read growth-tolerant (streaming scan/offset index).

**13M3 — the depth doors disagree by one.** `shred/types.rs:118` admits 64 struct levels; `load/lower.rs:69` accepts 63. Honest depth-64 schema + structs-off destination → `Error::internal` recorded in the WAL and never cleared (or cleared then re-delivered by re-extraction → crash loop). *Fix:* align the door to `>=` (one character) + boundary pins at both seams.

**13M4 — the first-parent-after-memoization corner.** The stale-memo shape (none→Some after a batch memoized root=t) still passes `loader.rs:241-266` with no branch fired; the silent-row-loss trace stands. *Fix:* a `has_memo` probe on `Chain` + refusal in the Delta arm; pin the trace; correct the "ENFORCED" doc (3 of 3 shapes, or soften the claim).

**13M5 — the quadratic shred (now three seats).** `table.rs:236-239` (observe: linear column lookup per key per row), `build.rs:115-116`→`slab.rs:148-156` (build: linear object lookup per column per row), `infer.rs:170-184` (struct fields: linear find per key per row). ≈68G compares ≈ 3–6 minutes of uninterruptible CPU per legal byte-budgeted push, repeatable. *Fix:* a key→slot HashMap beside each Vec (the resolve/name seats' own pattern; the arena parser's duplicate-key dedup already does exactly this at `parse.rs:285-319`).

**13M6 — the retained cursor term.** Post-parse node-count walk (refuse > ~64k nodes) at `serve/source.rs:459`; bounds the 48 GiB product without touching cursor semantics. The "no count cap can apply to an opaque value" rationale addresses a different lever.

**13M7 — the ungated streams emit.** Mirror the client's line gates at `serve/source.rs:376-383` (count ≤1024 before serializing; optionally per-line ceiling) + an admission cap on the Streams RPC.

### New this round

**14M1 — the Write seat's Arrow column count is uncapped: ~300–500 MB transient per rogue frame.** `crates/rdlt-connector-sdk/src/serve/destination.rs:256-309` caps rows at 1M but never `num_columns()`; a ≤64 MiB Write frame whose schema message declares ~1–1.5M tiny flatbuffer fields (~40–60 B each) decodes to ~1.5M `Field`s + arrays before `backend.write`. The Arrow twin of 13L2; same fix vocabulary (a cell cap mirroring the engine's 2²⁸ budget). Rogue-client-authored, one-shot per frame, repeatable — Medium by the 12M1 calibration.

---

## 4. New findings — Low

- **14L1 — no conformance clause drives a Read against a hostile config.** The C-clause drives `check()` only; S-suite reads only the well-configured fixture. The harness already owns the tools to catch the 13M1 class (the 60 s read deadline and retention ceiling at `conformance/source.rs:179,274` — nothing points them at a `/dev/zero`/FIFO/oversized path). This is why every automated gate passes while 13M1 stands. An S6-style hostile-read arm in testkit + certify.
- **14L2 — the C-clause's handshake pass arm cannot distinguish refusal reasons** (`clause/c.rs:65-66` matches any `ClientError::Handshake`): a hostile doc refused for an unrelated reason (typo, malformed) passes S5/D7 vacuously. Verify the refusal names the seat, or document the trust placed in the operator's document.
- **14L3 — the loader's lineage guards fire after `wal.record` and the destination ensure** (`loader.rs:184-214` before the guards at `:241-266`): every refused mutation delta is durable in the manifest, and recovery replays it under last-wins attribution with no guard — the two halves of the rule `lineage.rs` says "must agree forever" deliberately disagree. Move the guards ahead of the record, or document the asymmetry at the refusal seat.
- **14L4 — the ReadState emit serializes backend-retained state ungated** (`serve/destination.rs:626-631`): 13L3's uncapped publishes are the cheap growth lever; the reply builds in full before the 64 MiB encode cap; the 16-deep reply channel multiplies the hold (its own doc concedes "16 such documents, workload-sized").
- **14L5 — the v0→v1 doc-staleness family, four live seats** (beyond the open 13L8/13L9): protocol `README.md:250` ("the RPC protocol stays at `0`"), `Cargo.toml`'s "FROZEN (2026-08-07)" without the 2026-08-19 lift the proto header records, plus the two open carryovers. Contract documentation for foreign integrators; one sweep closes all four.
- **14L6 — the certify README under-documents the S5/D7 wave it documents**: the synopsis omits `--hostile-config`; the never-refusing-skip enumeration at :82 is now incomplete; the module map lacks a `c.rs` row; "Using the library" never mentions the `c` family (an embedder gets no S5/D7 verdict). No falsehoods — the rows themselves are pinned true.
- **14L7 — protocol README:126 says "all ten protocol clauses" — it is 13** (P1–P13; the freeze-era 29 at :22 is accurate as history).

---

## 5. New findings — Info

- **5.1 — 14M1's siblings declined-with-rationale are accurate**: the ~1× comments at `handshake.rs:281-286`, `destination.rs:414-418`, `wire.rs:165` all check out against the typed shapes; the protocol README documents the capabilities decline.
- **5.2 — the serve-gate refactor is clean**: full-diff comparison — the three destination gates moved verbatim, the Fatal/`None` semantics identical, all 12 call sites re-route, none dropped; module private. (The Read-seat count caps predate the refactor, from `2a5fb2b1`.)
- **5.3 — `mark_committed` GC has no post-unlink dir fsync** (`writer.rs:342-370`): unlinked names can reappear after power loss; any reappearing segment is unreferenced and ignored by replay (documented at :362-363). The one WAL mutation without a barrier; benign.
- **5.4 — replay's lowering refusal remains a hard Err** (`replay.rs:108,121`) — unreachable except via the open 13M3, of which it is the delivery mechanism, not a separate defect.
- **5.5 — the hostile hint-flood transient** (one 8 MiB spec line of minimal type-hint keys ≈ 40–50 MB before the 4096 gate refuses) — bounded by the line ceiling and short-circuit; consistent with the admitted bulk-decode posture; the decode doc's phrasing slightly understates this one shape.
- **5.6 — the python hostile-config fixture exercises only the earliest arm** (handshake refusal; python's `Check` is a no-op `Ok` and never driven) — sound for what it pins; the Rust reference's own check- refusal arms carry the real coverage.
- **5.7 — SECURITY.md's "~5–5.5×" figure has no in-tree anchor** — plausible (analytic check ≈5×) but drifts silently as `StreamSpec` evolves; anchor the arithmetic in a comment at the decode seat the way the wave anchored the ~1× replies. All other SECURITY.md numerics re-verified against code this round.
- **5.8 — the child-table memo is a linear Vec scan per child key per row** (`shred/json.rs:322-330`): bounded by 1024 children ≈ 0.5–1 s/push worst legal shape — the same Vec-instead-of-map family; fold into 13M5's remediation.

---

## 6. Recommended fix order

1. **13M1 + 14L1** — the source read gate AND the hostile-read conformance arm that keeps it honest (the round's strongest finding; the clause shape is already written for check — mirror it for read).
2. **13M2** — the receipts write-side gate (honest input bricks the store today).
3. **13M3** — the one-character door alignment + boundary pins (un-wedges honest schemas and closes the crash-loop path).
4. **13M4 + 14L3** — the `has_memo` probe, the first-parent refusal, guards-ahead-of-record, and the loss-trace pin; fix the "ENFORCED" doc.
5. **13M6** — the cursor node-count walk (the named-not-bounded rationale answered with the actual lever).
6. **13M5 + 5.8** — the three-plus-one linear-lookup seats (one HashMap pattern, applied four times).
7. **13M7 + 14M1 + 13L2/13L3 + 14L4** — the count-cap family at the serve seats (emit, Write columns, Ensure columns+merge keys, meta sub-maps, ReadState emit).
8. **13L1, 13L5, 13L6, 13L7, 14L2** — the remaining one-seat Lows.
9. **13L8/13L9 + 14L5/14L6/14L7 + 5.7** — one documentation sweep.

---

## 7. Caveats

- Line numbers are from `421b477b` and will drift.
- **Calibration note:** two lanes independently rated 13M6/13M7 (and the conformance gap) High. This report keeps them Medium under the series' standing rule (same-UID adversary ⇒ SIGKILL-equivalent impact is not escalatory — the rule held 12M4, 13M1 Medium). Re-rate on any privilege separation.
- **Verification confidence:** every NOT-FIXED above was confirmed by reading the seat in the current tree (five of the six Mediums byte-identical to round 13's quotes; 13M4's new guards read line-by-line with the open shape traced); the wave's landed pieces (C-clause, refactor, spellings, ADR) were audited in full. All six lanes ran their suites green; the full gate ran 1006/1006 serial.
- **The series' trajectory:** High 4 → 6 → 3 → 2 → 1 → 0 → 0 → 0 → 0 → 0 → 0 → 0 → 0 → 0. Fourteen rounds in, the open set is entirely specified: seven Mediums (six carried verbatim + one new twin), each with a known-shape fix, none blocking a release under the trust model. The delta this round is execution, not discovery — §2 is the worklist.
- Severity assumes the documented trust model (D-038-1 per `SECURITY.md`; numerics re-verified claim-by-claim this round).
