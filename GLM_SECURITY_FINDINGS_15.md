# Security Analysis 15 — rdlt workspace (fifteenth deep review)

**Date:** 2026-08-20
**Scope:** the full workspace at **`29d456b2` on `main`** — the 069 fix wave (9 commits, `421b477b..HEAD`, +2996/−293 across 41 files) landing the entire round-14 worklist: the streaming reference source (shape gate before open, 8 MiB per-line bound), the growth-tolerant receipts log, the depth-door alignment, the three-shape pre-record lineage guard, the `SlotIndex` shred lookups, the serve count/node/emit caps, the hostile-read conformance clause, and the v1 doc sweep. Reviewed on top of it.
**Method:** six parallel comprehensive subsystem reviews (client/protocol; WAL+lineage+loader; engine shred/data-path; SDK serve+runtime; reference/certify/testkit; facade/CLI/docs). Each lane verified its assigned round-14 findings at the seat and hunted fresh over the new code. Full workspace gate: **1043/1043 serial, green** (up from 1006). The two new Mediums were hand-verified at their seats.

**Severity scale (unchanged):** High — the stated adversary can crash, hang, or abuse the host trivially, **or a legal input shape silently loses, duplicates, or alters data**. Medium — narrower preconditions or reduced impact; contract violations short of corruption. Low — defense-in-depth, hygiene, pin gaps, availability-only edges. Info — observations and recorded postures.

---

## 1. Executive summary

**The entire round-14 worklist landed — the first complete wave since round 11.** All seven carried Mediums plus 14M1 are fixed at their seats, and the risky rewrites were done to a standard worth naming: the streaming source read's cursor law was proven **byte-identical** to the old whole-file law (the `Tail` window is the consumed prefix exactly; cross-build resume holds because `resume` reads its window from the file, not the streaming state; chunk-boundary equivalence pinned end-to-end); the `SlotIndex` coherence question — the silent-data-corruption risk of 13M5's fix — came back **clean across every mutation path** (append-after-miss at all four seats, in-place state replacement leaves slots stable, rollback rebuild is the only removal path, the arena index is build-once over immutable rows); and the lineage guard now runs **before the WAL record** with a pin proving refused deltas never reach the manifest. The hostile-read conformance clause (S6) drives the Read directly with park/flood/lying verdicts — the 14L2 vacuity was caught and fixed inside the wave itself (`29d456b2`).

**Ninth consecutive zero-High round**, and the Medium band collapsed to **two** (from eight open last round) — both found this round, both in the new code's own corners:

1. **15M1 — the 13M5 fix's rollback path is itself quadratic**: under a Discard* schema policy, each discarded column runs `retain` + a wholesale index `rebuilt` (every remaining key re-cloned and re-hashed) + a full `nested_fields` re-derive — per discarded change. K discarded adds ≤ 4096 cost ~8.4 M hash inserts per push with short keys, and with the byte budget spent on long keys the rebuild's clone volume reaches **~128 GiB of memcpy+hash per legal push** — minutes of uninterruptible `spawn_blocking`, repeatable every push.
2. **15M2 — 13M7's residual half: no admission cap on the unary connector RPCs** — `Streams` and `Check` (both roles) invoke the connector's own backend-touching code with no bound; only Read has a semaphore and OpenSession the one-session slot. A rogue same-UID client fires unbounded concurrent unary calls; the per-call work is connector-dependent (probes, pool acquisitions, file handles), and the sdk is the template.

Eight Lows (the emit blob's missing *total* cap, the deliberately-unwalked commit-meta cursor values, the nested-field count, a replay-scan depth ceiling serde imposes below the doors', the receipts scan's linear per-publish cost, one classification wobble, and three doc-arithmetic leftovers in the wave's own new text), and the Infos record postures — including the honest observation that the node-count prose in the 13M6 fix overstates the threat ~10× and understates the residual ~2–3× (the caps themselves are sound).

---

## 2. Round-14 fix verification

| Round-14 | Status | Evidence (current tree, `29d456b2`) |
|---|---|---|
| **13M1** source ungated whole-file read | **Fixed** | `gate_regular_file()` before every open (read AND check share it — agreement by construction); line-streaming `BufReader` with the 8 MiB per-line bound enforced in both arms; `/dev/null`/FIFO/directory pins (FIFO under a fail-fast detached-thread harness); the read is now cancellable (all-async). **Cursor equivalence proven**: the `Tail` window law is byte-identical to the old prefix hash (CRLF `\r` included in lines; whitespace-only lines pushed raw; `consume(newline+1)`/`offset += len+1` aligned); old-build cursors validate against the new read (`resume` hashes straight from the file); 3000-row re-derivation pin |
| **13M2** receipts self-wedge | **Fixed** | Streaming scan (`store.rs:179-215`), one line + 8 KiB buffer in memory regardless of log size; the total ceiling retired with rationale; the per-line bound (8 KiB) enforced on BOTH sides (reader refuses over-bound lines as corruption; the append side refuses to write one) — the log can never hold a line its own scans refuse; the torn-tail window reads only `min(len, 8193)` bytes; past-the-old-ceiling growth pinned (replays AND accepts the next append) |
| **13M3** depth doors off by one | **Fixed** | `types.rs:134` `>=`; boundary pins both ends (63 maps/64 refuses at the door; `the_depth_doors_agree_at_the_boundary` — what the door admits lowers at structs-off); the JSON ingest gate re-confirmed on the same constant and convention; both round-14 wedge paths dead |
| **13M4 + 14L3** first-parent corner; guards after record | **Fixed** | `guard_parent_link` — all three shapes in one match (re-parent, first-parent-after-walk via `Chain::has_memo`, drop), the FIRST statement of `process`, before `wal.record` and the ensure; idempotent same-link passes; `a_refused_mutation_delta_never_reaches_the_manifest` pin; the "ENFORCED" doc now 3-of-3 truthful; `has_memo` atomicity verified (all-of-chain-or-nothing inserts); hostile/pre-fix WALs degrade safely (last-wins, one frozen map) |
| **13M5** quadratic shred seats | **Fixed** — regression found (15M1) | `SlotIndex` (16-entry linear prelude, lazy HashMap, probe meter) at column observation, struct fields, child tables; the build-side object lookup rides the parser's persisted index (≥128-key objects; narrower stay linear, bounded ~127 compares, documented). Complexity pins at every seat (linear probes at R×W shapes). **Coherence: clean** — every mutation path enumerated and verified; arena index build-once over immutable rows; rollback is the one removal path → but see **15M1** |
| **13M6** retained cursor term | **Fixed** | `refuse_dense_cursor` (`gate.rs:209-227`) — iterative walk counting every Value node (keys 1:1 with values), cap 65,536, checked mid-walk, typed FATAL terminal frame; both retained terms bounded (spec ≈ ≤4.4 MB, cursor ≈ 4.5–6.5 MB); 200k-node flood pin + honest-nested-cursor positive pin. Prose nits → Info |
| **13M7** streams emit ungated | **Partial** | `declaration_jsonl` gates count ≤1024 BEFORE the join builds + per-line 8 MiB (mirrors by value, both sides say so); typed refusal arm; empty-blob and boundary pins. **No admission cap on the Streams RPC** → **15M2**; no total-blob cap → 15L1 |
| **14M1** Write Arrow column count | **Fixed** | `MAX_RECORD_BATCH_COLUMNS = 4096` judged on the schema after `try_new`, before any batch pulls; mirrors the engine; width-alone pin (4097 columns, zero rows, backend never sees it); the no-cell-budget divergence documented and sound |
| 13L1 `_staged-` window | **Fixed** | `MAX_PART_NAME_BYTES = 247 = 255 − len("_staged-")`, fatal with the derivation in the refusal; 247/248 boundary pins + end-to-end staged-form pin |
| 13L2 Ensure counts | **Fixed** | columns ≤4096 and merge keys ≤4096 before `ensure_table`, mirroring the engine; over-wide pin. Merge-key arm lacks its own pin (note); nested struct count residual → 15L3 |
| 13L3 meta sub-map counts | **Partial** | `MAX_STATE_SUBMAP_ENTRIES = 256 Ki` on both sub-maps + every key through the identifier ceiling, at Replay AND Publish; 262,145-cursor flood pin. Cursor **values** deliberately unwalked → 15L2 |
| 13L4 capabilities gate | **Declined** (accurate, documented) | Re-verified: all-scalar, ~1×, 18 MiB-capped; README documents the decline |
| 13L5 JSONL pins | **Fixed (with substitution)** | Trailing-newline, empty-interior-line, and `\r\n`-accepted pins landed; the 1024-newline flood refuses one seat earlier at the count gate (separately pinned) — every shape pinned where it actually refuses |
| 13L6 state write side | **Fixed** | Encode-then-refuse-fatal at `MAX_DOCUMENT_BYTES` before persist; both sides pinned |
| 13L7 certify v1 gaps | **Fixed** | P3 protocol-version skew probe (`clause/p.rs:270-311` + wire pins); the streams-blob framing arms (trailing newline refuses, empty line refuses, CR accepted, empty blob = zero) |
| 13L8/13L9/14L5 doc sweep | **Fixed** — three leftovers | lib.rs v1 truth; README per-call-cap truth; Cargo.toml freeze/lift dated; README:250 fixed. Leftovers → 15L7 (clause count 29→35; retired `state_format_versions` field name at :291; certify lib.rs "four families") |
| 13L10 ADR | **Fixed** (round 14) | — |
| 13L11/12L8 D7 clause | **Fixed — extended** | S6 honest-READ clause on top: direct-drive (not `read_all`), 10 s deadline, metered 8 MiB flood ceiling, park/Ok/typed/flood verdicts; certify `source_read` with S6-explicit arms; README/report/census updated |
| 14L1 hostile-read conformance | **Fixed** | As above; wired live on the reference at the directory shape; doubles that park, flood, and lie each fail by name |
| 14L2 vacuous passes | **Fixed** | S5's handshake arm reports S5 only; S6 drives its own spawn with timeout/Ok/Err distinguished; the `streams()`-err pass arm now a documented operator-trust window |
| 14L3 guards after record | **Fixed** | See 13M4 |
| 14L4 ReadState emit | **Fixed** | Serialize-then-refuse at the 8 MiB ceiling, FATAL frame naming the seat; both arms + None pinned |
| 14L6/14L7 certify README; clause count | **Fixed** | Synopsis/skips/module map/library section all carry the c family; "thirteen protocol clauses" |
| Info 5.7 ratio anchor | **Fixed** — anchor's own arithmetic wrong | SECURITY.md 5–6× derived at the decode seat… but the derivation paragraph contradicts itself and the wire (claims "2x" for the minimal spec where layout gives ~8–9×; a nonexistent `Str` variant) → 15L8 |

---

## 3. New findings — Medium

### 15M1 — The rollback path re-introduces the quadratic 13M5 closed: per-discarded-column index rebuild under Discard* policy
**Where:** `crates/rdlt-engine/src/shred/resolve.rs:376` (`enforce_discards` calls `revert_column` per discarded change) → `shred/table.rs:205-224` (prior-None arm: `retain` O(W) + `column_slots.rebuilt` O(W) with `key.clone()` per remaining entry + a full `nested_fields` re-derive) → `shred/slots.rs:83-93` (`rebuilt` re-collects every remaining key into a fresh HashMap).
**Verified by hand:** each discarded AddColumn with prior-None (new this push) removes one column and rebuilds the whole live index. K discarded adds ≤ W=4096 under a DiscardRow/DiscardValue policy ⇒ Σ K·W ≈ 8.4 M hash inserts + 8.4 M String clones per push — ~0.5–2 s with short keys; with the 64 MiB byte budget spent on W=4096 long distinct keys (~16 KB avg), the rebuild clone volume is (W²/2)·avg ≈ **128 GiB of memcpy+hash per legal push** — minutes of uninterruptible `spawn_blocking`, repeatable every push (the rollback empties the table; the identical push re-triggers it). The `rebuilt` doc calls the path "cold" — adversarially it is the hot path. Precondition: an operator-configured Discard* policy plus otherwise-legal input (the rogue source's lever).
**Fix:** batch the rollback — collect the offending keys across the discarded changes, remove in one pass, `rebuilt()` once, re-derive `nested_fields` once; or make `rebuilt` lazy (flag dirty, rebuild on next `slot_of`). Pin: a Discard* wide-add push bounds probes/clone volume linearly.

### 15M2 — No admission cap on the unary connector RPCs: `Streams` and `Check` invoke connector-internal backend work unbounded
**Where:** `serve/source.rs:347` (`check`), `:403-424` (`streams`); `serve/destination.rs:372` (`check`); builders at `source.rs:707-718` / `destination.rs:883-894` set no `concurrency_limit_per_connection` and no accept bound. The only admission seats are Read's `read_admission` semaphore (1024) and OpenSession's one-session slot — verified by grep.
**Verified:** a rogue same-UID client fires unbounded concurrent unary calls over one or many connections (h2 defaults admit it). Per-call memory is now gated (the emit caps), so the lever is **concurrent invocation of the connector's own code** — DB probes, pool acquisitions, file handles — cost connector-dependent, and the sdk is the template every connector copies. Availability-only, trivially driven; Medium under the standing same-UID calibration.
**Fix:** one small shared semaphore (mirroring `read_admission`) acquired around the backend-calling unary RPCs; optionally `concurrency_limit_per_connection` and an accept cap at both builders.

---

## 4. New findings — Low

- **15L1 — No total-blob cap at the declaration emit** (`serve/source.rs:383-399`): 1024 just-under-8 MiB lines are per-item-legal ⇒ ~8 GiB of `lines` plus an equal `join` copy before the 64 MiB encode cap refuses. Connector-authored (defective-connector path) — accumulate the running byte total and refuse past `MAX_FRAME_BYTES`; the blob must fit one frame anyway.
- **15L2 — Commit-meta cursor values still never node-walked** (`gate.rs:94-95`, deliberate): a dense 8 MiB meta parses to ~4 M nodes (~100–190 MB) and is planted into backend state; one-session prevents multiplication; ReadState refuses it on the way out. Run `refuse_dense_cursor` over each cursor value inside `gate_commit_meta`.
- **15L3 — Nested struct field count uncapped at Ensure** (`gate.rs:121-130` recurses names; `refuse_ensure_counts` caps top level only): one struct column with ~300–380 k minimal nested fields inside the 8 MiB doc → ~30–40 MB transient. Count total column nodes in the same walk, held to 4096.
- **15L4 — The replay scan cannot decode schemas as deep as the doors admit** (lane B): serde_json's default recursion limit (128) prices a `WalRecord::Delta`'s `ColumnType::Struct` at ~3–4 JSON levels per struct level — schemas past roughly 32–42 levels (both doors admit 63; the writer serializes without limit) fail `decode_line` → Corrupt → Damaged → clear → re-extraction. Safe direction only; state the effective replay-scanable depth at the door or lower the door beneath the serde limit.
- **15L5 — `find_receipt` is a linear scan from byte 0 on every publish** (`store.rs:179-215`, called per commit): per-commit latency grows linearly with lifetime commits (45 MB/publish at 1 M commits); lifetime IO quadratically. Memoize `(len, scan_offset)` per session and resume for append-only growth, or compact periodically.
- **15L6 — `UnexpectedEof` in the resume window classified transient** (`cursor.rs:111-119`): a truncation between the shape gate and `read_exact` retries once before the fatal shrink refusal lands. Map it fatal.
- **15L7 — The doc sweep's three leftovers**: protocol README:22 "29 conformance clauses" (the table has 35); README:291 names the retired `state_format_versions` field (the wire field is `state_format_versions_json`); certify lib.rs:15-16 "four clause families" (five since c). Plus the `--hostile-config` CLI help describing only D7/S5 when the flag now also drives S6.
- **15L8 — The new typed-expansion anchor's arithmetic is wrong** (`client source.rs:156-172`): claims the minimal spec "lands near 2x" where layout gives ~8–9×, quotes a nonexistent `Str` serde variant, and inverts its own maximizer — the count-gate rationale is unaffected (the reply-level cap bounds minimal-spec floods regardless), but the paragraph whose stated purpose is that the number *not drift* drifts on arrival. Re-derive against the real serde spelling.
- **15L9 — The client crate's Cargo.toml still asserts the freeze** the protocol crate's manifest now dates as lifted ("frozen 2026-08-07 and re-opened 2026-08-19") — mirror the dated sentence.

---

## 5. New findings — Info

- **5.1 — All-wide-objects push roughly doubles the value budget's documented peak**: ~125 K objects of exactly 128 keys retain ~0.9–1 GB of persisted indexes atop the ~0.9–1.4 GB arena (per-push, dropped after resolve); the "same order" claim holds at 2×.
- **5.2 — A test bypasses the index** (`json.rs:675` writes `child_tables` directly): benign today (linear fallback answers correctly); the pattern is one `pub(crate)` field away from a silent desync.
- **5.3 — `REPLY_CHANNEL_BUDGET`'s doc understates its own product 4×**: 16 × 8 MiB = 128 MiB, not "a few tens of megabytes".
- **5.4 — The 13M6 docs' prose**: "tens of millions of nodes" for a legal 4 MiB cursor is ~10× high (honest max ≈ 2.1 M); "a few gigabytes" total retention is honestly ~10–11 GB at saturation — real, and ~5× below the pre-fix 48 GiB, but the caps deserve accurate prose.
- **5.5 — Parse-transient before the cursor walk is scheduler-bounded**, not permit-borne (parse+walk is one synchronous block) — documented at the seat; acceptable.
- **5.6 — Mid-read rewrite posture**: consumed bytes are hash-protected at the next resume; the newline-less-tail TOCTOU is inherent to the documented tailing posture.
- **5.7 — `append_receipt`'s `created` window** (exists-check after the torn-tail cut): a hostile directory writer creating the log in the window skips the creation fsync — durability edge under the documented adversary only.
- **5.8 — Certify's S6 drains without metering** (the testkit's does): a served source that floods within channel budget then refuses passes certify S6 — defensible division of labor, recorded.
- **5.9 — The python proof connector diverges from the hardened reference** (emits the newline-less final line and checkpoints past it; offset-only cursor without the tail hash; 64 MiB per-line cap): note-only — a separate implementation, untouched by 069.
- **5.10 — The state ceiling is checked after parts persist** (session.rs): a refusal can orphan deterministic, receipt-less parts — invisible to readers, fatal, pre-existing shape.

---

## 6. Recommended fix order

1. **15M1** — batch the rollback rebuild (the only uninterruptible-minutes lever left in the shred).
2. **15M2** — the shared unary-RPC semaphore (one pattern, mirroring `read_admission`).
3. **15L1 + 15L2 + 15L3** — the emit/blob/node-walk completion trio at the serve seats.
4. **15L4** — state (or lower) the replay-scanable depth at the door.
5. **15L5 + 15L6** — the receipt scan resume and the classification one-liner.
6. **15L7 + 15L8 + 15L9 + 5.3/5.4** — the doc-arithmetic sweep (the wave's own new text this time).

---

## 7. Caveats

- Line numbers are from `29d456b2` and will drift.
- **Verification confidence:** the two Mediums hand-verified at their seats (the rollback path's per-change retain+rebuild+re-derive loop; the semaphore grep across both serve files). The streaming-read cursor equivalence and the SlotIndex coherence — this round's two corruption-class risks — were verified in detail by their lanes (state-machine induction, cross-build resume reasoning, every mutation path enumerated) with pins green. All six lanes ran their suites; the full gate ran 1043/1043 serial.
- **The series' trajectory:** High 4 → 6 → 3 → 2 → 1 → 0 → 0 → 0 → 0 → 0 → 0 → 0 → 0 → 0 → 0; open Mediums 8 → 2. The 069 wave is the first since round 11 to land everything asked, and the two new Mediums are the fix's own edges (a cold path that isn't; a cap family one seat short) rather than new families — the convergence the program has been driving toward.
- Severity assumes the documented trust model (D-038-1 per `SECURITY.md`, re-verified claim-by-claim against the post-069 seats).
