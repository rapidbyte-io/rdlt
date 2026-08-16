# Security Analysis 7 — rdlt workspace (seventh deep review — the correctness round)

**Date:** 2026-08-15
**Scope:** same surface as rounds 1–6, reviewed at **`4812a778`** on `047-security-hardening` (working tree clean) — on top of the wave-7 remediation commit (33 files, ~1,600 lines: the segment density rule, the document/cursor ceilings at the client's remaining seats, the row cap at both decode seats, the ctime-bearing fstat compare, and the one-renderer refactors).
**Method:** six parallel subsystem reviews (client/protocol; WAL; engine data-path; SDK serve + runtime; certify/testkit/reference; config surface/CLI/CI). Per this round's widened mandate — "rock solid and bug free" — each lane verified its wave-7 fixes AND ran a correctness audit of its semantics: exactly-once choreography, crash-consistency arrow walks, row accounting, type-lattice behavior, session interleavings, certifier vacuity. Every Medium below was re-verified by hand at its seat. No test execution on the analysis host (standing linker constraint); the commit's own gate record (872/872 serial on the pinned 1.96.0) is the execution authority.

**Severity scale (this round's High explicitly includes silent data corruption):**
- **High** — the stated adversary can crash, hang, or abuse the host trivially, **or a legal input shape silently loses, duplicates, or alters data**.
- **Medium** — narrower preconditions or reduced impact; contract violations short of corruption.
- **Low** — defense-in-depth, hygiene, pin gaps, availability-only edges.
- **Info** — observations and recorded postures.

---

## 1. Executive summary

**Every wave-7 finding is verified fixed** — 6H1's density rule closes the sparse lie (the reviewing agent probed `st_blocks` behavior on btrfs/tmpfs directly and constructed the max hostile input that passes all three rules: disk→memory amplification is capped at exactly ×4 by design); 6M1–6M4 and all ten Lows landed with pins; the refactors hold (one renderer for JSON parse errors on both wire sides, one bounded `panic_text` for all four Arrow-decode belts, one identifier constant shared by wire gate and engine mirror).

**This is the first round with zero High findings** (trajectory across rounds: 4 → 6 → 3 → 2 → 1 → 0). What the widened correctness lens found instead clusters in three places:

1. **The exactly-once token is trusted blindly.** `Session::commit` treats ANY receipt returned by `existing_receipt` as proof that this exact `(load_id, commit_seq)` already published — it never checks the receipt's own identity fields. A destination with a buggy receipt lookup (stale cache, wrong key — squarely inside the project's "rogue **or buggy** connector" adversary) turns every subsequent commit into a silent no-op: data never published, WAL marked committed, segments reclaimed, run reports success. One guard closes it (7M1).
2. **The losslessness contract has two leaky edges.** The JSONL inference path escalates a Float64 column to text when an integer beyond ±2^53 appears ("losslessness is enforced at runtime, never assumed" — pinned). The **hint-pinned** builder arm casts the same value with a bare `as f64` and silently rounds it (7M5), and the structured passthrough's cross-batch Int64→Float64 widening does the same via arrow's `cast` — while its own module doc claims values are "cast LOSSLESSLY" (7M6). Same lattice join, two paths, two answers; the wrong one is silent.
3. **The ceiling families have mirror-image seats on the other side of the wire.** Waves 6–7 capped every *client* decode seat; the **serve** session seats (`commit_meta_json` — a typed shell around untyped cursors, the exact 6M1 shape — plus replay/ensure/read payloads) and the **runtime's spec probe** still parse up to 64 MiB frames into untyped Values with no gate and verbatim serde echo (7M2, 7M4), and the serve-side inbound identifiers (ensured table names, session-long retention) carry no length cap (7M3).

The WAL crash-consistency walk came back **clean**: the agent traced all six crash arrows (after segment write / manifest line / dir fsync / destination commit / Committed line / GC) and found no duplication or loss window beyond the documented D3 redelivery window — which is precisely why 7M1 matters: that window's safety now rests entirely on an unchecked token. The certifier vacuity sweep also came back clean (no judging inversions, no default-pass on error paths, no way for a contract-violating connector to pass P-clauses).

Ten Lows include one that breaks `make docs` outright (a stale intra-doc link from the wave-7 rename, caught only outside PR CI) and one genuine availability edge on ext4 (the density rule can refuse *honest unsynced* segments on delayed-allocation filesystems — safe direction, host-dependent, and the fix's own doc misstates the write order).

---

## 2. Wave-7 fix verification

| Round-6 | Status | Evidence (current tree, `4812a778`) |
|---|---|---|
| 6H1 sparse-segment density | **Fixed** | `extents_within_file` third rule: `Σ extents ≤ 4 × st_blocks×512` (`replay.rs:200-250`); `st_blocks` semantics reasoned across ext4/xfs/tmpfs/NFS/reflink/btrfs-compression and probed live; honest writer verified dense (no set_len/fallocate outside the pin); max hostile input passing all three rules caps amplification at exactly ×4 (constructed and measured); sparse pin robust on tmpfs. **Premise doc inaccuracy** → 7L2 |
| 6M4 ctime compare | **Fixed** | `(mtime, mtime_nsec, ctime, ctime_nsec)` before/after (`replay.rs:113-121, 164-178`); unix-wide `MetadataExt`, no portability regression; residual doc states exactly what survives (a complete in-window swap, which arrow's footer verification does NOT catch) |
| 6M1 state-doc gates | **Fixed** | raw 8 MiB gate before parse + per-cursor serialized-form loop (`client/destination.rs:529-552`); wire-pinned (inflate + no-token-echo). Parse transient honestly ~16× not ~4× (Info) |
| 6M2 spec_json gate | **Fixed** | 8 MiB before parse at the handshake (`client/handshake.rs:278-285`), pinned. **The runtime's own spec probe seat missed** → 7M4 |
| 6M3 row caps | **Fixed** | client `source.rs:288-295` + serve `serve/destination.rs:375`, identical text/comparator (`>`), cap-before-one-batch-check precedence, pinned both sides. Serve-side at-cap + precedence pin gaps → 7L5 |
| 6L1 serialized-cursor gate | **Fixed** | inbound + state-doc + pre-send all through `contract::cursor_within_contract` (measures `to_vec`); wire-pinned (inflate refuses, exactly-at-cap passes). Boundary recomposed with the wave-7 caps: worst legal Checkpoint line = 4,196,460 < 5,242,880 ✓ (the JSON-escaped-name doubling included) |
| 6L2 identifier length | **Fixed** | part-event table (`destination.rs:309-316`) + Arrow field names (`source.rs:186-193`, full container walk), both pinned |
| 6L3 count caps | **Fixed** | 64 PK / 4096 hints before per-field gates (`source.rs:108-125`), pinned with at-cap acceptance; an honest max-hint spec survives the engine's column cap exactly |
| 6L4 CRLF budget | **Fixed** | `ReadLine { text, terminator }`, all four boundary shapes verified |
| 6L5 exhaustion pin | **Fixed** | `#[should_panic(expected = "identifier probe exhausted")]` at max_len=2 |
| 6L6 facade knobs | **Fixed** | forwarding through the engine setters (zero → 1, one clamp chain); refusal names both spellings (cells); pin drives a 1-cell budget end-to-end. Stream-cap refusal names only the engine spelling → 7L7 |
| 6L7 shared renderer | **Fixed** | all seven client seats + sdk delegation; crate-wide census: zero serde token echoes remain |
| 6L8 capacity probe | **Fixed** | pin parses `"[0]"` and measures `items.capacity()` — verified against vendored serde_json that the text→Value path has no size-hint reserve, so the probe models the real chain |
| 6L9 bounded panic_text | **Fixed** | one implementation, four belts, truncation pinned char-safe |
| 6L10 docs | **Partial** | structure honest; figures don't match the measured 1.96.0 layout (overstates ~1.5×, conservative direction) → 7L11 |
| 6.x infos | 6.12 ✓; 6.13 SECURITY.md **every claim re-verified seat-by-seat**; 6.15 rename ✓ (one stale doc link → 7L8) | |

---

## 3. New findings — Medium

### 7M1 — The exactly-once token is never identity-checked: any receipt from `existing_receipt` silently suppresses publish
**Where:** `crates/rdlt-connector-sdk/src/destination.rs:388-395` (`Session::commit`); the client forwards the reply verbatim (`client/destination.rs:442-468`); the engine proceeds to `wal.mark_committed` and GC on `Ok` (`load/loader.rs:546-561`).
**Verified:** `if let Some(receipt) = self.backend.existing_receipt(&meta.load_id, meta.commit_seq)` → `replay` → `return Ok(receipt)` — **no check that `receipt.load_id == meta.load_id && receipt.commit_seq == meta.commit_seq`**. `CommitReceipt` carries both fields (`rdlt-core/src/commit.rs`); nothing anywhere reads them for agreement.
**Impact:** a destination whose receipt lookup is buggy (returns a receipt for the wrong key — a stale cache, an unkeyed lookup, an off-by-one) makes every commit after the first a silent no-op: staged data never publishes, the WAL records `Committed`, segments are reclaimed, the run reports success with rows that exist nowhere. This is the D3 redelivery window's entire safety mechanism — the crash-consistency walk this round confirmed every other arrow holds, which concentrates exactly-once on precisely this unchecked token. The adversary model explicitly includes the buggy connector; the reference and memory backends implement it correctly, but the SPI seat and the reference-as-template do not enforce it.
**Fix:** one guard in `Session::commit` — `DestinationError::fatal` when the returned receipt's `(load_id, commit_seq)` disagrees with the request. Pin with a backend that answers `existing_receipt` with a mismatched receipt (the testkit's `MemoryDestination` shape makes this a two-line rogue).

### 7M2 — The serve-side session JSON seats have no document ceiling — the 6M1 class, serve direction
**Where:** `serve/destination.rs:665` (Publish `commit_meta_json`), `:653-654` (Replay `commit_meta_json`/`receipt_json`), `:592-593` (Ensure `table_schema_json`/`write_mode_json`), `serve/source.rs:457` (Read `stream_spec_json`).
**Verified:** the only serve-side document gates are the handshake's `config_json` (8 MiB) and `since_cursor_json` (4 MiB); grep confirms no `MAX_DOCUMENT_BYTES`/`MAX_CURSOR_BYTES` reference anywhere in the serve modules. `CommitMeta.state: StateDoc → cursors: BTreeMap<_, Cursor(Value)>` is the exact typed-shell-around-untyped-Values shape 6M1 was filed against: a 64 MiB dense frame materializes ~2 GB of `Value` transiently in the connector process, repeatable per frame by the rogue same-uid client.
**Fix:** `refuse_oversized_document(field, bytes)` before each `from_slice` (the helper's shape is in the client crate; hoist it beside the constants in the SPI so both sides import one spelling). `commit_meta_json` is the priority seat.

### 7M3 — Serve-side inbound identifiers carry no length cap and accumulate for the session's lifetime
**Where:** `serve/destination.rs:597` (Ensure's table name → `WriteGuard.ensured: BTreeSet<TableName>`, session-long), `:607` (Write's table), `:561-562` (Open's `pipeline`/`load_id`), plus Ensure's schema column names.
**Verified:** `TableName`/`PipelineId`/`LoadId` are unvalidated wrappers by design; the client's mirror seats are all capped at the hoisted `MAX_WIRE_IDENTIFIER_BYTES` — the serve modules reference it nowhere (grep empty). A rogue client Ensures tables with ~60 MiB names (within the frame cap), retained in the guard set for the session: unbounded memory growth plus log swelling.
**Fix:** import the SPI constant and gate the four inbound seats (mirror the client's refusal text).

### 7M4 — The runtime's spec probe: ungated `spec_json` parse with verbatim serde echo — the eighth seat the sweep missed
**Where:** `crates/rdlt-runtime/src/local.rs:414-418`.
**Verified:** `serde_json::from_slice(&reply.spec_json)` with no size gate and `"undecodable spec_json in the Spec reply: {error}"` — serde's data arms quote the token verbatim, and `ConnectorSpec.config_schema` is an untyped `Value`. Wave-7 converted the client's handshake seat; the runtime's own probe (used by the CLI `schema` command and any `provider.spec()` caller) parses the same field from the same adversary with neither defense: several hundred MB transient per probe plus multi-MB token echo into `ProviderError`.
**Fix:** gate against `MAX_DOCUMENT_BYTES` and render through `rdlt_connector::json::describe_parse_error` — two lines, the exact shape of the wave-7 client fix.

### 7M5 — A hint-pinned Float64 column silently rounds JSON integers beyond ±2^53
**Where:** `crates/rdlt-engine/src/shred/build.rs:287` — `Some(ValueKind::Int(i)) => Some(i as f64)` in `scalar_float64`.
**Verified:** the JSONL inference path escalates such columns to Utf8 — "losslessness is enforced at runtime, never assumed" (`infer.rs:4-5`, pinned by the beyond-2^53 escalation test) — but a *hint-declared* Float64 column never observes values, so `9007199254740993` builds as `9007199254740992.0`: non-null output, not counted as a misfit, silently altered. `value_fits` refuses the same value for Float64 (`contracts.rs:48-52`) — the discipline exists everywhere except this builder arm (the adjacent comment even documents the equivalent UInt discipline).
**Fix:** apply the ±2^53 exactness check (`i64 → f64 → i64` round-trip or bit test) and return `None` → counted misfit → Discarded-with-evidence, matching the inference path's contract.

### 7M6 — The structured passthrough's cross-batch Int64→Float64 widening is lossy, contradicting its own "cast LOSSLESSLY" claim
**Where:** `crates/rdlt-engine/src/shred/passthrough.rs:228-236` (assembly `cast`), the join at `:82`; the module doc's claim at `:8-9`.
**Verified:** registry Float64 × incoming Int64 batch → `join_column_types` = Float64 (correct lattice) → `arrow::compute::cast(Int64 → Float64)`, which rounds beyond ±2^53. The JSONL path escalates the identical value shape to Utf8; the passthrough path silently rounds it. The lattice *types* agree across both paths; the value-exactness discipline does not.
**Fix:** refuse the cast (typed, counted) when an Int64 column must widen to Float64 and any value is outside ±2^53 — or widen to Utf8 like the JSONL path. Correct the module doc either way.

---

## 4. New findings — Low

- **7L1 — Pass 2 never re-verifies row counts; the pass1→pass2 swap window is seconds-to-minutes wide.** The manifest cross-check exists only in pass 1 (`replay.rs:322-374` vs `:407-456`); a same-user writer swapping a same-layout/different-rows segment between passes applies rows the cross-check never saw, while the 6M4 residual doc describes only the within-open microsecond window. Recount in pass 2 (or carry pass-1's stat forward) and widen the residual doc.
- **7L2 — The density rule's premise misstates the write order, and ext4 delayed allocation can refuse honest unsynced segments.** The doc claims segments are "fsynced before the manifest names them" — the manifest line is appended immediately after `write_segment`; fsync lands at `sync_for_commit`. On ext4 delalloc, `st_blocks` of a dirty unflushed segment ≈ 0, so replay of a mid-run-crash span degrades to re-extraction (safe direction) where pre-wave-7 it replayed. Probed harmless on this repo's btrfs/tmpfs hosts (full `st_blocks` pre-fsync). Fix the parenthetical; optionally fsync segments before their manifest lines.
- **7L3 — The read_state per-cursor refusal interpolates the stream name raw** (`client/destination.rs:546-548`): a hostile StateDoc key (`"x\nFORGED LINE"`) plus an over-contract cursor plants the forged line in the diagnostic — the 5L4 class through the seat wave-7 added. Render through `escape_control_characters`.
- **7L4 — The cell budget bounds each assembled batch only; the loader's `concat_batches` accumulation is unbounded by it** (`load/loader.rs:388-441`): with `every_rows(N)` large, the destination write and concat transient exceed the cell budget by an operator-chosen factor (default policy unaffected; commits never span it). Consult the knob in `accumulate`.
- **7L5 — Serve row-cap pin gaps:** no at-cap acceptance pin on the serve seat (a serve-only `>=` regression would pass the suite) and no two-batch-over-rows precedence pin; the client unit pins cover its own seat only.
- **7L6 — A torn receipt tail splitting a multi-byte UTF-8 char wedges `existing_receipt`** (reference `destination.rs:256-265`): `read_to_string` fails `InvalidData`, classified transient — the load stalls retrying forever instead of reading the tail as absent (only `publish` truncates it). Read bytes and decode complete lines, like `truncate_torn_tail` does.
- **7L7 — The stream-cap refusal names only `EngineConfig::with_max_streams_per_source`** (`validate.rs:134-142`) — the facade spelling is absent, unlike the cell-budget refusal. One sentence.
- **7L8 — The stale `[`sanitize`]` intra-doc link breaks `make docs`** (`ui/mod.rs:52`): rustdoc runs `-D warnings`; PR CI doesn't run docs, so this sits green in CI and fails `make check`. Rename-era miss.
- **7L9 — `WriteModeSpec` is the one typed node without `deny_unknown_fields`** (`pipeline_spec.rs:92-105`): a typo inside a merge block silently ignores the field, inconsistent with the document surface everywhere else.
- **7L10 — `CommitPolicy::check()` runs only at the YAML facade:** the builder/engine setter path accepts degenerate policies (no spin — the loader's final flush always commits — but the "whole run is the crash window" shape the type refuses elsewhere). Call `check()` in `build()`.
- **7L11 — The arena/value-budget doc figures don't match the measured 1.96.0 layout** (measured: `Cow<str>` 24 B, node 24 B, entry tuple 32 B ≈ 56 B/value steady vs the documented ~80; peak with parse transients ~120-144 B): overstates cost ~1.5× — conservative for a budget justification, still wrong on paper. Correct when touching the module.

---

## 5. New findings — Info

- **5.1 — The unexpected_reply bound is 2 KiB pre-escape but ~20 KiB post-escape** (each control char expands to ~10 bytes): bounded and inert; note beside the constant.
- **5.2 — The count caps and the stream cap fire post-parse/post-collect** — decode-time stays bounded by the 64 MiB frame cap and typed expansion (~5-16×); the caps' documented plan-time benefit is honest; SECURITY.md's "the one discovery axis… is bounded" wording could name that the bound protects planning, not decode.
- **5.3 — Two spellings of the document-ceiling refusal** (hand-rolled at the spec seat vs `contract.rs`): consolidate when 7M2 hoists the helper to the SPI.
- **5.4 — The crash-consistency walk is clean**: all six arrows traced; no duplication/loss beyond the documented D3 window (which 7M1 now flags as resting on an unchecked token); the two-Run-header invariant holds through every recover arm including the voucher (Discard ⟺ provably nothing replayable); the fold handles multi-Run manifests defensively regardless; no end-to-end two-Run rescan test exists.
- **5.5 — Certifier vacuity sweep clean**: verdict functions read line-by-line — no inversions, no default-pass on error paths, P5 counts pre-pass and decode refusals identically, P6's terminality arithmetic correct, silence-can-never-pass stands, probe-clock abuse bounded. No way found for a contract-violating connector to pass or a good one to fail (beyond the documented ceiling-scope limits: a source whose single Read legitimately exceeds the retention ceilings cannot be certified — typed and documented).
- **5.6 — Row accounting and the type lattice verified clean end-to-end**: roots/values each spent exactly once across the scalar-list/child-table fork; discards disjoint and fully counted; `widen` property-tested associative/commutative including decimals; the 4M2 merge + Discard rollback + budget-before-apply sequence correct on both ingest paths; cancellation releases permits on every exit including the shred-panic path.
- **5.7 — StateDoc pipeline identity is the destination's SPI obligation, unchecked host-side** (`recover.rs:159-173` checks only format_version; the reference connector enforces it; conformance D-clauses don't pin it): one comparison closes the defense-in-depth gap.
- **5.8 — The per-batch row cap is absent at the certify and WAL-replay decode seats by design** (both count/drop or forward into capped seats; SECURITY.md's "every decode seat" wording is scoped to the connector wire — accurate under its heading).
- **5.9 — Reference-connector postures re-confirmed:** no internal publish guard (the recorded F-4 choreography trust — 7M1 is the host-side complement); K-matrix fixture skips exit 0 on third-party CLI runs (first-party cells panic); a rate-limiting source cannot pass the K-S2/S3 arms (defensible: the certifier demands a servable read); `--accept-skips` silently ignored for `--role destination`.
- **5.10 — Default-workdir leaf collisions between distinct pipeline names are always refused, never silently shared** (lock, ForeignPipeline, and destination state each catch it) — but the refusal's remedy text doesn't fit defaulted workdirs and nothing flags the collision at build time.
- **5.11 — Residual-width notes:** the 6M4 "microseconds" window assumes nanosecond-timestamp filesystems (coarse FS widens it to ~1 s); a UTF-8-splitting manifest tear degrades rather than truncates (safe direction); ext4 `inline_data` (non-default) would refuse tiny segments via the density rule (safe direction).
- **5.12 — CI verified clean once more** (SHA-pinned actions, no untrusted input into `run:`, least-privilege permissions; the `postgres:16` service image is tag- not digest-pinned — standard, informational). **PR CI does not run `make docs`** — which is how 7L8 landed; consider adding the docs leg to PR CI.

---

## 6. Recommended fix order

1. **7M1** — the receipt identity guard: two lines in `Session::commit` plus a mismatched-receipt rogue pin. The single highest-value diff in the report; everything else in the exactly-once story was just verified sound.
2. **7M5 + 7M6** — the Float64 losslessness pair: the exactness check in `scalar_float64` and the refuse-or-escalate in the passthrough cast (plus the module-doc correction). Silent data alteration on legal inputs is the round's only corruption-class finding.
3. **7M2 + 7M3 + 7M4** — the remaining ceiling seats: hoist `refuse_oversized_document` to the SPI, gate the five serve JSON seats, cap the four serve identifier seats, convert the runtime spec probe. One theme, one helper, one afternoon — the mirror image of the wave-7 client sweep.
4. **7L8** — the one-character doc-link fix (unblocks `make docs`/`make check`), plus the PR-CI docs leg from 5.12 so the class can't recur.
5. **7L1 + 7L2** — the WAL pair: pass-2 row recount (or stat carry) and the corrected density premise/ext4 note.
6. **7L3–7L7, 7L9–7L11** — as scheduled hardening.
7. **Infos** as recorded posture.

---

## 7. Caveats

- Line numbers are from `4812a778` and will drift.
- **Verification confidence:** 7M1 (the `Session::commit` arm read directly — no identity check exists), 7M5 (`build.rs:287` `i as f64` read directly), 7M6 (the cast arm read directly), 7M4 (`local.rs:414-418` read directly), 7M2/7M3 (greps confirming zero gate references in the serve modules), and 7L8 (the link text confirmed present) were all hand-verified at HEAD. The correctness audits (crash-arrow walk, row-accounting trace, lattice associativity, certifier vacuity) are the reviewing agents' line-level work with quoted evidence; the density-rule probes were executed live against btrfs/tmpfs.
- No tests were executed on the analysis host (no linker); the repo's gate record (872/872 serial, clippy zero, fmt clean) stands as the execution authority for the wave-7 commit under review.
- The High-count trajectory (4 → 6 → 3 → 2 → 1 → 0) and this round's Medium profile (correctness and mirror-image seats, not new vulnerability classes) both indicate convergence; the remaining work is the fix list above, dominated by small diffs at known seats. The one structural observation worth carrying forward: every "sweep all seats" fix so far has found one more seat the next round — 7M2/7M3/7M4 are the third such instance — so the SPI-level helpers recommended there (one ceiling function, one identifier gate, both importable by every crate) are the terminal form of these classes.
- Severity assumes the documented trust model (D-038-1 per `SECURITY.md`); the correctness findings (7M1, 7M5, 7M6) hold under *honest* actors with bugs, which the model explicitly includes.
