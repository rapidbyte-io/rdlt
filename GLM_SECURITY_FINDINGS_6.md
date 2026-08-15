# Security Analysis 6 — rdlt workspace (sixth deep review)

**Date:** 2026-08-15
**Scope:** same surface as rounds 1–5, reviewed at **`d816e410`** on `047-security-hardening` (working tree clean) — the wave-6 remediation commit (~2,170 lines across 47 files: the shared `rdlt_connector::ipc` pre-pass installed at all three Arrow decode seats, the arena HashMap dedup + the rows/values budget split, the exact 4 MiB cursor contract with the 5 MiB WAL line, the segment extent-sum bound, `ident_rules` validation at three seats, actual-retention testkit metering, and the EngineConfig knobs).
**Method:** six parallel maximally-pedantic subsystem reviews (client/protocol; WAL; engine arena/budgets; SDK serve + runtime; certify/testkit/reference; CLI/core/YAML/CI), each re-verifying its round-5 findings at the seat with counterexample hunts and reading the wave-6 diffs hunk-by-hunk. The one new High and all four Mediums were re-verified by hand against the current tree. No tests were executable on the analysis host (no C linker); all claims are source-level, with the repo's own gate record (843/843 twice-clean at the pinned 1.96.0) as the execution authority.

**Severity scale (unchanged):** High — the stated adversary (rogue connector, corrupted-WAL writer, rogue same-uid client, hostile YAML) can crash, hang, or abuse the host with trivial effort. Medium — narrower preconditions or reduced impact. Low — defense-in-depth/hygiene. Info — observations.

---

## 1. Executive summary

**Wave-6 is the cleanest landing of the series: 21 of 23 actionable round-5 findings fixed clean, 2 partial.** The two headline items closed with unusually strong verification:

- **5H1** — the framing pre-pass is now ONE SPI implementation (`rdlt_connector::ipc`), installed at all three Arrow decode seats (client read, serve-destination write, certify P5 — the last gaining its missing `catch_unwind` belt too). A statement-by-statement diff of the old private walk against the new shared one found **zero semantic drift**; the walk itself was re-verified panic-free, terminating, and never more permissive than arrow's reader on any over-declaring input. The enumeration game this class has driven for three rounds is over.
- **5H2** — the arena's object dedup now goes through a lazily-built HashMap past a 16-entry linear prelude with **exactly preserved semantics** (first position, last value — pinned in prelude, post-map, and mixed directions; 200 K-entry linear-time pin), and the parse budget splits rows (roots, 1 M) from values (16 M object entries + nested elements), metering the object-only slab shape and curing the row/element conflation (the 300 K × 4-element-list push passes, pinned).

**One new High — the first single-High round (trajectory: 4 → 6 → 3 → 2 → 1).** The WAL segment pre-pass's gates are all *relative* to `file_len`: a **sparse** giant segment (`truncate` to 64 GiB ≈ zero disk, valid tail footer declaring one huge block) passes the footer cap, the per-extent bound, and the Σ ≤ 2×file_len sum bound simultaneously — because every one of them scales with the lie. Reading holes *succeeds* (zeros), so arrow's `read_exact` fully commits the declared buffer: ~64 GiB resident → OOM-kill under default overcommit, abort under strict. The in-code residual note explicitly records the sparse case but asserts the read "fails typed at EOF" under default overcommit — **that claim is factually wrong for holes**, and the fix (compare declared extents against `stat.blocks() × 512` from the fstat the pre-pass already takes) is one field away.

**Four Mediums, all members of previously-fixed classes at newly-enumerated seats** — the series' persistent theme, now narrowing: two remaining untyped-`Value` decode seats the cursor contract's own "≈1× typed fields" claim denies (`state_doc_json` — a typed shell around untyped cursors — and `spec_json`'s `config_schema`, cached for the session lifetime); a missing row-count bound at the serve-destination Arrow seat (the dimension the framing pre-pass cannot see — Null/REE columns carry millions of rows in bytes); and the 5L1 fstat-compare being defeatable by any same-size rewrite (seconds-granularity mtime, no ctime, restorable timestamp) with its failure direction mischaracterized.

The two round-5 partials: **5L1** (the fstat-compare above) and **5L5** (the 1024-byte identifier cap landed at stream-metadata and identity seats but not the part-event table or Arrow field-name seats).

---

## 2. Round-5 fix verification

| Round-5 | Status | Evidence (current tree, `d816e410`) |
|---|---|---|
| 5H1 shared pre-pass | **Fixed** | `rdlt-connector/src/ipc.rs:46-98` (`refuse_overdeclared_ipc_framing`, `pub`, semver-minor); installed at client `source.rs:235`, serve `serve/destination.rs:358`, certify `wire.rs:797` (pre-pass then `caught_decode` belt — both, in order); statement-level drift diff: none; belts live in release (`panic = unwind` everywhere); serve pin asserts the pre-pass spelling *exactly* (`"…a declared metadata length of 2147483632 bytes exceeds the 24-byte frame"`); certify P5 census counts refused frames; new `raw_arrow_read_frame` rogue knob |
| 5H2 arena dedup | **Fixed** | `arena.rs:491-509` — 16-entry linear prelude, lazily-built map from **all** current entries at the 17th distinct key; in-place slot update preserves first-position/last-value in both paths; pins: prelude-dup, 200 K-entry mixed-dup linear-time, canonical-bytes differential |
| 5M1 cursor contract | **Fixed** | `MAX_CURSOR_BYTES = 4 MiB` (`connector/lib.rs:148`) at client inbound (`source.rs:414`, gate-before-parse), client pre-send (`:355`), serve gate (`serve/source.rs:478`) — one constant, `>` everywhere; WAL line cap 5 MiB with ≈1 MiB pinned margin (`record.rs:81`, `scan.rs:731-750`); README rescoped per-field; early typed failure replaces the crash-loop. **Two sibling seats remain uncapped** → 6M1, 6M2 |
| 5M2 extent-sum bound | **Fixed** | `extents_within_file` (`replay.rs:168-202`): Σ extent ≤ 2×file_len, saturating, both dictionary and record-batch lists, degrade arm, pinned. **All-relative gates** → 6H1 |
| 5M3 cell-budget knob + order | **Fixed** | `DEFAULT_MAX_BATCH_CELLS = 2^28` / `DEFAULT_MAX_STREAMS_PER_SOURCE = 1024` as `EngineConfig` setters clamping zero (`config.rs:22-29, 126-138`); refusal texts name the knobs; check hoisted above `registry.apply` at both seats (`drain.rs:254-261`, `passthrough.rs:193-198`). No chunking (still refusal-only); **facade gap** → 6L6 |
| 5M4 row/element conflation | **Fixed** | rows spend only at roots (`arena.rs:198-204, 442-447`); nested elements spend values; the 300 K×4-list push passes (pinned both halves) |
| 5M5 object metering | **Fixed** | `visit_map` spends one value per entry at any depth (`arena.rs:478-485`); progressive during parse; `MAX_JSON_VALUES_PER_PUSH = 16M`; pins assert the arena stopped at the cap |
| 5M6 ident_rules validation | **Fixed** (pin gap → 6L5) | `8..=4096` at all three seats (handshake `:312`, plan-time `validate.rs:124` — before `recover_wal` so scanned rules are always in-range, sidecar `scan.rs:462-471` → Damaged → clear → fresh sidecar); zero refused at seats, clamped in writers (pinned); exhaustion-space arithmetic verified (16⁷ at the floor vs ≤ 4096+reserve names per namer). **The commit's "probe exhaustion regime pinned" claim is only indirectly true** — no test drives the assert |
| 5M7 testkit metering | **Fixed** | post-parse `retained_bytes` walk (honest per-shape: slot + capacity + children; admitted 2–3× slack under-count); per-push 4 MiB transient bound with ×256 demoted and pinned against the measured chain form; honest multi-MiB reads certify again (pinned); ceiling unchanged 64 MiB |
| 5L1 fstat-compare | **Partial** | size+mtime before/after, handle-based (`replay.rs:98-100, 143-150`) — but seconds-only mtime, no ctime, restorable via `utimensat`; residual text misstates the failure direction → 6M4 |
| 5L2 manifest budget | **Fixed** | 1 GiB with honest ~2 h arithmetic at 1 sweep/s + seam pin + passing-under-budget control. CRLF undercount → 6L4; cadence heuristic → 6.x |
| 5L3 WAL line fit | **Fixed** | maximal-cursor-line pin; write-time refusal stands. Float round-trip inflation residual → 6L2 |
| 5L4 `{:?}` seats | **Fixed** | all three refusal seats render via `escape_control_characters`; whole-inventory mechanical sweep pin (`no_inventory_character_survives_the_escape_raw`, U+0..U+10FFFF) |
| 5L5 identifier caps | **Partial** | `MAX_WIRE_IDENTIFIER_BYTES = 1024` at stream-metadata + identity seats (boundary pinned); `MAX_STATE_FORMAT_KINDS = 64`. Missing at part-event table + Arrow field-name seats → 6L3; type-hint/primary-key counts uncapped → 6.x |
| 5L6 serve serde echo | **Fixed (sdk)** | one `describe_config_parse_error` helper at every serve decode seat; concrete-`serde_json::Error` parameter prevents future misuse; zero verbatim echo left in the sdk. Client-crate mirror seats remain → 6L7 |
| 5L7 starvation | **Fixed (as designed)** | shred bounded by 16 M values × O(1); SharedBudget doc states finiteness; sibling starvation now seconds-graded |
| 5L8 passthrough O(cols²) | **Fixed** | map-backed lookups |
| 5L9 stream-cap knob | **Fixed** | config knob named in the refusal; facade gap shared with 5M3 → 6L6 |
| 5L10 check-before-apply | **Fixed** | both seats |
| 5L11 boundary pins | **Fixed** | drain pin models 2 root system columns; "~260-column" doc honest (exact 266 source + 2 system) |
| 5L12 chain pin | **Fixed** | chain form asserted ≥ dense form, factor covers it, corrected 376 B/level arithmetic. Allocator-constant hardcoding residual → 6L8 |
| 5L13 max_len 0 | **Fixed** | refused at seats (0 < 8), clamped `"_"` in writers, pinned |
| 5L14 CLI display | **Fixed** | stream rows via `sanitize_identifier` (full inventory); `sanitize`'s predicate upgraded to the shared inventory; `validate` aligned; joiners visibly spelled out, pinned |
| 6.x riders (round-5 Infos) | 6.1 footer-cap pin **added** (4 096-column footer ×4 ≤ cap, round-tripped); 6.10 helper `hash_len_for` **landed** (rename declined per the finding's own wording); 6.13 SECURITY.md **updated and verified claim-by-claim**; perf baseline re-record honest (+3.761% matches the commit exactly, dated refs untouched) | |

---

## 3. New findings — High

### 6H1 — Every WAL segment gate is relative to `file_len`: one sparse file defeats them all — a 64 GiB lie costs a tail write and kills the process
**Where:** `crates/rdlt-engine/src/wal/resume/replay.rs:168-202` (`extents_within_file` — per-block `end ≤ file_len`, sum `total ≤ 2 × file_len`), `:82-155` (the pre-pass, which takes the fstat but never reads `blocks()`); the residual note at `:68-75` records the sparse case but with a wrong safety claim.
**Verified mechanism:** every gate scales with the declared file size. `truncate(seg, 64 GiB)` costs ≈ no disk; write a valid ~4 KiB trailer + footer at the tail declaring one block `offset = 8, body ≈ 64 GiB − overhead`: `footer_len ≤ file_len − 10` ✓, `footer_len ≤ 16 MiB` ✓, per-block `end ≤ file_len` ✓, `Σ ≤ 2 × file_len` ✓ (two 32 GiB blocks also pass). At decode, arrow's `read_block` allocates `MutableBuffer::from_len_zeroed(body)` and `read_exact`s the extent — **reading a hole succeeds and returns zeros**, so there is no EOF failure: every page of the declared buffer is written and becomes resident. ~64 GiB resident → kernel OOM-kill under default overcommit (or `handle_alloc_error` → abort where the single request exceeds the heuristic). The in-code residual asserts "under default overcommit the zeroed allocation maps lazily and the read fails typed at EOF" — true only for a *truncated* file (read past EOF), false for holes; the recorded mitigation for the strict-overcommit half is right, the default-overcommit half is not.
**Impact:** the corrupted-WAL adversary (same-OS-user write access — the model every 4H1/5M2 gate was built for) crashes the embedded host process with one `truncate` plus a tail write, no race, deterministic, repeatable per run. This is the sole surviving member of the 4H1 abort/OOM family — the wave-5 gates closed absolute declarations, and relative bounds cannot close relative lies.
**Fix:** in `refuse_overdeclared_segment_layout`, compare the declared extents against the file's *allocated* size: `stat.blocks() × 512` from the fstat already in hand (`MetadataExt::blocks()`) — refuse typed when `Σ extent` (or any block's `end`) exceeds it by a generous factor. Honest writer segments are dense (no fallocate/punch anywhere in `writer.rs`), so `blocks×512 ≈ file_len` for every honest segment; a sparse giant has `blocks×512 ≪ file_len` and refuses. This preserves the documented design constraint that absolute segment size is unbounded (replay.rs:72-74) — density, not size, is the honest invariant. Pin the sparse fixture.

---

## 4. New findings — Medium

### 6M1 — `state_doc_json` is an uncapped untyped-`Value` inbound seat the cursor contract's own claims deny
**Where:** `crates/rdlt-connector-client/src/destination.rs:487-496` — `serde_json::from_slice::<StateDoc>(&bytes)` with no size gate; `StateDoc.cursors: BTreeMap<StreamName, Cursor>` and `Cursor(serde_json::Value)` (`rdlt-core/src/state.rs:40-46`, `cursor.rs:10`).
**Verified:** the client's own comment claims the checkpoint frame is "the one UNTYPED inbound document seat" and the README claims typed `*_json` fields ride "typed serde structs with a ≈1× parse factor" — both false here: `StateDoc` is a typed shell around untyped cursor `Value`s. A rogue destination's ≤ 64 MiB `ReadState` reply of compact JSON materializes hundreds of MB of `Value` retained by the caller; the cursors also feed engine resume state (the pre-send gate would refuse an over-4 MiB cursor on re-send — after the expansion already happened).
**Fix:** gate `state_doc_json.len()` (and/or each decoded cursor's re-serialized length) against `MAX_CURSOR_BYTES` at this seat — the same one-line shape as the checkpoint seat.

### 6M2 — `spec_json`'s `config_schema` is an uncapped untyped `Value`, cached for the session lifetime
**Where:** `crates/rdlt-connector-client/src/handshake.rs:272-276` — `from_slice::<ConnectorSpec>(&ok.spec_json)`, no size gate; `ConnectorSpec.config_schema: Option<serde_json::Value>` (`rdlt-connector/src/spec.rs:18`); the serve side serializes it uncapped (`serve/common.rs:593`).
**Impact:** a rogue connector's 64 MiB `spec_json` with an object-heavy `config_schema` expands many-fold into a `Value` cached inside `Source`/`Destination` for the whole session — worse than transient, repeatable per handshake. A config schema is a hand-authored document measured in KB; the config ceiling's rationale applies verbatim.
**Fix:** enforce `MAX_DOCUMENT_BYTES` on `spec_json` before parsing.

### 6M3 — No row-count bound at the serve-destination Arrow decode seat: the dimension the framing pre-pass cannot see
**Where:** `crates/rdlt-connector-sdk/src/serve/destination.rs:350-375` (`decode_arrow_ipc_erring`) — the seat enforces the framing pre-pass and the one-batch rule but never checks `num_rows()`; grep confirms no `MAX_RECORD_BATCH_ROWS` reference in the file.
**Verified:** the pre-pass bounds declared *framing lengths*, not a RecordBatch's `length` field. Null-type columns carry zero body buffers and run-end-encoded columns carry row counts with little or no body — a small Write frame yields a batch with an enormous `num_rows()` handed straight to `backend.write`. The engine enforces the 1 M-row cap at its own ingress for the read direction (`shred/passthrough.rs:47`); the SPI's own constant doc names row count "an independent memory dimension from encoded bytes" — this seat is the unguarded one (the client's decode and certify's counter share the absence but have downstream caps or only count).
**Impact:** rogue same-uid client, per Write frame — backend-dependent per-row amplification unbounded by the 64 MiB frame ceiling.
**Fix:** after the first batch decodes, refuse typed when `num_rows() > MAX_RECORD_BATCH_ROWS`, mirroring the engine's refusal shape; consider the same line in the client's decode for symmetry.

### 6M4 — The 5L1 fstat-compare is defeatable by any same-size rewrite, and its residual misstates the failure direction
**Where:** `replay.rs:98-100, 143-150` — compares size + `mtime()` (seconds only; `mtime_nsec()` unused; no `ctime`).
**Verified:** a same-size footer rewrite inside the guarded window that does not cross a second boundary changes neither field; a deliberate same-user writer can restore the mtime outright with `utimensat`. `ctime` — which the kernel bumps on every write *and* on `utimensat`, and which the writer cannot restore — is not compared. The residual text claims a slipped-through swap is caught "typed, safe direction" by the reader's footer verification — arrow's verification is FlatBuffer-validity only and does not re-check extents against file size (4H1's entire premise), so a complete hostile swap re-opens the abort class.
**Impact:** an attacker flapping benign/hostile same-size footers during recovery (retrying across restarts) reaches the abort/OOM class with material probability; the guard's real protection holds only against size-changing or second-crossing incidental rewrites.
**Fix:** compare `mtime() + mtime_nsec()` (or `Metadata::modified()`) **and** `ctime()/ctime_nsec()`; state the deliberate-adversary residual honestly (the microsecond post-compare window is genuine and should be recorded as such).

---

## 5. New findings — Low

- **6L1 — Cursor float round-trip inflation can exceed the 5 MiB line's 1 MiB margin.** The inbound 4 MiB gate measures *wire bytes*; the WAL line carries the `Value` *re-serialized* by serde_json — ryu's pretty mode renders `1E15` as `1000000000000000.0` (≈3.3× with separators; verified in the vendored ryu source). A wire-legal ≤ 4 MiB float-heavy cursor re-serializes past 5 MiB → loud write-time refusal → a crash-looping run against a source that keeps re-emitting it (fail-safe, availability-only; the pin covers only the string shape). Gate the re-serialized form at the decode seat or document the number-heavy bound.
- **6L2 — Two identifier seats missed by the 5L5 length cap:** the part-event `event.table` (`destination.rs:277` — content gate only) and Arrow field names post-decode (`source.rs:151`) — a multi-MiB control-free table name or field name rides into host telemetry/column names within the frame cap. Add `is_oversized_identifier` at both.
- **6L3 — Count caps landed only for state-format kinds:** `primary_key` field count and `type_hints` key count are uncapped (a spec with hundreds of thousands of tiny hint keys passes every gate within one 64 MiB frame; the engine caps streams, not hints-per-stream). Bounded, plan-time cost — cap or accept as Info.
- **6L4 — The manifest budget undercounts CRLF terminators** (`scan.rs:211` adds `len+1` after stripping `\r\n`): on-disk read amplification up to 2× the 1 GiB budget for `\r\n`-heavy files; retained memory unaffected. Count `+2` when a `\r` was stripped.
- **6L5 — The probe-exhaustion assert remains unpinned** (`naming.rs:190-193`, release-active): no `#[should_panic]` or typed-outcome test drives it; wire seats all validate ≥ 8 so it is embedder-reachable only — but the wave-6 commit's "probe exhaustion regime pinned" claim is only indirectly true (the landed pin covers 24 colliders at max_len 8..=12, of which only 8 shortens the hash). Pin it or make the arm a typed refusal.
- **6L6 — The facade does not plumb the new knobs:** `PipelineBuilder` forwards byte budget/policies/modes but not `max_batch_cells`/`max_streams_per_source`, and the pipeline document has no fields — so the escape hatch the new refusal texts advertise ("raise the budget with `EngineConfig::with_max_batch_cells`") is unreachable for `rdlt::Pipeline`/CLI users, leaving 5M3's honest-shape collateral (267+-column tables at the row cap) without an operator remedy on the primary surface.
- **6L7 — Verbatim serde echo remains at six client-crate decode seats** (`destination.rs:430, 469, 492`; `handshake.rs:274, 305`; `source.rs:317, 426`) — the mirror image of 5L6's serve-side fix, where the adversary is the rogue *connector*: serde's data arms quote the parsed token, so a malformed multi-MiB state doc rides a fragment into host logs. Share the kind-and-location renderer across the wire.
- **6L8 — The testkit chain pin hardcodes the allocator strategy it derives from** (`source.rs:456-480`): capacity-4 first-`Vec` and the 8-byte header are constants in the formula while `size_of::<Value>()` is measured — a std/allocator change to capacity-8 doubles the real chain form past the factor while the pin stays green. Probe the real capacity (`"[0]"` parsed, `items.capacity()`) in the pin.
- **6L9 — Unbounded panic-text length in certify P5 violations** (`wire.rs:804-811`): the belt formats the panic payload verbatim; the 100-violation cap bounds count, not length. Contrived today (arrow panics are static strings and the belt's live inputs now refuse at the pre-pass) — cap the text (first 4 KiB) for the same discipline as every other evidence line.
- **6L10 — The value-budget docs undercount the object shape by ~2×** (`channel.rs:57-66`, `arena.rs:146-148`): an object entry costs node (40 B) **plus** obj-entry tuple (40 B) ≈ 80 B/value — worst pre-refusal arena ≈ 1.25 GB, not the "~640 MB" the "~40-byte node" figure implies; and the dedup HashMap's escaped-key clones add up to ~2× key bytes on top (unbudgeted, per-object, freed at close). Correct the comments; optionally a hash-only index.

---

## 6. New findings — Info

- **6.1 — `2 × file_len` is an unchecked multiply** (`replay.rs:194`): overflows only past 8 EiB sparse files; debug panic lands inside `caught_decode`, release wrap *tightens* the check. `saturating_mul` for clarity.
- **6.2 — Replay's commit clones the whole recovered state** (`replay.rs:430`): peak recovery RSS ≈ 2–2.5× the manifest budget with a budget-filling span. Bounded by design; worth a doc note beside the 1 GiB arithmetic.
- **6.3 — In-process stream names carry no length cap** (wire names capped at 1024 B; `validate_streams` caps count only): a > 1 MiB embedded name + maximal cursor → loud write-time refusal. Optional mirror of the wire seat.
- **6.4 — The "~2 hours" honest-span figure assumes ~1 checkpoint sweep/s**: at `every_checkpoints(1)` with fast delivery the 1 GiB budget fills proportionally faster; word the doc as a rate bound (budget / sweep-rate).
- **6.5 — No at-boundary acceptance pin at the cursor seats** (both 5M1 tests use over-bound payloads; `>` is source-verified consistent everywhere — the config seat has its at-cap pin).
- **6.6 — Stale doc comment** (`client/sanitize.rs:20`) still describing the `{:?}` rendering the 5L4 fix replaced.
- **6.7 — `unexpected_reply` renders whole proto replies via `{reply:?}`** (`destination.rs:212-216`) — the 5L4 class at a fourth seat (fillers pass `Debug` raw); reachable only on a wrong-variant reply; not a frozen spelling.
- **6.8 — Typed-field "≈1× parse" is optimistic for degenerate shapes** (many tiny `Vec<String>`/BTreeMap entries parse at ~3–5×): bounded by the frame cap, transient; an optional README precision.
- **6.9 — `Document::from_json`/`from_value` carry no byte cap** while `from_yaml` gates — in-process caller-trusted input; JSON has no anchor expansion. Consistency observation.
- **6.10 — `retained_bytes` under-counts allocator slack ~2–3× on adversarial shapes** (capacity-vs-len, index bytes, rounding — admitted in its doc): at the 64 MiB refusal point real retention can be ~130–190 MiB. Flood-guard purpose holds.
- **6.11 — Testkit's arrow arm caps certifiable reads below the SPI's row cap** (1 M rows × 72 B = 72 MB > the 64 MiB ceiling → practical ~932 K rows/read): honest charge (rows materialize as `Value::Null`s); availability note for the harness's philosophy.
- **6.12 — One naked `StreamReader` outside the shared-defense discipline** (`testkit/fixtures.rs:138` — decodes bytes the same function just encoded): self-authored; a comment (or the pre-pass, for census uniformity) would keep the inventory honest.
- **6.13 — The flatbuffer verifier diagnostic rides one frozen spelling** (`ipc.rs:85-86` → P5 evidence): `VerifierError` carries offsets, not buffer content — bounded, non-injectable; recorded because foreign-library text in a pinned surface is worth knowing about.
- **6.14 — `Spec.pipeline` has no length cap** (operator-authored, rendered once, normalized for the workdir leaf): log-cost only.
- **6.15 — `sanitize` vs `sanitize_identifier` have converged to the same predicate** (only `\n`/`\t` exemption differs): the names no longer communicate the difference; the round-5 rename suggestion is now more warranted.
- **6.16 — Reference-connector caller-contract family unchanged** (6.14 of round 5 carried: zero-part publish via redelivery, re-publish overwrite, orphan temps, retry-with-rewrite doubling, replay drops staging) — pre-existing, unreachable by the engine's choreography.
- **6.17 — `MAX_JSON_VALUES_PER_PUSH` is not configurable** (a `pub const`, unlike the same wave's cell/stream knobs): honest > 16 M-value single pushes must be split source-side; symmetry note.

---

## 7. Recommended fix order

1. **6H1** — the density check: compare declared extents against `stat.blocks() × 512` from the fstat already in hand. One field, one comparison, closes the last member of the 4H1 family; correct the residual note's wrong claim in the same edit.
2. **6M1 + 6M2** — cap the two remaining untyped-`Value` seats (`state_doc_json`, `spec_json`): one constant each, the same gate-before-parse shape as every landed seat.
3. **6M3** — the row cap at the serve-destination decode (and symmetrically the client decode): the framing pre-pass's blind dimension, one comparison after the first batch.
4. **6M4** — ctime + nsec in the fstat-compare; honest residual text.
5. **6L6** — plumb the two knobs through the facade (the refusal texts advertise a remedy the primary surface cannot reach).
6. **6L1–6L5, 6L7–6L10** — the residual family: float-round-trip gate, the two missed identifier seats, CRLF count, the exhaustion pin, the client-crate renderer sharing, the capacity probe, the panic-text cap, the doc corrections.
7. **Infos** as scheduled hygiene.

---

## 8. Caveats

- Line numbers are from `d816e410` and will drift.
- **Verification confidence:** 6H1 was hand-verified (the gates at `replay.rs:168-202` read directly — all-relative arithmetic confirmed; the residual note read; POSIX hole-read semantics are standard); 6M1/6M2/6M3 were hand-verified (the three seats read directly, the `num_rows` grep). The round-5 fix table relies on the six subsystem reviews' line-level evidence, several of which included statement-level drift diffs (5H1), counterexample hunts (5M3's boundary arithmetic re-derived), and measured layouts (5M7's 72-byte `Value`). No tests were executed on the analysis host; the repo's gate record (843/843 twice-clean) is the execution authority.
- The High-count trajectory across rounds (4 → 6 → 3 → 2 → 1) reflects both genuine convergence and review economics: each round's fixes are verified more deeply than the last, while the remaining findings concentrate in seat-enumeration gaps of already-fixed classes. The 6M1/6M2/6M3 pattern suggests one more enumeration sweep of "every untyped decode seat" and "every row-count consumer" would be cheap insurance before closing the engagement.
- Severity continues to assume the documented trust model (D-038-1 per `SECURITY.md`): operator-installed connectors with untrusted wire outputs, plaintext UDS, same-OS-user WAL directory writer, same-uid serve-socket holders.
- No network advisory scan was run (standing caveat since round 1).
