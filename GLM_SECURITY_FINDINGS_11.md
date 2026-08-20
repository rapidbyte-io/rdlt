# Security Analysis 11 — rdlt workspace (eleventh deep review)

**Date:** 2026-08-19
**Scope:** the full workspace at **`bf4c5976` on `main`** (working tree clean apart from findings docs) — reviewed on top of the round-10 remediation and the 062–066 feature waves: the Pipeline ctor refactor (`from_{file,text,document,document_with}`), the dynamic-linker env forwarding, round-10's fixes (065), and 066's new surface — the `rdlt check` command replacing `validate`, bounded stream-read concurrency, jittered retries under a configurable policy, and the document's `resources`/`schema_policy` fields.
**Method:** six parallel comprehensive subsystem reviews (client/protocol; WAL; engine data-path — re-dispatched after a timeout; SDK serve + runtime; certify/testkit/reference; facade/CLI/CI). Each lane read its full diff `ee6e1e6e..HEAD`, verified its assigned round-10 fixes at the seat, and ran its suites through the containerized harness (the full workspace gate: **954/954 serial, green**, before review began). The three Mediums below were re-verified by hand at their seats.

**Severity scale (unchanged):** High — the stated adversary can crash, hang, or abuse the host trivially, **or a legal input shape silently loses, duplicates, or alters data**. Medium — narrower preconditions or reduced impact; contract violations short of corruption. Low — defense-in-depth, hygiene, pin gaps, availability-only edges. Info — observations and recorded postures.

---

## 1. Executive summary

**Every round-10 finding is fixed** — both Mediums and all three Lows landed with pins (the arrow-cause renders bounded at all three seats plus a fourth sibling the fix found on its own; the reconcile read `.take()`-bounded with a genuine growth-injection pin; the recursive `gate_column`; the reference replay receipt verification with four pins). The new 066 surface came back **solid**: the `rdlt check` command spawns and dials exactly what a run does under the same hygiene, sends config only through the documented private-socket handshake, touches no workdir/WAL/session (pinned), and the engine's new concurrency/retry machinery has sound jitter, a real attempt cap, and cancellation-aware backoff — the facade lane verified the document's new `resources`/`schema_policy` fields refuse unknown spellings and clamp every knob so nothing spells to "off". The 062 dynamic-linker env addition is search-path class (LD_LIBRARY_PATH), not preload class — no injection reopened.

**Fifth consecutive zero-High round** (trajectory: 4 → 6 → 3 → 2 → 1 → 0 → 0 → 0 → 0 → 0). Three Mediums, and the series' two long-running families each grew what should be their last members:

1. **The render family (11M1):** `unexpected_reply` bounds the text it *keeps* but first materializes the *complete* Debug of the reply — prost renders bytes fields as per-byte decimal lists, so one wrong-variant reply carrying a 64 MiB payload allocates a ~260–320 MiB string to keep 2 KiB of it. Every sibling seat was capped in waves 9–10; this seat caps its output, not its intermediate.
2. **The count/materialization family (11M2):** the streams reply has no count cap, and each spec's maps materialize before their count gates fire — one 64 MiB unary reply yields ~0.5–0.7 GB transient plus a multi-second executor stall. The row-cap precedent ("the seat serves every host") states where this belongs.
3. **The WAL time-amplification family (11M3):** chain resolution memoizes only the queried table, so a hostile 1 GiB manifest of linear parent chains costs ~4.5×10¹² hops — hours-to-days of single-threaded recovery work, memory bounded, verdict eventually correct. The lane's own extent-sum standard exists to refuse exactly this shape; this seat predates it and was never held to it.

The Low band is consistency completion (three ungated identifier seats the 065 sweep missed on the serve side, the identity-mismatch joiner-invisible render, a trailing-slash check-probe false positive, a README contradiction and a stale example), and the Infos are recorded postures on the new machinery.

---

## 2. Round-10 fix verification

| Round-10 | Status | Evidence (current tree, `bf4c5976`) |
|---|---|---|
| 10M1 arrow-cause renders | **Fixed — plus a fourth seat** | `render_message` at all three seats (`source.rs:218, 226, 252`) + the encode sibling (`destination.rs:233`); the hostile-metadata PoC pin (2 MiB BEL metadata → bounded refusal naming true length); filler residual closed by construction (shared `push_escaped`) |
| 10M2 reconcile take-bound | **Fixed** | `writer.rs:72-81` — `.take(window)` on the measured length; growth-injection pin at the fstat-to-read seam |
| 10L1 recursive identifier walk | **Fixed** | `serve/destination.rs:229-238` — `gate_column` recursing all depths; exhaustive match (a new variant fails compilation); two-level wire pin |
| 10L2 reference replay verification | **Fixed** | `session.rs:141-177` — both guards (unequal-to-meta; never-issued) before `clear_staging`, renders bounded; four pins incl. staging-kept-then-published |
| 10L3 mixed-shape contract | **Fixed** | `cast.rs:115-126` — the v1-contract refusal arm, recursive, dictionary-unwrapping; two pins (struct⊔scalar, list⊔scalar — the latter replacing arrow's silent Display cast the belt cannot see) |
| 5.1 ADR/README/unused-deps | **Fixed** | dated amendments in both ADRs; protocol README scoped to what is gated; deps dropped, `cargo check` clean |
| 062 dynamic-linker env | **Safe** | `spawn.rs:113-135` — LD_LIBRARY_PATH class only; LD_PRELOAD/DYLD_INSERT_LIBRARIES remain cleared; verbatim `OsString` pass-through; pin asserts pass-through AND HOME still cleared |
| 066 riders | **Fixed (one residue → 11L6)** | check ok line counts streams (pinned); no-write claim scoped at the CLI seats — the library-layer seats keep the universal claim |

---

## 3. New findings — Medium

### 11M1 — `unexpected_reply` materializes the full escape-amplified Debug before truncating: one wrong-variant reply allocates ~260–320 MiB to keep 2 KiB
**Where:** `crates/rdlt-connector-client/src/destination.rs:202` (`let debug = format!("{reply:?}");`), reached from every method's wrong-variant arm (`:390, :408, :437, :456, :476, :530, :545`).
**Verified:** `REPLY_RENDER_CAP` (2048) bounds the kept prefix, but the Debug of the reply is materialized whole first. The variants reaching this seat carry bytes fields (`receipt_json`, `state_doc_json`); prost 0.14.4's generated Debug delegates bytes to `Vec<u8>`'s Debug — a per-byte decimal list, ~4–5 chars/byte (verified in prost-derive's `debug_inner`). A rogue answering any Backend call with a wrong-variant reply carrying a 64 MiB payload (admitted by the frame cap) allocates a ~260–320 MiB `String`, then keeps 2 KiB. Repeatable per call.
**Impact:** transient ~4–5× frame-cap memory spike per hostile reply — OOM-adjacent under a memory-limited host. The next member of the render family: waves 9–10 bounded the kept text at every seat; this seat's *intermediate* was never bounded.
**Fix:** bound before Debug — truncate payload fields to a small prefix in a describe-only copy of the reply, or Debug through a counting writer that stops at the cap.

### 11M2 — No count cap on the streams reply, and per-spec maps materialize before their count gates: one unary reply yields ~0.5–0.7 GB plus an executor stall
**Where:** `crates/rdlt-connector-client/src/source.rs:283-295` — the `stream_spec_json` decode loop; per-spec gates at `:85-116` run only after `serde_json::from_slice::<StreamSpec>` has materialized the spec.
**Verified:** nothing caps the number of specs in one `StreamsReply`. A 64 MiB frame of minimal specs (`{"name":"a"}` ≈ 12–15 bytes each) yields ~4.5 M `StreamSpec`s (~104 B each ≈ 470 MB) on top of the ~180 MB prost `Vec<Vec<u8>>` — ≈11× the frame cap — plus a multi-second synchronous parse+gate loop with no await. The single-spec sibling: one spec whose `type_hints` holds millions of tiny keys materializes ~5–10× before the 4096 gate refuses. The engine's `max_streams_per_source` refuses only after full materialization.
**Impact:** one rogue unary reply = transient ~0.5–0.7 GB + executor-thread stall. The crate's own law ("count caps beside the content gates"; the row-cap precedent serving "every host") says the seat, not the engine ingress, is where this belongs.
**Fix:** `gate::count("declared streams", list.stream_spec_json.len(), cap)` before the loop (the engine default 1024 is an honest cap), plus a per-spec raw-byte ceiling (the document-gate pattern) to bound the map materialization.

### 11M3 — Quadratic chain resolution: a hostile 1 GiB manifest costs ~10¹³ operations — an hours-to-days recovery wedge the manifest budget never priced
**Where:** `crates/rdlt-engine/src/wal/scan.rs:511-526` (`chain_of`) via `lineage.rs:67-85` (`Chain::resolve`), called per segment record (`scan.rs:657`) and in `live_tables` (`:552-557`).
**Verified:** `Chain::resolve` memoizes only the queried table (`chains.insert(table, path)` — `lineage.rs:82`); intermediate ancestors are not memoized, so each distinct table `t_i` in a recorded linear chain `t_i → … → t_0` walks `i` hops, each a `BTreeMap::get` plus a `TableName` clone. A checksummed (unkeyed-blake3) hostile manifest of ~300 B deltas + ~130 B segment lines within the 1 GiB budget plus one covering checkpoint and a matching sidecar gives N ≈ 3 M → ~4.5×10¹² hops/allocations — ~13 hours at 10 ns/hop, realistically days. Memory stays bounded; the verdict is eventually correct (Discard/Damaged).
**Impact:** pure CPU/time amplification, safe direction — but inconsistent with the lane's own standard (the extent-sum bound exists to refuse "~70 TB of read_exact from ~100 MiB") and the budget's stated goal bounds bytes, not super-linear compute. Honest chains are shallow, which is why ten rounds missed it.
**Fix:** memoize each walked node's suffix chain (share via `Rc`/persistent tail, O(N) memory) making the scan O(N log N) — the cleaner close over a depth cap, which could refuse honestly nestable chains.

---

## 4. New findings — Low

- **11L1 — Publish/Replay `CommitMeta.load_id` ungated at the serve seats** (`sdk serve/destination.rs:585-618`): Replay gates `receipt.load_id` but not `meta.load_id`; Publish gates neither — a ~8 MiB load id reaches backend refusal text (the class 228becf4's own rationale names). One-line mirrors of the receipt seat.
- **11L2 — The serve Read seat's `StreamSpec` identifiers ungated** (`sdk serve/source.rs:410-448`): name/primary_key/cursor_field/type_hints keys bounded only by the document ceiling; retained per read, quoted by connector refusals — the one mirror of the client's stream gates still missing.
- **11L3 — Reference-connector refusal seats render wire identifiers raw** (`destination/session.rs:185-192, 247-258`; `source/connector.rs:73-79`): the 065 render fix covered the replay guards; the publish wrong-load, read_state foreign-pipeline, and unknown-stream refusals still interpolate raw — multi-MiB echo + injection material at three sibling seats the sweep missed (11M1's class in the template third parties copy).
- **11L4 — Identity/version mismatch renders joiners invisibly** (`client error.rs:56-74`): `required 'ab', reported 'a\u200Cb'` renders as a self-contradictory `reported 'ab'` — the crate's own rule spells joiners out in display renders. Wrap in `gate::escape`.
- **11L5 — Derived `Debug` of `Error::Transport` renders the Status whole** (`client error.rs:24`): contingent on an embedder's `{:?}`; a manual Debug routing through `render_message` closes it.
- **11L6 — The library layer keeps the universal no-write claim** (`rdlt pipeline.rs:464-465`, `engine.rs:98-99`, `check.rs:7-8`): the CLI's honest engine-scoped wording was not mirrored — an embedder may promise users "check writes nothing" about connector-owned behavior the host cannot vouch for.
- **11L7 — Destination `check()` false positive on a trailing slash** (`reference destination/connector.rs:78-91`): `stat("/path/file/")` → ENOTDIR walks to the parent and passes while `connect` fails *transiently* — retry bait, worse than the documented dangling-symlink optimism.
- **11L8 — Reference source `check()` passes a directory its own read refuses fatal** (`source/connector.rs:53-58`): require `is_file()`.
- **11L9 — `ensure_table` refusal renders the table name raw** (`reference session.rs:105-116`): bounded at the wire, unbounded for direct Backend drivers; `render_diagnostic` for consistency with `part::name`.
- **11L10 — README workdir row self-contradictory and wrong for documents** (`README.md:173`): "unset means no WAL" is builder semantics; a document always defaults `.rdlt/<pipeline>` — an operator believing the row won't expect the WAL directory (which carries resume material) a run creates.
- **11L11 — README's constructed-`Document` example no longer compiles** (`README.md:142-153`): missing the two fields 066 added; README rust blocks are not doctested, so CI cannot catch it. Add the fields (or doctest the README).
- **11L12 — `RetryPolicy` has no delay floors** (`engine config.rs:31-57`, `retry.rs:66-97`): `base_delay: 0` → zero-delay hot-loop retries against a recovering destination; `base > max` silently degrades. Clamp in `with_retry_policy` like the sibling knobs.

---

## 5. New findings — Info

- **5.1 — Replay row-count accumulators wrap where every sibling checks** (`replay.rs:53, 161-162`): three `i64::MAX`-row NullArray batches wrap the u64 sums; pass-2's sits outside the per-batch `catch_unwind`. Bounded (the cross-check is a damage detector, the attacker controls both sides); `checked_add` for posture.
- **5.2 — WAL reconcile residuals (10M2-adjacent):** a shrink-race `set_len` extends the file minting a NUL hole (safe-direction Damaged); the `O_APPEND` terminator lands at live EOF in the grown case, where the doc overstates. Both within the racing writer's direct power; unpinned.
- **5.3 — Record-batch field count uncapped at the decode seat** (`client source.rs:122-158`): ~1–2 M tiny legal fields ≈ 3–4× frame — consistent with the admitted-frame posture, unlike 11M2's 11×.
- **5.4 — Engine/SDK concurrency pools uncoordinated** (per-run 16-default vs per-connector-process 1024): two runs against one served source can exceed the ceiling and receive typed refusals — documented behavior, no hang.
- **5.5 — Retry guard `attempt + 1` unchecked** (`retry.rs:159,166`): overflow at ~2³² attempts — practically unreachable; `u64` for posture.
- **5.6 — The new `check` contract has no conformance/certifier clause** — unit-pinned implementations, coverage gap by the suites' own convention.
- **5.7 — Reference `store.rs` corrupt-content refusals render raw serde errors** — local-disk-corruption reachability only.
- **5.8 — Stale `validate` spellings** in the Makefile comment and ADR 0001 (the CLI surface itself is clean and pinned).
- **5.9 — Doc simplifications honest at the seat that matters:** the Resources byte-budget threading note (the `from_document_with` seam is stated at the method); the duckdb caveat references the sibling repo.
- **5.10 — Reference `replay` clears all staging unkeyed** (pre-existing posture, consistent with the client-owns-staging model).

---

## 6. Recommended fix order

1. **11M1 + 11L3 + 11L4 + 11L5** — the render family's completion: bound the unexpected_reply intermediate; the three reference refusal seats; the mismatch joiner render; the Transport Debug. One helper discipline, five seats.
2. **11M2** — the streams-reply count cap + per-spec ceiling at the client seat (two gates, matching the row-cap precedent).
3. **11M3** — suffix-memoized chain resolution (O(N log N) scan); pin with a deep-chain manifest fixture.
4. **11L1 + 11L2** — the two remaining serve identifier gates (one-line mirrors).
5. **11L7 + 11L8 + 11L9** — the check-probe honesty pair and the ensure render.
6. **11L10–11L12, 5.1–5.10** — README corrections (doctest the README to keep them true), the retry floors, and the posture items.

---

## 7. Caveats

- Line numbers are from `bf4c5976` and will drift.
- **Verification confidence:** 11M1 was hand-verified at the materialization line plus the prost-derive Debug source; 11M2 at the ungated loop (no count check anywhere on the path); 11M3 at the memoization site (single-table insert confirmed directly). All six lanes executed their suites; the engine lane re-ran after a dispatch timeout with identical conclusions on the fixed items. The 066 surface findings (check path, retry, concurrency) carry the facade and engine lanes' line-level evidence with pins.
- **The series' trajectory:** High 4 → 6 → 3 → 2 → 1 → 0 → 0 → 0 → 0 → 0 → 0 across eleven rounds. The Medium band this round is three members of the two families the series has been closing all along (unbounded renders; uncounted materialization) plus one pre-standard seat in the WAL — each with a bounded, known-shape fix. Nothing found this round blocks a release under the project's own trust model; the fix list above is completion work, not remediation.
- Severity assumes the documented trust model (D-038-1 per `SECURITY.md`, re-verified against source this round).
