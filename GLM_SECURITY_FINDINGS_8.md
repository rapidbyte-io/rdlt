# Security Analysis 8 — rdlt workspace (eighth deep review)

**Date:** 2026-08-16
**Scope:** same surface as rounds 1–7, reviewed at **`83f98814`** on `047-security-hardening` (working tree clean) — on top of the wave-8 remediation commit (33 files, ~1,080 lines: the receipt-identity guard, the Float64 losslessness pair, the SPI-hoisted document ceiling across the remaining seats, the pass-2 row recount, and the config-surface completions).
**Method:** six parallel subsystem reviews (client/protocol; WAL; engine data-path; SDK serve + runtime; certify/testkit/reference; config surface/CLI/CI). This round every lane could AND DID execute tests through the containerized harness (serial runs; ~all suites re-run green). The three arrow-cast findings were re-verified by hand with a standalone probe against the pinned arrow 58.3.0 (results inline); the receipt-render finding was verified against the wave-8 diff directly.

**Severity scale (unchanged):** High — the stated adversary can crash, hang, or abuse the host trivially, **or a legal input shape silently loses, duplicates, or alters data**. Medium — narrower preconditions or reduced impact; contract violations short of corruption. Low — defense-in-depth, hygiene, pin gaps, availability-only edges. Info — observations and recorded postures.

---

## 1. Executive summary

**Every wave-8 finding is verified fixed at its seat** — 7M1's guard is sound (pure field equality before replay; the Mismatched spy pin proves neither replay nor publish runs; the serve relay preserves identity end-to-end; the engine discards returned receipts, so even a forged *publish* receipt is inert in-tree), the SPI ceiling helper covers every remaining parse seat on both wire sides, the identifier gates hold at their scoped seats, the loader's cell flush composes correctly with commits, and the config-surface completions (deny_unknown_fields, policy check, CI docs leg) are pinned and passing — `make docs` now runs green in PR CI.

**The second consecutive zero-High round** (trajectory: 4 → 6 → 3 → 2 → 1 → 0 → 0). What round 8 found instead is a tight cluster: **the numeric-losslessness family has three more members**, all on the structured passthrough's cast arm, all empirically confirmed on the pinned arrow:

1. **8M1** — the wave-8 Int64→Float64 guard checks only the **top-level** array; a struct field or list element widening Int64→Float64 bypasses it and silently rounds (probe: `2^53+1` → `2^53.0` through both `Struct` and `List` children). Exactly the corruption 7M6 exists to refuse, one nesting level down.
2. **8M2** — nanosecond→microsecond temporal casts **truncate toward zero** (probe: `1500ns→1µs`, `-1500ns→-1µs` — pre-epoch values round *up*). Arrow's default unit for many producers is ns; the engine's canonical unit is µs, so every ns-precision structured stream silently loses sub-microsecond data — contradicting the module's own "cast … where the cast is EXACT" claim.
3. **8M3** — Date64→Date32 truncates toward zero too, which **mis-dates pre-epoch intra-day values by one day** (probe: `1969-12-31T12:00Z` → day 0 = 1970-01-01).

These three share one fix: a recursive pre-cast exactness walk over (source type, target type) pairs — the same shape `join_column_types` already uses — refusing typed at any leaf the cast would alter. The wave-8 guard was the right idea executed one level shallow.

The other two Mediums: **8M4** — the 7M1 guard's own refusal interpolates the forged `load_id` raw (unescaped, uncapped, up to frame-cap size): terminal-injection and a 64 MiB diagnostic from the very message wave-8 added — the exact class every neighboring seat escapes; and **8M5** — the certifier's P10 closes only the receipt-identity half of its recorded F-4 bar: the no-`existing_receipt` double-publish shape (the wire-reachable exactly-once violation the SDK's own docs assign to the backend) is never driven, so a backend that re-applies rows on publish-after-receipt passes certification.

The crash-consistency and choreography audits came back **clean** again: the pass-2 recount is per-segment and overflow-safe, degrade-after-partial-application is safe for both shipped destinations (verified against their close/open paths), the loader's flush/commit interplay cannot double-write or lose a batch, and the serve session arms each yield exactly one reply on every restructured path.

---

## 2. Wave-8 fix verification

| Round-7 | Status | Evidence (current tree, `83f98814`) |
|---|---|---|
| 7M1 receipt identity | **Fixed** | `sdk/destination.rs:401-410`: field equality before replay; spy pin proves `calls == [ExistingReceipt]`; relay serde-transparent; engine discards returned receipts (`loader.rs:566-570`, `replay.rs:493-500`). **The refusal's own render leaks** → 8M4; **the certifier consumes receipts unchecked** → 8M5/8L8 |
| 7M2 document ceilings | **Fixed** | `decode_document` at the five serve seats + `stream_spec_json`; client delegates to the one SPI helper; full census: zero ungated untyped parses remain in the sdk serve modules. One hand-rolled spelling survives at the OUTBOUND config pre-send seat (no adversary, count-only — defensible). **Serve gates unpinned** → 8L7 |
| 7M3 serve identifier caps | **Fixed as scoped** | Open/Ensure(table+columns)/Write gated before backend or guard mutation. **Residual seats** (ExistingReceipt `load_id`, ReadState `pipeline`, `schema.parent`, Merge keys) → 8L6 |
| 7M4 runtime probe | **Fixed** | Gate + shared renderer; the single spec parse seat in runtime; IdMismatch still reachable |
| 7M5 hint Float64 exactness | **Fixed** | `build.rs:280-299`; misfit counting is positional so the new nulls tally; pin covers the boundary |
| 7M6 passthrough widening refusal | **Partial** | Top-level guard correct (short-circuits, right taxonomy; no other top-level lossy pair exists — `Float64 ⊔ Decimal = Utf8` escalates first). **Nested positions bypass** → 8M1; **temporal/date casts lossy at every depth** → 8M2/8M3 |
| 7L1 pass-2 recount | **Partial (code correct, unpinned)** | Per-segment reset, per-record comparison, overflow unreachable, degrade-after-partial-application safe for both shipped destinations (verified). **No test drives a mid-pass-2 mismatch** → 8L1 |
| 7L2 density doc | **Fixed** | Write-order claim matches the writer's code (record-time line, commit-time fsync); ext4 consequence named |
| 7L3 stream-name escape | **Fixed** | `Cow` Display renders the escaped form; wire pin passes. **No length cap at this seat** → 8L3 |
| 7L4 loader cell flush | **Fixed** | Width constancy by induction; single-flush; `finish` flushes; pin passes |
| 7L5 serve row-cap pins | **Fixed** | At-cap + precedence pins pass |
| 7L6 bytes-first receipts | **Fixed** | Boundary composes with `truncate_torn_tail`; the pin discriminates. **Interior invalid-UTF-8 still transient** → 8L9 |
| 7L7–7L11, 5.x infos | **Fixed** | Refusal naming, doc link (docs leg now enforces the class), deny_unknown_fields (three-shapes + typo pins pass), policy check at build (pin passes), doc figures re-measured correct, state pipeline identity at both session sites, two-Run pin matches fold semantics, CI docs leg runs and passes at HEAD |

---

## 3. New findings — Medium

### 8M1 — Nested Int64→Float64 widening bypasses the 7M6 guard: struct fields and list elements silently round
**Where:** `crates/rdlt-engine/src/shred/passthrough.rs:239-254` — the guard downcasts only the top-level array (`source.as_any().downcast_ref::<Int64Array>()`); the join at `:402-419` recurses struct fields and joins list item types via `widen`, so nested Int64→Float64 widenings are produced and reach `cast()`, which recursively casts children.
**Verified (probe, pinned arrow 58.3.0):** `Struct{f:Int64}` → `Struct{f:Float64}` and `List(Int64)` → `List(Float64)` both cast `9_007_199_254_740_993` to `9007199254740992.0`. Reachable: batch 1 with a struct field declared Float64, batch 2 with the same field Int64 carrying a >2^53 value. The JSONL path is guarded at all depths (`build_column` recursion → `scalar_float64`); only the structured path leaks.
**Impact:** the exact silent 1-ulp corruption 7M6 exists to refuse, one nesting level down.
**Fix:** extend the pre-cast exactness check recursively — walk (source type, target type) pairs depth-capped like `join_column_types_at`; wherever a Float64 leaf meets an Int64 leaf, scan that child's values and refuse with the same message.

### 8M2 — Nanosecond→microsecond temporal casts silently truncate on the structured path
**Where:** `passthrough.rs:255` (`cast(source, &target_type)`) with the engine's canonical µs targets (`build.rs:45-50`).
**Verified (probe):** arrow 58.3.0's timestamp/time downcast is integer `o / divisor` truncating toward zero — `1500ns→1µs`, `1_000_499ns→1000µs`, `-1500ns→-1µs` (pre-epoch values round *up*). `column_type_from_arrow` maps any `Timestamp(_, tz)` to the logical temporal type regardless of unit, so the first ns batch (arrow's default for many producers) truncates immediately.
**Impact:** silent sub-microsecond data alteration on every ns-precision structured stream — contradicting the module doc's "cast … where the cast is EXACT".
**Fix:** mirror the 7M6 discipline: before casting ns→µs (timestamp or time), scan for values not divisible by 1000 and refuse typed with the remedy (deliver unit-consistent batches, or declare the column text).

### 8M3 — Date64→Date32 mis-dates pre-epoch intra-day values by one day
**Where:** the same cast seat; arrow's `(Date64, Date32)` arm is `(x / MILLISECONDS_IN_DAY) as i32` — truncation toward zero, not day-floor.
**Verified (probe):** `1969-12-31T12:00Z` (−43,200,000 ms) → day **0** = 1970-01-01; a floor would give −1. Positive intra-day values keep their date.
**Impact:** silent one-day mis-dating for pre-epoch intra-day Date64 values.
**Fix:** refuse (or scan-and-refuse) Date64→Date32 when any value is pre-epoch and not a whole multiple of a day.

### 8M4 — The 7M1 guard's refusal interpolates the forged `load_id` raw: unescaped and uncapped connector-authored text
**Where:** `crates/rdlt-connector-sdk/src/destination.rs:401-407`; reached over the wire via the ungated `receipt_json` decode (`client/src/destination.rs:460-470`).
**Verified:** `LoadId` is an unvalidated transparent String newtype with a raw Display; the client applies no identifier gate to `receipt_json`. When the guard fires, `receipt.load_id` *necessarily differs* from the host's — so it is arbitrary wire text: ANSI/OSC-52 sequences and forged newlines render raw, and the value can be up to ~64 MiB (frame cap). The sdk's own pin asserts the raw name survives with a clean fixture.
**Impact:** the terminal/log-injection class the `sanitize` module exists to stop, plus an unbounded diagnostic violating the crate's own "a diagnostic line is not a firehose" principle — introduced by the wave-8 fix message itself.
**Fix:** render the forged identity through a shared escape and cap it (the 2048 discipline); the helper belongs beside `json.rs` in `rdlt-connector` so the sdk can import it.

### 8M5 — P10 certifies only the receipt-identity half of the recorded F-4 bar: the no-ask double-publish shape is never driven
**Where:** `crates/rdlt-certify/src/destination.rs:683-704` vs the F-4 record at `sdk/src/serve/destination.rs:43-60` ("drive `Publish` twice over the WIRE with no `ExistingReceipt`/`Replay` in between and assert the second either replays the first receipt or is refused — never silently re-applies").
**Verified:** P10 pass 2 always asks `existing_receipt` and `replay` first; its same-receipt arm asserts nothing about rows. A backend that re-applies rows on publish-after-receipt while returning the identical receipt passes P10, P12, and the D-suite (D3's re-commit rides the Session choreography, where the second publish never runs). Runtime hosts remain protected by the 7M1 guard — this is a certifier blind spot for a wire-reachable violation the SDK's own docs assign to the backend, bordering the High rubric line; Medium because it needs a non-conforming client and the fresh-mint arm *is* pinned.
**Fix:** with the row-counting probe available, assert row counts across pass 2's second publish; add the no-`existing_receipt` double-publish shape (the serve doc itself asks for this clause).

---

## 4. New findings — Low

- **8L1 — The pass-2 row recount has no pin.** The only mismatch pin drives the pass-1 check; nothing drives a mid-pass-2 mismatch (feasible via a spy `LoadSession` whose `apply_delta` — invoked between the passes — rewrites the segment). The wave-8 commit's "closing the window" claim is unpinned.
- **8L2 — Identifier control-refusals render the FULL value before the length gate runs** (4 seats: handshake identity, stream names, Arrow field names, part-event tables): a ~64 MiB name containing one control byte quotes the whole value escaped (~6× expansion). Check length before content, or cap the render.
- **8L3 — State-doc stream names have no length cap** (7L3 added the escape, not the cap): an ≤8 MiB doc can carry one multi-megabyte name into the refusal. Apply `is_oversized_identifier` at this seat.
- **8L4 — Same-count between-passes swap is undetected and undocumented:** the recount compares counts only; same-rows/different-content swaps pass both checks and commit. Threat-model-consistent (at-rest writer power) — but the 6M4 residual doc records only the microsecond window; one sentence delimiting what the recount closes (or a per-segment content digest) would make the boundary honest.
- **8L5 — Zero-row segment-record amplification:** ~141-byte `Segment{rows:0}` lines all naming one 762-byte empty-batch segment — the name gate admits repeated seqs — fit ~7.6 M records under the 1 GiB budget: minutes of recovery, ~15 M `spawn_blocking` handoffs, millions of staged empty batches. Bounded and availability-only; a span segment-count cap (or zero-row coalescing) would close it.
- **8L6 — Residual ungated serve identifiers:** ExistingReceipt's `load_id`, ReadState's `pipeline`, Ensure's `schema.parent` name, and `WriteMode::Merge` key columns — transient at the serve layer but reaching backend lookups and error text (the 7M3 rationale minus session-lifetime retention).
- **8L7 — The wave-8 serve/runtime gates are unpinned:** no test drives an over-ceiling session document, an over-1-KiB serve identifier, or an oversized probe `spec_json` — deleting a gate call passes the suite (the exact class 7L5's rationale names).
- **8L8 — The certifier consumes `existing_receipt` output without identity-checking it** (`certify/src/destination.rs:642-656`): the 7M1 bug shape survives certification — pairs with 8M5; two lines per site (parse + compare against the asked pair).
- **8L9 — Interior invalid-UTF-8 receipt corruption is classified transient** (reference `destination.rs:288-294`): the writer never produces it and a torn append is always newline-less, so bytes before the last newline failing `from_utf8` are permanent corruption — the same endless-transient wedge 7L6 fixed, under a different cause. Classify fatal, matching the parse arm.
- **8L10 — Part-filename tuple encoding is ambiguous across loads:** `{table}-{load_id}-{commit_seq}.jsonl` collides for `(t="a", L="b-c")` vs `(t="a-b", L="c")`. Engine-minted ids carry a fixed dash count so in-engine loads never alias; a direct-`Backend` host with dash-rich ids — the lane this template tutors — silently overwrites. Injective encoding (embed a tuple hash) closes it.
- **8L11 — JSONL temporal parses drop sub-µs digits uncounted** (`build.rs:405,353,368`): a 9-digit fraction truncates silently while the Decimal arm refuses over-scale fractions as counted misfits — the same inexactness class, two disciplines.
- **8L12 — `with_commit_policy`'s setter doc was hijacked by `with_batch_policy`** (`config.rs:87-101`): the one deliberately-infallible seat — the exact 7L10 posture decision — has no contract statement where callers see it.

---

## 5. New findings — Info

- **5.1 — Tonic transport `Status` messages render raw** (`error.rs:139-143`): h2 forbids CR/LF/NUL in headers but ESC-class bytes pass; bounded by transport header limits. Pre-existing, transport-dependent.
- **5.2 — Arrow-authored error renders in `decode_one_batch` are raw but bounded** (`{error}` behind the frozen prefix; `panic_text` capped 4 KiB): a hostile field name that trips an arrow *error* rides unescaped (the name gates run only on the success path). Frame-bounded, dependency-authored.
- **5.3 — Publish's returned receipt has no identity guard** (sdk `destination.rs:412`): inert in-tree (the engine discards it); 039-style embedders holding it as the token would want the mirror check. Defense-in-depth.
- **5.4 — serde boundary: integer literals beyond u64::MAX become rounded f64s** (`18446744073709551616` → Float64 column holding 1.8446744073709552e19): standard serde behavior; the escalation rule cannot see integer-ness past it. Accept-and-document.
- **5.5 — `saw_float` is a dead field** (`infer.rs:28,69`): written, never read (`joined == Float64` already implies it). Remove or comment.
- **5.6 — Two oversized-document refusal spellings remain** (serve/common's pinned handshake wording vs the SPI helper's): both fire at 8 MiB, counts-only; folding the handshake seats onto the helper finishes the consolidation.
- **5.7 — Serve-side `commit_meta_json` cursors are not per-cursor contract-checked** (document ceiling only): a rogue client can persist over-contract cursors that a later honest host `read_state` would refuse. No new capability for the same-uid adversary; consistency observation.
- **5.8 — Reference postures re-confirmed:** F-4 stands (7M1 strictly additive); the receipt log is unbounded and whole-scanned per commit (O(commits) per commit — correctness unaffected).
- **5.9 — Engine-direct embedders never run `CommitPolicy::check`** (both product surfaces covered; the loader's final flush keeps the degenerate policy from losing data). Recorded posture; one line at run entry if ever tightened.
- **5.10 — Verified clean:** desugar never constructs a shape the new refusals reject; CLI exit codes cover the new families; CI unchanged except the docs step; the two-Run fold, CRLF accounting, and WAL open-count/TOCTOU coverage verified; testkit fixtures all under the new caps; the reference certifies green through the gated serve path.

---

## 6. Recommended fix order

1. **8M1 + 8M2 + 8M3** — one generalized pre-cast exactness walk in the passthrough: recursively compare (source, target) leaf types (depth-capped like the join); refuse typed at any Int64→Float64 leaf with an out-of-range value, any ns→µs leaf with a non-divisible value, any Date64→Date32 leaf with a pre-epoch intra-day value. One walk, three refusal arms, pins for each nesting shape. This is the terminal form of the losslessness family — the third time a member has appeared one level shallower than the fix.
2. **8M4** — escape + cap the 7M1 refusal's forged-identity render (hoist a bounded-escape helper beside `json.rs`); fix the sdk pin to assert inertness.
3. **8M5 + 8L8** — the certifier's identity check on consumed receipts + the no-ask double-publish P10 shape with row-count assertions.
4. **8L1** — the pass-2 mismatch pin (spy session at the `apply_delta` seam).
5. **8L2 + 8L3** — length-before-content ordering at the four identifier seats; the state-doc name cap.
6. **8L6 + 8L7** — the four remaining serve identifier gates + one pin per gate class.
7. **8L9–8L12** and the Infos as scheduled hygiene.

---

## 7. Caveats

- Line numbers are from `83f98814` and will drift.
- **Verification confidence:** 8M1/8M2/8M3 were confirmed by a standalone probe executed against the pinned arrow 58.3.0 on the pinned toolchain (outputs quoted in §3); 8M4 was verified against the wave-8 diff and the sdk pin's own assertion; the wave-8 table's PARTIAL entries (7M6, 7L1) carry the reviewing lanes' line-level evidence plus executed test results. All six lanes ran their suites through the containerized harness this round (serial; everything green at HEAD before review began — 881/881).
- The High trajectory (4 → 6 → 3 → 2 → 1 → 0 → 0) with Mediums now clustering in fix-completeness (the nested guard, the render of a new refusal, the untested half of a certification bar) rather than new vulnerability classes indicates the review is scraping the bottom of the barrel the fixes keep restocking: each wave's new code introduces its own adjacent seats. The generalized-walk fix in order item 1 and the shared escape helper in item 2 are the two structural moves that end their families' enumeration games.
- Severity continues to assume the documented trust model (D-038-1 per `SECURITY.md`). The numeric-losslessness findings hold under honest actors with real data — they are correctness, not attack, findings.
