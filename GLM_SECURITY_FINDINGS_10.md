# Security Analysis 10 — rdlt workspace (tenth deep review — the final pass)

**Date:** 2026-08-18
**Scope:** the full workspace at **`ee6e1e6e` on `main`** (working tree clean) — reviewed on top of the round-9 remediation wave (commits `f7202a72..ee6e1e6e`, "061": the cast-walk range leaves + null-count belt, the reference publish guard, the bounded diagnostic renders, the source-Read admission ceiling, the vouched torn-tail reconcile, the backend-panic close).
**Method:** six parallel terminal-depth subsystem reviews (client/protocol; WAL; engine data-path; SDK serve + runtime; certify/testkit/reference; facade/CLI/CI). Every lane ran its suites through the containerized harness (the full workspace gate: **917/917 serial, green**, before review began) and carried two mandates: (a) verify every round-9 fix at its seat — pedantically, against the vendored arrow/tonic sources where the fixes guard dependency behavior — and (b) a final hunt at terminal depth, with nothing out of scope. The two Mediums below were re-verified by hand at their seats; the engine lane's fix verification was performed pair-by-pair against arrow-cast 58.3.0's actual implementation, and the client lane's Medium carries a live PoC.

**Severity scale (unchanged):** High — the stated adversary can crash, hang, or abuse the host trivially, **or a legal input shape silently loses, duplicates, or alters data**. Medium — narrower preconditions or reduced impact; contract violations short of corruption. Low — defense-in-depth, hygiene, pin gaps, availability-only edges. Info — observations and recorded postures.

---

## 1. Executive summary

**Every round-9 finding is fixed — 15 of 16 fully, one (the ADR doc drift) untouched by design of the wave and standing as Info.** The flagship closures verified at depth: the cast-exactness walk now carries all six leaf arms (three truncation, three range) **and** a recursive null-count belt that covers *every* safe-mode nulling arrow can produce — the engine lane re-derived the complete reachable (source, target) pair table against arrow-cast 58.3.0 and found the losslessness family at its **true terminal state** (one Low availability asymmetry remains, no corruption path); the reference connector's `publish` now carries the durable receipt guard its own SDK names as the only exactly-once defense at that seat, with the restaged-republish and cross-key pins driving the raw backend; the diagnostic-render family is complete at every seat the censuses can find — **SECURITY.md was re-verified claim-by-claim against the code and is exactly true.**

**Fourth consecutive zero-High round** (trajectory: 4 → 6 → 3 → 2 → 1 → 0 → 0 → 0 → 0). Two Mediums remain, and both are members of the one family that has resisted terminal closure all series — *the unbounded render*:

1. **10M1** — the arrow decode seat appends arrow's own error text raw: an 8 MiB legal frame whose schema field carries hostile *metadata* produces a 40 MiB error string through arrow's `Field` Display (PoC-verified; ~384 MiB at the full 64 MiB cap). Round 9 verified arrow's error renders inert and marked them Info; round 10's deeper PoC found the `metadata: {…}` interpolation round 9 missed — the 9M5 class through the Err-arm, three one-line fixes at the seats 9M5 itself hardened.
2. **10M2** — the wave-061 torn-tail reconcile's `read_to_end` is fstat-windowed but not `.take()`-bounded: a same-user writer appending concurrently keeps the reader chasing EOF, making the one read seat in the WAL whose bound is expectation-only. One line, matching the module's own two precedents.

Three Lows: the Ensure identifier walk is one level deep (nested struct field names escape the ceiling — the last remainder of the "every identifier at every seat" program); the reference's raw `replay` accepts any receipt and drops staging unconditionally (engine paths safe via the wrapper; template-hardening edge); and the struct⊔scalar join refuses rather than renders (an availability asymmetry with no corruption — the JSON path's twin handles it by widening to text).

The WAL six-arrow crash walk (including the new vouched-torn-tail path at every stacking depth), the certifier vacuity census, the belt-soundness analysis (no under-count constructible), and the SECURITY.md verification all came back clean. **The codebase is, by the evidence of ten rounds, as rock-solid as this review series can make it: what remains is two bounded-diff Mediums in a known family, three template-polish Lows, and recorded postures.**

---

## 2. Round-9 fix verification

| Round-9 | Status | Evidence (current tree, `ee6e1e6e`) |
|---|---|---|
| 9M1 Date64 i32-wrap | **Fixed** | `shred/cast.rs:164-195` — `i32::try_from(value / MS_PER_DAY)` leaf using the exact truncated-day expression arrow's `as i32` wraps on; disjoint from and ordered before the pre-epoch arm; edge pins (`2^31` refuses, `i32::MAX/MIN` pass) |
| 9M2 upscale overflow + decimal over-precision + belt | **Fixed** | `cast.rs:131-154` (`checked_mul` on min/max extremes — monotone-sufficient), `:196-227` + `rescale_decimal` (the decimal leaf is **load-bearing**: arrow's fast path *carries* out-of-precision values the belt cannot see); the belt `grown_nulls` (`arrow.rs:288-323`) compares logical null counts recursively and covers **all** safe-mode nulling, not just walked leaves (pinned by an intentionally-unmodeled pair); source nulls never double-counted (growth-only) |
| 9M3 reference publish guard | **Fixed** | `session.rs:148-170` — `find_receipt` match ⇒ clear staging + return prior (idempotent, replay semantics); cross-key fatal before any write; part-name/receipt key split closed by the guard; duplicate-line growth stopped; raw-backend pins (restaged republish, cross-key) |
| 9M4 nested-omission null-fill | **Fixed** | `arrow.rs:331-354` — name-projected manual construction + `new_null_array`, recursive; arrow's struct cast then takes its names-in-order fast path; walk and belt run on the projected array; no-Delta-churn pin |
| 9M5 ErrorFrame cap | **Fixed** | `gate::render_message` (cap 4096 on escaped output, true-source-length marker) at `error.rs:116,135`; worst-case message ≈ 4134 bytes; 4-MiB-BEL pin through all three mappers |
| 9L1 expected_role gate | **Fixed** | `serve/wire.rs:433-458` — length-first (no echo), then bounded render for unknown, frozen spelling for wrong-role; 1-MiB pin |
| 9L2 vouched torn-tail | **Fixed — stronger than suggested** | `writer.rs:51-87` — trim-to-last-newline (a leading newline would have *promoted* torn bytes to mid-manifest damage); complete-unterminated-record gets its newline; anything else truncates; window soundness proven (Discard ⇒ tail ≤ 5 MiB); two stacked-failure pins. **The read itself lacks its `.take()`** → 10M2 |
| 9L3 Read admission ceiling | **Fixed** | `serve/source.rs:99-109, 243-252, 390-397` — `try_acquire_owned` semaphore, permit in the FrameStream, `RESOURCE_EXHAUSTED` naming the ceiling; 1024-parked-reads pin; honestly documented as not-yet-configurable at both seats |
| 9L4 backend panics close | **Fixed** | `serve/destination.rs:697-754` — `CatchUnwind` future wrapper; the tail close is itself belted and *runs* on the panic path; slot Drop releases; `panic_on_write` pin asserts close in the log |
| 9L5 nested misfits counted | **Fixed** | `build.rs:135-209` — counter threaded through struct recursion + positional ScalarList counting; nested sub-µs pin counts exactly one Discard |
| 9L6 foreign-StateDoc pin | **Fixed** | testkit `with_foreign_state` hook (labeled non-conforming, no conformance suite uses it) + the engine pin |
| 9L7 Transport escape | **Fixed** | `error.rs:85` — Status message through `render_message`; filler + 1-MiB pins |
| 9L8 posture note | **Fixed** | `clause/p.rs:806-819` — names both real enforcement seats (the SDK wrapper and the backend guard), verified source-true |
| 5.4 private-dir cleanup | **Fixed** | `serve/wire.rs:185-188` — `remove_dir` on the chmod-failure path; pin |
| 5.6 doc drifts | **3 of 4 fixed** | runtime comment, space run, "at least" wording all landed with pins; **ADR drift untouched** (the 061 diff has no docs/ hunks) → Info |

---

## 3. New findings — Medium

### 10M1 — Arrow decode causes appended unbounded: hostile field *metadata* amplifies to a ~384 MiB error string through the one-batch refusal
**Where:** `crates/rdlt-connector-client/src/source.rs:213, 217, 240` — the three `{ONE_BATCH_REFUSAL}: {error}` appends; root seats arrow-ipc 58.3.0 `reader.rs:180, 245, 633` × arrow-schema `field.rs:990-1005`.
**Verified by live PoC (arrow 58.3.0, framing pre-pass included):** round 9 verified arrow's error renders inert and marked this seat Info — but checked only that field *names* are Debug-quoted. `Field`'s Display also renders `metadata: {metadata:?}`, and metadata is connector-authored, frame-scale text carried in the IPC schema. A legal 8 MiB stream whose schema field carries BEL-metadata and whose record-batch message omits that field's node produces a **41,943,155-byte** `ArrowError` through `reader.rs:633` — 5× amplification now, ~6× of the full 64 MiB cap ≈ **384 MiB** materialized inside `SourceError::fatal`. The framing pre-pass passes the frame clean (its declarations are honest); the control bytes are Debug-escaped (no terminal injection) — the size is the threat, the exact 9M5 class through the Err-arm. The same vector is reachable at the other two reader seats, and `DataType::Extension` name/metadata renders are a further instance.
**Fix:** wrap the three appends in `gate::render_message(&error.to_string())` — one helper, three one-liners, the 9M5 fix's own shape. This also closes the filler residual round 9 recorded.

### 10M2 — The torn-tail reconcile's `read_to_end` is fstat-windowed but not `.take()`-bounded: a concurrently-growing manifest makes the read unbounded
**Where:** `crates/rdlt-engine/src/wal/writer.rs:66-68` — the window derives from `metadata().len()` at time T1, then `read_to_end` drains to EOF at T2 with no hard bound.
**Verified:** `read_to_end` loops until a `read()` returns 0; a same-user writer appending at ≥ the reader's drain rate (both page-cache memcpys — comparable rates) keeps the reader chasing EOF while the Vec retains every byte. Every other read seat in the module hard-bounds with `.take()` precisely because the threat model is a same-OS-user directory writer (marker `take(len+1)`, sidecar `take(8 KiB+1)`, lines `take(5 MiB+2)`) — this is the one seat whose bound is expectation-only. Reachable through the vouched path (plant an un-clearable subdir over a Discard manifest → voucher → helper appends during `Wal::open`); static plants cannot trigger it (fstat bounds them).
**Impact:** unbounded recovery memory → OOM kill; availability/DoS only (the bytes are never interpreted beyond the tail logic — no corruption, loss, or duplication).
**Fix:** `(&mut manifest).take(window).read_to_end(&mut bytes)` — one line, matching the module's two existing precedents.

---

## 4. New findings — Low

- **10L1 — Ensure's identifier walk is one level deep: nested struct column names escape the ceiling** (sdk `serve/destination.rs:507-511`): `ColumnType::Struct { fields }` carries recursively nestable `Column`s whose names are never length-checked — megabyte-scale nested names bounded only by the 8 MiB document ceiling, defeating the arm's own stated rationale. Make the walk recursive (a `gate_column` helper, three lines).
- **10L2 — The reference's raw `replay` accepts any receipt and drops staging unconditionally** (`session.rs:136-146`): engine paths are safe (the SDK wrapper validates identity first), but a direct wire client replaying a fabricated receipt gets `replayed` while its staged rows are silently discarded — the template-hardening twin of the 9M3 guard. Verify `find_receipt` for the supplied receipt before clearing.
- **10L3 — Struct/list ⊔ scalar joins refuse rather than render** (`arrow.rs:206-229` × `types.rs:104`): a mixed-shape structured evolution lands on Json→Utf8, whose cast arrow refuses for non-primitive sources — a legal stream shape fails typed (nothing silently lost; the JSON path's twin widens to canonical text). Render struct/list to canonical JSON at the cast seat, or document the refusal as the v1 structured-stream contract.

---

## 5. New findings — Info

- **5.1 — ADR path/fact drift (the unfixed 5.6 remainder):** 0002 twice names `sdk::yaml` (folded into `config` at 051); 0001's D1 amendment still describes the removed desugar table. One dated amendment each.
- **5.2 — `capabilities_json` parses without the document ceiling** — verified fully typed, no `Value` materialization, `max_len` range-gated: symmetry only. The protocol README's "capped at both ends" blanket sentence overstates by this one field.
- **5.3 — The losslessness terminal table** (engine lane, recorded for the record): UInt64/Dictionary/Duration refused at admission; zone relabels epoch-exact; Time32→Time64 overflow-impossible; decimal joins upscale-only with the load-bearing leaf; byte-container casts typed-error on overflow; Binary→Utf8 invalid-UTF-8 belt-refused; the one gap is 10L3. The belt cannot under-count (list offsets unchanged across casts; struct children keep own validity; nulls never become non-null; dictionaries unreachable).
- **5.4 — WAL posture notes:** the repeated-name gate's cost argument is slightly optimistic vs hardlinks (proportionality survives); the reconcile's `set_len`/newline is not fsynced before the first append (power-loss re-glue self-heals to Discard→clear); the six-arrow walk including vouched stacking verified safe at every depth.
- **5.5 — Serve posture notes:** worst committed memory 1024 × per-read budget (tens of GiB at the extreme — honestly owned as "a bound on a runaway client, not on the host's honest budget"); Check/Streams/Spec remain rate-unbounded; "per connector process" = per `SourceServer` instance; the cursor-oversize refusal says "document ceiling" while quoting the 4 MiB cursor number.
- **5.6 — Reference/template postures:** P7 pass-by-construction (proto-enforced, pinned); retention ceilings convict production-sized streams (certification is a bounded-fixture posture — messages say so); mid-publish orphan parts reader-visible before the receipt (convergent on re-drive; new-load re-extraction doubling is outside the load-keyed contract); `find_receipt` is a linear scan (O(n²) cumulative for million-commit warehouses); `_staged-*` temps never swept; re-certification against a used dir fails D6 loudly; K-D fail arms have no designated rogue (census completeness, not vacuity).
- **5.7 — Misc:** runtime-spawn failure exits 2 (the config code) where 74 fits better; protocol crate declares unused serde/serde_json deps; the two render helpers (`render_message` vs `render_diagnostic`) implement the same law in two conventions — each documented for its seat, but 10M1's fix is the moment to consolidate; `Error::Dial`'s transport chain renders raw (no payload-echo seat found in tonic/h2); derived `Debug` of `Error::Transport` embeds the Status whole (Display is the safe path); an explicit JSON `null` at a ScalarList position builds a valid empty list (documented deliberate).

---

## 6. Recommended fix order

1. **10M1** — `render_message` on the three arrow-cause appends (+ pin with the metadata-PoC fixture). Closes the unbounded-render family's last member.
2. **10M2** — the `.take(window)` on the reconcile read (+ a concurrent-append pin if feasible). One line.
3. **10L1–10L3** — the recursive identifier walk; the replay receipt verification; the Json-join render-or-document decision.
4. **Infos** as hygiene — the ADR amendments and the README blanket-sentence scoping are the two that touch operator-facing documents.

---

## 7. Caveats

- Line numbers are from `ee6e1e6e` and will drift.
- **Verification confidence:** 10M1 carries a live PoC against the pinned arrow 58.3.0 (8 MiB frame → 40 MiB error, quoted in the lane report) plus my hand-verification of the three append seats; 10M2 was hand-verified at the read (the fstat-then-`read_to_end` shape confirmed directly). The fix table's flagship entries were verified against vendored dependency sources (arrow-cast pair-by-pair; tonic Status Display) and every lane executed its suites through the harness.
- **The series' trajectory:** High 4 → 6 → 3 → 2 → 1 → 0 → 0 → 0 → 0 across ten rounds; Mediums have collapsed from vulnerability classes to fix-completeness edges to, this round, two bounded diffs in the one family (unbounded renders) that regrew a member after each closure. The residual Low band is template polish and consistency. Barring the two Mediums above, nothing found in this round would block a release under the project's own trust model.
- Severity assumes the documented trust model (D-038-1 per `SECURITY.md` — itself verified claim-by-claim this round). The numeric findings hold under honest actors with real data.
