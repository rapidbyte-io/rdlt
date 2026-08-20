# Security Analysis 9 — rdlt workspace (ninth deep review — post-refactor)

**Date:** 2026-08-18
**Scope:** the full workspace at **`f7202a72` on `main`** (working tree clean) — reviewed for the first time since the 057–060 refactor waves rebuilt the crate layouts: the engine's surface (`engine.rs`, `run/` by noun), shred by input (`json/arrow/resolve/types/cast/limits`), WAL by noun (`format/dir/writer/segment/scan/replay`), the facade on "the blueprint" (`pipeline.rs` + `document/`), the reference connector split into `destination/{config,connector,part,session,store}` + `source/…`, the sdk's handshake hoisted to `serve/wire.rs`, and rdlt-core slimmed (naming moved to the engine).
**Method:** six parallel subsystem reviews (client/protocol; WAL; engine data-path; SDK serve + runtime; certify/testkit/reference; facade/CLI/CI). Each lane carried two mandates: (a) verify every wave-9 fix **survived the refactor at its new home** — moved code is the highest-drift-risk code — and (b) a fresh comprehensive pass over the new layout, with the seams the refactor created as primary targets. The full serial suite was executed through the containerized harness before review began (**899/899 green**); each lane also ran its own suites. Every Medium below was re-verified by hand at its seat (the engine-lane cast findings against the vendored arrow-cast 58.3.0 source; the reference finding against both split modules).

**Severity scale (unchanged):** High — the stated adversary can crash, hang, or abuse the host trivially, **or a legal input shape silently loses, duplicates, or alters data**. Medium — narrower preconditions or reduced impact; contract violations short of corruption. Low — defense-in-depth, hygiene, pin gaps, availability-only edges. Info — observations and recorded postures.

---

## 1. Executive summary

**Every wave-9 fix survived the 057–060 refactors intact at its new home — 100% of the spot-checked gates, with their pins.** The reviewers diffed the old seats against the new ones (`refuse_inexact_cast` → `shred/cast.rs`, `render_diagnostic` → `gate.rs`, `receipt_matches` → `clause/p.rs`, the density rule → `segment.rs`, the duplicate-name gate → `scan.rs`, the receipt guard → sdk `destination.rs`, the document refusals → `document/model.rs`): same orderings, same constants, same frozen spellings. Two fixes came back **stronger** than committed (the state-doc stream-name gate gained the content check; the commit-policy check is now double-seated). No refactor drift was found in any lane — a genuinely clean structural migration.

**Zero High findings for the third consecutive round** (trajectory: 4 → 6 → 3 → 2 → 1 → 0 → 0 → 0). What remains clusters in two places:

1. **The cast-exactness walk models truncation but not range.** The walk built in wave 9 refuses values arrow would *round*; it does not refuse values arrow would *wrap* or *null*. Three concrete members, all verified against the vendored arrow-cast source: a Date64 beyond ±(2³¹−1) days takes an **unchecked `as i32`** in arrow's cast and silently becomes a wrong date (9M1); an extreme-magnitude timestamp upscaled under CastOptions' safe mode becomes a **silent NULL** with no misfit counted, and the same safe-mode nulling covers decimal values exceeding their declared precision (9M2). These are the same silent-losslessness-violation class as 8M1–8M3 — the walk's terminal form still needs its range dimension.
2. **The reference connector's raw `publish` still lacks the durable receipt guard its own SDK documents as "the ONLY thing that saves exactly-once" at that seat** (9M3). Every engine-ridden path is protected (the `Session` wrapper guards both legs), but a choreography-violating client driving the wire directly can restage rows under a receipted `(load, seq)` and silently replace the committed part — and the reference is the template third parties copy. The certifier's new pass 3 catches fresh mints but cannot distinguish deterministic-overwrite from refusal (no row read-back), so the reference certifies green while missing the guard its own contract states.

One availability bug worth its Medium on its own: **a structured batch omitting a *nested* struct field refuses the whole push** where top-level omission null-fills — an ordinary producer shape (optional nested fields) fails the run with a confusing cast error (9M4). And the client lane found the last unbounded escape seat: ErrorFrame messages are escaped but uncapped, so one legal 64 MiB frame of control bytes expands to a ~450 MiB error string (9M5).

The WAL lane came back completely clean apart from one stacked-failure availability residual; the facade lane found nothing above Info (four doc/cosmetic drifts); the crash-consistency walk and the certifier vacuity sweep both hold.

---

## 2. Wave-9 fix-survival verification

| Wave-9 fix | Status | New home (evidence) |
|---|---|---|
| 8M1–8M3 recursive cast-exactness walk | **Survived** | `shred/cast.rs:36-173` — all three leaf arms, struct-by-name/list/dictionary recursion; all five pins moved to `shred/arrow.rs` tests |
| 8M4 bounded diagnostic render | **Survived** | SPI `gate.rs:90-107`; the receipt guard's forged identity rendered through it (sdk `destination.rs:398,418`); hostile-identity inertness pin green |
| 8M5+8L8 certifier receipt identity + pass 3 | **Survived** | `clause/p.rs:851-864` (`receipt_matches`), consumed at `:732` and `:768`; pass 3 at `:806-837`; the no-ask rogue `FreshMintOnNoAskRepublish` pinned at `:2307` |
| 8L1 pass-2 recount + swap pin | **Survived** | `replay.rs:183`; pin at `:275` (swaps at the `ensure_table` seam) |
| 8L2 length-before-content ordering | **Survived** | `gate.rs:30` before `:37`; identical at all four seats |
| 8L3 state-doc stream-name cap | **Survived — strengthened** | `destination.rs:509-511` now applies the FULL identifier gate (length AND content), where wave-9 had length-only |
| 8L4 same-count residual doc | **Survived** | `replay.rs:174-182` (in-source, accurate) |
| 8L5 duplicate-segment-name gate | **Survived** | `scan.rs:283-284, 345-353`; pin `a_span_naming_one_segment_twice_is_damage` at `:1101` |
| 8L6 serve identifier gates | **Survived** | Full inbound census re-enumerated at the new homes — every request field gated except `Handshake.expected_role` → 9L1 |
| 8L7 serve-gate pins | **Survived** | Oversized-document and oversized-identifier pins green in the new suite layout |
| 8L9 interior-UTF-8-fatal | **Survived** | reference `store.rs:160-166`, both pins green |
| 8L10 injective part names | **Survived** | `part.rs:63-72` (domain-separated blake3 digest); gate on every construction path (one caller, grep-verified) |
| 8L11 temporal sub-µs misfit | **Survived** | `fraction_within_micros` `build.rs:316-325` gating all three builders; pin green |
| 8L12 setter doc | **Survived** | `config.rs:99-119` with the 7L10 posture stated |
| 5.3 publish mirror guard | **Survived** | sdk `destination.rs:411-422` + its pin |
| Density rule / layout gates / ctime compare / fsync ordering / marker clear / O_NOFOLLOW | **Survived** | `segment.rs`, `writer.rs`, `dir.rs`, `run/lock.rs` — every pin relocated and green; the six-arrow crash walk re-verified |
| Facade knobs / policy check / CI docs leg / 8 MiB cap / YAML gate / deny_unknown_fields | **Survived** | `pipeline.rs:126-138, 200-202`; `document/model.rs:133-171`; the scanner in sdk `config.rs:178`; document-owned `WriteMode` with refusal Visitor — **stronger than the old `WriteModeSpec`** |

---

## 3. New findings — Medium

### 9M1 — Date64→Date32 out-of-range values wrap silently: the walk checks mis-dating but not range
**Where:** `crates/rdlt-engine/src/shred/cast.rs:139-155` (the walk's Date64 arm refuses only `value < 0 && value % 86_400_000 != 0`); the mechanism is arrow-cast 58.3.0 `cast/mod.rs:1697-1700` — `unary(|x| (x / MILLISECONDS_IN_DAY) as i32)`, an **unchecked `as i32`**.
**Verified:** a Date64 millisecond magnitude beyond ±(2³¹−1) days (~±5,879 years — i.e. `|ms| > ~1.9×10¹⁷`, comfortably expressible in an i64 an IPC batch carries) truncates to the low 32 bits: a silently wrong date, not a null, not an error. Nothing engine-side validates Date64 range; `column_type_from_arrow` maps any Date64 to `Date` whose arrow target is Date32, so the **first structured push** of a Date64 column takes this cast.
**Impact:** one hostile or badly-buggy producer value silently corrupts every stored date in the batch — the exact losslessness class the walk exists to refuse; the wave-9 pin covers the mis-dating arm, not the wrap arm.
**Fix:** extend the Date64 leaf to also refuse when `value / 86_400_000` falls outside `i32::MIN..=i32::MAX`; pin with an out-of-range Date64 batch beside the existing pre-epoch test.

### 9M2 — Safe-mode cast overflow becomes a silent, uncounted NULL: the walk's missing overflow dimension
**Where:** the walk covers ns→µs truncation but not unit-upscale overflow or decimal-precision overflow; engine calls plain `cast()` (`shred/arrow.rs:228`) = `CastOptions::default()` whose `safe: true` (arrow-cast `mod.rs:93-99`) turns `checked_mul` failure into NULL (`mod.rs:1806-1812`) and decimal out-of-precision into NULL (`cast/decimal.rs`, `unary_opt`).
**Verified against the vendored source:** a `Timestamp(Second)` value beyond ~9.2×10¹² seconds (~year 292,471) upscaled to µs overflows i64 → NULL; a decimal value whose magnitude exceeds the declared precision (arrow does not validate decoded values against precision — the codebase itself notes the validator gap) → NULL. The arrow path has **no misfit accounting** (only the JSON build path counts misfits), so the loss is silent *and uncounted* — unlike every JSON-path loss, which is counted-as-Discarded by contract.
**Impact:** silent uncounted data loss on extreme or hostile arrow-native values, from a legal frame. Same root cause as 9M1: the walk models rounding, not range.
**Fix:** extend the walk's timestamp arm to any unit change (refuse when the upscale multiplier would overflow for the max present value — a cheap per-leaf scan), and add a decimal magnitude-vs-precision leaf (or count safe-mode nulls as misfits by comparing null masks before/after the cast).

### 9M3 — The reference backend's `publish` lacks the durable receipt guard its own SDK names as the only exactly-once defense at that seat
**Where:** `crates/rdlt-connector-reference/src/destination/session.rs:148-177` — no `find_receipt` anywhere in `publish`; the stated obligation at `sdk/src/serve/destination.rs:24-28`: "a foreign client CAN send Publish twice … the ONLY thing that saves exactly-once is the destination's own durable receipt guard inside `Backend::publish`".
**Verified:** the wire serve layer passes Publish's decoded meta straight to the raw backend, deliberately unrefereed. On a no-ask republish of a receipted `(load, seq)`: (a) a duplicate receipt line appends unconditionally — a looping client grows the log without bound; (b) a second publish carrying **restaged rows** rewrites the part under the same deterministic name — the committed part's rows are silently **replaced** while both receipts vouch; (c) part names key on the session's open load while the receipt keys on `meta.load_id`, so a cross-keyed publish lands a receipt over another load's part names. Every engine-ridden path is protected (the `Session` wrapper guards both legs; the memory destination guards), but the sdk's trust model explicitly scopes choreography-violating clients to this seat, and the reference is the template third parties copy.
**Impact:** "reference loses rows" under the sdk's stated threat model — pre-existing (the split predates it; the rounds missed it because the engine-side guard made it latent), not a refactor regression. The certifier's pass 3 cannot catch it: deterministic-overwrite + same-receipt is indistinguishable from refusal without a row read-back.
**Fix:** at the top of `publish`, consult `store::find_receipt(&meta.load_id, meta.commit_seq)` — on a match, return the prior receipt (or refuse fatal); key part names on `meta.load_id` or refuse a cross-keyed publish. Pin with a restaged-republish-over-receipted-load test.

### 9M4 — A structured batch omitting a NESTED struct field refuses the whole push, where top-level omission null-fills
**Where:** `crates/rdlt-engine/src/shred/arrow.rs:212-235` (assembly) + `shred/cast.rs:59-60` (comment claims a missing target field is "null-filled elsewhere" — true only at the top level); arrow-cast `mod.rs:2223-2260` + `struct_array.rs:131-134`.
**Verified:** registry struct `{a, b}`, later batch struct `{a}`: the join keeps `{a,b}`, the source type differs, the walk passes (it skips target fields missing from the source), `cast()` zips source columns against target fields, stops at the shorter list, and `StructArray::try_new` errors on the length mismatch — surfaced as `cannot cast Struct{a} to Struct{a, b}` and **the push is refused**, instead of null-filling `b` the way `new_null_array` does for omitted top-level columns (pinned behavior).
**Impact:** a legal, ordinary producer shape (optional nested fields across schema evolution) fails the run with a misleading error — an availability/evolution bug, not corruption (typed refusal). Inconsistent with the append-only/no-churn contract the module documents and with the top-level omission pin.
**Fix:** in the assembly's struct-mismatch branch, build the target struct manually (project source fields by name, `new_null_array` for the missing) instead of casting the whole column — or detect the missing-field shape and refuse with the accurate message. Pin with a batch-omits-struct-field test mirroring the top-level one.

### 9M5 — ErrorFrame messages are escaped but unbounded: one legal frame yields a ~450 MiB error string
**Where:** `crates/rdlt-connector-client/src/error.rs:113,132` — `from_frame`/`handshake_refusal` run `gate::escape(&frame.message).into_owned()` with no cap.
**Verified:** the frame is legal up to `MAX_FRAME_BYTES` (64 MiB); one-byte control chars expand ~6–7× through the escape, so a single hostile ErrorFrame materializes a ~450 MiB `String` inside the SPI error, roughly doubled again at Display. Every sibling diagnostic seat caps (`PANIC_TEXT_CAP` 4096, `REPLY_RENDER_CAP` 2048, the receipt guard 256) — this is the one escaped-but-unbounded seat. Pre-existing since before the refactor (not drift).
**Impact:** trivially-executable memory amplification from one legal frame — OOM-kill pressure on a memory-limited host, the exact threat class the document ceilings were built against.
**Fix:** apply a `render_diagnostic`-style cap at the two seats (escape a bounded prefix, name the true length).

---

## 4. New findings — Low

- **9L1 — `HandshakeRequest.expected_role` is the one ungated inbound field, rendered verbatim into the refusal** (sdk `serve/wire.rs:420-426`): a ~64 MiB role echoes back un-escaped in the ErrorFrame. Apply the identifier ceiling or render through `render_diagnostic` — the inbound census's last row.
- **9L2 — Vouched-residue append can glue the new Run header onto a torn final line** (WAL `writer.rs:107-120` × `run/recover.rs:80-90`): stacked failures (torn-tail Discard + failed clear + vouched run crashing again) leave a glued line the next scan classifies Corrupt → safe-direction degrade, unpinned. Write a leading newline in the vouched branch (the scan's blank-line arm already tolerates it); pin with a torn-tail voucher fixture.
- **9L3 — No process-wide cap on concurrent source-Read RPCs or accepted connections** (sdk `serve/source.rs:356-426`): per-read bounded (~72 MiB worst, pinned) but multipliable across connections; the destination role's one-session ceiling has no source-side analogue. A small semaphore mirrors it.
- **9L4 — A connector-Backend panic skips the best-effort `close`** (sdk `serve/destination.rs:699-705`): the module doc admits the abort gap but not the panic gap; the session leaks until process death. A `catch_unwind` belt around backend calls, or extend the doc's admitted-gap note.
- **9L5 — Nested temporal sub-µs values null silently uncounted** (engine `build.rs:108-126` misfits compare only top-level cells): the JSON-path round-8 fix's nested twin — inference accepts a 7-digit-fraction struct field, the build gate nulls it, no `Discarded` counted. Thread a misfit counter through `build_column`.
- **9L6 — The state-identity defense-in-depth check is unpinned** (`run/recover.rs:186-192`): the memory destination conforms, so the non-conforming arm is never exercised; the WAL-occupant half IS pinned. A testkit hook to seed a foreign StateDoc closes it.
- **9L7 — Transport Status text relies on Debug quoting; the Hangul-filler class rides raw** (client `error.rs:142-147`): the `unexpected_reply` seat covers exactly this residual with `gate::escape`; the Transport arm doesn't. One-line fix.
- **9L8 — Certifier F-4 posture note names a backstop that does not exist** (`clause/p.rs:810-814`): pass 3's comment says silent re-application "is the table-probe's read-back to catch — the D-clauses' probe gate", but the D-suite drives re-commit through the guarding `Session` wrapper and never reaches raw `publish`. Fixing 9M3 closes the substance; the comment should name the sdk wrapper as the enforcement seat, or a probe-gated restage-readback clause should be added.

---

## 5. New findings — Info

- **5.1 — arrow decode causes appended unescaped** (client `source.rs:210-240`): verified largely inert by a live PoC against arrow-ipc 58.3 (Field Display Debug-quotes names; panic payloads carry enum Debug values only) — the filler class and arrow-dependence remain.
- **5.2 — `capabilities_json` parses without the document ceiling** (client `handshake.rs:256-276`): fully typed target, no `Value` materialization — symmetry only.
- **5.3 — Worst hostile WAL quantified under all gates:** a 1 GiB manifest of distinct zero-row segments forces ~34 GB allocated + ~9 M inodes (the duplicate-name and density gates make attacker disk ∝ recovery work) for ~10 minutes of bounded recovery and ~1.5 GB peak — no unbounded path remains.
- **5.4 — Reference `create_private_dir` leaks its dir on a failed chmod** (serve `wire.rs:168-182`): reclaimed by the next sweep; negligible.
- **5.5 — Test-harness flakes, not production** (`a_destination_spawn_carries_its_role` ETXTBSY under parallel; unique fixture filenames fix it).
- **5.6 — Facade doc drift (all cosmetic):** a stale runtime comment about dial-budget threading (`local.rs:58-63`); a mid-sentence space run in a frozen refusal (`cli events.rs:40`); ADR path drift (0002 names `sdk::yaml`, now `sdk::config`; 0001 describes the removed desugar table); the oversized-file refusal understates true size ("is {MAX+1} bytes" where "at least" would be honest).
- **5.7 — Deliberate asymmetries re-confirmed:** JSON escalates inexact int⊔float to Utf8 where arrow refuses typed (registry committed — documented); P7 passes by construction (protobuf enforces the map shape — pinned by the P3 rogue); kill-matrix receipt consumption unchecked (P10 owns identity; K verifies rows through the probe).

---

## 6. Recommended fix order

1. **9M1 + 9M2** — the cast walk's range dimension: Date64 i32-range leaf, timestamp upscale-overflow leaf, decimal precision-vs-magnitude leaf (or post-cast null-mask misfit counting, which covers all safe-mode nulling in one seat). One walk, three arms, pins for each — the true terminal form of the losslessness family.
2. **9M3** — the reference's durable receipt guard in `publish` (+ the cross-key refusal), with the restaged-republish pin; closes 9L8's substance with it.
3. **9M4** — the nested-omission null-fill (or accurate refusal) in the assembly's struct branch; the ordinary-producer availability bug.
4. **9M5 + 9L1 + 9L7** — the last three render seats (ErrorFrame cap, expected_role gate, Transport escape) — one helper, three one-liners.
5. **9L2, 9L5, 9L6** — the vouched-branch newline + pin; the nested misfit counter; the foreign-StateDoc testkit hook.
6. **9L3, 9L4** — the source-side admission semaphore and the backend-panic belt (or doc note).
7. **Infos** as hygiene.

---

## 7. Caveats

- Line numbers are from `f7202a72` and will drift.
- **Verification confidence:** 9M1/9M2 were verified against the vendored arrow-cast 58.3.0 source (`as i32` wrap, safe-mode `checked_mul`→NULL) and the walk's own code re-read directly; 9M3 was verified against both split modules and the sdk's obligation text, with the certifier's pass-3 limits checked in `clause/p.rs`; 9M4's cast-chain was traced through arrow-cast's struct fallback; 9M5's expansion arithmetic re-derived. The WAL lane executed its suites and re-walked the six crash arrows; the facade lane ran both crates' suites; all lanes re-ran their pins at the new homes.
- The fix-survival table rests on the six lanes' seat-by-seat diffs against the wave-9 tree (`4963f59d`), including pin relocation checks (a pin that moved with its code and still asserts the right thing).
- The High trajectory (4 → 6 → 3 → 2 → 1 → 0 → 0 → 0) with Mediums now exclusively in (a) dependency-behavior dimensions the walk hasn't modeled and (b) one pre-existing reference-connector gap the engine-side guard made latent — both bounded, known-shape work. The 057–060 refactor itself introduced **zero** regressions found by this review, which is the strongest structural finding of the round.
- Severity assumes the documented trust model (D-038-1 per `SECURITY.md`). The numeric findings hold under honest actors with real (extreme) data.
