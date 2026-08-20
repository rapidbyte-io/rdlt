# Security Analysis 16 — rdlt workspace (sixteenth deep review)

**Date:** 2026-08-20
**Scope:** the full workspace at **`6c769e35` on `main`** — three waves since round 15 (`29d456b2..HEAD`, +1115/−117 across 24 files): **072** (the round-15 worklist: the batched rollback, the probe-admission semaphore, the emit/cursor/nested count caps, the receipts scan resume, the depth note, the measured numbers), **071** (a NEW `pem::Material` type in the SDK — inline-PEM-vs-path config plumbing for the future TCP+mTLS binding; no transport landed), and **070** (a YAML wildcard/alias gate fix in the SDK config scanner). Reviewed on top of all three.
**Method:** six parallel comprehensive subsystem reviews (client/protocol; WAL+lineage; engine shred/data-path; SDK serve+runtime+**the new PEM surface**; reference/certify/testkit; facade/CLI/docs). Each lane verified its assigned round-15 findings at the seat and hunted the new surface. Full workspace gate: **green — serial, exit 0, 44/44 suites ok** (confirmed on a second warm run); the six lane suites individually all passed. The three new Mediums were hand-verified at their seats.

**Severity scale (unchanged):** High — the stated adversary can crash, hang, or abuse the host trivially, **or a legal input shape silently loses, duplicates, or alters data**. Medium — narrower preconditions or reduced impact; contract violations short of corruption. Low — defense-in-depth, hygiene, pin gaps, availability-only edges. Info — observations and recorded postures.

---

## 1. Executive summary

**Round-15's two Mediums are properly fixed** — the rollback is batched to one `retain`/`rebuilt`/derive per push with a `rebuilds`-meter pin, and the probe-admission semaphore (64) guards `check` and `streams` at both roles with a typed RESOURCE_EXHAUSTED refusal. The count-family trio landed (the emit's total-frame cap refuses before the join; committed cursor values ride the same iterative node walk as read cursors at both Publish and Replay; the nested struct-field count runs in the same recursive walk, held to the same 4096). **But two of the fixes are partial at their edges**: the receipts scan-resume memo forfeits exactly the canonical case (from a load's *second* commit onward it rescans the whole log — the O(N²) 15L5 named is still live), and the depth note states the bare-column boundary (42) where the Delta-record boundary the scan actually hits is 39–40, with the pin guarding the wrong artifact.

**Three new Mediums — two are old families' next members, one is the new feature's own hole:**

1. **16M1 — the DiscardValue overlay is the last unbounded linear per-key walk**: `Row::top_level` scans `nulled` (a `Vec<String>`) with `.any()` per lookup, called per offense per row at enforcement and per column per row at build — K=4096 discarded keys over the value budget prices a legal push at ~3.3×10¹⁰ compares ≈ **30–65 s of uninterruptible CPU**, repeatable every push. The observation seat got its `SlotIndex`; the overlay never did.
2. **16M2 — the new `pem::Material` ships the 13M1 class on the template surface**: `read()`/`read_to_string()` do a bare `std::fs::read` of the config-authored path — no file-kind check, no symlink refusal, no size ceiling. `/dev/zero`, a FIFO, or a huge file OOMs/hangs the connector exactly as the reference source did before round 15's fix. No in-tree caller exists yet (the type is the *blessed primitive* connectors will reach for), which keeps it Medium today — the first adopter inherits the hole.
3. **16M3 — the handshake is the one remaining seat that runs connector code without admission**: every *failed* attempt (and a rogue client may fire unbounded concurrent ones, each with a fresh 8 MiB `config_json`) runs the full parse → validate → assemble path — connector-authored code — plus the redaction sweep in the refusal arm. 072's own completeness claim ("every other seat that reaches a connector's backend already has an admission bound") makes this stale the day it shipped.

The Lows are edges and doc-truth (the reconcile's mid-tail gluing corner, two micro-linear scans in the rollback path, the `pem` feature being **unreachable by every gate** — its redaction pins never run, the one security property the type has — plus the fix-wave's own doc regressions: an over-replaced field name, a five-lists-four count, a false connector-authoring claim). Tenth consecutive zero-High round.

---

## 2. Round-15 fix verification

| Round-15 | Status | Evidence (current tree, `6c769e35`) |
|---|---|---|
| **15M1** rollback quadratic | **Fixed** | `revert_columns` (batched slice): restores in place slot-stable, removals in ONE `retain`, ONE `rebuilt`, ONE nested-fields derive (`table.rs:227-251`); single call site (`resolve.rs:387-394`); pin `a_wide_discard_rebuilds_the_column_index_once` asserts `0 < rebuilds ≤ 1` (regression = 64). Edge verdicts: snapshot-absent corner still unreachable; infallible mid-batch; no stale-index window. Two micro-Lows → 16L4/16L5 |
| **15M2** unary admission | **Fixed** | `MAX_CONCURRENT_PROBES = 64` (`wire.rs:720`), `try_acquire_owned` BEFORE shell/backend at source check (`serve/source.rs:358`), source streams (`:432`), destination check (`serve/destination.rs:382`); typed RESOURCE_EXHAUSTED naming the number; one semaphore shared across both services; seat-level pin (wire-level pin would need a parking connector — noted). **The handshake seat missed** → 16M3 |
| **15L1** emit total cap | **Fixed** | Running `blob.len() + delimiter + line.len() > MAX_FRAME_BYTES` check BEFORE `extend_from_slice` (in-place join, no second copy); 16 × ~8 MiB pin refuses at the ~9th line |
| **15L2** cursor values | **Fixed** | `gate_commit_meta` runs `refuse_dense_cursor` over every `(stream, cursor)` value at Replay AND Publish; 200k-node flood pin over the wire |
| **15L3** nested field count | **Fixed** | `gate_column_at` carries a running `counted` across the recursion, same 4096 as the top level; 5000-hidden-fields pin; exhaustive match |
| **15L4** replay-scan depth | **Partial** | A bound is stated at the door and pinned in format.rs — but the stated 42 is the BARE-COLUMN figure; measured (scratch-crate, pinned serde) the whole-`WalRecord::Delta` line hits the serde-128 limit at **39–40** struct levels (create-delta shape 39), and the pin guards the bare column, so envelope changes move the real boundary while the pin stays green → 16L6 |
| **15L5** receipts scan resume | **Partial** | Memo implemented (complete-line resume points, shrink invalidation, torn-tail re-read, both-direction equivalence pin) — but `load_seen` forces a full rescan from a load's SECOND commit (`store.rs:236` + `session.rs:65-68`), forfeiting the canonical one-load-many-commits case: commit #k reads k−1 lines, N commits cost N(N−1)/2 — the O(N²) 15L5 named, still live → 16L7. No cost pin |
| **15L6** UnexpectedEof | **Fixed** | The resume-window `read_io` maps it to the fatal shrink refusal (`cursor.rs:119-128`); mid-read-race shape un-pinnable, code-read verified |
| **15L7** doc leftovers | **Mostly fixed** | 35 clauses ✓ (verified against the clause arrays); wire field name ✓ — but the replace-all over-reached at README:295-296, now naming a nonexistent field on the type it cites → 16L8; "five families" ✓ but the parenthetical still lists four → 16L9; `--hostile-config` help still says D7/S5 → 16L10 |
| **15L8** anchor arithmetic | **Fixed — measured** | Real serialization in a pin: minimal spec exactly 1.22× (86 wire / 105 retained), collections 5.04× asserted in (4.0, 7.0); the inversion is machine-checked dead. Residue: the hint-shape clause still quotes a wrong wire spelling and claims 5× where the model gives ~3.2× → 16L11 |
| **15L9** freeze comment | **Fixed** | Both manifests carry the identical dated lift; separate-publish statement aligned. lib.rs crate docs still open → 16L12 |
| Info 5.3 (reply-channel doc) | **Fixed** | "128 MiB" with the arithmetic |
| Infos 5.1/5.2/5.7/5.10 | **Postures unchanged** | Wide-object 2× peak acknowledged-not-gated; the test index-bypass stands; the append `created` window; part orphaning |

---

## 3. New findings — Medium

### 16M1 — The DiscardValue overlay is the last unbounded linear per-key walk in the shred: O(R·K²) per legal push under Discard policies
**Where:** `crates/rdlt-engine/src/shred/resolve.rs:53-62` (`Row::top_level`: `self.nulled.iter().any(|k| k == key)` — a linear scan of the nulled-keys Vec per lookup), driven per offense per row by `enforce_discards` (`resolve.rs:398-425`, with `row.nulled.push` growing the scanned list per nulling offense) and per column per row by `build_batch` (`shred/build.rs:111-117`).
**Verified by hand at the scan site.** The observation seat got its `SlotIndex`; the arena's `obj_get` rides the persisted wide-object index; this overlay never did. K=4096 discarded keys (the column cap) non-null in every row, R ≈ 16M/(K+1) ≈ 3.9 k rows under the value budget → per row Σ ≈ K²/2 ≈ 8.4 M compares → ≈3.3×10¹⁰ ≈ **30–65 s per push** inside one `spawn_blocking`, repeatable (the rollback reverts; the identical push re-offends). The Widen shape adds the same order at build. Same precondition class as 15M1 (operator-configured Discard\* + connector-chosen legal wide slabs) — availability, not corruption.
**Fix:** `nulled: HashSet<String>` (the only consumers are `any` and `push` — no ordered iteration anywhere) or a sorted-vec binary search; a probes-style complexity pin.

### 16M2 — The new `pem::Material`'s read is the 13M1 class on the template surface: bare `std::fs::read` of a config-authored path
**Where:** `crates/rdlt-connector-sdk/src/pem.rs:93-112` — `read()`/`read_to_string()` path arms do `std::fs::read(&self.0)` / `read_to_string` with **no file-kind check, no symlink refusal, no size ceiling**.
**Verified by hand at the seat.** The lifecycle otherwise lands well: inline material is bounded by the 8 MiB config-document ceiling before parse; `Debug` renders inline as a placeholder and there is **no `Display`** (so no implicit render path exists); the handshake refusal's redaction sweep covers the inline PEM as a scalar; no TLS transport arrived (UDS-plaintext posture unchanged; SECURITY.md owes nothing yet). But the PATH variant is the exact shape round 14's 13M1 was: `{"key": "/dev/zero"}` → unbounded buffer growth → OOM; a FIFO → a parked opener; a symlink → followed. The type exists precisely so every connector reaches for it — the template teaches the ungated open, and the first adopter inherits the hole client-reachably (the rogue same-UID client authors `config_json`).
**Fix:** the reference connector's own `gate_regular_file` shape at this seat — `symlink_metadata`, refuse non-regular kinds, refuse `len()` over a material ceiling (PEM is kilobytes; even 8 MiB mirrors the document cap), then a bounded read. Pin: `/dev/null`, FIFO, oversized-file refusals.

### 16M3 — The handshake is the remaining unadmitted seat that runs connector code
**Where:** `crates/rdlt-connector-sdk/src/serve/wire.rs:438` — `handshake(slot: &OnceLock<Arc<S>>, …)` takes no admission; `MAX_CONCURRENT_PROBES` guards only check/streams (verified: the semaphore's only non-test references are the three probe seats).
**Verified:** the `OnceLock` stops only a second *success* — every failed attempt runs the full path: the 8 MiB `config_json` parse into an untyped `Value` (several-× expansion), then the shell's `from_config` → `Document::from_value` → the connector's own `validate` AND `assemble` (connector-authored code — pools, dials, key reads), and on failure the redaction sweep (a re-parse plus a clone of every scalar leaf, deliberately inside the refusal arm). A rogue same-UID client fires unbounded concurrent `Handshake` RPCs, each with a fresh document — the exact shape 15M2 closed for Check/Streams; 072's own completeness claim ("every other seat that reaches a connector's backend already has an admission bound", `wire.rs:705-710`) overlooks that a pre-success handshake reaches it too. Pre-existing, not a 072 regression — but the claim is now false as written.
**Fix:** the same semaphore shape around the handshake body (or a dedicated small one — handshakes are rare in honest operation), refused with the same RESOURCE_EXHAUSTED Status; update the completeness claim.

---

## 4. New findings — Low

- **16L4 — Snapshot `find` is O(K·N) in the batched revert** (`table.rs:221-226`): per-key linear scan over the snapshot; ~8–16 M short compares ≈ tens of ms per push at the caps. A `HashMap<&str, &ColumnState>` built once per call.
- **16L5 — Arrow-path projection `retain` uses a linear `Vec::contains`** (`shred/arrow.rs:176`): O(W·K) per discarded batch, tens of ms. A HashSet.
- **16L6 — The depth note states the bare-column boundary (42); the Delta-record boundary is 39–40** (`shred/types.rs:114-125` vs measured decode limits; `wal/format.rs:319-366` pins the bare column, so envelope changes move the real boundary while the pin stays green): pin the LINE (a create-delta at 39 verifies / 40 lands Corrupt) and state the record figures at the door.
- **16L7 — The receipts memo forfeits the canonical case** (`store.rs:236`, `session.rs:65-68`): `load_seen` forces a byte-0 rescan from a load's second commit — N commits cost N(N−1)/2 lines read; the O(N²) 15L5 named. Memo `(load, offset, max_seq_seen)` and resume when `commit_seq > max_seq_seen`; add the missing cost pin.
- **16L8 — The 15L7 fix over-replaced**: protocol README:295-296 now names `state_format_versions_json` on the client `Outcome` — a field that exists nowhere (`handshake.rs:128` says `state_format_versions`; `Managed::outcome` returns that type). The wire field (:291) and the Outcome field (:296) are correctly different spellings; restore the second.
- **16L9 — "five clause families (`s`, `d`, `p`, `k`)"** (`certify/src/lib.rs:15-16`): count five, list four — add `c`.
- **16L10 — `--hostile-config` help still says D7/S5** (`bin/rdlt-certify.rs:95-96`): the flag drives S6 too; "(D7/S5/S6)".
- **16L11 — The anchor's hint-shape clause is still wrong** (`client source.rs:161-162`): quotes a bare `"Bool"` wire spelling that never appears (real: `"a":{"type":"bool"},` ≈ 20 B) and claims 5× where the model gives ~3.2×; the pinned fixture carries a primary key only. Pin the hint shape or scope the claim; soften "MEASURED" (the denominator is measured, the numerator is a model).
- **16L12 — lib.rs crate docs still assert the freeze** both manifests now date as lifted (`protocol/src/lib.rs:3`, `client/src/lib.rs:33`; README:206-207 claims lib.rs "mirrors this status" — it mirrors only the freeze).
- **16L13 — The reconcile can glue a run header onto a mid-tail cut** (`wal/writer.rs:85-98`): when an unterminated tail longer than the window has no newline anywhere in it and `window_start > 0`, `tail_start = 0` addresses the window and `set_len(window_start)` cuts mid-line — the head survives unterminated and the new header glues onto it, the exact corruption reconcile exists to prevent → permanent Damaged. Hostile-only (honest tails are ≤ cap) plus a failed clear; adds nothing over planting any corrupt line — Low. Refuse the open instead of truncating mid-tail.
- **16L14 — The `pem` feature is unreachable by every gate** (`sdk/Cargo.toml:50`; Makefile lint/test legs cover `serve`/`schema` only): the module's six tests — including the inline-material Debug-redaction pins, the type's one security property — **never run**, and the module is never linted; only `make docs`' `--all-features` rustdoc compiles it. Add a clippy leg and a one-module nextest leg (the `schema`/`serve` precedent).
- **16L15 — `docs/connector-authoring.md:26-30` is now false**: "PEM material … is the connector's own: the sdk carries only what is true of every connector" — the sdk carries it since 071, added precisely so connectors don't diverge. Strike the item; also the SDK README's feature inventory omits `pem`.
- **16L16 — ADR 0001:325 still names `state_format_versions`** as today's field (the 15L7 family's one missed doc).

---

## 5. New findings — Info

- **5.1 — The probe-admission pin is seat-level only** (semaphore + Status, no wire-driven parking-connector form — unlike the read ceiling's end-to-end pin). Noted by its own comment; defensible.
- **5.2 — The 070 wildcard/alias fix is sound**: the scanner's token-start set remains a superset of the parser's; no differential constructs alias acceptance; over-refusal is the only failure direction; the comma-in-flow crux is pinned. Nit: the acceptance pin doesn't run the serde parse half the other acceptance pins do.
- **5.3 — The `Material` inline/path misjudgment is pathological-only** (a relative path literally beginning `-----BEGIN` reads as inline — self-inflicted, availability-only).
- **5.4 — The scan-resume memo grants the hostile writer nothing new**: resumed ranges re-validate fully; the skipped prefix has no integrity field by design (append-only invariant); a length-preserved swap converges (deterministic part names) or refuses at the next fresh scan; per-session memo, honest restart cost documented.
- **5.5 — The Python connector did not inherit the probe-admission seat** (no semaphore equivalent) — the sdk's own 072 rationale says the template must teach the bound; note-only (a separate implementation).
- **5.6 — Protocol README:221-223's "never log `config_json`" rationale is now incomplete** — it names `Secret` only; inline `Material` (deliberately not a `Secret`) is covered by the operative rule but not the stated reason.
- **5.7 — "Millions of StreamSpecs" prose is ~2× high** (honest max ≈ 0.97 M at the frame cap) — client source.rs:130/:647.
- **5.8 — Round-15 Infos 5.1/5.2/5.7/5.10 remain postures** (wide-object 2× peak; the test index-bypass at `json.rs:681`; the append `created` window; part orphaning after the state ceiling).
- **5.9 — The store.rs:412 doc backlink names the deleted `find_receipt`** (eed98d06 removed it; `find_receipt_from` carries the contract now).

---

## 6. Recommended fix order

1. **16M2 + 16L14** — gate `Material::read` (the `gate_regular_file` shape) AND wire the `pem` feature into the gate: the template's hole and its untested redaction property are one commit.
2. **16M3** — the handshake admission semaphore + the completeness-claim correction (one pattern, already proven at three seats).
3. **16M1** — `nulled` as a HashSet + a complexity pin (the shred's last linear-per-key walk).
4. **16L7** — the receipts memo's `(load, offset, max_seq)` resume + the cost pin (the O(N²) still live).
5. **16L6** — pin the Delta-record depth line and state the real figures at the door.
6. **16L13** — the reconcile mid-tail refusal.
7. **16L4/16L5** — the two micro-linear scans (one HashMap pattern).
8. **16L8–16L12, 16L15, 16L16, 5.6/5.7/5.9** — the doc-truth sweep (this wave's own regressions first).

---

## 7. Caveats

- Line numbers are from `6c769e35` and will drift.
- **Calibration note:** lane F rated 16L14 (the pem feature outside every gate) Medium — this report keeps it Low as a test-infrastructure gap per the series' conformance-gap precedent (13L8/13L11), while ranking it FIRST with 16M2 in the fix order because the property it leaves untested is the new type's only security property. 16M2 is Medium (not High) only because no in-tree caller exists yet — the first connector that adopts `Material::read` makes it client-reachable; gate it before that adoption, not after.
- **Verification confidence:** 16M1 verified at the scan site and its call graph (hand); 16M2 at the bare `fs::read` arms (hand); 16M3 by signature and semaphore-reference grep (hand); 16L6's depth arithmetic measured in a scratch crate against the workspace-pinned serde by lane B; the PEM lifecycle traced end-to-end by lane D with every render seat named (no Display exists; redaction covers the refusal arm). All six lanes ran their suites green; the full workspace gate ran serial to exit 0 twice — the second run confirmed **44/44 suites ok** (the first run's log rotated before its per-suite totals could be summed; the lane-scoped runs total ~900 tests across their crates).
- **The series' trajectory:** High 4 → 6 → 3 → 2 → 1 → 0 → 0 → 0 → 0 → 0 → 0 → 0 → 0 → 0 → 0 → 0 across sixteen rounds. The Medium band stays at three, but its composition tells the story the program wants: one is the true last member of the shred's linear-walk family, one is a brand-new surface's day-one hole, one is a completeness claim one seat short. Nothing here is unexplained or unbounded-unknown; every item has a known-shape fix.
- Severity assumes the documented trust model (D-038-1 per `SECURITY.md`; the PEM feature's no-transport status verified — the plaintext-UDS posture is unchanged and owes nothing until the TCP+mTLS binding lands, at which point SECURITY.md:51-52 and the trust paragraph must move with it).
