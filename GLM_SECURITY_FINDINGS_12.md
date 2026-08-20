# Security Analysis 12 — rdlt workspace (twelfth deep review)

**Date:** 2026-08-19
**Scope:** the full workspace at **`92c0f7e1` on `067-security-findings-11`** — the 067 fix wave (7 commits, `bf4c5976..HEAD`, +1611/−202 across 28 files) landing every round-11 finding, reviewed on top of it. Working tree clean apart from findings docs.
**Method:** six parallel comprehensive subsystem reviews (client/protocol; WAL+lineage; engine data-path; SDK serve+runtime; certify/testkit/reference; facade/CLI/docs). Each lane verified its assigned round-11 fixes at the seat and hunted fresh over the 067 diff and its full lane surface. Full workspace gate: **984/984 serial, green** (up from 954 — the wave added 30 pins). All four Mediums below were re-verified by hand at their seats.

**Severity scale (unchanged):** High — the stated adversary can crash, hang, or abuse the host trivially, **or a legal input shape silently loses, duplicates, or alters data**. Medium — narrower preconditions or reduced impact; contract violations short of corruption. Low — defense-in-depth, hygiene, pin gaps, availability-only edges. Info — observations and recorded postures.

---

## 1. Executive summary

**Every round-11 finding is fixed** — the three Mediums and all twelve Lows landed with pins, several beyond what was asked: the render fix didn't just bound `unexpected_reply`'s intermediate but replaced the whole materialize-then-truncate discipline with a streaming `BoundedWriter` sink (`render_debug`/`render_display`) migrated across the arrow-cause seats too; the chain fix memoizes every walked suffix with an iterative `Drop` (stack-safe on million-deep chains) and the scan fails fast on cycles; the README is now wired into `cargo test --doc` so its examples can never silently rot again; and the wave closed round-11 Infos in passing (5.1 checked sums, 5.5 u64 attempts, 5.7's store renders upgraded from posture to pinned). The 066/067 serve surface holds: the exhaustive wire-seat sweep found every client-authored field gated (lengths), the reference render sweep is closed, and the check-probe honesty pair is pinned.

**Sixth consecutive zero-High round** (trajectory: 4 → 6 → 3 → 2 → 1 → 0 → 0 → 0 → 0 → 0 → 0 → 0). Four Mediums — every one a *residual layer or mirror* of the families the series has been closing, made visible by the fixes themselves:

1. **12M1 + 12M2 (the materialization family's pre-decode layer):** the 11M2 gates run *after* prost has already materialized the container — the count cap sees `list.stream_spec_json.len()` only once a `Vec<Vec<u8>>` exists. One 64 MiB streams reply of 2-wire-byte empty entries is ~33.5 M `Vec<u8>` headers ≈ **805 MB–1.2 GB**; the handshake's `map<string,uint32>` seat is the same shape at ~600–700 MB. The series bounded what the *crate* materializes; prost's decode was the ungated layer beneath.
2. **12M3 (11M3's loader-side sibling):** the suffix memoization fixed both consumers for *terminating* chains automatically, and the scan fails fast on cycles — but the loader degrades non-terminating walks without memoizing (`unwrap_or_else(|| table.clone())`), and calls `root_of` **per batch**: a rogue connector's cyclic deltas cost O(M·N) again — minutes-to-days of loader CPU.
3. **12M4 (11M2's serve-side mirror):** the serve Read seat retains the parsed spec and cursor documents *for the read's lifetime* (its own comment says so) and has no count caps — 1024 concurrent stalled reads each retaining ~15–35 MiB of typed maps is a **15–35 GiB** OOM of the served connector, the one term the "per-read budgets" doc never prices.

The Lows are completion work (two render leftovers, an unbounded-arrow-cause serve twin, the state sub-map identifiers, hostile-local-file read-back, a D7 conformance gap, the ADR's stale `validate`), and the Infos record postures.

---

## 2. Round-11 fix verification

| Round-11 | Status | Evidence (current tree, `92c0f7e1`) |
|---|---|---|
| 11M1 unexpected_reply intermediate | **Fixed — family upgraded** | `destination.rs:192-205` renders through `gate::render_debug(2048, reply)`; `BoundedWriter` (`gate.rs:160-207`) escapes-as-it-arrives, counts and discards past the cap — no full rendering exists anywhere on the path; `render_message(&x.to_string())` seats migrated to `render_display` (`destination.rs:221`, `source.rs:264,272,298`). Pins: 8 MiB payload (cap+envelope, marker names >4× payload), 64 MiB Firehose sink, escaped-prefix cap |
| 11M2 streams reply caps | **Fixed at the crate layer** | `decode_stream_specs` (`source.rs:139-160`): `gate::count` (1024) before the loop, per-spec `gate::document` (8 MiB) before `from_slice`, then the per-value gates; order pinned structurally (non-JSON fixture proves parse-never-ran). Residuals → 12M1 (prost layer) and 12I1 (typed floor ≈5×) |
| 11M3 chain memoization | **Fixed** | `lineage.rs:129-163` — memo check before each hop, every visited node inserted with shared `Arc<Link>` tail; iterative `Drop` (`:81-91`, try_unwalk, stack-safe); complexity pins K=1,000 (unit) and K=100,000 (scan-scale), hops ≤ 2(K+1). Cycle corner hand-verified: `filter_covered`'s `?` (`scan.rs:664`) fails the scan on the first cyclic table — one bounded walk. Loader residual → 12M3 |
| 11L1 meta gates | **Fixed** | Publish + Replay gate `meta.load_id` and `meta.state.pipeline` (`serve/destination.rs:598-604, 618-627`) before the backend; three pins. Residual → 12L4 (state sub-maps) |
| 11L2 Read spec identifiers | **Fixed (lengths)** | `refuse_oversized_spec_identifiers` (`serve/source.rs:118-141`) — name, every primary-key field, cursor field, every type-hints key at 1024 B; exhaustive against the type. No count caps → feeds 12M4 |
| 11L3 reference renders | **Fixed** | All three seats via `render_diagnostic(·, 256)` (`session.rs:193-202, 262-272`, `source/connector.rs:85-91`) with control-byte pins; the cursor echo seat now doesn't echo at all |
| 11L4 joiner render | **Fixed** | `error.rs:64-87` — both mismatch arms through `gate::escape`; pin spells `\u{200c}` out |
| 11L5 Transport Debug | **Fixed** | Manual `Debug` (`error.rs:124-168`) routes Transport through `render_message(status.message())`; 4 MiB pin. Residual → 12L2 (`source()`) |
| 11L6 no-write claim | **Fixed** | ENGINE-scoped wording at `engine.rs:97-101`, `check.rs:5-10`, `pipeline.rs:464-468`, and the CLI seats — verified consistent across all four |
| 11L7 trailing-slash probe | **Fixed** | `connector.rs:70` — `components().collect()` normalization; `stat("/occupied/file/")` now fatal; pinned |
| 11L8 directory probe | **Fixed** | `connector.rs:60-66` — `!meta.is_file()` fatal; pin asserts check/read agreement |
| 11L9 ensure_table render | **Fixed** | `session.rs:111-119` via `render_diagnostic`; pinned |
| 11L10 README workdir row | **Fixed** | `README.md:183` — document-default `.rdlt/<pipeline>` stated, builder-only WAL-less carve-out named; matches `pipeline.rs:311-321, 416` |
| 11L11 README example | **Fixed — structurally** | Example complete (`README.md:148-160`); `lib.rs:67-69` wires the README as doctests (`cfg(doctest)` include); both rust blocks `no_run`; ran green as merged `readme_doctests` |
| 11L12 retry floors | **Fixed** | `config.rs:251-256` clamps base ≥1 ms and max ≥ base at the single accepting seat (no deserialization seat exists); `retry.rs` delay math overflow-free; attempt counter now u64 (5.5 closed in passing); three pins |
| 5.1 replay counters | **Fixed** | `replay.rs:64-68, 183-189` — checked sums, typed degrade; the debug-build panic removed outright |
| 5.7 store renders | **Fixed beyond posture** | Both corrupt-content seats through `render_diagnostic` (`store.rs:171-178, 195-203`); pin |
| 5.8 stale `validate` | **Partial** | Makefile fixed; **ADR 0001:402 still says `validate`** → 12L9 |

---

## 3. New findings — Medium

### 12M1 — prost materializes the streams reply's `Vec<Vec<u8>>` before the 067 count gate can see it: one 64 MiB frame peaks ~805 MB–1.2 GB
**Where:** `crates/rdlt-connector-client/src/source.rs:316-328` — `client.streams(...)` decodes the frame inside the `.await` (tonic's codec, bounded only by `max_decoding_message_size(MAX_FRAME_BYTES)` at `wire.rs:149-163`); `decode_stream_specs`'s count gate runs after `.into_inner()`.
**Verified:** prost 0.14.4's unpacked repeated decode is `values.push(value)` per field occurrence. An empty bytes entry costs 2 wire bytes (tag+len), so a 64 MiB frame yields 33,554,432 empty `Vec<u8>`s → 24 B each → **~805 MB** in the outer `Vec` alone; Vec doubling holds old+new transiently ≈ **1.2 GB peak**. The count gate then refuses (33.5 M > 1024) and the memory drops — bounded, transient, typed, once per `streams()` call. The fix comment's "gated BEFORE anything materializes" is true only of *this crate's* materialization.
**Impact:** ~12–19× the frame cap in one shot — OOM-adjacent under a memory-limited host; ~3× worse than the ~3–4× posture round 11 recorded as Info-consistent.
**Fix:** genuinely narrow — a legal reply may fill the whole 64 MiB frame (14 fat honest specs), so a lower decode cap refuses legal replies. Options: a custom codec pre-scanning field-1 tags to count specs before decode (heavy), a proto change (`stream_spec_json` as one bytes blob), or recording the prost amplification honestly in the admitted-frame posture. Filing it is the main ask.

### 12M2 — Same layer at the handshake: `map<string,uint32>` materializes ~600–700 MB before the count gate — and here a per-RPC decode cap refuses nothing legal
**Where:** `crates/rdlt-connector-client/src/handshake.rs:244-253` — `gate::count` on `ok.state_format_versions.len()` runs after `into_inner()`; generated type `HashMap<String,u32>`.
**Verified:** duplicate keys collapse, so the attacker sizes for distinct keys: ~10–11 wire bytes per entry → ~6 M entries in 64 MiB → hash table + key heap ≈ **600–700 MB**. The seat's own comment (`:240-243`) names the attack shape ("a state-format map of millions of keys passes every content gate within the frame cap") — the gate just runs one materialization too late.
**Fix (concrete, unlike 12M1):** the handshake reply's *legal* maximum is two 8 MiB documents + ids + a ≤64-kind map of ≤1024-byte keys ≈ **16.1 MiB** — a per-RPC decode limit of ~17–18 MiB on the handshake client (tonic's per-client `max_decoding_message_size`, the same lever `wire.rs` already uses per channel) cuts the amplification ~4× while refusing nothing any honest server sends.

### 12M3 — The loader's cyclic-chain degrade doesn't memoize and runs per batch: a rogue connector's cycle graph costs O(M·N) — the 11M3 shape at the one consumer the fix didn't cover
**Where:** `crates/rdlt-engine/src/load/loader.rs:154-163` (`root_of`: `parent_of` is `Infallible`; `.unwrap_or_else(|| table.clone())` degrades a non-terminating walk without memoizing — `lineage.rs:148-150` inserts nothing for `Ok(None)`), called per Batch item at `loader.rs:294`.
**Verified:** the scan seat fails fast (`filter_covered`'s `?` → Damaged on the first cyclic table — one bounded walk; hand-verified, and `live_tables` only runs post-filter over memoized chains). The loader does not: each `root_of` on a table inside a cycle re-walks up to `parents.len()+1` hops with per-hop `TableName` clones into `visited`. A rogue connector (in threat model) emits N Deltas forming a cycle, then M batches on a cyclic table: `loader.rs:222-225` inserts parent links unconditionally and nothing validates the graph. N=M=100 k → ~1.7×10¹⁰ BTreeMap compares — minutes-to-hours of loader-task CPU; N=M=1 M → ~2×10¹², days. Attribution stays self-consistent (a cyclic table is its own root) — availability only, safe direction.
**Fix:** memoize the failure — on a non-terminating walk, insert a sentinel `Link` (root = the table itself) for **every** visited node, making subsequent batches O(1) memo hits; or refuse cyclic chains typed at the loader, mirroring the scan (benign shreds cannot produce cycles, so refusal is free). Pin with a cyclic-delta fixture asserting linear hops.

### 12M4 — The serve Read seat retains spec and cursor documents for the read's lifetime with no count caps: 1024 stalled reads retain ~15–35 GiB — an OOM of the served connector the "per-read budgets" doc never prices
**Where:** `crates/rdlt-connector-sdk/src/serve/source.rs:429-491` — `stream_spec_json` (8 MiB ceiling) parses to a typed `StreamSpec` whose `type_hints` has **no count cap** (the client's twin gates primary-key ≤64 / type-hints ≤4096; the serve mirror gates lengths only), `since_cursor_json` (4 MiB) parses to an untyped `serde_json::Value`; both move into `ReadRequest` (`:488`) owned by the spawned read task (`:490-491`) — the comment at `:433-435` says it outright: "both documents are RETAINED for the read's lifetime."
**Verified:** a rogue same-UID client (the stated serve-side adversary) issues reads up to `MAX_CONCURRENT_READS` (1024/process), each with an 8 MiB spec of ~400 k single-char type-hints keys (every length gate passes) and a 4 MiB cursor, then never drains: the forwarding loop parks on `BYTE_FRAME_BUDGET`, the read task parks on `READ_CHANNEL_BUDGET`, and the retention is indefinite. Typed expansion ~2–4× → **~15–35 MiB per read × 1024 ≈ 15–35 GiB** — OOM kill of the served connector. The concurrency doc's worst-case sum bounds *production* memory only; the retained-request term is unmentioned.
**Fix:** mirror the client's count caps (64/4096) in `refuse_oversized_spec_identifiers` — collapses the spec expansion to ~0.4 MiB/read; price reads × retained documents in the documented sum (the cursor `Value`'s ~3–5× expansion is the residual term to either bound or name); optionally reconsider the 1024 default against the retained product.

---

## 4. New findings — Low

- **12L1 — `BoundedWriter` streams hostile Debug to completion** (`client gate.rs:193-197`): saturation still returns `Ok` so the marker can name the true length — prost's per-byte decimal Debug makes a 64 MiB wrong-variant bytes field ~2–6 s of synchronous CPU inside error construction (no await, deadline-blind). Fix: past a hard source ceiling (say 2× cap), return `Err(fmt::Error)` and render the marker as "≥N source bytes".
- **12L2 — `Error::Transport`'s `#[source]` hands the raw `tonic::Status` to chain renderers** (`client error.rs:99`): the crate's own Display/Debug are bounded, but anyhow/tracing chain walks render Status's message whole (≤ ~16 MiB via h2's header-list cap — bounded, verified). Fix: a Display-bounded source shim.
- **12L3 — The loader memo's no-invalidation invariant is documented but unenforced** (`load/loader.rs:222-225` vs `lineage.rs:30-36`): a later Delta may re-parent a memoized table; loader and scan can then attribute to different roots. Both disagreement directions degrade safely (re-extraction from committed cursors); contract violation short of corruption. Fix: refuse a re-parenting Delta typed.
- **12L4 — `CommitMeta.state`'s sub-map identifiers ungated at Publish/Replay** (`sdk serve/destination.rs:598-604, 623-626`): `cursors`/`schema_hashes` keys, `last_commit.load_id`, `engine_version` ride only the 8 MiB document ceiling into persisted state. One `gate_commit_meta` walking the state fields through the identifier ceiling, shared by both seats.
- **12L5 — The serve Write decode seat renders Arrow's cause unbounded** (`sdk serve/destination.rs:300, 303, 323`): the 065 fix bounded the client's mirror seats; this twin — where the adversary authors the embedded schema that rides arrow's error text — was not touched. `render_diagnostic(&error.to_string(), 256)` as at the client.
- **12L6 — The last unbounded driver-authored render** (`reference store.rs:71-73`): the jsonl-encode refusal interpolates the table name raw; `io_refusal` (`:248-253`) renders the full staged temp path. Over the wire the serve gates bound names at 1024 B; a direct driver hits ENAMETOOLONG — classified **transient** (retry bait, the 11L7/11L8 shape). Fix: bounded render + a name-length gate in `part::name` with fatal classification.
- **12L7 — No read-back ceiling or file-kind check on hostile local state** (`reference store.rs:154, 190, 222`): a same-UID swap to a sparse multi-GiB receipts/state file pre-allocates that size on read (OOM under limits); a FIFO hangs the read. The codebase ceilings file-authored reads everywhere else. Fix: `metadata()` length ceiling + regular-file check before reading.
- **12L8 — Conformance gap on the check contract** (testkit `conformance/destination.rs:20-21` defers D7; certify `clause/d.rs:41` has D1–D6, D8): the 067 check-probe honesty fixes are pinned only in the reference crate's own tests — a third-party connector whose `check()` lies still certifies clean. A D7/S clause driving a hostile seat (file-behind-trailing-slash, directory) asserting fatal refusal.
- **12L9 — ADR 0001:402 still says `validate`** (5.8 carryover): the subcommand is pinned retired; one wording fix.
- **12L10 — `lower_column` lacks the depth cap its batch-half twin enforces** (`engine load/lower.rs:50-80` vs `:125-130`): unreachable today (every reaching schema is depth-capped upstream; `LogicalType` cannot declare structs) — belt parity only.

---

## 5. New findings — Info

- **5.1 — Typed-expansion floor at the streams seat within the new caps ≈ 5–5.5× the frame** (client): the 59 MiB hypothetical cannot reach the wire (frame binds first at ~3.2 M entries ≈ 320 MB retained as a *legal* reply); the 8 MiB per-spec ceiling does not widen the multiplier (per-byte constant). Consistent with the recorded ~3–4× posture, slightly above.
- **5.2 — `capabilities_json` / `receipt_json` parse seats carry no document ceiling**: harmless for the current all-scalar/`{String,u64}` shapes (≤1× frame); re-evaluate on the next typed-shape change, the way `state_doc_json` got its ceiling.
- **5.3 — Derived `Debug` on `Remote` / `handshake::Outcome` renders the cached `ConnectorSpec` whole** (≤ ~20 MiB transient under an embedder's `{:?}`) — the 11L5 family's smallest sibling.
- **5.4 — Scan total materialization is a ~3–4× constant over manifest bytes** (records + schema clones + Chain links) — bounded over the documented 1 GiB budget; no million-deep `Drop` pin exists (the 100 k pin covers the shape).
- **5.5 — `Retry-After: 0` bypasses the base-delay floor** (`run/retry.rs:80`): documented hint precedence; bounded to max_attempts−1 retries; total sleep ≤ (attempts−1)×max_delay.
- **5.6 — Serve postures re-verified unchanged**: `from_json`/`from_value` un-gated by design (embedder entries, bounded upstream at the wire); runtime env/spawn/reaper hygiene all clean; the 067 serve diff is purely additive.
- **5.7 — `render_diagnostic` passes Cf format chars raw** (bidi overrides, joiners): the sdk's documented "diagnostic, not display" posture — recorded for the render-family ledger; the CLI's display boundary spells them out.
- **5.8 — Replay pass-1 overflow degrades via the shared "segment unreadable" arm** rather than naming overflow as pass 2 does — naming nit; `batches` saturates with an honest rationale.
- **5.9 — Infra note:** one cold-build full-workspace gate failed at the rdlt doctest step with E0463 on all workspace deps; the isolated `-p rdlt --doc` run and every warm rerun are green (984/984). A cargo/cold-build ordering flake, not a tree defect — noted in case CI ever shows it.

---

## 6. Recommended fix order

1. **12M2** — the handshake per-RPC decode cap (~17–18 MiB): one lever, refuses nothing legal, ~4× off the amplification.
2. **12M4** — the serve Read count caps (mirror 64/4096) + price reads × retained documents in the concurrency doc (bound or name the cursor-`Value` term): the largest OOM lever closed cheaply.
3. **12M3** — memoize-or-refuse the loader's cyclic degrade (sentinel `Link` per visited node, or typed refusal mirroring the scan) with a cyclic-delta pin.
4. **12M1** — the streams prost layer: custom-codec tag pre-scan if the team wants it gated, else record the ~12–19× prost amplification honestly in the admitted-frame posture (SECURITY.md / gate docs).
5. **12L5 + 12L6** — the two render leftovers and the ENAMETOOLONG retry-bait class.
6. **12L1–12L4, 12L7–12L10, 5.1–5.9** — the source-shim, the Err short-circuit, the state-sub-map walk, the read-back ceilings, the D7 clause, the ADR wording, the depth-cap parity, and the posture items.

---

## 7. Caveats

- Line numbers are from `92c0f7e1` and will drift.
- **Verification confidence:** 12M1/12M2 verified at the decode path plus prost 0.14.4's repeated-field/map decode source; 12M3 at the degrade line and per-batch call site with the scan's fail-fast contrast hand-traced; 12M4 at the retention seats (the code's own comments confirm retention) with the concurrency ceiling read from the pool constant. All six lanes executed their suites green; the full gate ran twice (cold flake above, warm 984/984).
- **The series' trajectory:** High 4 → 6 → 3 → 2 → 1 → 0 → 0 → 0 → 0 → 0 → 0 → 0 across twelve rounds. This round's Medium band is what a closing series looks like from the inside: each fix exposed the next layer (crate materialization → prost decode; scan walk → loader walk; client caps → serve mirror). None blocks a release under the project's own trust model; the fix list is the last known members of two families and their mirrors.
- Severity assumes the documented trust model (D-038-1 per `SECURITY.md`, re-verified against source by the facade lane).
