# Security Analysis 2 — rdlt workspace (second deep review)

**Date:** 2026-08-14
**Scope:** same as round 1 — all 13 crates in `crates/`, `connectors/python/`, `fuzz/`, `tools/`, `.github/workflows/`, `docs/adr/`, dependency manifests — reviewed **on top of the `047-security-hardening` branch (working tree @ `a10e2daf`)**, i.e. including the ~6,000 lines of fix code landed since round 1 (commit `92fbd263`).
**Method:** six parallel subsystem deep-dive reviews (client/protocol wire edge; WAL; engine extract/load/shred + channel/parquet; SDK serve + runtime; certify/testkit/reference; CLI/core/python/fuzz/CI), each tasked with (a) verifying a assigned subset of the round-1 findings and (b) a fresh adversarial pass over its files, the fix code included. The highest-severity new claims were then **re-verified by hand against the current tree and the vendored dependency sources** (arrow-ipc 58.3.0 `convert.rs`, serde_yaml 0.9.34 `de.rs`, tonic 0.14.6 codec) — every High, every hang-class Medium, and the meter-s semantics finding below carry personally confirmed line-level evidence; the remainder carry agent-quoted line-level evidence and are marked as such where the confidence differs.

**Severity scale (unchanged from round 1):**
- **High** — an actor inside the project's own stated adversarial position (a rogue or buggy connector, a corrupted WAL directory, hostile pipeline YAML) can crash, hang, or abuse the host process with trivial effort.
- **Medium** — same actor, narrower preconditions or reduced impact; or a systemic gap on a declared-untrusted input surface.
- **Low** — defense-in-depth, hygiene, or issues requiring an operator-level mistake to matter.
- **Info** — observations and accepted-design notes worth recording.

---

## 1. Executive summary

**The round-1 fix program landed and holds.** All 21 actionable round-1 findings (H1, M1–M7, L1–L13) are fixed in the current tree — every one re-verified at its seat, most with regression pins — or, in exactly two cases, closed as a documented decision: L11 (serde_yaml successor — ADR 0002 records serde_yaml_ng as the proven drop-in with migration deliberately owner-triggered) and L13 (part-closed flood — bounded on the silence side by the round-1 deadlines, with the sustained-flood residual documented at the loop). The CI actions were not merely SHA-pinned but the pinned SHAs were resolved against upstream and confirmed genuine. This is an unusually clean remediation record.

**The second pass found four new High findings.** They cluster in two places, and both clusters are *about the round-1 fixes' boundaries*:

1. **Arrow decode containment is proven against panics, not against stack exhaustion.** The fuzzer-found panic class was correctly contained at the client seat with `catch_unwind` (11a396ed) — but arrow's schema converter recurses once per nesting level with no depth cap anywhere (verified in the vendored arrow-ipc 58.3.0 source), and stack overflow is a guard-page abort that no `catch_unwind` intercepts. One small frame with a ~10k-deep nested schema kills the host process. Worse, the WAL replay seat never received the panic containment at all: `blocking.rs` *deliberately re-raises* panics, so even the ordinary (catchable) crafted-frame panic class — the exact one the client seat pins a 160-byte reproducer for — becomes a permanent recovery crash loop under the corrupted-WAL adversary (2H1, 2H2).
2. **The memory ceiling can be defeated with fully valid input.** The byte-budget meter charges variable-width values buffers only the span their offsets reference — a batch whose buffers are larger than its offsets declare is valid Arrow, meters near zero, and neutralizes every downstream budget that leans on that one number (2H3). Separately, the engine materializes a constant `load_id` string column **per row**, so a ~6 MiB boolean-packed frame (≈50 M rows) forces multi-GB allocations through the channel, WAL, and destination (2H4).

No new Critical. The Medium band is five items: a YAML alias-expansion OOM that defeats the 8 MiB document cap by quadratic materialization (serde_yaml's alias guard is only `events × 100` — verified in the vendored source), one unbounded uncancellable await that survives embedder cancellation, unbounded column-cardinality growth per table, the ungated handshake socket-path seat (raw render into host errors plus config-forwarding to any named path), and two retention-fidelity gaps beside the round-1 L5 ceilings. The Low band is largely *one seat over* from a round-1 fix: the sanitizer's remaining identifier seats, the reference connector's ungated `load_id` filename component, the vouched manifest open that still follows symlinks, the serve-side encode/decode seats that never got the client's ceilings.

---

## 2. Round-1 fix verification

| Round-1 | Status | Evidence (current tree) |
|---|---|---|
| H1 deadlines | **Fixed** | `client/src/dial.rs:60-72` (`connect_timeout` + h2 keepalive + whole-dial `with_deadline`), `handshake.rs:197-207`, `source.rs:265-287`, `destination.rs:331-350`; typed `ClientError::Timeout` at `error.rs:63-74`; `DEFAULT_RPC_DEADLINE = 10 s` (`handshake.rs:37`); the missed spawn-bins dial site re-pointed (`runtime/tests/cases/test_spawned_bins.rs:173-177`); live-wire pins for silent-transport, silent-handshake, mid-stream stall, and flood-then-silence (`tests/cases/test_deadline.rs`) |
| M1 Arrow recursion | **Fixed at the three named walks** — but see new 2H1: the cap guards *engine* walks, not arrow's own schema conversion | shared `MAX_ARROW_DEPTH = 64` (`connector/src/channel.rs:46`); `lowering.rs:123-128`, `passthrough.rs:236-241, 281-286`, `channel.rs:399-401`, each with a pin |
| M2 certify unlink | **Fixed** | one `remove_file` site in the crate, `wire.rs:217-227`, guarded by `symlink_metadata(...).is_socket()`; all former seats route through it (enumerated: `wire.rs:243, 318, 386`; `target.rs:448-450, 501-503`); rogue-advertising-a-regular-file pin at `wire.rs:1739-1761` |
| M3 segment name | **Fixed** | `record.rs:153-172` rejects separators/`..` and enforces the `{load_id}-NNNNNN.arrow` shape (ASCII-digit, ≥6); applied at scan (`scan.rs:217-221`) *and* re-checked immediately before the join (`replay.rs:53-55` — "the name gate and the join must never separate") |
| M4 manifest checksum | **Fixed** (with 2L3: unkeyed, order-unbound) | `record.rs:70-76` per-line blake3 trailer; `record.rs:108-135` verify; mismatch and stripped-trailer → `Damaged` (`scan.rs:121-142`, pinned) |
| M5 control-char rule | **Fixed** — one rule, three dispositions (`sanitize.rs:34-56`): identifiers refuse (`handshake.rs:227-228, 253-254`; `source.rs:233`; `destination.rs:277-283`), messages escape (`error.rs:180-182`), data documents stay data. Residual seats → 2M4, 2L1, 2L2 |
| M6 part filename | **Fixed for the table component** (`reference/src/destination.rs:407-422`, refusal on empty/`\`/`/`/`..`/control) — the `load_id` component at the same seat is still ungated → 2L7 |
| M7 fuzz coverage | **Fixed in substance** — 4 new targets on production seats (`wire_frame_decode` → real prost module; `wal_manifest_line` → `wal::record::decode_line` + the name gate; `handshake_line` → `Line::parse`; `arrow_ipc_decode` → the client's hardened seat via `fuzzing.rs`), all in the PR smoke run set — except `arrow_ipc_decode` itself, deliberately excluded (→ 2I1, and it bears directly on 2H1/2H2) |
| L1 checked seq | **Fixed** | `scan.rs:254-260` `checked_add` → `Damaged`; u64::MAX pin at `scan.rs:1580` |
| L2 meter overflow | **Fixed** | `channel.rs:292-296` saturating window; the `(len+1)*width` at `:302` is reachable only after a successful bounds-proving `get` |
| L3 null-merge panic | **Fixed** | `lowering.rs:203-210` typed refusal; pin at `:544-562` |
| L4 symlink writes | **Fixed on default paths** — sidecar/manifest/segments all `create_new` (`writer.rs:128-156, 361-365`); the vouched-residue manifest open keeps plain `create`+`append` and follows a planted symlink → 2M7 |
| L5 retention ceilings | **Fixed at the named seats** (`wire.rs:129, 544-554` saturating ceiling incl. slot size; `testkit conformance/source.rs:52, 105-120`; probe stdout 1 MiB cap `bin/rdlt-certify.rs:259, 502-511`) — retention-fidelity gaps beside them → 2M5, 2M6 |
| L6 probe stdout | **Fixed** | as above, cap-vs-past-cap distinguishable, cap verdict ordered before exit-status verdict |
| L7 retry clamp | **Fixed** | `error.rs:211, 222-226` — single clamp site at 60 s, `min` before any arithmetic; u64::MAX pin |
| L8 config echo | **Fixed** | `sdk serve/common.rs:346-354` kind+location only; typed arm via `redact_values` (`:401-419`); numeric-leaf trade-off → 2L9 |
| L9 python | **Fixed (all four)** | symlink rejection `:183`, per-line cap with +1 read `:327-330`, bind-return check `:438-440`, honest race comment `:415-417` |
| L10 parquet range | **Fixed** | `parquet.rs:66-75` per-codec windows, `validate()` `:237-248`; no wildcard arm (a future codec fails compilation, not silently) |
| L11 serde_yaml | **Documented decision** | ADR 0002 (`2d071bec`): serde_yaml_ng proven drop-in (165 tests byte-identical), migration is an owner trigger. The dep remains in-tree — and a new behavioral finding rides on it → 2M1 |
| L12 CI hygiene | **Fixed** | every `uses:` SHA-pinned (20 refs; the five upstream actions resolved against the GitHub API and confirmed genuine upstream commits), `permissions: contents: read` on both workflows, no `pull_request_target`, no `${{ }}` in `run:` blocks, PR fuzz-smoke leg live (`ci.yml:59-83`) |
| L13 part-closed flood | **Partial, documented trade-off** | each reply await is deadline-bounded (`destination.rs:344-350`), so flood-then-silence fails typed; a *sustained* flood spins the O(1)-memory loop until host cancel, documented at `destination.rs:306-316` — residual recorded at 2I2 |
| I8 riders | fuzz `Cargo.toml` unused deps **cleaned**; **SECURITY.md still absent** (2I13) | |

**Verdict on the round-1 program:** the premise that all issues are fixed holds. Nothing regressed; several fixes are stronger than asked (the M3 double gate, the L12 SHA genuineness, the H1 four-arm live-wire pin set).

---

## 3. New findings — High

### 2H1 — Deeply nested Arrow IPC schema aborts the host: arrow's schema conversion recurses without a depth cap, and stack exhaustion is not a catchable panic
**Where:** the root cause is in the dependency: arrow-ipc 58.3.0 `src/convert.rs:416-475` — `From<fb::Field> for Field` re-enters itself via `children.get(0).into()` for List/LargeList/Struct/Map/REE arms (verified in the vendored source: **zero** occurrences of any depth/recursion guard in that file). Reachable seats, both verified in-tree:
- client decode: `crates/rdlt-connector-client/src/source.rs:136, 153-155` — `catch_unwind(|| StreamReader::try_new(...))` hands the connector's unverified flatbuffer straight to that recursion;
- WAL replay: `crates/rdlt-engine/src/wal/resume/replay.rs:26, 61-68` — `FileReader::try_new_buffered` + batch iteration, same converter, no guard of any kind (see 2H2).
**Impact:** a rogue source connector sends one `arrow_ipc` read frame (a few hundred KB — far under the 64 MiB ceiling) whose schema nests List-of-List-of-… ~10k+ deep. Each conversion level consumes stack; tokio worker stacks default to 2 MiB; exhaustion trips the guard page → SIGSEGV → process abort. `catch_unwind` **cannot intercept this** — the round-1 M1 depth caps (`MAX_ARROW_DEPTH = 64`) guard the engine's *own* walks (lowering/passthrough/footprint), which run only *after* a successful decode; nothing bounds the depth *inside* arrow's conversion. Under the corrupted-WAL adversary the same frame planted as a segment aborts the engine at recovery, every run, forever.
**Corroborating signal:** this is exactly the class the `arrow_ipc_decode` fuzz target exists to find — and it is the one target deliberately excluded from every run set (`Makefile:80-85`; see 2I1).
**Fix:** bound nesting *before* arrow sees the bytes at both seats — an iterative pre-pass over the schema flatbuffer counting child-vector depth (refuse typed past 64, matching `MAX_ARROW_DEPTH`), and/or decode on a dedicated thread with a deliberately small stack so exhaustion fails contained. Add a deep-nesting rogue to the certify wire clauses and a WAL-side damaged-segment test. Upstream arrow-rs deserves the report too.

### 2H2 — WAL segment decode has no panic containment at all: the fuzzer-found crafted-frame panic class kills recovery in a crash loop
**Where:** `crates/rdlt-engine/src/wal/resume/replay.rs:20-28` (`open_segment` → bare `FileReader::try_new_buffered`), decoded at `replay.rs:61-68` (pass 1) and `replay.rs:130-165` (pass 2); panic policy at `wal/resume/blocking.rs:11-27`, which **deliberately resumes panics** ("it is a bug in decode logic, not a damaged WAL").
**Evidence the class is real:** the client crate's own seat documents that "arrow's IPC reader PANICS on some crafted frames instead of returning Err — the schema converter aborts on e.g. an Int field declaring a negative bit width (found by the arrow_ipc_decode fuzz target)" (`client/src/source.rs:124-127`, 160-byte reproducer pinned at `:474-515`) and contains it with `catch_unwind`. The WAL seat calls the same arrow 58.3 converters with no guard, on input from the *stated* corrupted-WAL adversary.
**Impact:** an attacker who can write the WAL directory plants a crafted `{load_id}-000000.arrow` plus one re-checksummed manifest line (blake3 is unkeyed — 2L3 — so re-checksumming is trivial). Next run's recovery panics inside `spawn_blocking`; `blocking.rs` re-raises on the recovery thread; the process dies. Every subsequent run repeats it: a **permanent crash loop** requiring operator intervention — strictly worse than the damage arms' designed degrade-to-re-extraction. Secondary: no fuzz target exercises the WAL `FileReader` path (`wal_manifest_line` covers lines only; `arrow_ipc_decode` points at the client's stream seat and doesn't run — 2I1).
**Fix:** wrap segment open+decode at the `off_runtime` seam in `catch_unwind`, mapping a caught panic to the existing degrade-to-re-extraction arm (the round-1 rationale — "would hide a defect" — deserves revisiting: a *crafted external input* panic is damage, not defect; the client seat already made exactly this call); add a fuzz target over the WAL file-format reader.

### 2H3 — The byte-budget meter under-counts unreferenced buffer extents: a rogue defeats the resident-memory cap with fully valid Arrow
**Where:** `crates/rdlt-connector/src/channel.rs:436-445` (Utf8/Binary/LargeUtf8/LargeBinary arms) riding `offsets_window` at `channel.rs:285-308` — the values buffer (`buffers[1]`) is charged only `count_window(&buffers[1], data_start, data_len)` where `data_len` is the offsets-declared span (`last − first`, `channel.rs:303-304`); `(len + 1) * width` at `:302` likewise charges only the referenced extent.
**Verified semantics:** a values buffer *larger than its offsets reference is valid Arrow* — arrow validation checks offsets lie inside the buffer, nothing more. A wire frame ≤ 64 MiB carrying all-zero offsets over a 64 MiB values buffer, or a 0-row batch with two 64 MiB buffers (the offsets window then charges ~4 bytes), decodes to a batch the host fully holds while `arrow_batch_footprint` reports ≈ 0. The module's own docs name this as the failure direction that must never happen: "under-counting uncaps memory" (`channel.rs:319-321`). Existing pins assert `metered ≥ payload` only for *honest* batches; nothing pins the under-count direction.
**Impact:** that one number drives every budget in the pipeline — the records-channel semaphore (`channel.rs:137-161`), the stage channel and `LoadItem::bytes` (`engine/src/load/item.rs:43-49`), loader commit counters and accumulate thresholds (`loader.rs:262-287, 403-411`). With the meter neutralized, queued memory is bounded only by message caps (64 record messages × 64 MiB ≈ 4 GiB; 256 stage messages ≈ 16 GiB) against a 64 MiB configured budget → host OOM from one rogue connector sending *valid* frames. Memory-bounded DoS with full control of the rate.
**Fix:** close the semantic gap at the decode seat (refuse frames whose declared buffer lengths exceed what the arrays can reference, or trim buffers to the referenced extent after decode), and/or charge variable-width values buffers `min(buffer.len(), referenced_extent)` — no: charge the honest whole-buffer length for IPC-decoded batches where slicing is attacker-chosen. Add a pin asserting an oversized-unreferenced-buffer batch meters ≥ its buffer bytes (or is refused).

### 2H4 — Row-count amplification: one small valid frame forces a multi-GB constant `load_id` column
**Where:** `crates/rdlt-engine/src/shred/passthrough.rs:134-139` — `let rows = batch.num_rows(); … StringArray::from(vec![load_id.as_str(); rows])`; the JSONL sibling at `shred/build.rs:98-104`. Verified: no row cap exists anywhere on the structured path (neither `stream_task`/`passthrough_items` nor the client).
**Impact:** a boolean column packs 8 rows/byte, so a ~6 MiB frame carries ~50 M rows — fully valid and *honestly metered* as ~6 MiB (2H3 not even needed). The engine then materializes `Vec<&str>` of 50 M pointers (~0.8 GB) plus ~40 B/row of string data (~2 GB) for the constant column alone, and the amplified array flows on into WAL record serialization and the destination write. A 64 MiB boolean frame (≈512 M rows) is a guaranteed OOM. The byte budget cannot see it because it prices the *input* batch, not the *output* the engine materializes per row.
**Fix:** enforce a max-rows-per-batch on the structured path (typed refusal, or chunk the outgoing batch), and/or stamp `_rdlt_load_id` as a constant/dictionary column at the destination instead of materializing it per row.

---

## 4. New findings — Medium

### 2M1 — YAML alias expansion defeats the 8 MiB document cap: quadratic materialization OOMs or hangs `rdlt run`/`validate`
**Where:** `crates/rdlt/src/pipeline_spec.rs:219-224` (path-form config: `serde_yaml::from_str::<serde_json::Value>`) and `:152-163` (inline config materialized during `Spec` parse); same surface in `crates/rdlt-cli/src/run.rs:99-100`.
**Verified in the vendored source:** serde_yaml 0.9.34's recursion guard is depth-only (128) and its alias guard is `jumpcount > document.events.len() * 100` (`de.rs:478-479`). Each alias jump re-materializes the anchored subtree into fresh `serde_json::Value` nodes; with anchor size A and reference count R (A + R ≈ E events), materialized size approaches A·R ≤ E²/4 while jumps stay under the 100·E cap. A ~100 KB document can demand ~1.5·10⁸ Value nodes (~10+ GB); a ~1 MiB document is unbounded in practice. The 8 MiB cap bounds the *input text*, not the *expansion* — defeating the containment rationale recorded at `pipeline_spec.rs:179-185`.
**Impact:** hostile pipeline YAML (the stated hostile-input-adjacent adversary) makes `rdlt run` / `rdlt validate` OOM or hang. Analysis is source-level (the vendored guard was read; no live PoC was built) — but the guard arithmetic is unambiguous. Note ADR 0002's successor serde_yaml_ng inherits the same expansion behavior (it is a continuation), so this needs fixing regardless of the migration decision.
**Fix:** pre-pass rejecting documents with anchors/aliases beyond a small budget (or a materialized-node ceiling) before `from_str`; record the constraint in ADR 0002 as an input to the successor trigger.

### 2M2 — The reader task's post-`SourceFinished` join is unbounded and uncancellable: a connector that parks after closing its channel hangs the run forever, surviving embedder cancellation
**Where:** `crates/rdlt-engine/src/runtime/extract.rs:295-304` — every non-finished exit bounds the join with `READER_ABORT_GRACE` then aborts; `Ok(LoopExit::SourceFinished) => (&mut reader).await` alone joins with no timeout, no abort, and no `select!` on the cancel token.
**Impact:** the justifying comment ("its result is imminent") is a connector promise, not a mechanism. A rogue or buggy `Source` drops its `RecordsOut` and then parks inside `read()` forever: the push loop has exited, nothing selects on `cancel` anymore, `drain_loader`'s join loop (`runtime/drain.rs:50`) waits forever, and the embedder's cancellation token is ignored — the run hangs until the task is aborted externally. This is a residue of the round-1 H1 class (liveness above the wire), one branch deeper than the deadline sweep reached.
**Fix:** bound the `SourceFinished` join too (a longer grace is fine), or at minimum `select!` it with `cancel.cancelled()` and abort the reader on cancel.

### 2M3 — Unbounded column-cardinality growth: distinct-key accumulation amplifies a run's memory and WAL monotonically
**Where:** `crates/rdlt-engine/src/shred/table.rs:126-134` (`state_mut` appends per distinct key, never prunes), `passthrough.rs:189-206` (`schema_from_arrow` adds a column per distinct field); no per-table column cap in `TableBuffer`, `SchemaRegistry`, or `resolve_schema`.
**Impact:** within one 64 MiB budget window a source can declare millions of distinct keys (JSONL) or fields (Arrow); observation state, the registry schema, the emitted `Delta` (millions of `AddColumn` changes), WAL records, and per-batch build cost all grow with the *cumulative* distinct-key count — a sustained 10–30× amplification of pushed bytes with no ceiling, ending in OOM/disk-full at a rate the connector fully controls.
**Fix:** cap columns per table (and child tables per parent) with a typed refusal counted as `Discarded`, or bound total observation state per stream.

### 2M4 — The handshake socket path is ungated: raw-rendered into host errors (terminal injection), and dialed verbatim with config attached before any identity check
**Where:** `crates/rdlt-connector-protocol/src/handshake.rs:88-96` — `Line::parse` validates only non-emptiness of `socket_path`; `crates/rdlt-connector-client/src/error.rs:85` — `#[error("dialing the connector socket at {path}: {source}")]` renders the child-authored bytes raw; `crates/rdlt-runtime/src/local.rs:344-348, 415-446` — the path is dialed verbatim, and `client/src/handshake.rs:197-208` sends `config_json` in the same RPC whose reply is id-checked only afterwards.
**Impact:** (a) a rogue child prints `rdlt-connector|1|0|0|\x1b]52;c;…\x07` — the dial fails and the raw ESC/BEL bytes render wherever `ProviderError` goes (CLI, engine logs): the exact OSC-52/forged-line class the M5 rule exists to stop, one seat outside its coverage; a 64 KiB path also floods logs. (b) Round-1 I1, current state: the host sends the connector config (potentially credentials) to any UDS path a rogue child names — relative, traversal, or an absolute path to a helper the child planted — before identity verification can refuse. LifecycleGuard's unlink is socket-gated against this same child-authored path; the dial is not gated at all.
**Fix:** validate at `Line::parse` — absolute, ≤107 bytes (sockaddr_un), no control characters — refuse otherwise; escape the path in the `Dial` render; longer-term, verify the socket lives where the runtime expects (runtime-minted dir) before the config-carrying handshake.

### 2M5 — Certifier P5 violation strings and report entries are unmetered: a rogue read stream retains ~5–8× the retention ceiling (~1.5 GB)
**Where:** `crates/rdlt-certify/src/wire.rs:725-756` (`p5_violations` mints a formatted string per non-conforming frame, re-formatted with the census suffix), folded wholesale into report entries at `wire.rs:713-716` / `report.rs:395-400`.
**Impact:** the L5 ceiling meters only the `frames` vec. ~5 M one-byte undecodable `arrow_ipc` frames stay under the 256 MiB ceiling (~49 B each) but mint ~5 M violation strings (~150–200 heap bytes each) plus ~5 M report entries — roughly 1–1.5 GB actual retention, then `render_text` concatenates it all. Violations also accumulate across streams (the per-stream ceiling resets). Time-bounded (30 s) and bounded-in-principle, but the "a fast rogue could otherwise OOM the certifier" intent is only partially achieved; small CI hosts are pushable into OOM.
**Fix:** cap violations per clause (first N + "and M more"), and/or meter violation strings into the same `retained` budget.

### 2M6 — Testkit's retention ceiling meters wire bytes but retains parsed `serde_json::Value`s: ~12–16× expansion at the cap
**Where:** `crates/rdlt-testkit/src/conformance/source.rs:105-120` (metering counts `bytes.len()`) vs `:123-129` (parse into `Vec<Value>` retained).
**Impact:** compact scalar JSON (`[0,0,0,…]`, ~2 wire bytes per element, ~24–32 retained bytes per `Value`) forces ~1 GB actual retention at the 64 MiB counted ceiling; S1's row clones add a further 2–3× transient peak. The Arrow path's `num_rows() * size_of::<Value>()` approximation is honest by comparison. Same in-process OOM class as 2M5.
**Fix:** meter post-parse (deep-size estimate per document), or drop to count/hash comparison once a smaller raw-byte sub-ceiling is crossed.

### 2M7 — The vouched-residue manifest open follows symlinks: L4's residual appends the run's manifest to any pre-planted target
**Where:** `crates/rdlt-engine/src/wal/writer.rs:147-155` — `if tolerate_resolved_residue { manifest_options.create(true); }` … `.append(true).open(dir.join("manifest.jsonl"))` — plain create+append resolves through a symlink (the comment records why the vouched path keeps plain create; it does not address the symlink consequence).
**Impact:** narrow but attacker-reachable chain under the corrupted-WAL adversary: prior scan resolves to `Discard`, `clear()` fails (warned at `recover.rs:70-79`, which grants the voucher), attacker plants `manifest.jsonl → victim`; the Run header and every subsequent manifest line are appended to an arbitrary host file — structural corruption (config/cron/journal-style files), not truncation. The sidecar half stays safe (unlink + `create_new` still run).
**Fix:** in the vouched path, require `symlink_metadata` to show a regular file (or `O_NOFOLLOW` via `OpenOptionsExt::custom_flags`) and refuse loudly otherwise.

---

## 5. New findings — Low

- **2L1 — The identifier disposition misses remaining connector-authored identifier seats.** `StreamSpec.primary_key` / `cursor_field` / `type_hints` keys cross ungated after the `spec.name` gate (`client/src/source.rs:224-236`); Arrow schema *field names* inside `arrow_ipc` frames are forwarded ungated post-decode (`source.rs:297-299`); `HandshakeOk.state_format_versions` map keys collected raw (`handshake.rs:279`, currently inert). Same log-forging/identifier-spoofing class M5 closed. Route them through `contains_control` at the same edge.
- **2L2 — `contains_control` misses non-Cc dangerous Unicode.** `sanitize.rs:34-36` uses `char::is_control` (Cc only): U+2028/U+2029 line/paragraph separators (Zl/Zp), bidi overrides (Cf), and zero-width characters pass every gate. Line-splitting in log/JSON viewers and identifier spoofing via homoglyphs; extend the refusal predicate (the escape seat can stay Cc-only).
- **2L3 — WAL manifest integrity is unkeyed and order-unbound, and the format docs oversell it.** `record.rs:62-76` sells the digest against "a forged `Committed` sequence", but an attacker controlling the directory recomputes unkeyed blake3 trivially (acknowledged only in test commentary, `scan.rs:1504-1505`); per-line verification with no chaining accepts reordered/duplicated/spliced intact lines from an older manifest of the same run — checkpoint splices silently commit stale cursors. State the damage-detection-only boundary in the format docs/ADR; if the shared-storage sibling is in scope, key the digest (0600 key file) and/or chain per-line.
- **2L4 — WAL durability gaps around the fsync barrier (safe direction, contradicts documented intent).** No fsync of the WAL directory after creating sidecar/manifest/segments (`writer.rs:118-156, 354-373`) — power loss can drop directory entries despite `sync_for_commit`'s "durability across POWER LOSS" claim (`writer.rs:248-253`); `mark_committed` appends `Committed` and GC-unlinks segments with no manifest fsync in between (`writer.rs:304-316`) — a crash in that window degrades to re-extraction (safe) but outside the documented replay window. fsync the dir after creates; fsync the manifest before GC unlinks.
- **2L5 — `open_segment` follows symlinks.** `replay.rs:24-25` `File::open` on the (name-gated) segment path follows a planted symlink; impact bounded by the row-count cross-check and mostly moot beside 2H2. `O_NOFOLLOW` for symmetry.
- **2L6 — Connector-declared decimal precision/scale enters the registry unchecked on the Arrow path.** `passthrough.rs:307-310` accepts `Decimal128(p, s)` with `s ≥ 0` only — `Decimal{255,127}` reaches destination-facing schemas, while the hint gate (`runtime/validate.rs:201-219`) enforces 1..=38 and scale ≤ precision. Contract skew, not a reachable engine panic (the `build.rs:410-414` expect is unreachable from this path) — but it violates the invariant that expect leans on, and third-party destinations may not tolerate it.
- **2L7 — The reference destination's part filename gates the table but not `load_id`.** `destination.rs:304` gates `table` only; `self.load_id` (verbatim from the client's `Open` frame, unvalidated by design) joins the name at `:319`. No escape is constructible today (the `{table}-` prefix forces ENOTDIR), but the containment is accidental formatting, and this connector is the template third parties copy — a refactor to `dir.join(table).join(load)` would be instantly vulnerable. Gate the whole part name.
- **2L8 — Reference destination staging is unbounded in memory.** `destination.rs:185, 227-234` stage every accepted `RecordBatch` with no count/byte ceiling before `publish`; a misbehaving client OOMs the reference destination with a few 64 MiB frames. The L5 fix established the ceiling pattern this template should model.
- **2L9 — Numeric and bool config leaves echo through the SDK's typed refusal.** `serve/common.rs:359-383` (`string_values_of` skips non-strings; trade-off documented at `:397-400` but untested): `{"password": 12345}` against a `String` field renders `invalid type: integer \`12345\`…` unredacted — numeric secrets leak where string secrets are shielded. Extend the needle set for long digit runs, or pin the accepted gap in a test.
- **2L10 — Serve side sets no `max_encoding_message_size`.** `serve/source.rs:631-640`, `serve/destination.rs:825-834` set only `max_decoding_message_size`; tonic's send cap defaults to `usize::MAX` (verified, tonic 0.14.6 `codec/mod.rs:101-102`), so a buggy in-process connector encodes >64 MiB pushes fully and only the rdlt client's own cap refuses them — foreign clients without their own cap receive them. Add `.max_encoding_message_size(common::MAX_FRAME_BYTES)` to both services.
- **2L11 — Serve-side Arrow decode lacks the `catch_unwind` belt its client mirror gained.** `serve/destination.rs:326-344` (`decode_arrow_ipc`, bare `StreamReader::try_new`) vs `client/src/source.rs:123-143`. The fuzzer-proven panic class is reachable here from a crafted `Write.arrow_ipc` (session task panics; process survives, slot releases). Host-trust model keeps this Low; mirror the catch for symmetry — it is one `match` away.
- **2L12 — An abandoned `Read` leaks the forwarding task when the connector never pushes again.** `serve/source.rs:506-518`: after a hang-up break the task still `read_task.await`s unconditionally; a connector parked between pushes (cancellation observed "at the next push", which never comes) parks this task forever. `read_task.abort()` on the hang-up exits.
- **2L13 — Spawned connectors inherit the host's entire environment.** `local.rs:191-209` sets stdio only, no `.env_clear()`: every connector receives whatever secrets the host carries in env, outside the config channel. With yama `ptrace_scope=1` the child cannot otherwise read the parent's environ, so this is real defense-in-depth. `.env_clear()` + minimal allowlist (`PATH`, `TMPDIR`, `LANG`, `RDLT_*`).
- **2L14 — No process-group anchoring: SIGKILL reaches only the direct child.** `local.rs:207` (`kill_on_drop`), `managed.rs:69` (`start_kill`): a rogue connector's forked survivors outlive teardown (grandchild leak). `.process_group(0)` at spawn + `kill(-pgid, SIGKILL)` — the certifier already proves the pattern (`bin/rdlt-certify.rs:293-331`).
- **2L15 — The pipeline name renders unescaped at two CLI seats, contradicting the documented "one sink".** `rdlt-cli/src/run.rs:249-252` writes the summary block directly (bypassing `stderr_line`), `ui/summary.rs:41` and `ui/pretty.rs:37,76` interpolate `report.pipeline` raw — while `ui/mod.rs:6-11` claims every line is sanitized at the one sink. A YAML-authored `pipeline: "\x1b]52;c;…\x07"` drives the operator's terminal from the final summary and the live header. Connector-authored strings are safe on these paths for other reasons; the YAML name is the residual exposure.
- **2L16 — A hostile `workdir:` spelling licenses directory creation and `remove_dir_all` on paths the engine never proved it owns.** `pipeline_spec.rs:466-474` takes an explicit `workdir:` as given (absolute spelling included); the only ownership proof before destructive clears is "no `manifest.jsonl` present" (`writer.rs:104-112`); clean-finish and recovery then `remove_dir_all` the engine-named `wal` leaf (`run.rs:387-389`, `recover.rs:71-115`). Containment today: the leaf is engine-chosen (so not arbitrary deletion) and the default leaf is sanitized — the explicit spelling bypasses the intent. Refuse non-empty target dirs lacking an engine-owned marker, or make `clear()` unlink only WAL-created names.
- **2L17 — `tools/check-git-deps.sh` writes metadata to a fixed `/tmp` path.** `:40` — a pre-planted symlink at `/tmp/rdlt-git-deps-meta.json` redirects the write (arbitrary clobber as the invoking user). `mktemp` + trap, matching `check-python-stubs.sh:48`.

---

## 6. New findings — Info

- **2I1 — `arrow_ipc_decode` is the one fuzz target excluded from every run set** (`Makefile:80-85`, documented reason: libfuzzer's abort-on-panic-start hook reads arrow's *contained* panic as a crash). Consequence: the single target positioned to find schema-decode aborts — 2H1's exact class, where production containment genuinely fails — never executes in any gate. Its containment proof lives in the client suite's embedded reproducer. If 2H1's pre-pass lands, the target becomes runnable and should join `FUZZ_TARGETS`.
- **2I2 — L13 residual stands as documented:** a *sustained* `part_closed` flood spins the reply loop with O(1) memory until host cancel (`destination.rs:306-316`); flood-then-silence is deadline-bounded and pinned.
- **2I3 — The caught-panic path emits one raw stderr line per crafted frame** (`source.rs:131-135`, honestly documented): arrow's fixed panic text + integers only, but repeatable per frame — stderr-noise DoS if stderr is captured to disk.
- **2I4 — The client `fuzzing` module is `pub` in fact despite `#[doc(hidden)]`** (`lib.rs:45-46`): safe, stateless, no capability beyond `Source::read`; noted because doc-hidden is not privacy.
- **2I5 — `byte_budget: 0` silently disables the byte budget** (`engine/src/config.rs:93-96`; `send` skips the semaphore) — an embedder foot-gun; warn or clamp.
- **2I6 — Arrow depth enforcement is centralized, not per-walk:** only `flatten_array`, `data_footprint`, and `join_column_types` defend their own depth; every downstream walk (`canon`, `build`, `infer`, `lowering`) relies transitively on the ingest caps. Sound today; any new Arrow entry path must re-establish the bound — and 2H1 shows the ingest cap does not reach arrow's own conversion.
- **2I7 — `bind_uds`'s stale-reclaim unlink is not socket-gated** (`serve/common.rs:245-248`): on a failed liveness probe it unlinks whatever file type sits at the path, unlike `LifecycleGuard`. Private 0700 dir and caller-chosen path keep this self-inflicted-only; gate it for symmetry.
- **2I8 — stderr remains an unsanitized connector→host channel by design** (`local.rs:199-203`, ADR 0001 D3) — bounds the value of M5-class escaping on the run path (not on the `quiet_stderr` spec probe, where 2M4 is the remaining channel).
- **2I9 — WAL scan buffers the whole manifest and reads unbounded line lengths** (round-1 I3, extended: `scan.rs:100-115` has no per-line byte cap, so one multi-GB line is read whole). Disk-bounded, attacker-scale under the corrupted-WAL model.
- **2I10 — A one-line `ForeignPipeline` brick:** the foreign-pipeline arm is reachable from an *unverified* bare Run header (`scan.rs:125-142`), so a directory-writer can wedge the pipeline with one line; availability-only, inherent to a writable dir, and the arm correctly refuses to clear.
- **2I11 — Python connector residuals:** discovery-to-open TOCTOU on the symlink gate (`rdlt_connector_pyjsonl.py:285` vs `:302` — operator-trusted dir, defense-in-depth); a boundary-legal line can exceed the grpc send ceiling by proto overhead (`:327-337` vs `:400` — typed transport failure, no crash).
- **2I12 — The 8 MiB document cap is stat-then-read** (`pipeline_spec.rs:190-202`, `run.rs:86-97`): a concurrent writer can grow the file between `metadata()` and `read_to_string`. A bounded `take(MAX+1)` read would make the cap airtight.
- **2I13 — Housekeeping:** `SECURITY.md` still absent (round-1 I8 rider); an untracked bench WAL workdir sits in the tree (`benches/harness/cells/pipelines/.rdlt-load1/` — bench residue that can hold real row data; the harness should clean up).
- **2I14 — `ParquetOptions::validate` has no in-repo production caller** (tests only; the consuming connectors live in the sibling rdlt-connectors repo) — the L10 gate must be invoked there to actually fire.

---

## 7. Recommended fix order

1. **2H1 + 2H2** — depth-bound pre-pass before arrow sees schema bytes at both decode seats + `catch_unwind` at the WAL seam (mapped to degrade-to-re-extraction); repoint/run `arrow_ipc_decode` once the pre-pass lands; add deep-nesting and crafted-panic WAL rogues to certify. One shared root cause, two seats; small diffs, closes the only trivially-reachable process-abort classes.
2. **2H3 + 2H4** — same class ("memory ceiling defeated by valid input"): honest whole-buffer charging (or decode-seat refusal) for IPC values buffers + a max-rows-per-batch cap; pin the under-count and amplification directions.
3. **2M1** — YAML alias/anchor budget pre-pass on the pipeline-parse surface (independent of the serde_yaml_ng decision).
4. **2M2** — bound or cancel-select the post-`SourceFinished` reader join; extend the kill matrix with a close-then-park rogue.
5. **2M4** — validate the handshake socket path at `Line::parse` (absolute, ≤107 bytes, control-free) + escape the `Dial` render.
6. **2M3, 2M5–2M7** — column cap; violation/report retention meters; testkit post-parse metering; vouched-open `O_NOFOLLOW`.
7. **Lows and Infos** as scheduled hardening — 2L7/2L8 (the reference connector is the template), 2L10/2L11 (serve-side ceiling symmetry), 2L13/2L14 (spawn isolation), 2L15–2L17, then the rest.

---

## 8. Caveats

- Line numbers are from the working tree at `a10e2daf` on `047-security-hardening` and will drift.
- Verification confidence: 2H1, 2H2, 2H3, 2H4, 2M2, 2M4, 2M7, the WAL/serde_yaml/tonic dependency claims, and the entire round-1 fix table were re-verified by hand in the current tree and/or the vendored dependency sources. 2M1's expansion arithmetic is source-level (vendored guard read; no live PoC built — the analysis sandbox could not link). The remaining Medium/Low findings carry line-level evidence from the subsystem reviews and were not all individually re-derived.
- The 2H1/2H2 dependency claim (arrow-ipc 58.3.0 `convert.rs` has no depth guard) is verified against the vendored source this workspace actually builds against; an upstream fix would change the calculus but not the recommendation (the pre-pass keeps the engine independent of arrow's internal posture).
- Severity continues to assume the documented trust model (D-038-1): operator-installed connectors, plaintext UDS accepted. As in round 1, several findings shift up if connectors are ever sourced from a less-trusted channel.
- No network advisory scan was run (round-1 §6 caveat stands; `cargo audit` in CI remains recommended).

---

## 9. Remediation result (2026-08-15)

The report was re-triaged against the current dependency sources and every
listed item was dispositioned. This repository supports the current format and
protocol only; no compatibility path was retained for unsafe legacy state.

### High and Medium

| Finding | Result | Remediation |
|---|---|---|
| 2H1 | Rejected as invalid | `flatbuffers` verifies generated `Field` tables recursively with its default maximum depth of 64 before Arrow's schema conversion runs. An 80-level real IPC schema regression proves rejection occurs in FlatBuffers verification before the claimed recursive Arrow seat. The `arrow_ipc_decode` fuzz target remains build-only because libFuzzer aborts when the separate, intentionally caught negative-bitwidth panic starts. |
| 2H2 | Fixed | Every WAL segment open/decode pass is panic-contained and mapped to a typed damaged-WAL outcome; segment opens also use `O_NOFOLLOW`. The containment seam has a panic regression. |
| 2H3 | Fixed | Variable-width Arrow arrays charge the complete values buffers, including unreferenced extents. Slice and unreferenced-buffer regressions pin the conservative accounting. |
| 2H4 | Fixed | Arrow and JSON ingestion reject more than 1,000,000 rows before lineage/constant-column allocation. |
| 2M1 | Fixed | Pipeline parsing rejects YAML anchors and aliases outside quoted strings/comments before deserialization, and all CLI/library parse paths use the same bounded parser. This intentionally removes graph-bearing YAML rather than preserving compatibility. |
| 2M2 | Fixed | Post-close reader completion is cancellation-aware and bounded; close-then-park and parked-source regressions prove prompt termination. |
| 2M3 | Fixed | Source columns are capped cumulatively at 4,096 per table and child tables at 1,024 per parent, with refusal tests at both accumulation seats. |
| 2M4 | Fixed for the documented connector trust model | Handshake paths must be absolute, control-free UTF-8 and at most 107 bytes; error rendering is escaped. Connector executables remain operator-trusted, while their wire outputs are validated. |
| 2M5 | Fixed | P5 retains only the first 100 violation details, lazily formats them, and records an omitted-count summary. |
| 2M6 | Fixed | Raw JSON is conservatively charged at 32× wire size before parsing; checkpoints use the same retained-value accounting. Compact-JSON amplification has a regression. |
| 2M7 | Fixed | The residue-voucher manifest open uses `O_NOFOLLOW`; symlink refusal is covered directly. |

### Low

| Finding | Result | Remediation |
|---|---|---|
| 2L1 | Fixed | Every `StreamSpec` identifier seat, handshake state-format key, and recursively nested Arrow field name passes the same wire-boundary identifier gate. |
| 2L2 | Fixed | The predicate and escaping cover Cc controls plus line/paragraph separators, bidi formatting, zero-width and other dangerous formatting ranges. |
| 2L3 | Fixed as documentation/trust-boundary correction | WAL digests are explicitly documented as unkeyed accidental-damage detection, not authentication or ordering protection. A writer to the private WAL directory already has the engine user's authority; keyed integrity is not claimed. |
| 2L4 | Fixed | WAL directory entries are fsynced at creation/commit boundaries, and `Committed` is synced before segment garbage collection. |
| 2L5 | Fixed | WAL segment replay uses `O_NOFOLLOW`. |
| 2L6 | Fixed | Arrow decimal precision is restricted to 1..=38 and scale to 0..=precision before registry insertion. |
| 2L7 | Fixed | The complete generated reference-part filename, including `load_id`, is validated. |
| 2L8 | Fixed | Reference destination staging has a 256 MiB Arrow-footprint ceiling and resets accounting on replay, publish and close. |
| 2L9 | Fixed | SDK refusal redaction includes string, numeric and boolean config scalars; wire integration tests pin the redacted validation wording. |
| 2L10 | Fixed | Both source and destination services set the 64 MiB maximum encoding size as well as the decoding size. |
| 2L11 | Fixed | Serve-side destination Arrow decode is panic-contained; the real crafted 160-byte reproducer returns a typed refusal. |
| 2L12 | Fixed | Response-stream abandonment signals the forwarding task from either wait, closes the SPI channel and aborts the connector reader. Tests cover active production and a connector parked between pushes. |
| 2L13 | Fixed | Connector processes start from an empty environment with a narrow locale/path/temp/timezone allowlist. A regression proves host `HOME` and secret variables are absent. |
| 2L14 | Fixed | Every provider spawn receives its own process group; teardown kills the group before reaping the group leader. EOF status probing uses `waitid(WNOWAIT)` so a reaped/recycled PGID is never signalled. Descendant cleanup is tested. |
| 2L15 | Fixed | Pipeline and table identifiers are escaped at pretty-header and summary seats, including control and Unicode formatting characters. |
| 2L16 | Fixed | WAL directories carry an exact `.rdlt-wal` ownership marker; only empty real directories may be adopted, and destructive clear refuses an absent/mismatched marker. Symlink and foreign-directory cases are covered. |
| 2L17 | Fixed | The git-dependency checker uses a unique `mktemp` file with cleanup instead of a fixed `/tmp` pathname. |

### Informational items

| Finding | Result | Disposition |
|---|---|---|
| 2I1 | Accepted design constraint | `arrow_ipc_decode` is compiled but not run because libFuzzer's panic hook aborts before production `catch_unwind` containment can complete. The real deep-schema boundary is now a normal regression, and FlatBuffers verification defeats the reported 2H1 path. |
| 2I2 | Accepted | A sustained trusted-connector event flood remains time-bound and O(1) in retained memory; flood-then-silence remains deadline-bounded. |
| 2I3 | Accepted | The caught dependency panic hook can emit fixed diagnostic noise to inherited stderr. Connector binaries are operator-trusted and stderr inheritance is an explicit boundary in `SECURITY.md`. |
| 2I4 | Informational only | The doc-hidden fuzz helper is public, stateless and exposes no additional authority. |
| 2I5 | Fixed | A zero engine byte budget clamps to the smallest enforceable window instead of disabling admission control. |
| 2I6 | Verified and documented | Ingest/decode gates establish the Arrow depth invariant; recursive meter and deep real-schema regressions guard the boundary. |
| 2I7 | Fixed | Stale UDS reclamation only unlinks an actual socket inode; regular files are preserved and live sockets refuse collision. |
| 2I8 | Accepted/documented | Connector stderr remains inherited by design for operator-trusted binaries; it is called out in `SECURITY.md`. |
| 2I9 | Fixed | WAL manifest scanning uses bounded 1 MiB line reads and refuses oversized lines. |
| 2I10 | Accepted trust-boundary behavior | A writer to the private WAL directory can deny service; foreign-pipeline evidence is intentionally preserved rather than destructively cleared. Ownership and permissions are documented. |
| 2I11 | Fixed | The Python proof connector atomically opens with `O_NOFOLLOW`, and its raw JSON ceiling reserves protobuf-envelope overhead below the gRPC cap. |
| 2I12 | Fixed | Config loading reads through `take(MAX+1)` and checks the bytes actually read, eliminating stat/read growth races. |
| 2I13 | Fixed | `SECURITY.md` now records the policy and trust boundaries; `.rdlt*` workdirs are ignored, and the benchmark workdir moved under `target/` so repository-local row/WAL residue is not created. |
| 2I14 | Fixed | `ParquetOptions` validates automatically during deserialization, so a consumer cannot omit the safety gate. |

### Verification

- `make lint` — passed (`cargo fmt`, dependency-source check, workspace/all-target clippy, and SDK `serve` clippy with warnings denied).
- `make test` — passed: 746/746 workspace tests, all separately gated feature suites, spawned reference connector and certifier/kill matrices, Python proof-connector certification, and workspace doctests.
- `make docs` — passed with rustdoc warnings denied across all features.
- `make test TARGET=sweep` — passed all 4 failpoint crash/WAL sweeps.
- Focused regressions additionally cover real deep Arrow schemas, the crafted Arrow panic frame, WAL symlinks/ownership/durability, row/column/retention ceilings, environment/process-group isolation, UDS reclamation, response abandonment and bounded config reads.
