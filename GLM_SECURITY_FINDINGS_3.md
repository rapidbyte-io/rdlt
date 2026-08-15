# Security Analysis 3 — rdlt workspace (third deep review)

**Date:** 2026-08-15
**Scope:** same surface as rounds 1–2, reviewed at **`fe46ad79`** on `047-security-hardening` — the round-2 remediation commit (~2,000 lines across 51 files: `libc` added to the engine, `rustix`/process to the runtime, `SECURITY.md` created).
**Method:** six parallel subsystem re-reviews (client/protocol; WAL; engine shred/load/channel; SDK serve + runtime; certify/testkit/reference; CLI/core/python/fuzz/CI), each tasked with (a) verifying its round-2 findings at the current seats and (b) a fresh adversarial pass whose primary target was the fix code itself, plus areas earlier rounds covered lightly (rdlt-core full, engine runtime/*, bench harness, tools, .specify). Every new High below was then **re-verified by hand against the current tree and the vendored dependency sources** (arrow-ipc 58.3.0 `reader.rs`, flatbuffers 25.12.19 `verifier.rs`, serde_yaml 0.9.34 `de.rs`); Medium/Low findings carry line-level agent evidence as noted in §8.

**Severity scale (unchanged):** High — the stated adversary (rogue connector, corrupted-WAL writer, hostile pipeline YAML) can crash, hang, or abuse the host with trivial effort. Medium — narrower preconditions or reduced impact. Low — defense-in-depth/hygiene. Info — observations.

---

## 1. Executive summary

**The round-2 program mostly landed: 22 of 28 findings fixed clean**, most with regression pins — including the two deepest engineering items (WAL panic containment with degrade-to-re-extraction; process-group teardown, whose soundness was verified down to tokio's orphan-reap internals and rustix's `Pid` typing). **Five findings are only half-closed**: the fix landed at the named seat, but the same class survives through an adjacent mechanism (2H3, 2H4, 2M1, 2M3, 2L1). One closure is deliberate (2I1, the fuzz-run-set exclusion).

**One honest correction to round 2:** 2H1's premise was partially wrong. The flatbuffers 25.12.19 *verifier* — which arrow-ipc's parse path always runs before conversion — already enforces `max_depth = 64`, so the deep-nesting abort was not reachable at the pinned versions. The landed "fix" (a pinning test asserting an 80-level schema is refused with a depth error, plus documentation) is the correct response: it converts a dependency-owned invariant into a tripwire. Round 2 verified `convert.rs` had no cap but missed the verifier gate in front of it; this round traced the full path through `root_as_message` → `flatbuffers::root` → `VerifierOptions::default()`.

**Six new High findings — and the pattern is consistent: guards land where the finding pointed, not where the class lives.**

1. **3H1** — a ~30-byte `arrow_ipc` frame declaring a 2 GiB metadata length makes arrow's stream reader `resize` (commit + memset) 2 GiB *before* `read_exact` discovers the bytes don't exist. Not a panic — `catch_unwind` is irrelevant; under any memory limit it's an OOM kill of the host engine. This is the amplification *cousin* of the depth class: the round-2 fix pinned the guard that existed, and the guard that doesn't exist (declared-length sanity) went unnoticed.
2. **3H2** — the 2H3 meter fix charges whole buffers only for variable-width *values* buffers; offsets, primitive values, booleans, and validity bitmaps are still charged offsets-declared windows. A 64 MiB `Int64` values buffer with node length 1 meters **8 bytes**.
3. **3H3** — the 2H4 row cap covers the Arrow path, but the JSON path counts *roots* only, and only after the arena parse: `{"a":[{},{},…]}` is one root with unbounded children.
4. **3H4** — the 2M3 caps are per-table-column and per-parent-child, but total table count is unbounded and per-push work is O(T²) — one frame mints millions of tables and hangs + OOMs.
5. **3H5** — the 2M1 alias guard is a character scanner, and a single apostrophe inside a plain scalar (`pipeline: john's orders`) blinds it to end of document: the original quadratic expansion is fully restored.
6. **3H6** — one `mkfifo` in the WAL directory hangs recovery forever: `File::open` on a FIFO blocks, and no read-side open has `O_NONBLOCK` or a regular-file gate (the write side got exactly that gate for its marker this round — the read side didn't).

Two of the five Mediums are defects *in the new fix code* (a protocol-conformance bug introduced by the task-abort fix; a serve-side `Value` amplification introduced by the unconditional redaction sweep), a repetition of round 2's lesson: fresh fix code is the review frontier.

---

## 2. Round-2 fix verification

| Round-2 | Status | Evidence (current tree, `fe46ad79`) |
|---|---|---|
| 2H1 deep-nesting abort | **Fixed** (premise corrected — see §1; guard is dependency-owned, now pinned) | client pin `source.rs:606-629` (80-level schema → "depth" refusal); WAL seat shares the same flatbuffers gate (footer verified with `max_depth: 64`) but has **no pin of its own** → 3L5 |
| 2H2 WAL panic containment | **Fixed** | `caught_decode` at every decode path (pass 1 `replay.rs:96-105`, pass 2 open `:171`, per-batch `:188-198`); caught → `Ok(None)` → degrade to re-extraction (`recover.rs:247-258`); synthetic-panic pin. Not delivered: WAL-file-format fuzz target → 3L6 |
| 2H3 meter under-count | **PARTIAL** | values buffers of Utf8/Binary/LargeUtf8/LargeBinary now charge whole (`channel.rs:445-459`, pinned); **offsets, primitive, boolean, FixedSizeBinary, validity still window-charged** → 3H2 |
| 2H4 row amplification | **PARTIAL** | Arrow path: `MAX_RECORD_BATCH_ROWS = 1_000_000` typed refusal before materialization (`passthrough.rs:47-54`, pin `:364-396`); JSON path caps **roots only, post-parse** (`tape.rs:102-109`) → 3H3 |
| 2M1 YAML alias expansion | **PARTIAL** | scanner guard exists and is wired into every production YAML seat (`pipeline_spec.rs:195-249`), ADR 0002 amended; **blindable with one quote character** → 3H5 |
| 2M2 post-SourceFinished join | **Fixed** | three-way `select!` — join / `cancel.cancelled()` / 5 s `READER_FINISH_GRACE` (`extract.rs:301-319`) |
| 2M3 column cardinality | **PARTIAL** | per-table 4096 columns (`table.rs:134-140`, `passthrough.rs:190-196`), per-parent 1024 child tables (`tape.rs:258-264`); **struct-field breadth and total table count escape** → 3H4, 3M2 |
| 2M4 socket-path gate | **Fixed** | `Line::parse` refuses non-absolute, >107-byte, control-carrying paths (`protocol/handshake.rs:99-114`); `Dial` renders `{path:?}` (`error.rs:85`). Residual documented: config still reaches any *conforming* absolute path before id-check (operator-trust boundary) |
| 2M5 P5 violation strings | **Fixed** | cap 100 with lazy formatting (`wire.rs:131-133, 745-784`), census + omitted-count preserve the verdict's information; pin `:1289-1299` |
| 2M6 testkit Value retention | **Fixed (core)** | ×32 pre-parse factor, check before parse (`conformance/source.rs:53-57, 110-130`), live flood pin. Residuals → 3L9, 3L11 |
| 2M7 vouched manifest symlink | **Fixed** | `O_NOFOLLOW` on both arms (`writer.rs:239-252`), victim-survives pin `:632-651` |
| 2L1 identifier seats | **PARTIAL** | primary_key/cursor_field/type_hints/state_format_versions/arrow field names all gated (`source.rs:101-118, 131-163, 218-222`; `handshake.rs:255-257`); **Dictionary-inner field names bypass the walk** → 3L1 |
| 2L2 invisible Unicode | **Fixed (flagged classes)** | extended predicate `sanitize.rs:39-52` (Zl/Zp/bidi/BOM/zero-width); residuals both directions → 3L2, 3L3 |
| 2L3 unkeyed digest docs | **Fixed** | honest boundary in `record.rs:63-70` + `SECURITY.md` |
| 2L4 WAL fsync gaps | **Fixed** | dir fsync after creates (`writer.rs:373-377`); manifest fsync before GC unlinks with failure keeping `pending_gc` intact (`:409-434`) |
| 2L5 segment symlink | **Fixed** | `O_NOFOLLOW` at `replay.rs:30-33`. Read-side scan opens still follow symlinks → 3L8 |
| 2L6 decimal gate | **Fixed** | `passthrough.rs:325-332` — 1..=38, 0 ≤ scale ≤ precision; pin `:426-438` |
| 2L7 load_id filename | **Fixed** | whole part name gated (`reference/destination.rs:331-333, 457-470`); pin `:596-601` |
| 2L8 staging ceiling | **Fixed** | 256 MiB ceiling, honest meter, checked_add, typed FATAL (`:54, 234-244, 411-424`); publish-time re-encode unmetered → 3L10 |
| 2L9 numeric config echo | **Fixed** | `scalar_values_of` collects numbers/bools (`serve/common.rs:368-394`), wired `:518-522`; pin with `{"password": 12345}` |
| 2L10 encode ceiling | **Fixed** | `max_encoding_message_size` on both services (`serve/source.rs:660-670`, `serve/destination.rs:845-856`) |
| 2L11 serve decode belt | **Fixed** | `catch_unwind` + frozen prefix (`serve/destination.rs:326-334`), 160-byte reproducer pin `:917-935` |
| 2L12 leaked forwarding task | **Fixed** | watch-channel `stream_gone` race + `read_task.abort()` (`serve/source.rs:489-545`). **Introduced 3M4** (Status after terminal ErrorFrame) |
| 2L13 env inheritance | **Fixed** | `env_clear()` + 8-var allowlist (`local.rs:235-245`), HOME-unset pin; trade-offs → 3I4 |
| 2L14 process-group kill | **Fixed, sound** | `process_group(0)` at spawn; once-only `kill_process_group` ordered before reap in every path incl. `Drop` (`managed.rs:76-110`, `local.rs:279-293`); pgid-anchoring, no-reuse, and pgid-0-impossible all verified against tokio 1.53.0 and rustix 1.1.4 internals; descendant-kill pin. Limits → 3I3 |
| 2L15 pipeline name render | **Fixed** | `sanitize_identifier` at summary + Pretty header (`summary.rs:27`, `pretty.rs:37`); "one sink" claim amended |
| 2L16 workdir ownership | **Fixed** | `.rdlt-wal` marker; refuse-to-adopt non-empty marker-less dirs; `clear()` verifies before `remove_dir_all` (`writer.rs:70-155, 504-528`); lock serializes the race. Lock-file symlink nit → 3L14 |
| 2L17 fixed /tmp path | **Fixed** | `mktemp` + trap (`tools/check-git-deps.sh:40-44`) |
| 2I1 fuzz run set | **Deliberate partial** | `arrow_ipc_decode` still excluded from `FUZZ_TARGETS` (`Makefile:84-85`) with explicit rationale (libfuzzer abort-on-panic-start); compiled, containment pinned in the client suite |
| 2I5 byte_budget 0 | **Fixed anyway** | clamped to ≥1 (`config.rs:93-99`) |
| 2I7 bind_uds unlink | **Fixed** | live-probe → `is_socket()` → unlink (`serve/common.rs:239-247`); regular-file-preserved pin |
| 2I9 manifest line length | **Fixed** | 1 MiB cap (`scan.rs:19-44`), pinned |
| 2I11 python TOCTOU/overhead | **Fixed** | `os.open(..., O_NOFOLLOW)` (`:309`); `MAX_FRAME_BYTES - 16` headroom (`:63-67`) |
| 2I13 SECURITY.md / bench residue | **Fixed** | `SECURITY.md` created and content-verified against code; residue gitignored (`.gitignore:50`), not deleted → 3I12 |

---

## 3. New findings — High

### 3H1 — Declared-length memory amplification at the Arrow decode seat: a ~30-byte frame forces a 2 GiB commit-and-memset in the host before failing cleanly
**Where:** `crates/rdlt-connector-client/src/source.rs:207-210` (the `catch_unwind`-hardened seat hands `Cursor::new(bytes)` to `StreamReader::try_new`); the mechanism is in arrow-ipc 58.3.0 `reader.rs:1829-1837` (`MessageReader::maybe_next`), with bounds at `reader.rs:1859-1891`.
**Verified mechanism (vendored source):** `read_meta_len` reads an attacker-chosen `i32` and rejects only zero and negatives — any positive value up to 2 GiB−1 passes. `maybe_next` then executes `self.buf.resize(meta_len, 0)` — `Vec::resize` **writes a zero into every new slot**, committing and memsetting the full declared size — *before* `read_exact(&mut self.buf)` discovers the frame doesn't contain that many bytes and fails with a clean EOF error. The sibling vector: `MutableBuffer::from_len_zeroed(message.bodyLength() as usize)` — a negative `i64` wraps to a huge `usize`; under strict overcommit the allocation failure aborts via `handle_alloc_error`, which is not a panic and not catchable.
**Impact:** a rogue source sends a ~30-byte `arrow_ipc` frame whose 4-byte length field declares ~2 GiB. The host engine commits and memsets 2 GiB inside `decode_one_batch`, then refuses the frame typed. Repeatable per frame, unthrottled by the 64 MiB frame cap, the byte budget, or `catch_unwind` (a memset is not a panic). Under any memory limit (cgroup/systemd/k8s) this is an OOM kill of the host engine on demand; without limits it's a sustained 2 GiB RSS + memory-bandwidth burn. The WAL file-reader seat has a milder sibling (`read_footer_length` → `vec![0; footer_len]`, up to ~2 GiB from a 4-byte field) — mostly contained there by the new `caught_decode` and by `alloc_zeroed`'s lazy mapping, but the kernel OOM kill remains.
**Fix:** a framing pre-pass at the decode seat before `StreamReader::try_new`: iterate the encapsulated-message framing (skip continuation marker, read the `i32` meta length, skip meta + declared `bodyLength`) and refuse typed when any declared length exceeds `bytes.len()` — one small loop kills both vectors at the only Arrow wire seat. Apply the same sanity at WAL `open_segment` (refuse segments whose file size is smaller than any declared length). Pin both.

### 3H2 — The 2H3 fix charged whole buffers for values only: offsets, primitive, boolean, and validity buffers still meter their referenced window
**Where:** `crates/rdlt-connector/src/channel.rs:441-494` — whole-buffer charging landed at `:454` and `:459` (Utf8/Binary/LargeUtf8/LargeBinary **values** buffers) but: offsets buffers charge `(len+1)×width` (`:446-447, 457-458, 461-468`), `FixedSizeBinary` and primitive values charge `len×width` windows (`:469-481`), booleans and validity bitmaps charge bit windows (`:441-443, 492-495`).
**Verified against the dependency:** arrow-data 58.3.0 validates only a *minimum* buffer size (`data.rs:866-873`), and the IPC reader preserves wire-declared buffer lengths (`reader.rs:59-67, 264-297`); the client decode seat trims nothing. So a 64 MiB frame declaring an `Int64` column with node length 1 and a ~64 MiB values buffer decodes fine and **meters 8 bytes** — same neutralized-budget consequence as round-2's 2H3: with a slow destination, queued memory approaches (64 msgs × 64 MiB) + (256 stage msgs × 64 MiB) ≈ 20 GiB against a 64 MiB configured budget, and each item WAL-encodes ~64 MiB to disk.
**Fix:** charge whole buffers for the remaining layouts exactly as `:454` does (offsets, primitive values, bitmaps, FixedSizeBinary), or trim/refuse at the decode seat when a buffer exceeds what its node length can reference. Extend the 2H3 pin to an `Int64` oversized-buffer frame.

### 3H3 — The JSON row cap counts roots only and fires after the arena parse: child rows per push are unbounded
**Where:** `crates/rdlt-engine/src/shred/tape.rs:100-109` — `roots.len() > MAX_RECORD_BATCH_ROWS` is the only row bound, checked after `arena.parse_rows(bytes)`; descendants flow through `shred_root` → `enqueue_children` (`:204-237`) into per-table row vectors with no cap; drain builds one whole batch per table per push (`drain.rs:234-259`, `build.rs:88-167`).
**Impact:** one root `{"a":[{},{},{},…]}` in a ≤64 MiB `RawJson` push (admitted by drain-the-budget-and-go even under a tiny budget) carries ~22 M child rows; each becomes a `Queued` (~136 B), a `TapeRow` (~136 B, `RowId` is `[u8;32]`), a `DrainRow`, and an output row materializing load_id + lineage ids (~210 B) — multi-GB allocation and OOM from one legal frame. The output batch can also exceed the 1 M-row cap (it is input-side only). Secondary: the cap fires only **after** `parse_rows`, so the dense-NDJSON variant (`[0,0,0,…]` → ~33 M roots) still allocates a ~1.2 GB arena before the refusal.
**Fix:** bound *total rows observed per push* (roots + descendants, counted in `shred_root`'s queue) and check roots progressively during parse; or cap per-table output rows and chunk the drain.

### 3H4 — No global table-count cap, and per-push work is O(T²): one frame mints millions of tables and hangs + OOMs the engine
**Where:** `crates/rdlt-engine/src/shred/tape.rs` — `MAX_CHILD_TABLES_PER_PARENT` (1024) at `:258-264` is per-parent only; `self.tables.push` at `:281` has no global bound; the miss path linearly scans all tables (`:278`); every push clones every table's column state into `rollback_snapshot` (`:97-98`) and allocates a row vector per table (`:112`).
**Impact:** a ~64 MiB crafted JSON push where distinct keys create distinct child tables (≈8–9 slab bytes per table) creates millions of `TableBuffer`s (~200–300 B each) → multi-GB resident, **plus** quadratic per-push work: the memo only helps repeated keys — distinct keys always hit the linear scan; `push_and_drain` re-clones all column states and re-runs `resolve_schema` over every table per push (`drain.rs:106-141`). Sustained hang + OOM from one frame. This is the third member of the cardinality family (2M3 → cap landed, breadth and totals didn't).
**Fix:** cap total tables per stream (e.g. 64 Ki) with the same typed refusal; consider making the table index a map keyed by name to kill the linear scan.

### 3H5 — The YAML alias guard is a character scanner blindable by one quote character inside a plain scalar: the 2M1 quadratic expansion is fully restored
**Where:** `crates/rdlt/src/pipeline_spec.rs:195-244` — in `State::Plain`, any `'` enters `SingleQuoted` and any `"` enters `DoubleQuoted` (`:209-210`); with no closing quote, every byte to EOF is treated as quoted data and the `&`/`*` checks (`:212-217`) never fire again.
**Verified semantics:** quotes are YAML indicators only at the start of a scalar — `pipeline: john's orders` is a legal plain scalar whose apostrophe the scanner misreads as quote-open. Cross-checked against libyaml-lineage semantics (PyYAML): `a: don't\nb: &boom value\nc: *boom` parses with a live anchor and alias, which serde_yaml 0.9.34 materializes (round-2's 2M1 premise, re-confirmed in the vendored `de.rs`). Deserialization need not even succeed — the expansion happens while materializing the inline config during the attempt.
**Impact:** hostile pipeline YAML restores the full round-2 amplification with one apostrophe: ≤8 MiB of anchor content plus millions of 3-byte alias spellings → O(E²/4) `serde_json::Value` nodes → OOM/hang of `rdlt run`/`rdlt validate`. The tests at `:757-779` cover quote-opens and comments but not quotes *mid-scalar*.
**Fix:** stop approximating YAML lexing — use the real parser's event stream before deserialization (serde_yaml's own `Loader`/event iteration sees `Alias` events pre-expansion; ADR 0002 records serde_yaml_ng exposes the loader API) and refuse on any Anchor/Alias event. A character scan cannot distinguish quote-open from quote-in-data without full context. This also fixes the over-rejection flip side (3L12).

### 3H6 — One `mkfifo` in the WAL directory hangs recovery forever: read-side opens have no `O_NONBLOCK`/regular-file gate and nothing up the stack has a timeout
**Where:** `crates/rdlt-engine/src/wal/resume/scan.rs:125-126` (`File::open(&dir.join("manifest.jsonl"))`), `scan.rs:357` (`read_to_string` on the rules sidecar), `replay.rs:30-33` (segment opens — `O_NOFOLLOW` rejects symlinks, not FIFOs).
**Verified:** POSIX `open(O_RDONLY)` on a FIFO with no writer blocks until a writer appears. The write side gained exactly the right gate this round (`writer.rs:87, 142-144` verify `file_type().is_file()` before touching the marker); the read side has nothing, `off_runtime`/`scan_off_runtime` have no timeout, and neither does `runtime/recover.rs`. The grep is exhaustive: the only file-type gates in `wal/` are the writer's marker checks.
**Impact:** the corrupted-WAL adversary (the stated threat model — same-OS-user write access to the workdir) runs `mkfifo <workdir>/wal/manifest.jsonl` once; every subsequent run's recovery blocks forever on the open — the run future never resolves and one blocking-pool thread is consumed per attempt. The host process survives, which an operator may weigh for severity, but the pipeline is permanently wedged until manual intervention — the same liveness class round-1 H1 was written about, now on-disk instead of on-wire.
**Fix:** open WAL-path files with `libc::O_NONBLOCK` in `custom_flags`, then `metadata()` on the handle and refuse non-regular files (`S_ISREG`) as `Damaged` (drop `O_NONBLOCK` afterward if desired); or stat-first with `symlink_metadata` + `is_file()`. The write side's marker gate is the in-repo template.

---

## 4. New findings — Medium

### 3M1 — Serve-side `serde_json::Value` amplification: one 64 MiB `config_json` frame expands past 2 GB inside the connector process
**Where:** `crates/rdlt-connector-sdk/src/serve/common.rs:508-518` — `handshake` parses `request.config_json` into an untyped `Value`, then **unconditionally** (before knowing whether the redaction path will even run) calls `scalar_values_of(&config)`, which clones every scalar leaf into a `String`; same shape at `serve/source.rs:463-471` (`since_cursor_json` retained inside the `Cursor`).
**Impact:** a compact 64 MiB document (`[0,0,0,…]`, ~2 wire bytes per element) expands to ~33 M `Value` nodes (~800 MB) plus ~33 M heap `String`s from the sweep — >2 GB transient in the connector process from one legal-size frame. The frame cap (64 MiB) bounds the *wire bytes*, not the *materialization* — the same gap class as 2M1/3H2, on the serve side. A failed handshake leaves the `OnceLock` unset, so the (same-uid, socket-owning) adversary can repeat it. This regression was *introduced* by the 2L9 fix (the string-only sweep used to be lazy inside the refusal arm).
**Fix:** refuse `config_json`/`since_cursor_json` over a small typed ceiling at these seats (host-side configs are already capped at 8 MiB before being sent); compute `scalar_values_of` lazily inside the `Err` arm of `from_config` so successful handshakes never pay the sweep.

### 3M2 — Struct-field breadth bypasses the 4096-column cap on both ingest paths
**Where:** Arrow: `passthrough.rs:190-196` caps only `batch.num_columns()`; the `Struct` mapping arm (`:333-346`) has no field-count limit. JSON: `infer.rs:144-158, 198-208` accumulates nested-object fields in `ColumnState::Struct(...)` without passing through `state_mut`'s cap.
**Impact:** a rogue declares one column `s: Struct<1M fields>` (~25–60 MiB schema frame): `schema_from_arrow` builds 1 M `ColumnDef`s (~100 MB), the registry **retains** them, and `join_column_types_at` + `registry.diff/apply` re-clone the struct every batch — sustained resident + CPU amplification ~4× the wire schema, surviving for the stream's lifetime. Depth is capped at 64; breadth is not.
**Fix:** count struct fields (recursively, bounded by the depth cap) toward the source-column cap, or cap per-struct field count.

### 3M3 — `stream_task` error paths leak the reader task and skip `input.close()`
**Where:** `crates/rdlt-engine/src/runtime/extract.rs:186, 188, 258, 260` — early `?` returns (shred/passthrough errors, including the new cap refusals) exit before the cleanup block at `:286-328`. The spawned reader (`:158`) is detached, holds an `Arc<dyn Source>` and the `RecordsOut`, and never watches the cancel token; dropping `input` closes the mpsc queue but **not** the budget semaphore (`close()` does; drop doesn't — `channel.rs:186-189`).
**Impact:** a rogue triggers a typed refusal cheaply (one 1,000,001-row boolean batch) and every run leaks one parked task plus its source/channel state; an embedder re-running on a schedule accumulates leaks unboundedly. Bounded per run (the refusals are non-retryable), so Medium.
**Fix:** structure the push loop so early errors still run `input.close()` and abort/join the reader (break-with-error instead of `?`).

### 3M4 — New-fix bug: a transport `Status` can follow the terminal `ErrorFrame` on the serve source path
**Where:** `crates/rdlt-connector-sdk/src/serve/source.rs:523-527` (encode-failure arm sets `terminal_sent = true; abort_reader = true`), `:543-545` (`read_task.abort()`), `:559-565` — the `Err(join_error)` arm sends `Status::internal(...)` through `frame_tx` **without checking `terminal_sent`**, unlike the `Ok(Err(_))` arm which is gated.
**Impact:** when an Arrow encode failure fires while the connector's read task is still running, the 2L12 abort turns its completion into a `JoinError`, and the ungated arm appends a transport error to a response stream that already ended with the protocol's *terminal* `ErrorFrame` — violating the contract the same loop documents ("once one is on the stream nothing may follow it", `:482-488`). Clients reading to the end (including the certifier's `read_frames`) misrender the fatal refusal as a mid-flight transport failure.
**Fix:** gate the `Err(join_error)` arm on `!terminal_sent`, mirroring the `Ok(Err(_))` guard. One-line fix, plus a pin.

### 3M5 — Reference destination: a transiently-failed `publish` drops staging, so a client retry-without-rewrite mints a receipt for a commit whose rows are gone
**Where:** `crates/rdlt-connector-reference/src/destination.rs:310-314` — `publish()` drains `self.staged` (and zeroes the accounting) *before* any persist; mid-publish failures classify TRANSIENT (`:523-527`); the receipt/state write follows at `:339-350`.
**Impact:** a transient failure mid-publish (e.g. disk full during the second table's persist) leaves earlier tables durable, staging gone; a foreign client that retries `publish` without re-writing gets a zero-parts publish that still writes state and appends the receipt — `existing_receipt` then vouches for a commit whose rows are partially absent, silently. The module's crash-then-rewrite convergence story (the engine's WAL recovery re-writes rows) does not cover retry-without-rewrite. Pre-existing, missed by rounds 1–2, surfaced because the 2L8 fix touched this function; the reference connector is the template third parties copy.
**Fix:** drain into a local only on success (persist from `self.staged` by reference, clear at the end), re-stage un-published tables on failure, or classify mid-publish failures fatal.

---

## 5. New findings — Low

- **3L1 — Dictionary-inner field names bypass the arrow field-name gate.** The walk at `client/src/source.rs:144-160` has no `DataType::Dictionary(_, value)` arm — a dictionary-encoded nested container (`Dictionary(Int32, Struct([...]))`, encodable by arrow's own writer) carries its inner field names into host vocabulary ungated. Add the arm (iteratively).
- **3L2 — Extended Cf coverage still misses code points**: U+0600–U+0605, U+06DD, U+070F, U+08E2, U+110BD, U+1D173–U+1D17A, U+1BCA0–U+1BCA3, U+3164, U+FFA0, and the tag block U+E0001/U+E0020–U+E007F (the classic filename/identifier-spoofing tool). Consider a general category test instead of hand-listed ranges.
- **3L3 — The extended predicate now refuses legitimate identifiers.** U+200C/U+200D (ZWNJ/ZWJ) fall in the refused range: load-bearing in Persian/Malayalam/Devanagari orthography and compound emoji — honest connectors with such stream/table names now get FATAL refusals at the wire edge. Record the trade-off as an owner decision or special-case 200C/200D in identifiers.
- **3L4 — Protocol socket-path check is Cc-only** (`protocol/handshake.rs:110-114`) while the client refuses the extended set — dependency direction prevents sharing; U+202E/U+200B in a socket path passes parse (the render escapes it, so display-safe; consistency nit).
- **3L5 — The flatbuffers depth guard is unpinned at the WAL seat.** The client pins the 80-level refusal (`source.rs:606-629`); the WAL `FileReader` seat relies on the same dependency default with no test — an arrow/flatbuffers bump that relaxes verification would silently reintroduce the abort class there. Add the client's test shape to the WAL segment-format suite.
- **3L6 — No WAL segment fuzz target.** `arrow_ipc_decode` covers the client's `StreamReader`; the `FileReader`+footer+`next()` path (with its new catch-and-degrade dance) has no coverage door for *new* panic arms.
- **3L7 — `blocking.rs:11-14` still teaches the round-1 re-raise policy** while `replay.rs:40-44` now states the opposite for decode panics; behavior is coherent (decode panics are caught inside the closure) but the seam's doc would mislead the next call-site author.
- **3L8 — Scan-side manifest/sidecar reads follow symlinks** (`scan.rs:126`, `:357`) while the writer/segment opens are `O_NOFOLLOW` — a `manifest.jsonl → outside-file` symlink steers scan verdicts by foreign content. Add the flag for a uniform boundary.
- **3L9 — Testkit's ×32 factor under-counts the allocator-rounded worst case** (~40× for chains of 1-element arrays, bounded by serde_json's 128-depth default), and `verify_source`'s replay fold holds ~3–4× the ceiling in peak (full + resumed + expected + `all_rows()` clone). Raise the factor or compare by hash past a sub-ceiling.
- **3L10 — Reference `publish` re-encodes every staged batch into one in-memory JSON `Vec`** (`destination.rs:317-333`) outside the 256 MiB staging meter — transient publish-time memory a few × past the ceiling for struct/JSON-heavy schemas. Stream-encode to the temp file or meter `encoded.len()`.
- **3L11 — `testkit`'s `read_all` has no overall deadline** (`conformance/source.rs:92-177`; the 5 s timeout covers only the post-drain join) — a source parking inside `read()` hangs direct third-party harness callers (the certifier bounds it externally at 30 s).
- **3L12 — The YAML pre-pass over-rejects legitimate documents** (fail-closed): `&`/`*` as data in block scalars and mid-plain-scalar (`key: a&b`), and `#` mid-token (`key: val#ue`), all refuse. Availability regression on the primary config surface; the event-based fix for 3H5 cures both directions.
- **3L13 — `rdlt-connector-sdk`'s `Document::from_yaml` is an unguarded serde_yaml seat** (`config.rs:91-92`) — a public trait method with no alias guard, size cap, or warning; the ADR's "production parsing rejects anchors and aliases" covers only the facade path. Share the (fixed) guard.
- **3L14 — The workdir lock file open follows pre-planted symlinks** (`runtime/lock.rs:30-35` — plain `create(true).write(true)`) in contrast to the WAL's `O_EXCL`/`O_NOFOLLOW` discipline. Consistency nit; requires workdir write access.

---

## 6. New findings — Info

- **3I1 — 2H1 premise correction (recorded):** the flatbuffers verifier (`max_depth: 64`) predates the workspace and gates both decode seats; the landed pin is the right enforcement. The guard remains dependency-owned — the pin and the (excluded) fuzz target are the only tripwires; keep both in mind on any arrow/flatbuffers bump.
- **3I2 — `arrow_ipc_decode` remains excluded from the run set** (documented rationale: libfuzzer abort-on-panic-start). Note the tension: the class it hunts (schema/framing decode failures) is exactly where 3H1 landed.
- **3I3 — Process-group kill verified sound in depth** (pgid anchoring holds through tokio's orphan queue; no pgid-reuse window; `kill(-0)` structurally unreachable via `rustix::Pid`'s `NonZeroI32`). Limits: a child calling `setsid()` escapes the sweep; no `PDEATHSIG` on host death. Descendant hygiene, not containment — matches the fix's framing.
- **3I4 — env allowlist trade-offs:** HOME/XDG_*/SSL_CERT_FILE/LD_LIBRARY_PATH are dropped (the security win: ambient `~/.aws`-style credentials no longer ride along; the cost: connectors needing user-site pip installs or custom cert/lib paths fail at exec). Worth a release note for connector authors.
- **3I5 — Redaction is plain substring replacement** over the whole refusal message — short scalar needles (`"0"`, `"true"`) rewrite those substrings anywhere, including connector-authored wording; cosmetic diagnostic degradation, pinned deliberately.
- **3I6 — No cross-`Read` admission cap on the source serve side** (per-RPC budgets only; unlimited connections × ~40–100 MiB pinned each vs the destination side's session CAS). Same-uid precondition; consider a process-wide semaphore.
- **3I7 — WAL replay pass 2 applies re-opened segments without re-verifying row counts** (pass-1 TOCTOU) — moot under the documented unkeyed-checksum boundary; noting for completeness.
- **3I8 — `wal` leaf mkdir is not followed by a parent fsync** — safe-direction (post-commit loss re-extracts); every fsync error path in `sync_for_commit`/`mark_committed` propagates honestly.
- **3I9 — The 1 MiB manifest line cap has no writer-side counterpart** — a legitimately huge `Delta` schema would make a run's own WAL scan `Damaged` (safe-direction availability loss only); a comment tying the cap to the shred-time bounds would make the intent explicit.
- **3I10 — WAL dirs written by pre-marker builds are no longer adoptable** (`writer.rs:113-121`) — loud, deliberate, consistent with SECURITY.md's no-shims stance.
- **3I11 — SECURITY.md gaps:** no reporting contact/channel ("report privately to the repository owners" — add an address or GitHub private-vulnerability-reporting reference); the "materialized cardinality… bounded" line rests on the guard 3H5 bypasses — soften or fix.
- **3I12 — Bench residue persists on disk** (`benches/harness/cells/pipelines/.rdlt-load1/` — gitignored but not cleaned by the harness; can hold real row data locally).
- **3I13 — Python connector off-by-one conservatism** (a row of exactly `MAX_FRAME_BYTES − 16` bytes plus its newline is refused — availability only); `.specify/` scripts use `eval` on repo-local output (dev-only, gitignored, not shipped).
- **3I14 — The meter's slice pin was deliberately weakened** (`sum <= chunks × whole` now allowed) to admit whole-buffer charging — the documented safe direction (over-count throttles, never uncaps); each decoded batch's buffers are disjoint slices of its own IPC body, so the 10–17× historical over-count does not return.
- **3I15 — No new certify rogues this round** — the deep-nesting and crafted-panic WAL cases landed as in-repo unit pins (client depth pin; WAL synthetic-panic pin) rather than out-of-process certification rogues; defensible given the dependency-level guard, but nothing in certify pins the behavior end-to-end.

---

## 7. Recommended fix order

1. **3H5** — replace the YAML character scan with an event-based guard (parse the event stream, refuse Anchor/Alias events). Also cures 3L12 and de-risks 3L13. The current scanner is a security boundary resting on a lexer approximation.
2. **3H1** — framing pre-pass at the Arrow decode seat (declared lengths vs `bytes.len()`) + WAL `open_segment` size sanity; pins for both. Small, self-contained, closes the only trivially-reachable OOM-abort on the wire.
3. **3H2 + 3H3 + 3H4** — finish the memory-ceiling family: whole-buffer charging for the remaining layouts; total-rows-per-push bound (progressive); global table cap (+ kill the linear scan). One theme, three small diffs, plus pins on the under-count/amplification directions.
4. **3H6** — `O_NONBLOCK` + `S_ISREG` gate on WAL read-side opens (the writer's marker gate is the template); a FIFO fixture in the crash-recovery suite.
5. **3M4** — the one-line `!terminal_sent` gate + pin (protocol conformance bug in fresh code).
6. **3M1** — serve-side `config_json` ceiling + lazy redaction sweep.
7. **3M2, 3M3, 3M5** — struct-breadth counting; `stream_task` cleanup-on-error; reference publish drain-on-success.
8. **Lows/Infos** as scheduled hardening — 3L5/3L6 (WAL depth pin + fuzz door), 3L1/3L2/3L3 (sanitizer edges and the ZWJ/ZWNJ owner decision), 3L8/3L14 (symlink uniformity), 3I11 (SECURITY.md contact).

---

## 8. Caveats

- Line numbers are from `fe46ad79` and will drift.
- **Verification confidence:** all six Highs were hand-verified in the current tree and/or vendored dependency sources — 3H1 (arrow-ipc `reader.rs` `maybe_next`/`read_meta_len` read directly; the resize-before-read mechanism and the bounds check confirmed), 3H2 (the meter arms at `channel.rs:441-494` read directly), 3H3 (`tape.rs:100-125` read directly), 3H4 (`tape.rs:240-289` + the per-push snapshot at `:97-112` read directly), 3H5 (the scanner at `pipeline_spec.rs:195-244` read directly; YAML plain-scalar quote semantics cross-checked against a libyaml-lineage parser), 3H6 (the scan open at `scan.rs:125-126` plus an exhaustive grep proving no read-side file-type gate exists in `wal/`). The Mediums and Lows carry line-level subsystem evidence; 3M1, 3M4, and 3M5 quote the exact new-fix code and are high-confidence but were not all re-derived by hand.
- **No tests were executed by this review:** the analysis host has no C linker, so `cargo test` cannot run. The repo's own round-2 record claims `make test` 746/746 at this commit; the claims here are source-verified, not execution-verified.
- The 2H1 correction (§1, 3I1) reflects an error in round 2's reachability analysis, re-traced this round through `root_as_message` → `flatbuffers::root` → `VerifierOptions::default()`. Round 2 verified `convert.rs` had no depth cap but missed the verifier gate in front of it.
- Severity continues to assume the documented trust model (D-038-1, now also stated in `SECURITY.md`): operator-installed connectors, plaintext UDS, same-OS-user WAL directory writer. Findings shift up if connectors are ever sourced from a less-trusted channel.
- No network advisory scan was run (standing caveat since round 1).
