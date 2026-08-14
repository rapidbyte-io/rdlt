# Security Analysis — rdlt workspace

**Date:** 2026-08-14
**Scope:** all 13 crates in `crates/`, `connectors/python/`, `fuzz/`, `tools/`, `.github/workflows/`, `docs/adr/`, dependency manifests (`Cargo.toml` + `Cargo.lock` versions; no network advisory scan was run — see §6).
**Method:** four parallel deep-dive reviews (SDK serve + client; engine/WAL; certify/testkit/reference; Python/fuzz/docs/CI) plus direct review of the core trust-boundary code (`rdlt-connector-protocol`, `rdlt-runtime` spawn/lifecycle, `rdlt-cli`, `rdlt::pipeline_spec`, `rdlt-core::commit`). The highest-severity findings below were re-verified against the source by hand, including line-level confirmation of every `remove_file`/`remove_dir_all` site in the tree, an exhaustive `timeout` grep over the client crate, and locked dependency versions.

**Severity scale used here:**
- **High** — an actor inside the project's own stated adversarial position (a rogue or buggy connector, a corrupted WAL directory) can crash, hang, or abuse the host process with trivial effort.
- **Medium** — same actor, but narrower preconditions or reduced impact; or a systemic gap on a declared-untrusted input surface.
- **Low** — defense-in-depth, hygiene, or issues requiring an operator-level mistake to matter.
- **Info** — observations and accepted-design notes worth recording.

---

## 1. Executive summary

The codebase is in unusually good security shape for its category. There is **no `unsafe`** anywhere except the CLI's two-integer `mallopt` FFI call (`crates/rdlt-cli/src/main.rs:58-67`, workspace lint `unsafe_code = "deny"`). Memory bounds are real and named — one shared 64 MiB frame ceiling installed on every served service and every client constructor, byte-budgeted channels with permit-held-until-drop semantics, a hard-capped arena presize. Secrets have masking types with hand-written `Debug` impls at every seam. The adversarial test surface (29 fail-proven certification clauses, a designated rogue for each, a nine-arm SIGKILL convergence matrix with its own vacuity arms) is stronger than most production software ships.

No Critical findings. The genuine gaps cluster in three places:

1. **Liveness above the wire** — the client crate has *zero* deadlines: a connector that binds its socket and then goes silent hangs dial/handshake/read forever, which contradicts the project's own advertised "typed error, not hang, within ten seconds" law (H1). This is the single most impactful fix available.
2. **The one seat that got missed wherever a defense exists elsewhere** — the runtime guards its socket unlink with an `lstat`-is-socket check, but `rdlt-certify` deletes whatever path the connector's handshake line named (M2); the control-character gate exists for source stream names but not for error text, table names, or spec payloads (M5); the JSONL path has a recursion depth cap but the structured Arrow path does not (M1).
3. **On-disk WAL trust** — manifest lines and segment filenames are consumed with no checksums and no name validation, so anything that can write the WAL directory can steer replay at arbitrary files or forge commit records (M3, M4). This is partly a documented design boundary ("replayable buffer, never the source of truth"), but a per-line CRC and a filename shape check are cheap closers.

---

## 2. Threat model

The project documents its own trust model, and this review holds the code to it:

- **D-038-1** (`crates/rdlt-connector-protocol/src/lib.rs:44-61`, ADR 0001): config documents (which may carry credentials) cross the UDS **in the clear**; there is no protocol-level auth/encryption in v0. A spawned connector inherits its operator's trust "exactly like any other child process." The socket is owner-only `0600`. **Plaintext-over-UDS is accepted by design and is not scored as a finding.**
- The identity check at handshake is explicitly documented as *a sanity check, not authentication* (`crates/rdlt-connector-client/src/handshake.rs:121-131`, `crates/rdlt-runtime/src/local.rs:59-66`) — string equality catches the wrong binary, never a malicious one. Content-digest pinning is a recorded future door.
- **Adversaries this review applied:** (a) a rogue or buggy connector binary (the certify crate's entire reason for existing — `rogue.rs` ships designated violators), (b) corrupted or crafted on-disk state (WAL manifest/segments), (c) hostile pipeline YAML, (d) the local multi-user/misconfigured-permission environment.

---

## 3. Findings

### High

#### H1 — No deadlines anywhere above the wire: a rogue or merely hung connector stalls the pipeline forever
**Where:** `crates/rdlt-connector-client/src/dial.rs:39-58` (no `connect_timeout`, no `http2_keep_alive_*` on the `Endpoint`), `crates/rdlt-connector-client/src/handshake.rs:132-148` (unbounded `.handshake().await`), `crates/rdlt-connector-client/src/source.rs:215` (`frames.message().await` with no per-frame deadline), `crates/rdlt-connector-client/src/destination.rs:293` (`replies.message().await`).
**Verified:** an exhaustive grep for `timeout` over `crates/rdlt-connector-client/src` returns *nothing*. The only liveness bound in the spawn path is the provider's **stdout-line** timeout (`DEFAULT_LINE_TIMEOUT = 10 s`, `crates/rdlt-runtime/src/local.rs:28`); everything after the line — dial, transport setup, handshake, every stream read, every destination RPC — is unbounded. The engine's own timeouts (`READER_ABORT_GRACE` in `extract.rs:297`, `load/item.rs`) sit above channel sends, not above the wire calls.
**Impact:** a connector that prints its handshake line and then never sends the HTTP/2 preface, or completes the transport and never answers `Handshake`, or stalls mid-`Read` — hangs the engine's task indefinitely with zero memory growth and zero surfaced error. No panic, no typed error: a full pipeline-liveness DoS from one connector process. It doesn't require malice — an ordinary connector deadlock produces the same forever-hang.
**Why it matters doubly:** the project's own certification law is "dead connector ⇒ typed error within ten seconds, never a hang" (kill-matrix K-S1/S2/S3, `crates/rdlt-certify/src/kill.rs`). That law is proven for *SIGKILL* (the socket dies, so h2 errors out) but not for the *silent-but-alive* connector, which is exactly the case a deadline would catch.
**Fix:** thread a configurable RPC deadline through `dial` (`Endpoint::connect_timeout` + `http2_keep_alive_interval/timeouts_with_retry`) and wrap handshake/each `message()` await in `tokio::time::timeout` with a typed `Timeout` error; add a rogue to the certify suite that binds, handshakes, then goes silent, and pin the typed-not-hang outcome.

### Medium

#### M1 — Unbounded recursion over connector-supplied Arrow schemas → stack overflow, which is an abort, not a catchable panic
**Where:** `crates/rdlt-engine/src/load/lowering.rs:108-140` (`flatten_array` recurses per struct child), `crates/rdlt-engine/src/shred/passthrough.rs:215-290` (`join_column_types` / `column_type_from_arrow`, inside `spawn_blocking`), `crates/rdlt-connector/src/channel.rs:330-425` (`data_footprint` recursion in the budget meter).
**Impact:** recursion depth is driven solely by the nesting level of the Arrow schema a source connector declares (structured streams accept `PushPayload::Arrow`). Thousands of nested `Struct` fields → stack overflow → SIGSEGV/abort of the host process. Panics in `spawn_blocking` are contained and mapped to `RdltError::internal` (`extract.rs:72-102`), but **stack overflow is not a panic** — task containment does not help.
**Preconditions:** a rogue or badly-behaved source connector on a structured stream. The JSONL path is safe: serde_json's default 128-depth recursion limit is never disabled anywhere in the workspace (verified). The codebase's own comments already treat remote Arrow as untrusted (`channel.rs:251-255`: IPC-built child data "skips the validator").
**Fix:** a depth cap (constant, e.g. 64) in `column_type_from_arrow` / `flatten_array` / `data_footprint` returning a typed error; extend the certify wire clauses with a deep-nesting rogue.

#### M2 — rdlt-certify deletes any file the connector's handshake line names
**Where:** `crates/rdlt-certify/src/wire.rs:196`, `:271`, `:339` — `let _ = std::fs::remove_file(&socket);` on every cleanup/drop path, where `socket` is the path parsed verbatim from the spawned connector's stdout (`Line::parse` validates only non-emptiness, `crates/rdlt-connector-protocol/src/handshake.rs:88-96`). Also parked for the P1 probe at `crates/rdlt-certify/src/target.rs:282-284`.
**Impact:** a connector under certification prints `rdlt-connector|1|0|0|/home/user/important_file`; the dial fails; the error path still unlinks the named file. The deletion happens *even though the dial already failed* — there is no "is it a socket / is it under the expected temp root" check. Rogue connectors are this tool's explicit subject, so the input is squarely adversarial. No privilege boundary is crossed (the connector already runs as the certifier's user), hence Medium not High.
**The proof this is a miss, not a choice:** the runtime's `LifecycleGuard::drop` guards the exact same operation with `symlink_metadata(...).is_socket()` and a comment explaining why (`crates/rdlt-runtime/src/managed.rs:70-83`: "a connector naming an unrelated file must not commission this host to delete it"). The certify crate never received the same hardening.
**Fix:** replicate the lstat-is-socket guard (or a temp-root prefix check) at all three sites in `wire.rs` and in `target.rs`.

#### M3 — WAL replay joins a manifest-supplied filename unsanitized (path traversal / absolute-path override)
**Where:** `crates/rdlt-engine/src/wal/resume/replay.rs:24` — `let path = dir.join(file);` with `file: String` taken verbatim from a `WalRecord::Segment` manifest line (`crates/rdlt-engine/src/wal/record.rs:49-53`). The writer always produces `{load_id}-{:06}.arrow` (`writer.rs:186`) but the reader never checks the shape.
**Impact:** `Path::join` with an absolute component replaces the base entirely; `../../x` escapes the WAL dir. An attacker who can write the WAL directory (shared storage, wrong permissions, compromised sibling process) can point replay at any readable file. The file must still decode fully as Arrow IPC (footer validated at open, `replay.rs:13-28`) and match the manifest's declared row count (`replay.rs:69-89`), so the practical exploit ships a planted `.arrow` file alongside the crafted manifest — trivial for someone who controls the directory. Read-only outside the WAL dir (GC only unlinks writer-created paths, `writer.rs:278-286`; `clear()` is `remove_dir_all(wal_dir)`, `writer.rs:348-353`).
**Fix:** reject any `file` containing `/`, `\`, `..`, or not matching the writer's `{load_id}-N.arrow` pattern before opening.

#### M4 — WAL manifest lines carry no checksums: valid-JSON corruption or forgery is accepted silently
**Where:** `crates/rdlt-engine/src/wal/writer.rs:290-303` (`append` = `serde_json::to_vec` + newline, no CRC), `crates/rdlt-engine/src/wal/resume/scan.rs:114-124` (any line that parses as JSON is accepted).
**Impact:** existing detection is real but content-blind: JSON parseability, the segment footer validation, the manifest↔segment row-count cross-check, version/pipeline/rules-sidecar gates. Corruption that yields *different valid JSON* is accepted silently:
- a flipped digit in a `Checkpoint` cursor commits a resume position the source never issued → the next extraction **silently skips rows** (permanent loss; `Cursor` is deliberately opaque, `rdlt-core/src/cursor.rs:5-7`);
- a forged `Committed { commit_seq }` truncates the replay span (`scan.rs:176-179`) → covered rows are never replayed while the destination never received them.
**Context:** the WAL is documented as "replayable buffer, never the source of truth" (`wal/mod.rs:3-8`), and silent disk corruption producing valid JSON is a narrow window — but under the malicious-on-disk threat model it is a real gap, and blake3 is already a workspace dependency (used for identity/checksums elsewhere) while `src/wal/` contains no checksum at all.
**Fix:** per-line CRC32C (or blake3) suffix on manifest lines, verified at scan; a torn or mismatched line degrades via the existing `Damaged` arm. This also converts M4's silent-skip class into a loud re-extraction.

#### M5 — The control-character gate covers exactly one seat; every other connector-authored string crosses the wire-edge ungated
**Defended seat (credit):** `crates/rdlt-connector-client/src/source.rs:93-102` refuses C0/C1 control characters in stream names decoded from `streams()` replies, renders refusals inertly, and is pinned by tests (`:282-317`).
**Ungated seats, all connector→host:**
1. `ErrorFrame.message` — `crates/rdlt-connector-client/src/error.rs:91-97, 123-141` clone the message verbatim into `SourceError`/`DestinationError`/`ClientError::Handshake`; it lands raw in engine events, tracing, and the CLI. A rogue connector can emit `ESC ]52;…BEL` (OSC 52 clipboard write on vulnerable terminal emulators), `\nFORGED …` (log forging), or ANSI resets.
2. `PartClosedEvent.table` — `crates/rdlt-connector-client/src/destination.rs:265-269`: `TableName::new(event.table)` straight into the SPI callback, no gate.
3. `spec_json` / `capabilities_json` — decoded and handed to the engine with only top-level `connector_id`/`connector_version` equality-checked (`handshake.rs:162-175, 177-194`); `spec.name`, `spec.version`, `config_schema` text crosses freely.
4. `checkpoint_cursor_json`, `receipt_json`, `state_doc_json` — passed through into engine state and reports (`source.rs:227-234`, `destination.rs:365-436`).
**Impact:** terminal/log injection into whatever renders these strings. The CLI escapes its *own* lines at display time (`rdlt-cli/src/ui/mod.rs:20-38`) but engine-side tracing and non-CLI embedders are uncovered. The codebase's own rationale for the stream-name gate (`source.rs:84-92`: "a stream name travels into events, tracing spans, and the CLI's lines") applies verbatim to these seats.
**Fix:** hoist the existing check into one `sanitize_connector_text` helper applied at the wire edge for all five seats (error message, part-event table, spec fields, cursor/receipt/state doc names).

#### M6 — Reference destination builds part filenames from unsanitized table names
**Where:** `crates/rdlt-connector-reference/src/destination.rs:318` — `format!("{table}-{}-{}.jsonl", …)` joined into the output dir at `:443-445`. `TableName`/`LoadId` are deliberately unvalidated (`crates/rdlt-core/src/ids.rs:15-18`: "an identifier is whatever the host calls it"), and table names derive from source-declared `StreamSpec.name` — i.e., third-party connector output.
**Mitigation:** engine hosts run names through `normalize_ident` (`rdlt-core/src/naming.rs:37-69`, which collapses everything outside `[a-z0-9_]` and neutralizes `/` and `.`), and the default workdir leaf is sanitized with a pinned traversal test (`rdlt/src/pipeline_spec.rs:578-589`). But the destination itself sanitizes nothing, so a host that skips normalization — or any direct `Backend` driver — gets `../../evil` written outside the configured output directory.
**Fix:** validate or normalize the table component at the destination's own filename construction; the reference connector is the template third parties copy.

#### M7 — Fuzz coverage does not reach the wire decode or the WAL
**Where:** `fuzz/fuzz_targets/` contains exactly three targets — `jsonl_slab` (production arena JSON parser), `shred_push` (full shred path with one real invariant: unique destination column names), `arrow_schema_map` (bounded depth-4 type mapping). Corpus is real and committed; nightly CI runs 1 h/target.
**What is conspicuously absent, given the threat model:**
- prost/tonic frame decode (`ReadFrame`, `SessionReply`, `Write.arrow_ipc`) — `rdlt-connector-client` sits opposite arbitrary third-party connector binaries and its decode path is exercised only by the certify suite's hand-built rogue frames;
- Arrow IPC decoding of wire bytes — a whole nested parser over attacker bytes, fuzzed nowhere;
- the handshake line parser (child-controlled stdout; tested, not fuzzed);
- WAL manifest/scan/replay over arbitrary bytes — `record.rs`'s own comments anticipate "hand corrupt input to the reader" (`record.rs:26`), and this is the classic fuzz target for a crash-consistency component;
- YAML pipeline parsing (serde_yaml, see L11).

**Impact:** the crash-consistency and wire-robustness claims rest on property tests and the rogue suite, not on a fuzzer. Severity is a gap rating (Medium), not a vulnerability.

### Low

#### L1 — `next_commit_seq = max_committed_seq + 1` is an unchecked add
`crates/rdlt-engine/src/wal/resume/scan.rs:229`. A crafted `{"rec":"committed","commit_seq":18446744073709551615}` panics debug builds inside recovery (re-raised deliberately, `blocking.rs:20-27`) and wraps to 0 in release — a `commit_seq` no legitimate writer ever emits (first commit is 1, `loader.rs:526`), so idempotence cannot mask it. One-line fix: `checked_add(1)` → degrade to `Damaged`.

#### L2 — Debug-build arithmetic overflow in the Arrow byte meter over IPC-skewed offsets
`crates/rdlt-connector/src/channel.rs:272-284`: `(offset + len + 1) * width` unchecked on `ArrayData::offset()/len()` from IPC-decoded data, which the module's own docs say "skips the validator". Release builds wrap into `start > end`, fall into the safe over-count branch (`None => (0, usize::MAX, …)`) — release is safe; debug builds panic inside `send()`. Use `saturating_mul/add` for symmetry with the rest of the hardened window read (the `typed_data` panic class was already fixed round-13 with pinned tests at `channel.rs:768-821`).

#### L3 — `with_merged_nulls` panics on struct children whose length ≠ parent
`crates/rdlt-engine/src/load/lowering.rs:184-199` — `NullBuffer::union` panics on unequal lengths; `.expect("null-merge preserves layout")`. Impossible from the engine's own builders, plausibly reachable from an IPC-decoded batch given the acknowledged validator gap. Fix: return a typed error instead.

#### L4 — WAL writes follow pre-planted symlinks
`crates/rdlt-engine/src/wal/writer.rs:120-131` (sidecar), `:323-337` (segments) use create+truncate+write with no `O_NOFOLLOW`/`O_EXCL` semantics. Requires prior write access to an *existing* WAL dir. Heavily mitigated: WAL dirs/files born `0700`/`0600` (`writer.rs:37-56`), fresh-open residue refusal (`writer.rs:104-112`), and 64-bit OS-entropy suffixes on segment names (`run.rs:61-94`) make pre-planting hard.

#### L5 — Certifier accumulates frames without an aggregate byte ceiling
`crates/rdlt-certify/src/wire.rs:476-490` collects every frame into a `Vec` until stream end; `p5_violations` does this per declared stream (`wire.rs:650-681`). Per-frame size is capped (64 MiB, proven to fire, `wire.rs:1363-1385`) but frame count and stream count are not; the only aggregate bound is `CLAUSE_TIMEOUT = 30 s`. A fast rogue can OOM the certifier within its own timeout. Same class: `rdlt-testkit/src/conformance/source.rs:41-133` retains every row ever pushed (`CHANNEL_BYTE_BUDGET = 16 MiB` bounds in-flight bytes only; direct `verify_source` callers have no timeout).

#### L6 — Certifier probe stdout is read unbounded
`crates/rdlt-certify/src/bin/rdlt-certify.rs:477-484` — `read_to_end` with no cap; only `PROBE_TIMEOUT = 20 s` bounds it. Output is never echoed (only its byte count), so impact is memory only, operator-local.

#### L7 — `retry_after_ms` forwarded unclamped
`crates/rdlt-connector-client/src/error.rs:114-116` — a rogue `RATE_LIMITED` frame with `u64::MAX` yields a ~584-million-year `Duration` that engine retry pacing may honor. Clamp at the wire edge.

#### L8 — SDK handshake config refusal can echo parsed config fragments
`crates/rdlt-connector-sdk/src/serve/common.rs:414-417` — `format!("invalid config_json: {error}")`; serde type errors embed the parsed token (`invalid type: string "…"`), which can carry a secret's value back through `ClientError::Handshake { message }` into host logs — violating the crate's own "never log `config_json` verbatim" rule (`protocol lib.rs:53-55`). Replace with error-kind-only rendering.

#### L9 — Python connector (proof connector, operator-trusted input)
`connectors/python/rdlt-connector-pyjsonl/rdlt_connector_pyjsonl.py`:
- `:179` — `isfile` follows symlinks: a symlink `evil.jsonl → /etc/passwd` inside the configured directory is listed and read as a stream. Cheap fix: reject `islink`.
- `:305-313` — no per-line length cap; a single 10 GB line is fully materialized before grpc's 64 MiB send ceiling aborts the RPC.
- `:402` — `add_insecure_port` return (0 on bind failure) unchecked; fails closed only incidentally via the subsequent `chmod` raising. Add the check.
- `:380-390` — the "sufficient and race-free" reclaim comment overstates: between `mkdtemp` (`:400`) and bind (`:402`) the dir is empty and a sibling's rmdir-reclaim can race it (fails closed per the previous item). Same-user, self-inflicted; fix the comment or probe-bind before claiming the name.

#### L10 — Parquet compression level has no range validation
`crates/rdlt-connector/src/parquet.rs:121-122, 203-207` — `ParquetOptions::validate` checks codec/level consistency but not the level's range; an out-of-window `i32` reaches the parquet library's setters, which panic. Precondition: a connector passing the level through unclamped.

#### L11 — serde_yaml 0.9.34+deprecated is a runtime dependency on the CLI's primary parse surface
`Cargo.toml:120`, locked at `0.9.34+deprecated` — the crate's terminal, self-labeled-deprecated release. Runtime dep of `rdlt` and `rdlt-cli` (pipeline YAML), dev-dep elsewhere. No maintained patch stream exists. The exposure is contained (documents capped at 8 MiB *before* the read, `pipeline_spec.rs:185-203` — good), but the migration decision (e.g. `serde_yml`, `saphyr`, or a YAML→JSON front-end) should be tracked.

#### L12 — CI supply-chain hygiene
`.github/workflows/ci.yml`, `deep-checks.yml`: actions pinned by mutable tags (`actions/checkout@v4`, `Swatinem/rust-cache@v2`, doubly-mutable `dtolnay/rust-toolchain@nightly`, `taiki-e/install-action@v2` — which also downloads prebuilt binaries at CI runtime); **no `permissions:` blocks** on any workflow (default token scopes apply). Compromise of an action ref runs arbitrary code with the runner's repo token. Mitigations present and honest: "No inputs by design: untrusted strings never reach `run:` commands" (`deep-checks.yml:12`), all steps invoke Makefile verbs, no secrets, no `curl | bash`. Fix: SHA-pin actions and add minimal `permissions:` blocks. Also: fuzzing is nightly-only — a PR breaking a fuzz invariant is caught at best the next night.

#### L13 — Rogue destination can flood `part_closed` events
`crates/rdlt-connector-client/src/destination.rs:292-308` — only `Error` ends the loop; a rogue may stream `part_closed` forever, spinning the reply loop and the host callback with unbounded CPU but bounded memory. A deadline (H1) bounds this too.

### Info

- **I1 — Identity check ordering:** `config_json` (which may carry revealed `Secret` values) is serialized into the `HandshakeRequest` and sent *before* the reply's `connector_id` is checked (`handshake.rs:132-175`), and to whatever socket path the child's stdout advertised (`local.rs:394-400`). Under D-038-1 (child already has operator trust) this crosses no boundary, but a rogue connector can forward the engine's credentials to any local socket the user can reach. The docs are honest that the check is a sanity check, not authentication; the content-digest pin remains the recorded future door.
- **I2 — No per-batch integrity on the wire:** the frozen proto has no digest field on `Write`/`ReadFrame`; a rogue connector may serve well-formed but silently wrong data. Trust-model-carried; the proto's freeze rules leave additive doors for it.
- **I3 — WAL scan buffers the whole manifest in memory** (`scan.rs:100`) — disk-bounded; the Discard arm clears residue across runs.
- **I4 — Deliberate panic when recovery is cancelled at runtime shutdown** (`wal/resume/blocking.rs:26`) — documented, embedder-controlled.
- **I5 — A single item larger than the whole byte budget passes by design** (drain-the-budget-and-go, `channel.rs:121-135`, documented): peak memory is budget + one item. Arena *reservation* is hard-capped (`MAX_PRESIZE = 64 Ki` nodes, `arena.rs:71-80`).
- **I6 — Process hygiene in the certifier is sound:** every kill goes through owned `Child` handles (`start_kill`), never a PID read from untrusted sources, never a name-matched `pkill` (`wire.rs:365-381`); the CLI's `--probe-cmd` `{{table}}` substitution is whitelisted to `[A-Za-z0-9_]+` rather than spliced (`bin/rdlt-certify.rs:271-278`); the process-group SIGKILL anchors the pgid by keeping the direct child unreaped (`bin:307-319, 427-438`), preventing kills of a recycled (innocent) pgid; `--` terminates kill arguments. `group_kill` shells out to `kill` resolved via PATH — operator-controlled environment, no boundary.
- **I7 — Publish-twice is not refereed server-side** (`rdlt-connector-sdk/src/serve/destination.rs:43-60`): exactly-once rests on the shipped `Backend`'s durable receipt guard; the module doc itself records the missing Backend-direct conformance clause. Host→connector direction (trusted).
- **I8 — Misc:** no `SECURITY.md` anywhere in the repo; `POSTGRES_PASSWORD: rdlt` on an exposed runner port (ephemeral runner, throwaway credential); `LoadId` collision hazard is documented with real consequences ("`load-1` from two orchestrators would read one pipeline's commit as another's replay, silently", `docs/connector-authoring.md:164-179`) — worth an engine-side uniqueness guard eventually; fuzz `Cargo.toml` declares `rdlt-core`/`rdlt-connector` deps no target uses.

---

## 4. Verified defenses (credit where due)

These were checked in source, not assumed:

- **Frame ceiling:** one shared `MAX_FRAME_BYTES = 64 MiB` (`rdlt-connector-protocol/src/lib.rs:114`) installed via `max_decoding_message_size` on **every** served service (`serve/source.rs:631-639`, `serve/destination.rs:825-833`) and **every** client constructor (`client/src/dial.rs:65-79`), replacing tonic's 4 MiB default in both directions.
- **Memory budgets, honestly metered:** byte-budgeted channels with permits held until the receiver *drops* the value (`rdlt-connector/src/channel.rs:44-146`, pinned tests); `DEFAULT_BYTE_BUDGET = 64 MiB` engine-side; `READ_CHANNEL_BUDGET = 8 MiB` / `BYTE_FRAME_BUDGET = 32 MiB` / `FRAME_MESSAGE_CAPACITY = 64` serve-side; `REPLY_CHANNEL_BUDGET = 16` + one-session ceiling via CAS (`serve/destination.rs:169, 784-793`); h2 windows derived from the engine budget so a rogue blast is window-bounded (pinned: `a_tiny_window_bounds_an_unread_blast`). The Arrow footprint meter dedups shared slices, counts viewed windows only, over-counts rather than panics, and its panic classes have regression pins (`channel.rs:768-821`).
- **No compression-bomb surface:** no `ipc_compression`/lz4/zstd anywhere in any manifest — Arrow IPC bodies are uncompressed slices of a ≤64 MiB frame.
- **Spawn hygiene:** `Command::new(bin).arg("--role=…")` — no shell, fixed argv (`local.rs:189-207`); handshake line read capped at 64 KiB via `.take()` *and* 10 s timeout *and* an EOF-exit grace (`local.rs:28-49, 209-254`); `kill_on_drop` + `LifecycleGuard` so every failure path kills the child; the guard's unlink is `lstat`-gated to sockets only (`managed.rs:63-84`); protocol range checked before dial (`local.rs:267-274`); connector id verified by strict equality, wrong id refused (`local.rs:353-358`).
- **Socket hygiene:** private `0700` per-process dir with atomic mkdir and 16-retry (`serve/common.rs:150-199`); `bind` + `chmod 0600`; stale-socket reclaim probes liveness before unlinking and never unlinks a live listener (`serve/common.rs:229-270`, TOCTOU window documented and mitigated).
- **Secrets:** `Secret` masks `Debug`/`Display`, `reveal()` is the sole accessor (`rdlt-connector/src/secret.rs:35-59`); `Spec`/`ConnectorRef`/`ConfigSource`/certify `Target` all carry hand-written `Debug` impls that elide config wholesale (`pipeline_spec.rs:170-177, 341-350`, `target.rs:37-50`, pinned by test); the certifier never echoes config bytes, probe command lines, or probe stderr; the Python connector refuses config with field-name-only messages ("a config may carry credentials").
- **WAL crash-consistency machinery** (independent of M3/M4): segments are Arrow IPC *file* format — footer validated at open, truncation refused (`replay.rs:13-28`); manifest↔segment row-count cross-check (`replay.rs:69-89`); two-pass replay (decode everything before any write, memory bounded to one batch); torn-tail tolerance only for a *final* unparsable line (`scan.rs:114-124`); exact version gates both directions (`record.rs:21-30`); foreign-pipeline occupancy gate; rules-sidecar gate; per-stream coverage rule (T7E) mirrored loader-side against mid-run commits; commit ordering segment→manifest→fsync→destination commit→`Committed`+GC with replay re-committing under the crashed run's `(load_id, commit_seq)`; workdir exclusive lock via fs4; WAL dirs `0700`, files `0600`.
- **Checked math everywhere it matters:** decimal precision/scale via `checked_mul/add/pow` with `DECIMAL_MAX_PRECISION = 38` gated at plan time (`build.rs:483-536`, `validate.rs:199-219`); arena `checked_idx` with contained panics (`arena.rs:29-31`, `extract.rs:72-102`); saturating backoff (`run.rs:100-102`); no truncating length `as`-casts found; no reachable division by zero.
- **Adversarial test surface:** 29 certification clauses each with a designated rogue proving it can fail; a nine-arm SIGKILL matrix pinning typed-error-within-10 s *and* exactly-once convergence by exact row counts, with vacuity arms proving the count judgment is live; silence-can-never-pass report folding (`NOT-REACHED` refusals); conformance suites with negative fixtures (amnesiac sources, receipt forgers, hang-on-close destinations); crash-injection destinations with a self-checked fault-point registry; the reference connector's durability barriers (parts→state→receipt ordering, fsync-before-rename, dir-sync-after) pinned structurally.
- **Config hygiene:** `deny_unknown_fields` on spec/options types; 8 MiB document cap enforced *before* the read via metadata (`pipeline_spec.rs:185-203`, `rdlt-cli/src/run.rs:85-103`); empty `CommitPolicy` refused ("never commit" is a data-loss window, `commit.rs:167-177`); workdir leaf path-sanitized with pinned traversal tests; per-pipeline default workdir so two pipelines can never scan each other's WAL.
- **Supply chain:** zero git dependencies (verified by grep *and* by the `tools/check-git-deps.sh` gate with an empty allowlist); no `[patch]`/`[replace]`; `protoc-bin-vendored` (no build-time network fetch); toolchain pinned 1.96.0; vendored Python stubs drift-gated by regeneration+diff (`tools/check-python-stubs.sh`); the mutation leg runs under `systemd-run` with `MemoryMax=12G` containment.
- **Python connector:** JSON-only parsing (no pickle/yaml/eval/exec/subprocess); stream names proven traversal-proof by construction (membership in an `os.listdir` basename set); cursor offsets type-checked (bool excluded), non-negative, and bounds-checked against `fstat` before `seek`; socket in a `mkdtemp` 0700 dir with `chmod 0600`; one-line-then-`/dev/null` stdout discipline (`dup2`) so no stray write can corrupt the machine channel.

---

## 5. Recommended fix order

1. **H1** — client deadlines (dial/handshake/per-RPC) + a silent-rogue certify arm. Small diff, closes the only unbounded-hang class.
2. **M2** — lstat-gated unlink in `rdlt-certify` (copy the `managed.rs:70-83` guard to `wire.rs` ×3 and `target.rs` ×1). Trivial.
3. **M3 + L1** — WAL segment filename shape check + `checked_add` in scan. A dozen lines.
4. **M1** — recursion depth cap over Arrow schema walks (`lowering`, `passthrough`, `data_footprint`).
5. **M4** — per-line checksum on WAL manifest records (blake3 is already in-tree).
6. **M5** — one shared connector-text sanitizer at the wire edge, applied to the five ungated seats.
7. **L7/L8/L12** — clamp `retry_after_ms`; stop echoing serde fragments in config refusals; SHA-pin CI actions + `permissions:` blocks.
8. **M7** — add fuzz targets for wire frame decode, Arrow IPC decode, and WAL scan/replay; promote at least a short fuzz leg to PR CI.
9. **M6, L2–L6, L9–L11, I8** — as scheduled hardening.

---

## 6. Caveats

- No network advisory scan was run (`cargo audit` / OSV) — dependency findings here are from manifests and `Cargo.lock` versions only (tonic 0.14.6, prost 0.14.4, hyper 1.10.1, clap 4.6.2, tokio 1.53.0, arrow 58.3.0, serde_yaml 0.9.34+deprecated). Running `cargo audit` in CI is recommended as a follow-up.
- Severity assumes the documented trust model (D-038-1): connectors are operator-installed and run with the operator's privileges. Several Medium findings become High if connectors are ever sourced from a less-trusted channel (the content-digest pin recorded in the docs is the mitigation door for that future).
- The engine/certify/sdk reviews were performed at the stated file paths and line numbers as of commit `92fbd263` on `main`; line numbers will drift.
