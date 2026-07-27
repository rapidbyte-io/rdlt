# Close-Out: 020 — Audit Remediation

**Feature**: 020-audit-remediation | **Branch**: `020-audit-remediation`
**Started**: 2026-07-26 | **Status**: IN PROGRESS — Phases 1–4 complete (41 of 189 tasks); US1 and US2 merge-ready

Executes `NEXT_STEPS.md` (audited 2026-07-26 @ `634222e`). Contract:
[contracts/audit-remediation.md](contracts/audit-remediation.md) AR1–AR8.

---

## Contract matrix (AR1–AR8)

| clause | status | evidence |
|---|---|---|
| AR1 — red before green | ON TRACK | procedure fixed in T007; US2's 5 pins each demonstrated red, output recorded in D-01 |
| AR2 — confirmed set is the work; refuted set is not | ON TRACK | 18 non-goals seeded below; **5 corrections to the audit recorded** (C-01…C-05) |
| AR3 — behaviour changes only where a defect is named | OPEN | |
| AR4 — persisted formats and identity | OPEN | |
| AR5 — typed taxonomy, or no distinction at all | OPEN | |
| AR6 — greenfield, and the deferral ledger closes | OPEN | |
| AR7 — the gate is the gate, and CI is not it | **PARTIAL** | T006 below: the full local gate CANNOT complete on this machine; the runnable portion is recorded |
| AR8 — one disposition per item, none silent | OPEN | ledger seeded with 157 items + 18 non-goals |

## Story matrix

| story | status | evidence |
|---|---|---|
| US1 — the record and the license | **COMPLETE** | T008–T026, 19 tasks. LICENSE added (byte-identical to canonical Apache-2.0, sha256 `a60eea81…`); CLAUDE.md 019 block rewritten COMPLETE with the recorded standing and the honest misses, 018 block marked superseded; 019's three internal contradictions resolved; FR-016 amended in place with D-03's inversion; PERF_ANALYSIS.md banner added naming four falsified claims; concurrency scaling documented in README (the deliverable US9's re-scope named); 8 further doc-truth corrections. **Zero behaviour change** — lint green, 677/677 tests |
| US2 — value fidelity | **COMPLETE** | T027–T041. 5 red pins in `tests/value_fidelity.rs`, all demonstrated FAILING on the pre-fix build (AR1); 4 defects fixed. Identity corpus **byte-identical** (`abc0bf0b…` before and after). Gate: 682/682 tests, lint clean, sweep 23/23 |
| US3 — file family (3 increments) | **COMPLETE** (US3a, US3b, US3c) | US3c: T060–T067, see D-08. | US3b: T048–T059, see D-06. US3a: T042–T047. Key-split fix + widened ownership. **Adversarially reviewed and the first attempt REJECTED — see D-05.** Gate: 692/692, lint, sweep 6/6, doc-tests |
| US4 — REST robustness | **COMPLETE** | T068–T079, see D-09. Reworked after review: 3 wrong instruments corrected, 3 dishonest pins replaced. Gate: 714/714, lint, sweep 6/6 |
| US5 — schema contracts | **COMPLETE, SCOPE REVERSED ON EVIDENCE** | T086–T096 superseded; see D-10. Within-run enforcement + inheritance; NO persisted-format change, NO semver break. Gate: 718/718, lint, sweep 6/6 |
| US6 — iceberg nested types | **COMPLETE** | T098–T106; T097 unperformed (no working container runtime). See D-11. Gate: **722/722 with containers ENABLED** — the merge-base red is fixed, not worked around |
| US7 — sharp edges | **COMPLETE** | T107–T120, 16 fixes. Gate: 723/723 twice clean (containers enabled), lint, sweep 6/6, doc-tests |
| US8 — the gate | **COMPLETE** | T121–T139; see D-25, D-27 through D-30. Fresh full run: **921 mutants, 97 survivors**, every one re-checked WITH containers (24 were never gaps). All 75 verified gaps dispositioned: 51 killed by red-verified pins, 18 equivalent with the argument at the call site, 3 dead-code deletions, 2 cosmetic, 1 untestable by construction, **1 real defect fixed** (`UniqueNamer::name_for` could spin forever). Suite 749 → 780 |
| US9 — publish readiness | **COMPLETE** | T140–T152. 220 undocumented public items documented; `make docs` added as a gate verb and wired into `check`, catching **14 dead intra-doc links** on its first run (D-19). Publish metadata, crates.io descriptions, feature-matrix and packaging checks recorded. CI-only verifications land **UNPERFORMED**, never green — E1 stands |
| US10 — recorded deferrals | **COMPLETE** | T153–T167; see D-20 through D-23, D-31, D-32. D17 taken (one byte-budget channel, engine copy deleted, AR6 verified); lowering parity now a machine-checked property over generated schemas × 4 capability combos; `DestSpec::File` embeds its config; `create_index_sql` and the duplicate-key diagnosis moved into sqlcore **with the golden pin they lacked**; `WalRecord::Segment.rows` given a consumer (no format bump); 4 dependencies removed. **Fired-but-undisposed deferrals: zero** (SC-014). Gate: 785/785, 0 skipped |
| US11 — performance queue | NOT STARTED | |

---

## Phase 1 — Setup

### T003 — disk headroom and container residue

Checked before any build, because 017 recorded the gate turning red twice from
exactly this (168 GB of podman residue; `target/` at 851 GB).

| measure | value |
|---|---|
| filesystem | 1.9 T total, **218 G available (89% used)** at start |
| `target/` | **578 G** — `debug` 519 G, `llvm-cov-target` 51 G, `release` 5.2 G, `dist` 1.6 G |
| podman residue visible from this container | 0 containers, 0 volumes, 0 images |

**Action taken**: removed `target/llvm-cov-target` (51 G, regenerated on demand
by `make coverage`) — availability **218 G → 264 G**.

**Action deliberately NOT taken**: `target/debug` at 519 G was left in place. A
full `cargo clean` would reclaim it but costs a complete rebuild on every
subsequent task, and 264 G is ample headroom for the increments in flight.
**Recorded as a hard precondition for T122** (the fresh mutation run), which
builds many variants and is the disk-hungry task in this feature — check
headroom again immediately before it, and clean then if needed.

### T004 — pre-change instrument state

Not yet captured: `benches/perf-baselines.json` and the bar statuses are read
by tooling that needs `valgrind` (absent — see T006). Deferred to the US11
phase, where the instruments are actually used. **Recorded rather than
skipped.**

### T005 — which test legs can actually run here

The session runs inside a **toolbox container** (`fedora-toolbox:44`, name
`dev`), not on the host. Toolchain is correct and complete for building:
`cargo`/`rustc` 1.96.0, matching `rust-toolchain.toml`.

| tool | present | consequence |
|---|---|---|
| `cargo-nextest` | yes | the test gate runs |
| `cargo-llvm-cov` | yes | coverage runs (T186) |
| `cargo-mutants` | yes | the mutation run is possible (T122) |
| `python3` | yes | — |
| **`hyperfine`** | **NO** | `make bench TARGET=cold` exits 1 → **`make check` cannot complete** |
| **`valgrind`** | **NO** | `make bench TARGET=iai` cannot run → instrument track unavailable |
| `podman` binary | **BROKEN** | a distrobox wrapper whose `distrobox-host-exec` is absent |

---

## Phase 2 — Foundational

### T006 — the gate on the merge base: RED, and why

Run on the merge base before any change. **This is the finding T006 exists to
produce, and it is recorded rather than worked around.**

| run | result |
|---|---|
| `cargo nextest run --workspace` | **317 passed, 1 FAILED**, 359 not run (fail-fast) |
| `RDLT_TESTKIT_FORCE_NO_CONTAINERS=1 cargo nextest run --workspace --no-fail-fast` | **677/677 passed, 0 skipped** |
| `make check` (the full gate) | **CANNOT COMPLETE** — `hyperfine` and `valgrind` absent |

The single failure is `rdlt-connector-iceberg::auth_probe
live_auth_rejection_classifies_fatal`:

```
starting docker.io/rustfs/rustfs:1.0.0-beta.11:
  /var/home/netf/.local/bin//podman: line 5: exec: distrobox-host-exec: not found
```

**The code is green.** With the container runtime forced off, every one of the
677 tests passes. The failure is environmental — but it exposes a real defect,
recorded as a new finding below.

**Consequence for this feature**: the gate of record on this machine is
`RDLT_TESTKIT_FORCE_NO_CONTAINERS=1 cargo nextest run --workspace
--no-fail-fast` plus `make lint`. Container-backed legs and the instrument
track are **unperformed** here and every increment that depends on them says so
explicitly (AR7). No increment may claim a full-gate green from this
environment.

### NEW FINDING (not in the audit) — the iceberg fixture violates Principle VII

The constitution requires container-backed tests to **skip-not-fail** when the
runtime is absent. The iceberg fixture does not.

- `crates/rdlt-testkit/src/containers.rs:38-58` — `runtime_available()` probes
  for a **socket** (`DOCKER_HOST`, `$XDG_RUNTIME_DIR/podman/podman.sock`,
  `/var/run/docker.sock`).
- `crates/rdlt-connector-iceberg/tests/common/mod.rs:51-62` — the fixture
  shells out to the **`podman` binary** with `.expect("podman run")` and
  `assert!(output.status.success(), …)`.

The two can disagree, and here they do: the socket exists, so the probe reports
the runtime available, while the binary is an unusable wrapper. Every
testcontainers-based fixture (`s3_live`, postgres CDC, duckdb) skips correctly;
this one **panics**, turning the whole gate red for an environmental reason the
constitution says must be a skip.

**Disposition**: routed to **US6** (which already owns this file for the Polaris
pin, T097) with a red-before-green pin — force the runtime unavailable and
assert the suite skips rather than panics. Recorded here because it is a
finding the audit missed, per AR2.

### T007 — the AR1 evidence procedure

Every defect fix in this feature captures its pin as follows, and cites this
entry:

1. Write the pin. Run it on the merge base (`git stash` or a merge-base
   checkout) and **record what the red run printed** — not merely that it
   failed.
2. Apply the fix. Run the same pin; it must pass.
3. Record both in the story's row below.

Two exceptions, both from the contract:

- **A skipping test is green.** A container-gated test can never be
  *demonstrated* red, so it is inadmissible as the AR1 pin. Capture the red pin
  container-free and treat the live cell as confirmation (US6 T098–T099 is the
  worked example).
- **Embedder-only defects** get synthetic pins built on `MemorySource` +
  `StreamSpec`, and are **recorded as synthetic** (US2 T030, US7 T114).

---

## Deviations and corrections to the audit (AR2/AR3)

### C-01 (US1) — the audit's "silently-ignored file-source knobs" is partly wrong

`NEXT_STEPS.md` lists `primary_key`, `validate` and `type_hints` together as
silently-ignored file-source knobs. Verified against the code, only part holds:

- **`primary_key` is honoured** (`source/mod.rs:121-122`) and declaring it on a
  parquet stream is **refused at configuration time**
  (`source/config.rs:135-138`) — the opposite of silently ignored.
- **`type_hints`** reach csv directly and jsonl through the shredder; they have
  no effect on a parquet stream and that case *is* accepted and ignored.
- **`validate` is JSONL-only** and is accepted and ignored on csv and parquet.

Documented precisely rather than as a blanket claim. The typed rejection for
the two genuinely-ignored cases is a behaviour change and is **routed out of
this doc increment**.

### C-02 (US1) — `json_type` does not do what its contract says

`DestinationCapabilities::json_type` was documented as "if false `Json` columns
lower to text". The field is **read nowhere in the workspace** — `needs_lowering`
consults only `structs` and `decimal`, so the engine never rewrites a `Json`
column. Two shipped destinations declare it `false` (file, Iceberg) and both map
`Json` in their **own** schema translation (Iceberg → string, `dest/schema.rs:50`).
The comment now states that, so a new destination author is not misled into
expecting the engine to lower for them.

### C-03 (US1) — a test module doc claimed an unfixed defect that was fixed

`direct_publish_guarantees.rs` claimed its second test was a "CONFIRMED, UNFIXED
defect" and `#[ignore]`d. Commit `09d7bfe` fixed it and removed the attribute;
the doc was never updated. Rewritten to state the invariants the file pins
(Principle VI: a live constraint, not a history).

### C-04 (Setup) — `mutants.out` is already gitignored and untracked

`NEXT_STEPS.md` §6 asks whether `mutants.out` and `mutants.out.old` "committed
at repo root" should be gitignored. They are already both — `.gitignore:11-12`
— and `git ls-files mutants.out` returns nothing. **No action; the item is
closed as not-a-defect.** Consequence for T123: the refreshed mutation record
cannot be "committed" as that task assumes, and the triage must live in this
document instead.

### C-05 (Setup) — the spec.md "Status: Draft" staleness is project-wide

017, 018 and 019 all carried `Status: Draft` while complete, so 019 was
following a convention rather than contradicting itself. Fixed in all three
rather than making 019 an outlier.

---

## Deviations (AR3)

### D-01 (US2) — the red evidence, recorded

All five pins failed on the pre-fix build before any fix was written. The
headline failure is the defect itself:

```
unsigned_integers_beyond_i64_survive_as_text
  assertion `left == right` failed: a u64 above i64::MAX must arrive intact, not as NULL
    left: Null
   right: String("18446744073709551615")
```

The other four: `unsigned_widening_is_order_independent`,
`a_type_hint_is_not_dropped_by_an_object_value`,
`a_decimal_beyond_its_declared_precision_is_refused`,
`an_unrepresentable_type_hint_is_a_typed_config_error` — 5 run, 5 failed. After
the fixes: 5 passed.

### D-02 (US2) — two schema-affecting changes (FR-027)

Both change the type a column is inferred to have, so they are recorded here
rather than discovered at review:

| column shape | before | after |
|---|---|---|
| any column observing a `u64` (at any magnitude) | `Int64`, with every value above `i64::MAX` written as **NULL** | `Utf8`, digits intact |
| a hinted column receiving an object or array | silently re-inferred to `Json`, subtree stored **verbatim** | keeps its declared type; the value is NULL and **counted** |

The second is the direction the audit got backwards — see C-06.

**No range condition** was used for the first. Deciding per value would make the
resolved type depend on arrival order; `unsigned_widening_is_order_independent`
pins that it does not.

### C-06 (US2) — the audit described the type-hint defect backwards

`NEXT_STEPS.md` says a hinted column receiving an array "silently created and
loaded a whole new child table". It never did: `TapeShredder::new` seeds a
hinted column as `ColState::Scalar(pinned)`, and the object/array arm sets
`ColState::Json` **before** `is_child_table()` is ever read.

The true prior behaviour is worse and points the other way — the subtree was
preserved **verbatim as a Json column**, and after the fix it is **NULLed**.
That is a data-visible narrowing, pinned by asserting the stored value rather
than only the resolved type. `type_hints: {c: json}` is the escape hatch for
anyone who wants the old behaviour.

### D-03 (US2) — representability misfits are NOT separable in the report

`LoadItem::Discarded` carries no reason, and `TableReport`/`CommitCounters`
merge both producers into one `discarded_values`. A downstream consumer cannot
tell a policy discard from an unrepresentable value.

A typed `DiscardReason` is the better end state and was **deliberately not
taken**: expressing the distinction as a second free-form string would leave
substring-matching as the only way to separate them, which AR5 forbids outright.
Recorded as a named deferral with the trigger **"the next feature that opens the
version window for another reason"** — 020 US5 opens it, so this may be taken
there if it is cheap.

### D-04 (US2) — a recorded residual, deliberately not fixed here

An explicit JSON `null` in a scalar-list column still produces a **valid empty
list**, not a NULL, and is therefore not counted. Unreachable through the shred
path (`ColState::ScalarList` degrades to `Json` on any non-array), and changing
it would turn today's `explicit-null → []` into `NULL` — a data-visible change
needing its own red-before-green pin and its own close-out line. Not smuggled
into this increment.

*This residual is also why the misfit counter is positional rather than a
difference of totals: with outputs able to outnumber non-null inputs, the
subtraction underflows and panics in debug.*

### C-07 (US3a) — `path_safe` does not prevent hidden names, though its doc says it does

`crates/rdlt-connector-file/src/dest/layout.rs:68-84` documents itself as
rendering a partition value "path-safe (never a separator or hidden name)". It
maps every character outside `[A-Za-z0-9._-]` to `_` — which stops separators,
but **`.` is explicitly preserved**, so a partition value of `.hidden` renders
unchanged and produces a hidden partition directory.

Found while reasoning about whether the widened ownership predicate could claim
a hidden directory. It cannot in practice — staging is a sibling of the table
root, not under it — but the comment is false and the naming guarantee is
weaker than stated.

**Not fixed here.** Making the renderer refuse leading dots changes the paths
written for such values, which is a behaviour change needing its own
red-before-green pin and its own line (AR3). Routed to the increment that owns
the file-destination layout, with the ownership interaction noted.

### D-05 (US3a) — the first ownership fix was defective; the review caught it before merge

The widened Replace-truncation predicate was reviewed adversarially before being
called done, because its failure mode is unrecoverable file deletion. **All three
review lenses returned NOT SAFE TO MERGE**, and two findings were confirmed by
executing the code. Recorded in full because the near-miss is the lesson.

| # | severity | defect in the first attempt | disposition |
|---|---|---|---|
| 1 | **critical** | `is_part` matched a bare `part-` prefix plus any of our extensions. `part-0.parquet` and `part-00000-<uuid>-c000.snappy.parquet` are the DEFAULT output basenames of pyarrow, Spark, Hive and Delta, so a Replace **deleted a user's own dataset** sitting under the table directory. Verified by execution. | FIXED — `is_written_part` now requires the exact shape `publish` writes: `part-<load>-<seq>-<index>.<ext>` with numeric seq and index |
| 2 | high | `walk_local_files` followed directory **symlinks** (`is_dir()` resolves them) and `delete_table_file` applied no containment check, so deletion could escape the destination root entirely. Verified by execution. | FIXED — `entry.file_type()` (does not follow) and symlinks skipped outright |
| 3 | high | An S3 zero-byte **directory marker** at the table root is listed without its trailing separator, so the new strict strip returned a typed FATAL — **permanently bricking Replace and row-counting** for that table. Previously the `rfind` search silently skipped it. A regression this increment introduced. | FIXED — the marker is recognised and skipped; genuinely foreign keys still fail typed |
| 4 | high | **The headline fix never reached the default configuration.** `frozen` still short-circuited to the pre-015 rule for local+parquet+unpartitioned, and `frozen_owns` sees neither nested tails nor `.jsonl` — so the exact scenario US3a exists to fix still failed. Verified: 3 rows where 1 is correct. | FIXED — the two rules are now a **union**, not exclusive |
| 5 | medium | `DestFormat::ALL`'s completeness "pin" was a compile-time tautology: `assert_eq!(ALL.len(), 2)` reads the array type. | FIXED — `ALL` is a slice and completeness is enforced by an exhaustive match (`in_all`) that cannot compile when a variant is added |
| 6 | low | `staging_is_not_under_a_table_root` asserted only that a constant was non-empty — a test that could never go red. | DELETED and replaced with pins that exercise the code |
| 7 | low | Module and function docs described parameters the change had deleted. | FIXED |

**Why the first attempt's tests passed.** They exercised `owns_part` directly and
never went through `truncate_table`, so they could not see the frozen selector.
The replacement pins go through the real commit path
(`replace_clears_earlier_loads_written_in_another_shape`,
`replace_never_deletes_a_foreign_dataset`) and were **demonstrated red against
the rejected shape**:

```
`spark-export/part-0.parquet` was not written by this destination and must survive
`eu/part-old-1-0.jsonl` was written by this destination and must not survive a Replace
```

**Carried forward, not fixed here** (each needs its own pin and close-out line):
the S3 tail is percent-encoded and re-encoded by `delete_table_file`; a partition
value of `..` survives `path_safe` and escapes the table directory on publish;
non-UTF-8 directory names are now a typed error rather than silently mis-depthed.

### D-06 (US3b) — parquet resume integrity, and the design Phase 0 rejected

**The defect.** A parquet input that GREW while its consumed prefix was
rewritten passed every tripwire: the shrink check sees growth, the same-size
identity check returns early because the sizes differ, and parquet recorded no
content hash at all. The read resumed at a row-group index that now named
different data and delivered wrong rows silently.

**The fix.** `FileProgress` gains `row_groups_hash` — a blake3 over a footer
DESCRIPTION of row groups `0..done` (per group: index, row count, byte size,
column count; per column chunk: dictionary page offset, data page offset,
compressed size). A genuine append rewrites the footer but leaves earlier groups
where they are, so it still matches; a rewrite of any consumed group does not.
`byte_range()` was deliberately not used — it asserts on a negative offset and a
footer is untrusted input.

**No `CURSOR_FORMAT_VERSION` bump** (research R3.4). The field is additive with
`skip_serializing_if`, exactly as `etag` and `tail_hash` were added, so jsonl and
csv cursor documents stay byte-identical — which a bump would NOT have preserved,
since `format_version` is serialized unconditionally. Migration note: a parquet
entry carries no integrity value until the next checkpoint rewrites it.

**The design Phase 0 rejected, and the pin that proves it.** The first design
built the prefix descriptor only when a check was present. On a first upgrade
there is no check, so the value recorded would have described `start..done`
while claiming to describe `0..done` — and every later append would then be
refused, permanently, for every pre-existing cursor. The descriptor is now built
**unconditionally**. Restoring the rejected shape makes
`a_cursor_without_an_integrity_value_recovers_rather_than_poisoning_itself` fail
with exactly the predicted symptom:

```
the run after an unverified resume must still accept an append:
Fatal("... was rewritten before the resume offset (the 2 row groups preceding it
changed since the last run); refusing to read from a stale offset ...")
```

**AR1 evidence.** Against the pre-fix build (no integrity value recorded),
`a_rewritten_prefix_is_refused_even_though_the_file_grew` and
`a_re_encoded_prefix_is_refused_even_when_the_rows_are_unchanged` both FAIL —
the silent-wrong-data defect. The other two pins guard the fix's own failure
modes rather than the original defect, which is why they pass there.

**Recorded narrowing (AR3/FR-002).** The description covers physical layout, so a
file regrown by a DIFFERENT writer or with different writer properties — even one
preserving the logical prefix — is now refused. That is defensible (a whole-file
re-encode is not an append) but it is a behaviour change, so
`a_re_encoded_prefix_is_refused_even_when_the_rows_are_unchanged` pins it
deliberately rather than leaving it to be discovered in the field.

**Also hardened here**: a recorded position past the end of the file is a typed
refusal; the check is armed only when `done_units > 0`, mirroring jsonl's filter,
so a hand-edited cursor cannot reach an underflow; a `FileProgress` carrying BOTH
hashes arms NEITHER check (it means two different readers wrote the entry, so
neither value describes this run); and a record-stream reader handed a row-group
check now REFUSES rather than silently skipping verification.

### D-07 (US3b) — the descriptor was rewritten after review: layout is not content

The first implementation described the consumed prefix by its FOOTER — per group
the index, row count, byte size and column count, and per column chunk the page
offsets and compressed size. **The adversarial review proved that closes
nothing**, and three of its four lenses reached the finding independently.

**None of those quantities depends on the values.** A consumed row group
rewritten with entirely different data, at the same cardinality, produces a
bit-identical description:

```
descriptor([[1,2,3]],          prefix=1) = 455bc78f1ae2dc53…
descriptor([[9,8,7],[4,5,6]],  prefix=1) = 455bc78f1ae2dc53…
```

Verified end to end (`E2E: ACCEPTED rows=3 values=[4, 5, 6]`) — the destination
ends up holding `1,2,3,4,5,6` while the file holds `9,8,7,4,5,6`. The blast
radius is every fixed-width physical type under PLAIN encoding and every
dictionary-encoded column whose cardinality is preserved: ids, timestamps,
measures, enums.

**And the pin passed for the wrong reason.** `[9,9,9]` has one distinct value
where `[1,2,3]` has three, which resizes the dictionary page. Changing the
literal to `[9,8,7]` — same writer, same everything else — flipped the test red.
A pin that passes by accident is worse than no pin, because it is cited as
evidence.

A second, structural failure followed from the same root: with uniformly-shaped
groups every page offset is a function of POSITION, so a regeneration that merely
PREPENDS a group leaves the prefix description identical — accepted, causing
silent duplication of already-delivered rows AND silent loss of the new ones.
Also verified end to end.

**The fix: hash content, not layout.** `prefix_digest` now folds in the file's
SCHEMA (column path, physical type, logical type) and a `TAIL_WINDOW` window of
the prefix's own BYTES, ending exactly where the consumed prefix ends
(`end_of_prefix`, computed with checked arithmetic and refused on a
self-inconsistent footer). This is the same discipline the record formats already
use for their tail window, costs one bounded read, and is free on the
object-store path where the object is already local. The schema is included
because a renamed column, or a width-preserving type change, alters what the
prefix MEANS without moving a byte.

**Evidence.** Three pins now discriminate the real property and all three go RED
against the rejected layout-only descriptor:
`a_rewritten_prefix_is_refused_even_though_the_file_grew` (now same-cardinality),
`a_prefix_shifted_by_a_prepended_group_is_refused` (new), and
`a_prefix_whose_schema_changed_is_refused` (new). The three guard pins — append,
re-encode, first-upgrade recovery — still pass there, as they must.

**What the review found CLEAN**, verified by execution rather than assumed: the
append-resumes property holds across defaults, snappy, zstd, bloom filters,
statistics-disabled, dictionary-disabled and small-page configurations; the
persisted-format claims hold in both directions with no version bump; and the
jsonl refactor introduced no regression.

**Recorded residual.** A COMPLETE file (`done == size`) is skipped without
verification, so a finished file replaced wholesale before more groups arrive is
not detected. This matches jsonl exactly and is the boundary research R3.7
already named; closing it would mean verifying every complete file on every run.

### D-08 (US3c) — the receipt-log bound was REVERTED after review; two smaller fixes corrected

**The change that was reverted.** `CommitLog::retain_recent` trimmed the receipt
log to the current load plus the one before it, on the reasoning that both
readers only ask about the current load. The review **reproduced a
data-destroying failure** and confirmed it was a regression from this diff by
disabling the call.

The SPI's contract has no recency clause
(`crates/rdlt-connector/src/lib.rs:134-137`): *"Re-committing the same
`(load_id, commit_seq)` returns the prior receipt without re-publishing."*
Trimming makes that conditional on how recently the load ran. Once two newer
loads commit, a redelivered load is no longer recognised, so the Replace guard
concludes it never committed and truncates — **destroying what later loads
published** — while Append re-publishes its parts. Measured: rows `1 → 3`
(a later load's data destroyed) and `5 → 6` (duplication).

Redelivery of an older load is reachable: WAL replay from a restored workdir
(`replay_span` opens the session with the span's own load id), two engines
sharing an output prefix (`WorkdirLock` is per workdir, not per output), and any
embedder driving `LoadSession` directly — which is what the SPI exists for.

**Disposition: reverted, and the growth documented instead** — the same branch
of FR-038 already taken for the file cursor, and the one that should have been
taken here. Bounding this safely needs a persisted watermark plus a TYPED
refusal for a commit falling below it: a design, not a trim. Recorded as a
named deferral with that shape.

**My four unit tests could not have caught it.** They asserted `retain_recent`
against its own definition, and the one named
`retention_preserves_what_the_readers_ask` checked only the current and previous
loads — precisely the case that was safe. The replacement pin is at the SESSION
level, where the property lives:
`a_redelivered_commit_is_recognised_after_later_loads_have_run` in
`tests/recovery.rs`, verified red against the reverted change
(`left: 3, right: 1`, load-c's data gone).

**Corrected, not reverted: the retry classification.** Inverting `is_recoverable`
to an allow-list was right in shape but too narrow. HTTP 409 becomes
`AlreadyExists` and 412 becomes `Precondition` — determined answers only to a
CONDITIONAL request, and this connector issues none. What a plain put, copy or
delete gets a 409 for is S3's `OperationAborted` ("try again"), and object_store's
own retry loop will not cover it: it retries 409 only when `retry_on_conflict` is
set, which upstream sets solely for its conditional put
(`object_store-0.12.5/src/client/retry.rs:167,405-407`). Making that fatal would
abort runs that used to recover. Both variants are now recoverable, with a pin.

**Two fixes shipped unpinned and now are not.** The review proved it by reverting
the CSV inferred-Bool arm and the directory-vs-missing-file message and watching
the suite stay green (103/103). Both now carry pins demonstrated red:
`an_inferred_bool_that_no_longer_parses_is_a_typed_two_pass_failure` (an
in-module unit test — an integration test cannot express the race, because both
passes read whatever the file holds when `read` starts) and
`a_directory_pattern_is_distinguished_from_a_missing_file`.

**Taken as landed**: the RAII fetch directory (`FetchDir`), which releases on
every exit from the read loop including the error and cancellation paths that
previously leaked a whole listing's worth of objects.

### D-09 (US4) — three mechanisms wired to the wrong instrument; three pins that proved nothing

The increment's shape was right — a shared client, a real deadline, a
generation-stamped 401, path encoding, a config guard that fires before any
request. Three of the mechanisms were wired wrong, and all three were reproduced.

**1. The reported Retry-After was clamped, and that is actively harmful.**
Three lenses found it independently. The engine sleeps on the reported value
verbatim (`runtime/run.rs:112-114`), so clamping it sends the next attempt back
inside a window the server said was closed. With `retry_after_cap_secs: 0`
(legal — validation refuses 0 only for the new timeout) the source reported
`Some(0ns)`: the engine's whole 5-attempt budget burns in milliseconds. With the
default cap and a server asking 3600s, it reported 300s and failed after ~20
minutes inside a 1-hour ban, where before it waited the window out.

The clamp also contradicted its own comment: `send` tests the RAW value against
the cap, so when the wait is under the cap the reported value was unclamped
anyway. The clamp therefore fired ONLY in the branch where the source had
declined to wait — exactly when the server's instruction is the only useful
information there is. **Removed**, with the reasoning recorded at the site.

**2. The deadline was a total request timeout, not a stall detector.**
`ClientBuilder::timeout` kills a body that is making continuous progress.
Reproduced against a server dribbling a chunked array with no gap over 200 ms
and a 3 s deadline: `after 3.001570378s: Err(Transient(... Body, TimedOut))`.
Because that is transient, the engine restarts and hits the same wall every
attempt — a large page could never complete. The stated intent was "a server
that accepts a connection and then stalls", and the instrument for that is
`read_timeout`, which resets after each successful read. **Switched.**

**3. `substitute_path` did not stop the attack it was written for.** `.` and
`..` are RFC 3986 unreserved, so the encoder passed them through and the URL
parser then performed dot-segment removal — `..` still walked up a segment, and
US4's own acceptance criterion failed. **Fixed** by escaping the dots when the
encoded value is exactly `.` or `..`; escaping `.` everywhere would mangle
ordinary ids.

Also taken: a date-form `Retry-After` at or before now now yields
`Duration::ZERO` rather than `None`. `None` discarded a real header, and a client
clock slightly ahead of the server's — routine in containers without NTP — makes
that the common case, not an edge one.

**THREE OF MY PINS PROVED NOTHING**, each demonstrated by reverting the fix and
watching the suite stay green:

| pin | why it was worthless | replacement |
|---|---|---|
| `a_zero_request_timeout_is_refused` | substring-matched a rendered error; `deny_unknown_fields` echoes an unknown key back, so it passed with the ENTIRE feature deleted | asserts through the config type: the field exists, carries its value, and defaults above zero |
| path encoding | both unit tests pass whether or not the driver calls the encoder — the call site was unpinned | drives the real child request and asserts on the URL the server RECEIVED; verified red when the call site is reverted |
| the generation counter | entirely untested; reverting to the unconditional clear left the crate green | unit-tests the late-401 interleaving directly; verified red against the pre-fix behaviour |

The date-form test was also wall-clock flaky (sampled 1/60): `fmt_http_date`
truncates to whole seconds, so a `now + 1s` header can already be in the past by
the time the request goes out, and the failure blamed the source for a header
that had expired. Rewritten to use a wide margin and to assert the reported wait
rather than racing it.

**Verified CLEAN by the review**, by execution: the generation counter's logic;
the shared client leaks nothing from the data path into the token fetch; the
fingerprint's body term really is invariant so removing it is provably a no-op;
`validate_post_body_pagination`'s paginator list is exactly the set that emits
parameters; and `httpdate` adds no dependency-tree entry.

**Recorded narrowing.** `substitute_path` percent-encodes `/`, so a parent field
that legitimately spans path segments (GitHub `full_name`, `project/repo`) now
reaches the server as one segment and 404s. That is a behaviour change, taken
deliberately: a broken path is LOUD, path injection is silent. An opt-out is
recorded as a named deferral rather than guessed at now.

**Operator error, recorded per AR3.** While verifying one of the pins I ran
`git checkout` on `client/auth.rs`, which reverted every uncommitted US4 change
in that file rather than the temporary stub I meant to drop. Reapplied and
re-verified; the gate is green and the file's diff is what it should be. Worth
recording because nothing in the process caught it — only re-reading the file
did.

### D-10 (US5) — the design was attacked BEFORE implementation, and the increment was scoped down

**Process change, and it paid for itself.** Five increments had by then each
shipped a defect caught only by post-implementation review, and the recurring
cause was structural: the pins were written after the fix, from the same mental
model, so they confirmed what was built rather than what was required. For US5 —
the increment that would change a persisted format and break a semver-sacred
crate — the design was attacked FIRST, with one lens explicitly asked to argue
against building it at all.

**Four lenses, 36 findings, two CRITICAL — all against a design that did not yet
exist.** The two that decided it:

1. **The governed diff reports drift on a NARROWING.** Run 1 widens `v` to
   Float64 (one batch had `1.5`); run 2's first batch is all integers, so the
   registry re-infers Int64 and `diff_against(baseline, observed)` yields
   `WidenColumn{Float64 → Int64}`. Freeze then aborts a run whose data fits the
   frozen schema perfectly — and in a debug build, which is the entire local
   gate, `registry.diff`'s own `debug_assert!(is_widening(..))` panics first.
   Phase 0's correction only neutralised the case where observed EQUALS baseline.
2. **Under Discard\*, a baseline-derived change annihilates the column.**
   `enforce_discards` rolls back through the WITHIN-RUN snapshot, which on drain 1
   of run N+1 is empty, so `revert_column` takes its `None` arm and deletes the
   column outright — conforming rows lose their values with **zero discard
   accounting**. "Counted, never silent" broken by the fix meant to uphold it.

Neither is reachable in the shipped code, because the design was never built.

**The scope verdict, adopted.** Every hole except the two inheritance items was
created by the CROSS-RUN BASELINE, not by the requirements — and the requirements
are all satisfiable without persisting one:

- **FR-030 is entirely within-run.** Policing table creation after a stream's
  first drain, plus ancestry resolution, is one rule and one helper. With no
  cross-run base the registry stays monotone, so its assertions stay sound, the
  Discard\* rollback target stays correct, and there is no migration, no version
  bump and no downgrade cliff.
- **FR-029 offers "or documented as diagnostic-only" in its own text.**
  `schema_hashes` is a real audit trail — the design doc names `from → to` hashes
  as the auditability mechanism — so it is now documented as diagnostic, with the
  reason a hash CANNOT drive a policy decision stated at the field.
- **FR-028's promise was one sentence, and it is now true.** It was literally
  correct except that a table creation was never policed. Fixing FR-030 makes it
  so; the added paragraph states the within-run boundary precisely.
- **FR-031 becomes vacuous**: nothing compares across runs, so nothing can report
  a subset as drift.

**Principle I settled it.** The baseline would have added a persisted sub-format,
a run-start object, a second diff on every drain of both paths, a new typed
error, and a policy-resolution change — all in the core — to produce an ALARM.
The destinations apply the DDL additively regardless, so Freeze protects no data;
it only decides whether the run aborts. Five ways a working pipeline could stop
working or write NULLs is not a trade worth making for that.

**The strongest counter-argument, and where it actually points.** The plan
rejected narrowing because "a contract that resets every run is close to
worthless". That is a fair point about VALUE — and it argues for a cross-run
**detector**, not a cross-run **gate**. Every failure found is a false or
inescapable abort. A detector that persists schemas and REPORTS divergence has
the same value with none of the abort risk, is additive (serde default, no bump,
revertible), and can land on its own. **Recorded as a named deferral** with the
two prerequisites the review identified: a monotone-safe comparison that joins
before diffing, and a rollback target for Discard\* that matches the compared
base.

**What shipped.** Table creation after a stream's initial drain is policed;
policy resolves through the table's ANCESTRY so freezing a stream freezes the
child tables its nested collections create, with an explicit entry on a child
still winning; and a mid-run table creation under Discard\* now drops and COUNTS
its rows instead of silently creating the table — the column-less-change hole
that made `enforce_discards` skip it.

Four pins, three demonstrated red on the pre-fix build
(`freeze_refuses_a_child_table_created_mid_run`,
`a_frozen_parent_freezes_the_child_tables_it_creates`,
`discard_refuses_a_mid_run_child_table_and_counts_its_rows`); the fourth
(`freeze_allows_the_tables_the_first_drain_establishes`) is a guard that must
pass on both sides and does.

**What did NOT change**: `STATE_FORMAT_VERSION` stays 1, `StateDoc` keeps its
shape, no crate's public surface moved, and the increment is revertible in the
field. The plan's Complexity Tracking entry for a breaking `rdlt-core` change is
**withdrawn**, and the recorded 0.2 → 0.3 bump returns to standing rather than
required.

**Behaviour change, recorded (AR3).** Ancestry inheritance can make a previously
working configuration fail: a pipeline that froze a parent and relied on its
child tables evolving freely now sees the freeze reach them. That is the point of
FR-030, the escape hatch is an explicit per-table entry on the child, and
`a_frozen_parent_freezes_the_child_tables_it_creates` pins it deliberately.

### D-11 (US6) — the divergence proved container-free, and the gate's own red fixed

**The audit's claim is CONFIRMED by execution, without a container.** The
catalog's table builder renumbers field ids LEVEL-ORDER while this crate assigns
them DEPTH-FIRST, so the two disagree from the moment a table exists:

```
WANTED (ours):    profile=1, city=2, zip=3, id=4
LIVE  (catalog):  profile=1, id=2,   city=3, zip=4
profile: equal_types=false
```

`Type`'s own `PartialEq` compares nested `NestedField`s including their ids, so
`current_field.field_type != field.field_type` reported drift for a stream that
had not changed — and the SECOND `ensure_table` of any struct-bearing table
failed as "contradictory drift". Phase 0 called this guaranteed rather than
plausible; this is the measurement.

**Why the pin is container-free, and why that mattered.** `test_support.rs`
already built its fixture through `TableMetadataBuilder::from_table_creation` —
the very normalizer that causes the defect — so parameterizing it reproduces the
divergence with no runtime at all. That is not a convenience: a container-gated
test SKIPS when no runtime is present, and **a skipping test is green**, so it
can never be the evidence that a fix was needed (AR1). The live cell
(`tests/nested_types.rs`) is confirmation and says so in its own module doc.

The three pins were verified red against the pre-fix comparison. One of them
asserts the ids really do diverge before asserting no drift is found — without
that, the pin would pass on a fixture that happened not to renumber, proving
nothing.

**Shipped**: an ID-insensitive recursive comparison in `dest/schema.rs` — the
module that assigns the ids, so the invariant and its insensitivity sit
together — matching nested fields BY NAME (a catalog may reorder) and refusing a
genuine nested addition, removal, rename or retype. Nullability is compared
**asymmetrically**: a table that REQUIRES a value the stream may not supply is
drift; the reverse is what additive evolution deliberately creates and stays
silent. Previously nullability was not compared at all and surfaced far away as
a generic batching failure.

**The gate's own red, fixed.** T006 recorded a NEW finding: the iceberg fixture
panicked instead of skipping, turning the whole workspace gate red for an
environmental reason. The cause was a disagreement between two probes —
`runtime_available()` checks the container SOCKET, while the fixture starts
containers through the `podman` BINARY, and a wrapper that cannot exec satisfies
the first and fails the second. `run_container` now returns `None` with a visible
SKIP line instead of asserting. **The workspace suite passes 722/722 with
containers ENABLED**, where at the start of this feature it failed.

**T097 NOT PERFORMED, recorded.** Pinning `apache/polaris:latest` requires
pulling a candidate and reading its version label off the pulled image. No
working container runtime exists on this machine, so the probe cannot be run and
a tag must not be invented. The floating tag stands, with the 017 D16 rationale
unchanged; carried with its trigger.

**Recorded deferral (AR6)**: nested ADDITIVE evolution — a struct that gains a
child field — is now reported as `Drift::NestedFields` and refused. That is a
strict improvement over refusing every struct re-ensure, but it is a real ceiling
for JSON sources, whose structs widen by appending children. The library exposes
`AddColumn::builder().parent(..)` for it; taking it is a separate increment.

**Both phase-2 doors re-probed and still CLOSED, with registry evidence**:
`Transaction` in iceberg 0.10.0 exposes exactly eight actions and the module
directory holds no overwrite/rewrite/delete action file, and there is no client
middleware for SigV4. The deferrals stand; scope was not opened.

### D-12 (US7) — sixteen sharp edges, and one flake that is now data

**Wire-level refusals instead of plausible wrong values.** The postgres encoder's
Decimal and Time arms were DELETED rather than patched: the representation match
below them already reads the scale off the ARRAY — the scale the i128 payload is
actually stored at — and already refuses a negative one, so the logical-type arms
could only ever disagree with the data. Time64 is now bounds-checked BEFORE the
cast (a negative or beyond-24h value wrapped into a plausible time, sending a
DIFFERENT time rather than refusing); the date epoch shift is checked i64
arithmetic; and the numeric weight uses `expect` rather than substituting
`i16::MAX`, because silently sending a different number is worse than a panic on
an unreachable path. **The byte-identity fixture is unchanged**, which is what
proves these are refusals rather than re-encodings.

**Consistent classification.** Eight DuckDB load-path sites — the emptiness
probe, target and stage DDL, add-column, alter-type, scd2 validity DDL, the
legacy index drop, the index mapper — now route through the crate's own
classifier instead of forcing fatal, so a transient file lock retries at ensure
exactly as it already did at write. One site was reverted after the compiler
showed it maps a plan error, not a `duckdb::Error`.

**Diagnostics attributed to work, not to threads.** The two async span guards
were replaced by `Instrument` at the spawn sites. A guard held across `.await`
stays on the worker thread's span stack while other tasks run there, so
concurrent streams attribute each other's events; binding the span to the FUTURE
is what "this stream's work" actually means. The two `spawn_blocking` `enter()`
calls stay — they are correct.

**Bounded recovery residue.** `Scan` gained a `Discard` variant, returned when a
manifest WAS read but holds nothing replayable. `Nothing` now means only "no
manifest". A pipeline that repeatedly dies before its first checkpoint no longer
accumulates manifest lines and orphaned segments. An existing assertion encoded
the old behaviour and was updated deliberately, with its reasoning.

**Honest error taxonomy.** Five impossible-unless-engine-bug sites moved from
`Config` to `Internal`; every other `RdltError::config` in the engine is
data- or configuration-adjacent and stays. The CLI maps `Internal` AND the
catch-all to 70 (EX_SOFTWARE) — not 2, which told a scripting caller to edit
their YAML for something the engine could not classify, and which a future
`#[non_exhaustive]` variant would have joined silently — plus a new `Io` variant
at 74 for files the CLI itself could not read or write.

**A contract that now holds at its own edges.** `normalize_ident` sizes its hash
suffix to the bound instead of using a fixed width, so a `max_len` below the
suffix no longer produces a name LONGER than the limit being enforced. The digest
is sliced locally rather than weakening the public `ident_hash`, whose 4..64
clamp is exactly what broke the bound. Pinned across `max_len` 1..=24.

**Failures that are now visible**: the postgres connection driver's terminal
error (one helper replacing two copies that discarded it — it owns the socket, so
its error was the only description of WHY later statements fail); dropped events
when a consumer lags, with the count; a broken CLI event feed as a warning that
does NOT fail a successful run; a corrupt-vs-absent bench report distinguished;
and a failed container inspect no longer read as "the container exited".

### D-13 (US7) — the skip-not-fail fix immediately earned its keep

`rdlt-connector-file::s3_live wrong_credentials_are_typed` failed once in the
full suite and passed in isolation and on both re-runs. It is not a regression:
**that test could not run at all before US6** fixed the fixture's panic-instead-
of-skip, so this is the first feature in which the S3 live legs actually execute
here. The flake is the recorded container-timing class (019 D-04, and the gate
weakness this feature already carries as an item), now observed rather than
inferred — which is exactly what the flake-recording work in US8 exists to turn
into data. Green claimed only from two consecutive clean full runs, per the
standard this feature set for itself.

### D-14 (US8) — the pins were verified by applying the mutants, not by naming them

Nineteen named mutants, each verified RED by hand-applying that exact mutation
and running only the pin claiming to kill it: **19/19 killed**. The
`--iterate`-style whole-run approach was abandoned for this purpose after
measurement — `test_workspace = true` runs all 749 tests per mutant, so the
scoped 264-mutant baseline was a ~6-hour job holding two cores, while a
hand-applied mutation answers the same question in seconds and answers it per
pin. Two things that surfaced only because the verification was real:

**A comment that had been wrong for four features.** `mutation_closures.rs`
claimed to kill `byte_size → 0/1` "via the bytes counter being real". It does
not, and could not: `ByteSized::byte_size` has exactly ONE consumer — the stage
channel's permit request — while `table.bytes` is read straight off the batch in
`Loader::process`. A constant `byte_size` leaves every counter in that test
correct and removes backpressure entirely. The comment is the reason nobody
looked again (Principle VI); it is corrected in both places it appeared, and the
property is now pinned by its consequence: a batch at budget sends, the next
PARKS, and it proceeds only after `recv` frees the permit.

**A `.max(1)` that no test could kill, and my first pin claimed otherwise.** The
`EveryCheckpoints(n)` mutant `n.max(1) → n` SURVIVED the first version of its
pin, which asserted a nonzero count where `1 >= 0` and `1 >= 1` agree. The
comment asserting `.max(1)` prevents "never committing" was simply wrong —
dropping it makes the predicate fire MORE eagerly. What it actually guards is
the zero-accumulation case, which is unreachable from the sole call site
(`policy_triggers` runs only after `checkpoints_since_commit += 1`). Pinned as
the function's own contract rather than recorded as equivalent, because that
invariant is what makes the `.max` correct for any future caller.

**A verification harness that was itself unsound at first.** Restoring sources
with `shutil.copy2` preserved their mtimes, so a file restored to an OLDER
timestamp than the artifact built from its mutated version was treated as
unchanged and the mutated object code stayed linked. Green tests read as red and
a previous mutation could remain co-active. Caught because the post-restore
green run FAILED; the harness now touches every file, and all 19 verdicts were
re-derived under forced rebuilds.

**The highest-consequence survivor was two lines.** `SchemaPolicy::freeze()`
→ `Default::default()` survived, and `Default` for that type is `evolve()` — so
a caller who explicitly asked to FREEZE the schema silently got Evolve, the
exact inversion of the contract. `evolve()` was "caught" only by accident: the
same mutation makes it call itself, and it is the stack overflow, not an
assertion, that failed the suite.

### D-15 (US8) — the reclaim verb, verified against a real aborted run

Every one of the SIX container start sites now carries `rdlt-test=1`, not the
two the task named: the testkit's two fixtures, the postgres TLS fixture, the
RUSTFS fixture, and the two that shell out to the container CLI directly
(iceberg's host-network Polaris/RUSTFS pair, and rdlt-bench). A reclaim verb
covering a subset is a false promise, and T136 asserts nothing labelled remains.

Verified by reproducing the incident rather than reasoning about it: the
conformance suite was started, allowed to boot **11 labelled containers**, then
SIGKILLed so testcontainers' `Drop` never ran. All 11 survived as orphans, still
running, under random names. `make reclaim` then removed exactly 11 — and left
all **184** unrelated containers on this machine untouched. That last number is
the point of scoping by label: a name pattern or a blanket `prune` here would
have destroyed 184 containers belonging to other projects. Volumes are removed
separately because an anonymous volume outlives its container, which is what let
the disk fill twice during 017.

### D-16 (US8) — two instrument defects found before they could corrupt evidence

Both would have produced confident, wrong measurements:

**`cargo mutants -f` is inert in 27.1.0.** A glob matching nothing
(`-f zzz/nope.rs`) still tested all 719 mutants; so did every correct path. Only
`--examine-re` filters. Trusting `-f` would have turned a whole-workspace run
into a "scoped baseline" and invited per-file conclusions from it. This is the
same defect class this feature keeps fixing in its own code — a flag that
accepts input and does nothing (US4's POST pagination).

**The Makefile's mutants recipe could not run on this machine at all.**
cargo-mutants builds one FULL debug workspace per job under `TMPDIR`; two jobs
measure **27 GiB**, against 22 GiB free on the default 32 GiB tmpfs. The run
aborted mid-build with a bare "Disk quota exceeded" (`EDQUOT`, not `ENOSPC`),
which reads as a host problem rather than a too-small scratch directory —
`cargo clean` freeing 562 GiB of `target/` changed nothing, because `/tmp` was
never what was full. The recipe now pins `TMPDIR` onto the repo's own
filesystem, inside `target/` so it stays gitignored and reclaimable.

### D-17 (US8) — flakes become data instead of a re-run convention

Six container flakes are recorded across 015-019 and all were handled the same
way: re-run, watch it pass, move on. That answers "did it pass eventually" and
never "how often does it fail" — the only number separating a timing-sensitive
fixture from a real intermittent bug. `[profile.flake]` (retries, JUnit) plus
`benches/record-flakes.sh` appends one line per test nextest ITSELF classifies as
flaky — failed, retried, passed — to a committed log.

Deliberately opt-in, NOT the default profile: retries in the gate of record
would let a real intermittent bug read as green, which is precisely the hazard
019 D-04 warns about. Verified both directions with a temporary probe that fails
once then passes — nextest reported `FLAKY 2/3` and the observation was appended;
a clean run appended nothing. The probe and its synthetic log line were then
deleted, because the log is evidence and must hold only real observations.

### D-18 (US9) — the license the repo declared but never shipped

Every `Cargo.toml` said `license = "Apache-2.0"` and the repository contained
no license text at all. Fixed at the root AND in all 12 publishable crates,
byte-identical (sha256 `a60eea81…`), because a root `LICENSE` satisfies GitHub
and the repository but **NOT the `.crate` tarballs** — verified per crate with
`cargo package --list`, which showed the README arriving and no LICENSE before
the fix and 12/12 carrying both after it.

`readme` is set PER CRATE rather than inherited from `[workspace.package]`: an
inherited `readme` resolves against the workspace root, so every crate would
have shipped the root README as its crates.io front page. Seven crates had no
README; they have one now.

Four descriptions were wrong or off-convention. The CLI said TOML and parses
YAML (`main.rs:150`); the file connector still said "source" though it has been
source AND destination, with CSV and S3, since 015; the testkit omitted the
container fixtures it has carried since 017; and sqlcore was the only
description not naming rdlt first.

### D-19 (US9) — 220 undocumented public items, and a docs gate that found 14 dead links

`#![warn(missing_docs)]` on the three semver-sacred crates — warn rather than
deny, and a per-crate attribute rather than `[workspace.lints]`, so an
undocumented item is a gap to fill and not a broken contributor build. The real
count only exists once the lint runs: **220** (rdlt-core 147, rdlt-connector 40,
rdlt 33). All 220 are now documented, and the counts are 0/0/0.

`make docs` (rustdoc under `-D warnings`, wired into `check`) earned its place
on first run by failing with **14 broken intra-doc links** across three crates:
public module documentation linking PRIVATE modules, so the link was dead for
every reader not building with `--document-private-items`. Demoted to code
spans. Making the modules public to satisfy the links would have widened the
public surface to fix a documentation defect — the wrong direction under
Principle I.

**T145 feature matrix, verified by building rather than by inspection**
(research R10.7 had judged the facade narrowing-safe by reading it): 20
configurations, all OK — the facade with no default features and with each of
`rest`, `duckdb`, `file`, `parquet`, `iceberg`, `postgres-source`,
`postgres-dest`, `postgres`, and `--all-features`; the SPI bare, `+schema`,
`+failpoints`; the testkit bare and `+containers`; core bare and `+failpoints`;
the postgres connector's own `source`/`dest` narrowing; and the file and
sqlcore crates.

**CI-blocked, recorded UNPERFORMED (AR7):** the semver job now covers
`--workspace` instead of 2 of 12 crates, and cannot be observed running.

### D-20 (US10, partial) — the mirror removed rather than guarded

`DestSpec::File` was a struct variant restating `FileDestConfig` field by
field, with a rebuild that reassembled it through builder calls. It now EMBEDS
`Box<FileDestConfig>`, the shape the Iceberg arm already used.

The hazard it removes is one-directional and silent: a field added to the
connector's config and not added to the mirror compiled fine, and was simply
unreachable from any pipeline document — configurable through the library,
invisible from YAML, with no error anywhere. Embedding makes that impossible
rather than detectable. The document shape is unchanged, proven by the existing
parse and build-parity suites passing untouched.

**The pin written for this in US8 survived the refactor, and its ADVICE did
not.** T133's field-set assertion was designed to outlive the embedding, and it
did — but its failure message still told the reader to update a mirror that no
longer exists. Rewritten to the property that remains true: the field set IS
the document vocabulary now, and vocabulary changes are user-facing (017's
`merge_key` → `merge_scope` broke real pipelines), so the assertion forces the
change to be deliberate. A test that passes while giving wrong instructions is
a Principle VI defect, not a passing test.

**pg and duckdb CANNOT follow, and are re-recorded rather than left implied.**
Neither connector has a deserializable destination config: `Postgres` derives
`Debug, Clone` and `DuckDb` derives `Clone`, because both are handle types
holding live connections. There is nothing to embed. Their `DestOptions` leg —
the part that IS a config vocabulary — already carries a schemars round-trip
guard in the postgres `config_schema` suite. **New trigger:** either connector
growing a deserializable destination config type.

**D19 is REJECTED, with its reason.** Its premise changed: it named a trio and
the code is now a quartet, and what it names is not a correctness invariant —
so "fix it" would be churn against a moving target with no defect behind it.
The shape that WOULD close it: one config-plumbing seam the connectors share,
rather than each threading its own. **New trigger:** a fifth member appearing,
or any member of the set acquiring a correctness obligation.

**The `ensure_table` choreography extraction is re-recorded** with the trigger
"the next feature that adds a third SQL destination, or that changes the
index-ensure protocol in either executor" — two implementations that agree
today are cheap to keep in step; three are not.

### D-21 (US10) — three of 017's eight residuals taken, on evidence

**Taken.** *Dead duckdb root re-exports* — the crate re-exported four types at
its root "to keep the old import paths working", which is precisely the compat
shim [[greenfield]] forbids; the single real consumer was repointed at the
canonical `dest` path and the re-exports DELETED, with `--all-targets` clean.
*TLS connect-arm twins* — the plaintext and rustls arms were four identical
lines differing only in the connector value, so error classification and driver
spawning had to be kept in step BY HAND; that is the exact shape of 017's own
F3, where two executors classified a shared plan's errors oppositely because
only one was updated. Folded into one generic `connect_with`, verified by the
live TLS matrix (25/25, both arms). *`is_local` durability-gating spread* — the
five call sites serve TWO concepts, and the reason was restated or implied at
each; the four durability/crash sites now read `filesystem_protocol()`, whose
doc says once why the stage → rename → fsync protocol and its crash points
exist only on a filesystem. The ownership rule deliberately still reads
`is_local()`, because it is a statement about which files may be DELETED, not
about durability. 105/105 file-connector tests including the S3 live legs.

**Folded:** the duplicated unique-index diagnosis folds into the sqlcore move,
which adds the golden pin it lacks today.

**Re-recorded with triggers:** scope-membership SQL duplication (the
`flagged_roots` half is taken separately; the remainder waits on a third
dialect), ShredOwner wrapper + retry-arm duplication, `clear_table`
DELETE-vs-TRUNCATE on persistent targets, and sequential per-part S3 publishes
— the last belongs to the measurement-first queue, not to cleanup, because it
is a throughput claim and this project has twice measured an "obvious"
allocation win as a LOSS.

### D-22 (US10) — the Principle VI sweep of the duckdb suite

Thirteen comments across eight test files cited planning IDs — `T057`,
`contract SM6`, `feature 013 US1`, `MR1/MR2`, `the 009 lesson`. A citation
tells a reader where a requirement came from and never what the file covers,
and it ages the moment its plan is archived. All rewritten to state the
behaviour and the reason on their own terms; zero citation IDs remain in that
directory. 43/43 green.

### D-23 (US10) — dependency hygiene, and one claim checked before acting on it

Removed after verifying each is genuinely unreferenced: `arrow-schema` from
rdlt-core, `futures` from rdlt-engine, `bytes` and `futures` from rdlt-testkit.
`tokio` is demoted to a dev-dependency of the facade, whose own source never
names it; `tokio-util` stays, because `cancellation_token()` hands one out.
Note the honest limit of that last change — tokio still reaches a consumer
TRANSITIVELY through rdlt-engine, which genuinely runs on it. What changed is
that the facade no longer DECLARES a runtime it does not use.

rdlt-core's library dependency tree is now exactly blake3 + serde + serde_json
+ thiserror, which is what its charter claims — and the charter itself was
wrong: it listed "arrow-schema for schema types" for a dependency the crate does
not use. Corrected, and it now says the sharper thing: arrow is deliberately
absent, because the schema vocabulary here is rdlt's own and mapping it onto
arrow types is the engine's job.

**One audit claim was checked and initially appeared false.** A strict grep put
two `bytes` hits in rdlt-testkit's source. They are a local binding NAMED
`bytes` in a `PushPayload::RawJson(bytes)` pattern, not the crate — `bytes::`
and `use bytes` both find nothing. The claim held, and confirming it cost one
command; removing a dependency that turned out to be used would have cost a
great deal more.

### D-24 (US10) — the gate was lying, and the flake recorder proved it

Three suite runs failed on THREE DIFFERENT container tests while the mutation
run held the machine. Each failure passed in isolation, which is the signature
of resource starvation rather than a defect — and the timings settled it:
`absent_retire_closes_missing_keys` took **154.9 s** against its usual ~5 s, at
a load average of **88–99 on 32 cores**, roughly 3x oversubscribed.

The flake recorder built in US8 earned its keep here on its first real outing,
capturing **five genuine observations** in one run — three in the S3 live legs
(one needing TWO retries), one CDC, one DuckDB differential. Every one is
container-backed, which gives the recorded flake class a shape it did not have
when it was six anecdotes: it is contention on container startup and I/O, not
a scatter of unrelated bugs. The observations are committed WITH the confound
stated, because a datum with a known confound is worth more than an anecdote
without one.

**The consequence for how this feature reports its gate:** a full-suite run
concurrent with the mutation run is not evidence. Paused, on a quiet machine,
the same tree runs **749/749, 0 skipped, in 21 s** — against 230 s and failures
under load. Every green claimed in this feature's close-out is from a quiet
machine, and the mutation run is paused for each one.

### D-25 (US8) — the mutation gate's timeout was manufacturing false results

The recipe auto-derives its per-mutant test timeout from a baseline measured at
startup, times `timeout_multiplier = 3.0`. With containers forced off the
baseline test phase is **9s**, so the timeout was auto-set to **28s** — and then
every mutant ran while TWO concurrent `cargo build` invocations each claimed all
32 cores. At a load average of 85 the test phase cannot finish in 28s, so
**73% of mutants were reported TIMEOUT, every one at exactly 28s**.

That is worse than slow, because a timeout is not a neutral outcome: it is
indistinguishable in the report from a mutant the suite failed to catch, and it
is recorded as not-caught. The stale committed run's timeout rate was 1%; this
was 73%. The gate was measuring its own contention.

Two fixes, both in the recipe of record:
- `--jobserver-tasks 16` caps build concurrency ACROSS jobs, instead of letting
  each job's build claim every core.
- `--minimum-test-timeout 180` puts a floor under the auto-derived value, so a
  merely-loaded test run is not recorded as a hang.

**The fix immediately converted false timeouts into real findings.** Three
mutants previously reported TIMEOUT at exactly 28s came back MISSED with genuine
test times of 24–27s — they had never been hanging, and they are uncaught
mutants the ceiling was HIDING. In the same pass a real hang was correctly
identified for the first time: `RecordsIn::close` replaced with `()` burned the
full 180s, which is right — removing the close means the receiver never learns
the sender finished, and the pipeline deadlocks.

Timeout rate after the fix: 1 in 24, and 12 genuine MISSED mutants surfaced.

**Standing caveat for triage:** the run sets
`RDLT_TESTKIT_FORCE_NO_CONTAINERS=1`, so container-gated legs skip. A mutant in
code exercised ONLY by a container test therefore survives for a reason that has
nothing to do with pin quality. Every survivor in a file this feature touched
gets checked against that possibility rather than assumed to be a hole.

### D-26 (US11) — the reqwest double tree: REJECTED, with the reason and a re-trigger

Two major versions of reqwest are in the tree, and neither is ours to choose:

- **0.12.28** ← `iceberg 0.10.0`, `iceberg-catalog-rest 0.10.0`, `object_store
  0.12.5`, and our own `rdlt-connector-rest` (the workspace pins `"0.12"`).
- **0.13.4** ← `opendal-core 0.57.0` ← `opendal 0.57.0` ←
  `iceberg-storage-opendal 0.10.0`.

Both arrive through the Iceberg destination, but along different upstream
chains that pin different reqwest MAJORS. Cargo cannot unify across a major, so
deduplicating requires iceberg-rust and opendal to agree on one — an upstream
change, not one available to us. Pinning either side down would mean forking or
downgrading a dependency to win a build-size argument, which is a bad trade
against a working Iceberg destination.

Two facts that bound the cost, and are the reason this is rejected rather than
merely deferred: the duplication is **entirely gated behind the `iceberg`
feature** (a facade built without it pulls reqwest zero times), and it costs
build time and binary size only — no correctness or runtime surface.

**Re-trigger:** `iceberg-storage-opendal` (or opendal) moving to reqwest 0.13's
line, or iceberg-rust moving to 0.13 — at which point `cargo tree -i` should
show ONE version and this entry can be closed by observation rather than by
argument.

### D-27 (US8) — the survivor list is inflated by the flag the task specifies, PROVEN

T122 specifies `RDLT_TESTKIT_FORCE_NO_CONTAINERS=1`, which makes the run
deterministic and roughly halves each mutant's test phase. It also means every
container-gated leg SKIPS — so a mutant whose only verifying test needs a
container survives for a reason that has nothing to do with pin quality.

This was flagged as a caveat and is now PROVEN rather than argued. Taking
`check_hard_delete -> Ok(())` (MISSED in the run), applying it by hand, and
running the suite WITH containers enabled: **caught**, by
`rdlt-connector-postgres::dest_conformance
strategies::child_hard_delete_is_rejected_typed`. One test, container-gated,
skipped by the flag. The validation function is genuinely pinned; the run simply
could not see the pin.

**Therefore no survivor may be called a hole on the run's word alone.** Each one
is re-checked with containers enabled before disposition, and that check is
cheap because the survivor list is small.

**Keeping the flag is nevertheless the right call**, and the reason is about
which direction the error runs. Without it, container tests execute under two
concurrent mutation jobs — the exact load that produced five recorded flakes in
one suite run (D-24). A flaky failure there would be recorded as **CAUGHT**,
because cargo-mutants cannot tell a genuine kill from a flake. That is a FALSE
NEGATIVE: it hides a hole, silently, in the direction the gate exists to
protect. A false MISSED merely creates triage work, and triage is exactly what
T123 is. Given a choice of which way to be wrong, be wrong in the direction that
produces work rather than false confidence.

**First substantive signal from the newly-examined surface.** Adding
`rdlt-connector-sqlcore` to `examine_globs` (T121) immediately produced 11 of
the first 14 survivors — including whole validation functions
(`check_hard_delete`, `check_scd2`) stubbing to `Ok(())`, and
`flagged_roots` returning `String::new()` or `"xyzzy"`. That is the crate every
SQL destination plans through, which is precisely the rationale recorded when it
was added: an uncaught mutant there is wrong SQL at every destination at once.
Whether each is a real hole or another container-gated skip is T123's job, on
the evidence above.

### D-28 (US8) — the fresh mutation run, and what its number actually means

**921 mutants, 97 survivors** (87 missed + 10 timeout), 175 caught in the final
pass alone. The committed run it replaces was 719 mutants / 29 missed and
predated features 006-019 entirely, so the two numbers are not comparable and
the close-out does not compare them. Three things changed at once: the codebase
grew by fourteen features, `rdlt-connector-sqlcore` entered `examine_globs` for
the first time (T121), and this run forces containers OFF.

**Where the survivors are** is the finding, more than the count:

| module | survivors |
|---|---|
| `shred/build.rs` | 15 |
| `shred/arena.rs` | 11 |
| `runtime/run.rs` | 10 |
| `shred/passthrough.rs` | 9 |
| `shred/table.rs` | 7 |
| `shred/canon.rs` | 5 |
| `shred/{infer,mod}.rs`, `view.rs` | 10 |
| sqlcore (4 files) | 11 |
| `rdlt-core`, `rdlt-connector` | 9 |

**Over half — 55 of 97 — are in the shredder.** That is the JSON→Arrow hot
path: the most performance-tuned code in the workspace, repeatedly rewritten
across 012/019 for throughput, and evidently the least pinned by assertion. It
is also the code whose defects are hardest to see from the outside, because a
shredding bug produces plausible data rather than an error. This is a
better-targeted result than a single coverage percentage: it names the module
that needs pins, not a number to argue about.

**The count is an upper bound, deliberately.** `RDLT_TESTKIT_FORCE_NO_CONTAINERS=1`
skips every container-gated leg, and D-27 PROVES that inflates the list. All 97
are therefore re-tested with containers ENABLED before any disposition is
written; a survivor that flips to caught was never a gap. The flag stays for the
main run because the error then runs in the safe direction — see D-27.

**Cost of the run, honestly:** ~2h for the final 329-mutant pass, but many hours
across restarts, and the restarts were mine — three misdiagnosed OOM kills
(D-30), a corrupted resume state from the tmpfs experiment, and repeated
re-testing of the survivor backlog, which `--iterate` never excludes because it
skips only caught and unviable. A future run should exclude confirmed survivors
by regex to avoid re-grinding them on every resume.

### D-29 (US8) — the survivor list verified against containers: 24 of 97 were never gaps

Every one of the 97 survivors was re-tested with containers ENABLED, because
D-27 proved the main run's `RDLT_TESTKIT_FORCE_NO_CONTAINERS=1` inflates the
list. Result: **24 (25%) flip to CAUGHT** — they were pinned all along, by tests
the main run could not execute. **~73 are real**: 66 missed plus 9 timeouts
needing individual judgement.

**The distribution refuted the prediction, and the reasoning behind it.** Before
running it, the expectation recorded here was that false survivors would
concentrate in sqlcore — which has almost no local tests and is pinned through
the postgres and DuckDB integration suites — while the engine's survivors would
be "almost certainly all real, since engine tests drive `MemoryDestination`
in-process and need no containers."

Backwards. **19 of the 24 false survivors are in `rdlt-engine`; only 5 in
sqlcore.** The flaw was reasoning about which tests TARGET the engine while
ignoring `test_workspace = true` in `.cargo/mutants.toml`: every mutant faces
all 749 tests, and engine code is exercised end-to-end by the postgres, file and
iceberg suites — all container-gated. The engine has MORE container-dependent
coverage than sqlcore, not less.

This is the strongest argument in the feature for verifying rather than
reasoning. Module-structure reasoning produced a confident wrong answer;
measurement produced the right one. Skipping the pass would have shipped 24
dispositions instructing someone to pin already-pinned code, concentrated in
precisely the crate the reasoning was most confident about.

**Real gaps by crate:** rdlt-engine 60, sqlcore 6, rdlt-core 5, rdlt-connector 4.
The shredder cluster survives verification intact, so the headline of D-28
stands: the JSON→Arrow hot path is the least-pinned code in the workspace, and
its unpinned edges are arithmetic and comparisons rather than control flow.

### D-30 (US8) — all 75 verified gaps dispositioned

Every survivor that outlived the container re-check now has a terminal
disposition. **51 killed** by pins each verified RED under its hand-applied
mutation, **18 equivalent** with the argument recorded at the call site, **3
dead-code deletions**, 2 cosmetic, 1 untestable by construction, and **1 real
defect fixed**.

**The defect.** `UniqueNamer::name_for` could spin forever. `suffixed` truncates
to `max_len - SUFFIX_LEN` BEFORE appending, so once a base reaches that bound
`suffixed(suffixed(x)) == suffixed(x)` — verified idempotent at three bounds —
and the collision loop stopped making progress. The comment three lines above
claimed it "extends deterministically rather than loop forever"; it has been
wrong since it was written. Each probe now hashes a distinct input and the loop
is bounded by the number of names taken. Found only because two mutants HUNG
rather than failed, which is a different signal from "missed" and was worth
chasing.

**The 18 equivalents matter as much as the 51 kills.** Taking the brief
literally would have meant 75 new assertions, but a quarter of these mutants
cannot fail any test — and a test that cannot fail is worse than none, because
it reads as coverage. Four recurring reasons, each recorded where it applies:
a guard redundant with the function it guards (`is_pinned`, the `is_object` /
`is_array` projections); a validator upstream making the state unreachable (the
empty `merge_scope`); a fast-path filter whose authority is downstream (the RFC
3339 digit checks — chrono decides); and a counter that cannot be zero where it
is read (`enforce_discards` is only called when something WILL be discarded).

**Two pieces of dead code, proven dead rather than argued.** `visit_string` on
both arena visitors — a `panic!` there never fired across the whole engine
suite, because `from_str`/`from_slice` borrow or use scratch and never produce
an owned `String`. And `fresh`'s `Kind::Null` arm, whose only caller returns
early on null three lines before dispatching.

**One untestable by construction, written down rather than papered over.**
Replacing the WAL's `sync_for_commit` with `Ok(())` passes every test this suite
can run: without the fsyncs the data is still in the page cache, so every read —
including a full recovery replay after `kill -9` — returns the same bytes. Only
a kernel death with the cache unwritten distinguishes them. A test asserting
"commit succeeded" would pass with the barrier removed and falsely claim it
covered.

**The recurring defect in the pins themselves was the tautology**: an assertion
that reads a value from the code under test and compares it to itself. The
parquet dictionary-page limit had one (`assert_eq!(parsed.x, THE_CONSTANT)`),
and so did two assertions written DURING this work — including an anti-vacuous
guard whose `||` let it pass on one counter while the other compared zero to
zero. Mutation testing finds these reliably; review does not, because they look
exactly like real assertions.

Suite grew 749 → 780.

### D-31 (US10) — one byte-budget channel, and the deadlock the unification caused

D17 fired in 019 and was never taken: the byte-bounded channel existed twice —
in the SPI specialised to source pushes, and inside the engine generic over its
stage items. Same semaphore-permit accounting, same oversized-item degradation,
same close-wake, maintained in parallel. The SPI copy is now the only one; the
engine's `runtime/channel.rs` is **deleted**, and a tree-wide search for its
module path and its `StageClosed` type returns zero hits (AR6).

**The two copies disagreed on one thing, and it was not cosmetic.** The engine's
`recv()` returned the value and released the byte budget at receipt; the SPI's
returned a `SourcePush` that carries the permit onward, so the budget stays spent
while the host works. A shared core cannot pick a side without changing one
caller's backpressure, so `recv()` now returns `Permitted<T>` — the value with
its permit still attached — and each caller states its own policy: the engine
calls `into_value()` (release now), the SPI calls `into_parts()` and re-wraps.
Neither behaviour changed.

**That type change then deadlocked two tests, which is the finding worth
recording.** The engine's two core tests were carried over verbatim, and
`let _first = rx.recv().await.unwrap();` silently changed meaning: under the old
contract that binding was a throwaway, under the new one it HOLDS 80 bytes of a
100-byte budget, so the pending 80-byte send could never proceed. Both hung on a
futex for 25 minutes rather than failing. Nothing in the type system can catch
this — both versions compile, and only the runtime lifetime of a permit moved.
The tests now `drop()` explicitly, and a new pin,
`a_held_permit_keeps_the_budget_spent`, asserts the rule outright so the next
reader meets it as a documented contract instead of a hang.

**A second source of truth went with it.** `RecordsOut::send` took a `size`
argument that four call sites each computed independently — `bytes.len()`,
`buf.len()`, `get_array_memory_size()`, and a literal `0`. With
`ByteSized for PushPayload` answering that question once, the parameter is a
number that can disagree with the payload it describes; it is deleted rather
than left as a hazard.

Gate: **785/785, 0 skipped, 21.8 s** on a quiet machine (load 1.7); clippy
`--all-targets --all-features` clean; `make docs` 0 errors; doc-tests green; the
28 golden/pin assertions byte-identical.

### D-32 (US10) — US10 closes with zero fired-but-undisposed deferrals (SC-014)

The three recorded deferrals whose triggers had fired all end this increment
with a terminal disposition, none of them silent:

- **D17** (SPI/engine channel duplication) — **TAKEN**, see D-31.
- **D18** (file-dest blocking + whole-part buffering) — **MOVED, not dropped**:
  it is a throughput and RSS claim, so it belongs to the measurement-first queue
  (US11), not to cleanup. This project has twice measured an "obvious"
  allocation win as a LOSS (019 D-13, D-21), and D18 is exactly that shape.
- **D19** (config-plumbing trio) — **REJECTED** with its reason recorded in
  D-20: its premise changed. It is a quartet, not a trio, and the code it names
  is not a correctness invariant. The shape that would close it and the new
  trigger are named there.

Alongside them, 017's eight verified-but-cut residuals were triaged rather than
carried: three taken, one folded, four re-recorded with triggers (D-21).

### D-33 (US11) — the owed merge-arm plan, captured; and what it says about the rest of the queue

FR-077's condition was "no change is proposed against that path until the plan
is in hand." The plan is now in hand, captured with `auto_explain` on a
postgres:16 fixture seeded from `benches/fixtures/seed_pg.sql` — content hashes
`e840f517…` / `7e208273…`, identical to the recorded bench dataset — running the
real `pg-to-pg-dedup-1m` specs through the release CLI. rdlt-bench was NOT
extended.

**Where the 4756 ms cell actually goes:**

| node | actual | share of cell |
|---|---|---|
| `COPY (SELECT … FROM events_v2) TO STDOUT (BINARY)` — the source read | 529.9 ms (Seq Scan 149.2 ms) | 11.1% |
| `INSERT … ON CONFLICT DO UPDATE` — the merge | **3820.2 ms** | **80.3%** |
|  ⤷ of which the dedup subquery (Sort → Unique → Seq Scan) | 461.5 ms | 9.7% |
|  ⤷ of which **the upsert itself** | **~3359 ms** | **~70.6%** |

The merge node verbatim, from `auto_explain` (`log_analyze`, `log_buffers`,
`log_wal`):

```
Insert on events_merged  (cost=177805.99..181258.12 rows=0 width=0)
                         (actual time=3820.180..3820.182 rows=0 loops=1)
  Conflict Resolution: UPDATE
  Conflict Arbiter Indexes: rdlt_ux_2e283ca583d39c5b
  Tuples Inserted: 0
  Conflicting Tuples: 1000000
  Buffers: shared hit=11048224 read=37157 dirtied=50133 written=62625,
           temp read=21229 written=21232
  WAL: records=4013669 fpi=18266 bytes=556468731
  ->  Subquery Scan on deduped  (actual time=205.918..461.452 rows=1000000)
        ->  Unique  (actual time=202.525..373.043 rows=1000000)
              ->  Sort  (actual time=202.522..305.737 rows=1000000)
                    Sort Key: id, __rdlt_arrival DESC
                    Sort Method: external merge  Disk: 169832kB
                    ->  Seq Scan on _rdlt_stage_…  (actual time=0.041..67.108)
```

**The dominant cost is not rdlt's to optimize.** 4,013,669 WAL records and
**556 bytes of WAL per row** — against a source row about 121 bytes wide — is
what an `ON CONFLICT DO UPDATE` costs: a new heap tuple version plus a new index
entry per row, all of it logged. 11.05M buffer hits for 1M rows is ~11 buffer
touches each. This was checked for a redundant-index cause and there is none:
`events_merged` carries exactly ONE index, the arbiter `rdlt_ux…(id)`, with the
heap at 351 MB against 43 MB of indexes.

**That is the number the rest of US11 must be judged against.** Roughly 71% of
this cell is Postgres doing MVCC and index maintenance. Every remaining lever in
the queue — allocator, COPY encoder fast path, canonical-JSON allocation — is
competing for the ~20% that is not the upsert and not the source read. Not a
reason to skip them; a denominator for reading their results, and it was
unknown before this measurement.

**One assumption in the code is falsified, and one change is measured and NOT
taken.** `UNIT_WORK_MEM` sets `SET LOCAL work_mem = '64MB'`, and its comment
said Postgres's 4 MB default "makes that spill to disk on any load worth
benchmarking" — implying 64 MB does not. It does: `external merge Disk:
169832kB` at the project's own flagship scale. The obvious fix was A/B'd on an
isolated stage-shaped probe of the same 1M rows:

| work_mem | sort method | execution |
|---|---|---|
| 64MB | external merge, Disk 50824kB | 259.1 ms |
| 128MB | quicksort, Memory 68535kB | 214.5 ms |
| 256MB | quicksort, Memory 165198kB | 234.0 ms |
| 512MB | quicksort, Memory 165198kB | 231.3 ms |

**Not taken.** The spill is worth at most ~45 ms against a 4756 ms cell — under
1% — and past 128 MB the number stops improving. Against that, the comment's own
stated reason for keeping the value modest still holds: this is a destination
that may be one of several against the same server, and work_mem is charged per
sort operation, not per transaction. Buying under 1% with a doubled per-sort
memory reservation on shared infrastructure is a bad trade.

**And the 45 ms is an upper bound that probably overstates the real gain**, which
is the kind of thing worth saying out loud: the isolated probe planned with two
parallel workers, while the real sort runs inside an `INSERT` and therefore
cannot go parallel. The measured delta comes from a plan shape the production
path does not have.

The false half of the comment is corrected in place — it now records that the
sort DOES spill at 1M rows, that this was measured, and what it was measured to
be worth — so the next reader does not re-run this experiment expecting a prize.

### D-34 (US11) — the allocator follow-up: its own stop condition fired, so it stops

The mimalloc/jemalloc question has been carried since 019 D-05 with a recorded
precondition (US4 + US6 landed) that is now met. T170 wrote the stop condition
BEFORE the measurement, which is the only way a negative result stays honest:
*if libc allocator symbols are under ~10% of cycles, record the negative and
stop; only if they still rank, run the A/B.*

`perf record -F 999 --call-graph=dwarf` on the release CLI over two cells,
summing every glibc allocator symbol (`malloc`, `cfree`, `_int_malloc`,
`_int_free_*`, `realloc`, `calloc`, `arena_get2`, tcache):

| cell | allocator share of cycles |
|---|---|
| pg-to-pg-1m | **3.29%** |
| pg-to-pg-dedup-1m | **3.41%** |

Both are a third of the threshold. **The A/B is not run.** Swapping allocators
cannot return more than the 3.3% the allocator costs in total, and it would add
a dependency, change the RSS profile, and put the memory edge over dlt — a
headline result — at risk for a prize bounded at three points. The question is
closed by measurement rather than left open for a third feature to rediscover.

**What the same profiles say about where the CPU actually is**, which is worth
more than the negative:

| symbol | pg-to-pg-1m | pg-to-pg-dedup-1m |
|---|---|---|
| `ColumnEncoder::encode_field` | 21.13% | 16.76% |
| `CopyDecoder::feed` | 11.32% | 15.53% |
| `__memmove_avx512_unaligned_erms` | 14.18% | 11.35% |
| `bytes::bytes_mut::shared_v_drop` | 8.32% | 6.55% |
| `PgSession::write_inner` | 4.30% | 7.57% |
| `copy_decode::uuid_text` | 6.18% | 2.72% |

The COPY encoder is the single largest rdlt-side symbol on both cells, which is
independent confirmation of 019's D-08 recording (41.6% of the encoder is bytes
plumbing) and makes T172 the best-supported candidate left in the queue. Note
also that this is CLIENT-side CPU: read against D-33, roughly 71% of the dedup
cell's WALL time is Postgres executing the upsert, so a win here is a win
against the remaining fifth.

### D-35 (US11) — the D-08 encoder fast path: TAKEN, on the instrument built for it

019 recorded a prize here (41.6% of the COPY encoder is bytes plumbing) and
declined it under PI3. With valgrind available, callgrind states the case
precisely rather than by estimate:

| | Ir | share |
|---|---|---|
| `bench_encode` (the arms, inlined) | 17,385,425 | 54.76% |
| `BytesMut::put_slice` | 7,893,440 | **24.86%** |
| `__memcpy_avx_unaligned_erms` | 3,747,995 | **11.80%** |
| `chrono::NaiveDate::from_num_days_from_ce_opt` | 1,240,053 | 3.91% |

**36.7% is framing, and the reason is structural.** `BufMut::put_i64` on a
`BytesMut` is *implemented via* `put_slice`, so an 8-byte integer pays a bounds
check, a chunk re-derivation and a `memcpy` dispatch. Every cell paid it at
least twice — once for a 4-byte length placeholder, once for the value — plus a
third `copy_from_slice` to backfill the placeholder once the length was known.
Six of the twelve wire types have a width fixed at compile time and were paying
all of it for nothing.

`fixed::<N>` writes the length and the value as one contiguous store, and four
arms (bool, int8, float8, uuid) now use it.

**Three measurements, deliberately in increasing order of realism:**

| instrument | baseline | fast path | delta |
|---|---|---|---|
| `pg_copy_encode_10k` (callgrind) | 31,751,299 Ir | 30,047,887 Ir | **−5.36%** |
| pg-to-pg-1m, whole-process instructions | 5.6688 G | 5.5562 G | **−1.98%** |
| pg-to-pg-1m, wall (hyperfine, 8 runs, interleaved) | 762.3 ± 13.8 ms | 760.1 ± 6.2 ms | **no resolvable change** |

The process-level count is the one that matters, and it is not a microbench: it
is the real 1M-row cell, reproducible to ±0.05% across three interleaved
repetitions of each arm. **The wall clock seeing nothing is the expected result,
not a contradiction** — a 2% CPU reduction on a load whose wall time is
dominated by the server (D-33: ~71% of the sibling merge cell is Postgres) sits
an order of magnitude below what a cell can resolve. `benches/iai_pg.rs` says
this in its own doc comment: instruction counts exist here precisely because
"the wall-clock cells cannot resolve a change this size against machine noise,
but callgrind can." Shipping on the designated instrument is the recorded
policy, not an exception to it.

**Byte-identity is pinned, not assumed** — 22/22 wire and round-trip tests
including the literal `pg_copy_values.hex` fixture, then 204/204 for the crate.

**The four remaining fixed-width arms are deliberately NOT taken**, and the
reason is at the site: Date, Time and both timestamps validate through chrono
and are written by `ToSql`. Converting them means re-deriving Postgres's epoch
arithmetic here, and with it the range rejections those arms perform on
purpose — a correctness surface traded for a fraction of two percent.

**Two unrelated baselines moved and are being re-recorded with this diff**,
which is said out loud rather than absorbed silently: `passthrough_10k`
607,197 → 617,325 (+1.67%) and `shred_nested_10k` 311,619,196 → 312,269,310
(+0.21%). Both are accumulated drift from this feature's own shred and value-
fidelity work, both were inside the 3% gate the whole time, and `--record`
rewrites the whole file. The gate's reference point for them is now the
post-020 tree.

### D-36 (US11) — what the WAL costs when nothing crashes: 8.5%, and the trade stands

The CLI cannot answer this question. Its spec path always resolves a workdir
(absent means `.rdlt`), so durable recovery is never actually off there;
`EngineConfig { workdir: None }` is reachable only by not calling
`PipelineBuilder::workdir`. A throwaway harness built on the facade ran the
identical pg-to-pg 1M-row pipeline with that one difference and nothing else,
four interleaved repetitions per arm, **every run rowcount-verified at
1,000,000** so an arm that silently moved nothing could not pass as fast:

| arm | runs (ms) | mean |
|---|---|---|
| workdir set (WAL on) | 741, 737, 734, 748 | **740.0 ms** |
| workdir unset (no WAL) | 676, 682, 692, 659 | **677.3 ms** |

**The WAL's residual cost is 62.8 ms — 8.5% of the load.** That is the real
price of durable recovery on the flagship cell, and it was previously a guess.

**No skip is taken, and 019's D2 is why.** The tempting move is to elide the WAL
automatically when every stream is Replace, on the reasoning that a Replace
target is rebuilt wholesale anyway. D2 rejected exactly that: without the WAL,
recovery is a full source re-extraction. Against a rate-limited API, a
paid-per-request source, or a slow export, re-reading everything costs far more
than 8.5% — and it costs it precisely when something has already gone wrong.
The number does not change that argument; it quantifies what the argument buys.

What the number does change is that the trade can now be stated to a user in
figures rather than adjectives: durability costs about 8.5% on a load of this
shape, and the alternative on failure is re-reading the source in full.

The harness is deleted with this entry, as its own doc comment said it would be
— it existed to produce one number, and the number is recorded here.

### D-37 (US11) — the nextval price was NOT small, and the fix is one DDL token

T178 was written expecting a not-taken disposition. The measurement says
otherwise, which is the entire reason the queue is phrased measure-first.

Two stage-shaped UNLOGGED tables, identical but for the `__rdlt_arrival`
BIGSERIAL, loaded with the same 1M rows by BINARY COPY — the format rdlt
actually sends — four interleaved repetitions each:

| table | runs (ms) | mean |
|---|---|---|
| with BIGSERIAL | 636.6, 644.2, 647.6, 639.8 | **642.0 ms** |
| without it | 428.5, 420.3, 432.3, 427.0 | **427.0 ms** |

**215 ms — a third of the stage COPY — is `nextval()`.** (A first pass using CSV
COPY produced a 280 ms delta against much larger absolute times; it was
discarded and re-run in BINARY, because client-side CSV parsing was dominating
numbers meant to price a server-side call.)

Then the cheapest possible fix was tried before any larger one: the sequence's
cache.

| CACHE | runs (ms) | mean |
|---|---|---|
| 1 (a plain BIGSERIAL) | 635.3, 645.4 | 640.3 ms |
| 32 | 517.7, 518.1 | **517.9 ms** |
| 1000 | 516.6, 517.1 | 516.9 ms |

122 ms of the 215 recovered, plateauing at 32 — 1000 buys nothing more.

**End-to-end on the real cell, and a lesson about run counts.** The first A/B
(6 runs) read 4.763 s vs 4.755 s — "no change" — with the cached arm carrying a
σ of 0.310 against the baseline's 0.037, one outlier run at 5.363 s. Re-run with
warmup and 12 runs:

| arm | mean | median | σ |
|---|---|---|---|
| BIGSERIAL (cache 1) | 4.769 s | 4778.0 ms | 0.055 |
| IDENTITY cache 32 | **4.612 s** | **4609.7 ms** | 0.050 |

**157 ms, 3.3% of the cell, at roughly 3σ.** Had the six-run result been trusted,
this would have been recorded as another negative — and it would have been
wrong. A high σ in ONE arm is a signal that the measurement is not ready, not
that the change does nothing.

**Taken.** The column is now
`bigint GENERATED BY DEFAULT AS IDENTITY (CACHE 32)`: one statement that creates
the sequence and sets its cache, replacing `BIGSERIAL`.

**What caching costs is contiguity, and nothing reads it.** The column orders
rows within one stage table, and `DISTINCT ON … ORDER BY id, arrival DESC` asks
only which value is larger. A session's block is still issued in increasing
order, and the engine opens destination sessions sequentially — the replay
session finishes before the run's own session opens — so no concurrent writer
can interleave blocks. Verified at the two `open` call sites rather than
assumed.

**Not a breaking change for an existing deployment.** `CREATE TABLE IF NOT
EXISTS` leaves an existing stage table alone, so it keeps its uncached sequence
and stays correct — merely as slow as before. Nothing migrates.

The golden pins are untouched, because they pin `commit_script`'s plan and this
DDL is emitted by `ensure_table`. 204/204 for the crate.

---

## Item ledger (AR8 — one disposition per item, none silent)

Seeded mechanically from the audit so no item can be dropped by omission.
**157 items**, each ending this feature as `fixed`, `rejected` with a reason, or
`deferred` with a named re-trigger.

| # | item | kind | story | disposition | evidence |
|---|---|---|---|---|---|
| 1 | Fix no-op ignore pattern in tools/interop/.gitignore | **bug**/low | US7 | OPEN | |
| 2 | Add a LICENSE file — repo declares Apache-2.0 but ships no license text | build | US9 | OPEN | |
| 3 | Add publish metadata (readme, keywords, categories) before the 0.2->0.3 window | build | US9 | OPEN | |
| 4 | Add a packaging/feature-matrix CI job ahead of publishing | build | US9 | OPEN | |
| 5 | CI never builds rustdoc; no missing_docs lint on the published surface | build | US9 | OPEN | |
| 6 | fuzz/Cargo.lock is stale: still records parquet as an rdlt-engine dependency after 019 US2 | build | US9 | OPEN | |
| 7 | Deep-tier suites memory_bound and spark_deep run in no CI schedule, and the Makefile header misdescribes TARGET=deep | build | US9 | OPEN | |
| 8 | make check hard-fails without hyperfine (cold-start prerequisite undocumented) | build | US9 | OPEN | |
| 9 | Extend the semver gate beyond rdlt-core/rdlt-connector before publishing 0.3 | build | US9 | OPEN | |
| 10 | Pin GitHub Actions to commit SHAs and stop compiling iai-callgrind-runner from source each run | build | US9 | OPEN | |
| 11 | Run the deterministic bars gate (make bench TARGET=gate) in CI | build | US9 | OPEN | |
| 12 | Remove unused arrow-schema dependency from rdlt-core (and its stale doc claim) | cleanup | US7 | OPEN | |
| 13 | Remove unused futures dependency from rdlt-engine | cleanup | US7 | OPEN | |
| 14 | Remove unused bytes and futures dependencies from rdlt-testkit | cleanup | US7 | OPEN | |
| 15 | Demote tokio to a dev-dependency of the rdlt facade | cleanup | US7 | OPEN | |
| 16 | Retire or archive the completed root working documents (PERF_ANALYSIS.md, REFACTORING.md, BENCH_REFINMENT.md) | cleanup | US7 | OPEN | |
| 17 | Deduplicate dev-dependencies that repeat regular dependencies | cleanup | US7 | OPEN | |
| 18 | Update stale CLAUDE.md: feature 019 is merged, not 'PLANNED, not yet implemented' | docs | US1 | OPEN | |
| 19 | Fix stale crates.io descriptions: rdlt-cli says 'TOML' (it parses YAML) and rdlt-connector-file says 'file source' (it is source+dest incl. CSV/S3) | docs | US1 | OPEN | |
| 20 | Makefile header omits the coverage verb, and the coverage recipe's scope contradicts its comment | docs | US1 | OPEN | |
| 21 | Track the duplicate reqwest 0.12/0.13 trees in the shipped CLI as a size lever | performance | US11 | OPEN | |
| 22 | Consider a fuzz target for WAL v2 Arrow-IPC segment replay | testing | US7 | OPEN | |
| 23 | history.jsonl lines drop the forced/quiet annotation, so forced medians enter Trends as evidence | **bug**/low | US7 | OPEN | |
| 24 | Give RdltError::Internal its own CLI exit code instead of falling into 2 (config) | **bug**/low | US7 | OPEN | |
| 25 | Pin the floating apache/polaris:latest image (017 D16 deferral still open) | build | US9 | OPEN | |
| 26 | Honor CARGO_TARGET_DIR when locating the release CLI in rdlt-bench | build | US9 | OPEN | |
| 27 | Bench-setup portability: unbounded pg_isready wait and hardcoded mise kubectl fallback | build | US9 | OPEN | |
| 28 | Prelude omits PipelineBuilder despite claiming crate-root parity | cleanup | US7 | OPEN | |
| 29 | Delete or justify unused bench schema surface: Cell::primary_fixture and the non-Wall Timing variants | cleanup | US7 | OPEN | |
| 30 | Unify PgFixture/CdcPgFixture API and deduplicate client()/seed() | cleanup | US7 | OPEN | |
| 31 | Update stale CLAUDE.md: feature 019 is implemented and merged, not "PLANNED" | docs | US1 | OPEN | |
| 32 | Correct benches/README.md artifact format_version (says 2, is 3) | docs | US1 | OPEN | |
| 33 | Fix stale rdlt-cli package description: pipeline specs are YAML, not TOML | docs | US1 | OPEN | |
| 34 | Record the rdlt build (git SHA) and fixture image versions in bench artifact fingerprints | feature | US7 | OPEN | |
| 35 | Re-export EventStream (and CancellationToken) at the rdlt facade root | feature | US7 | OPEN | |
| 36 | Add rdlt --version (and consider a check/validate subcommand) to the CLI | feature | US7 | OPEN | |
| 37 | Decide and document whether the connector SPI is reachable through the facade | feature | US7 | OPEN | |
| 38 | Fix the dedup-cell hand-mirror hazard in DestSpec::File by embedding a deserializable config (iceberg precedent) | refactoring | US10 | OPEN | |
| 39 | Implement the recorded container reaper/labeling convention (testkit fixtures leak on fail-fast) | testing | US7 | OPEN | |
| 40 | Fix keys_of_table's rfind tail-splitting: a partition value equal to the table name corrupts S3 ownership listing | **bug**/medium | US3 | OPEN | |
| 41 | Grown parquet rewrite resumes from a stale row-group offset undetected | **bug**/medium | US3 | OPEN | |
| 42 | Replace truncation keeps stale parts after a format or partition_by config change | **bug**/medium | US3 | OPEN | |
| 43 | Inferred-Bool CSV cells silently coerce to false in pass 2 instead of the typed two-pass error | **bug**/low | US3 | OPEN | |
| 44 | is_recoverable classifies deterministic object_store failures as transient | **bug**/low | US3 | OPEN | |
| 45 | normalize_ident violates its max_len contract when max_len < 9 | **bug**/low | US3 | OPEN | |
| 46 | Temp fetch directories leak when planning or staging fails | cleanup | US3 | OPEN | |
| 47 | Per-file cursor entries accumulate forever for rotated-out files | cleanup | US3 | OPEN | |
| 48 | Commit log grows without bound and is fully rewritten on every commit | cleanup | US3 | OPEN | |
| 49 | resolve_files reports an existing directory as "does not exist" | cleanup | US3 | OPEN | |
| 50 | partition_by doc claims Hive-style `<column>=<value>` dirs; the code writes bare `<value>` | docs | US1 | OPEN | |
| 51 | Stale/inconsistent source-config comments and silently-ignored knobs (primary_key, validate, type_hints) | docs | US1 | OPEN | |
| 52 | Fix S3-parquet up-front fetch: complete, unchanged objects re-download every run | performance | US3 | OPEN | |
| 53 | Fold the engine's byte-budget channel into the SPI's (deferred D17 — its trigger has fired) | refactoring | US3 | OPEN | |
| 54 | Pin DestSpec::File mirror parity with FileDestConfig by test | testing | US3 | OPEN | |
| 55 | Add request timeouts to the REST source's reqwest clients | **bug**/medium | US4 | OPEN | |
| 56 | Reject pagination params silently dropped for POST streams with non-object bodies | **bug**/medium | US4 | OPEN | |
| 57 | Retry-After HTTP-date form is silently ignored | **bug**/low | US4 | OPEN | |
| 58 | on_unauthorized drops a concurrently refreshed OAuth2 token, causing redundant token fetches under fan-out | **bug**/low | US4 | OPEN | |
| 59 | Parent placeholder values are substituted into URLs without percent-encoding | **bug**/low | US4 | OPEN | |
| 60 | reconcile() compares struct field types INCLUDING nested field IDs — catalog-normalized IDs can trigger spurious 'contradictory drift' | **bug**/medium | US6 | OPEN | |
| 61 | Extend the credential-header blocklist beyond authorization/x-api-key | cleanup | US4 | OPEN | |
| 62 | Reconcile ignores nullability drift; required-vs-nullable mismatch surfaces late as an align error | cleanup | US6 | OPEN | |
| 63 | Document the snapshot-retention constraint on iceberg replay detection | docs | US1 | OPEN | |
| 64 | Schedule the recorded phase-2: Glue/SigV4 catalog support probe | feature | US6 | OPEN | |
| 65 | Schedule the recorded phase-2: re-probe Replace/overwrite on iceberg-rust upgrade | feature | US6 | OPEN | |
| 66 | Hash the POST body template once per sequence instead of Debug-rendering it per page | performance | US4 | OPEN | |
| 67 | No live test covers struct/list columns against the catalog despite structs:true/scalar_lists:true capabilities | testing | US6 | OPEN | |
| 68 | Use the Decimal128 array's own scale in column_wire, not the ensured schema's | **bug**/low | US7 | OPEN | |
| 69 | Reject out-of-range Time64 values instead of silently wrapping them | **bug**/low | US7 | OPEN | |
| 70 | Route duckdb ensure/commit probe errors through classify, not unconditional fatal | **bug**/low | US7 | OPEN | |
| 71 | Execute the deferred T0xx tag sweep in duckdb tests | cleanup | US7 | OPEN | |
| 72 | Drop the underscore from the used _ctx parameter in Postgres::open | cleanup | US7 | OPEN | |
| 73 | Decide and record a retention story for _rdlt_cleared and _rdlt_commits growth | cleanup | US7 | OPEN | |
| 74 | Fix stale module doc claiming an ignored, unfixed defect in direct_publish_guarantees | docs | US1 | OPEN | |
| 75 | Route flagged_roots through the dialect dedup seam instead of hardcoding DISTINCT ON | refactoring | US10 | OPEN | |
| 76 | Move create_index_sql and the duplicate-merge-key diagnosis into sqlcore | refactoring | US10 | OPEN | |
| 77 | Extract the shared ensure_table merge choreography into a sqlcore plan | refactoring | US10 | OPEN | |
| 78 | Tracing span guards held across await points in stream_task and Loader::process | **bug**/medium | US7 | OPEN | |
| 79 | Repeated crash-before-first-checkpoint leaks manifest growth and orphaned segment files | **bug**/low | US7 | OPEN | |
| 80 | Lowering misses decimals nested inside structs and scalar-list items (latent SPI hole) | **bug**/low | US7 | OPEN | |
| 81 | Use WalRecord::Segment.rows as a replay integrity check — today it is write-only | cleanup | US7 | OPEN | |
| 82 | new_load_id's uniqueness comment overclaims; epoch-fallback + pid reuse can collide across restarts | cleanup | US7 | OPEN | |
| 83 | State the WAL-before-validation invariant for merge-key NULL checks (replay has no such check) | docs | US1 | OPEN | |
| 84 | Answer to D18: blocking fs/encode on the executor — encode resolved with evidence, recovery path and small residuals remain | performance | US11 | OPEN | |
| 85 | Unify engine ByteTx/ByteRx with SPI RecordsOut/RecordsIn (recorded deferral D17) — they have already diverged | refactoring | US10 | OPEN | |
| 86 | Deduplicate the shred/passthrough forward blocks and unify send-failure handling in stream_task | refactoring | US10 | OPEN | |
| 87 | Fix silent NULLing of u64 values above i64::MAX on the shred path | **bug**/high | US2 | OPEN | |
| 88 | Enforce (or document) schema policies across run boundaries — Freeze only works within one run | **bug**/high | US5 | OPEN | |
| 89 | Stop type-hint pins from being silently overridden by object/array values | **bug**/medium | US2 | OPEN | |
| 90 | Make parse_decimal respect precision — out-of-range Decimal128 values flow downstream | **bug**/medium | US2 | OPEN | |
| 91 | Decide and count hinted-column value misfits instead of silent NULLs | **bug**/medium | US2 | OPEN | |
| 92 | Close the Freeze bypass for new child tables appearing mid-run | **bug**/medium | US5 | OPEN | |
| 93 | Lower decimals nested inside preserved structs and scalar lists | **bug**/low | US2 | OPEN | |
| 94 | Validate embedder-supplied type hints — invalid Decimal precision panics in build | **bug**/low | US2 | OPEN | |
| 95 | Reclassify internal-invariant failures from Config to Internal | cleanup | US2 | OPEN | |
| 96 | Correct the json_type capability contract or implement it in lowering | docs | US1 | OPEN | |
| 97 | Mechanize the lower_column/flatten_array parity — the deferred duplication has drifted (decimal nullability) | refactoring | US2 | OPEN | |
| 98 | Add the missing decimal edge-case tests for build and lowering | testing | US2 | OPEN | |
| 99 | Document the measured 8.43x concurrent-pipeline scaling — the close-out's own instruction, still undone | docs | US1 | OPEN | |
| 100 | Fix CLAUDE.md's stale claim that feature 019 is 'PLANNED, not yet implemented' | docs | US1 | OPEN | |
| 101 | Document primary_key declaration as the free JSONL performance lever it was measured to be | docs | US1 | OPEN | |
| 102 | Run the owed EXPLAIN (ANALYZE, BUFFERS) on the merge arm — the largest single number left in the matrix | performance | US11 | OPEN | |
| 103 | Measure the WAL's residual cost post-019, then take the all-Replace skip and a spec-level opt-out if it pays | performance | US11 | OPEN | |
| 104 | Re-attribute blocked time on the post-019 pipeline before any further serial-path work | performance | US11 | OPEN | |
| 105 | Prototype the D-08 fixed-width field fast path — 41.6% of the COPY encoder is bytes plumbing, ~20% prize recorded | performance | US11 | OPEN | |
| 106 | The mimalloc/jemalloc follow-up recorded in D-05 is now due — its precondition (US4+US6 landed) is met | performance | US11 | OPEN | |
| 107 | Heap-profile the parquet destination's whole-part Vec<u8> buffering — the identified but unfixed RSS peak | performance | US11 | OPEN | |
| 108 | Run the never-run network-latency (tc netem) experiment, then coalesce the unit preamble's serial round trips if it pays | performance | US11 | OPEN | |
| 109 | Probe the canonical-JSON per-object allocations and key escaping — with the D-13/D-21 reversal pattern as the explicit null hypothesis | performance | US11 | OPEN | |
| 110 | Price the merge stage's 1M nextval() calls before believing __rdlt_arrival is free | performance | US11 | OPEN | |
| 111 | Micro-gate the partitioned-write path's per-row String rendering before anyone ships partition_by at scale | performance | US11 | OPEN | |
| 112 | Schedule the second recorded session: grant/deny the pg-to-s3parquet bar and tighten the flagship's ±20% spread | testing | US7 | OPEN | |
| 113 | Log the tokio-postgres connection driver's terminal error instead of discarding it | cleanup | US7 | OPEN | |
| 114 | Surface (or at least log) dropped events when EventStream lags | cleanup | US7 | OPEN | |
| 115 | Stop classifying report-write I/O failures as Usage errors in the CLI | cleanup | US7 | OPEN | |
| 116 | Distinguish corrupt report.json from absent in the bench runner | cleanup | US7 | OPEN | |
| 117 | Bench container poll conflates a failed inspect with container exit | cleanup | US7 | OPEN | |
| 118 | CLI swallows a panic from the event-feed task | cleanup | US7 | OPEN | |
| 119 | Repair CI: all four jobs fail in 3-5s with zero steps; owner-deferred in 019 D-01 and still open | build | US9 | OPEN | |
| 120 | Strip T0xx/SMx history tags from duckdb tests — the 017 deferral to increment 6 was never executed | cleanup | US1 | OPEN | |
| 121 | 017's eight verified-but-cut review residuals remain recorded and unscheduled | cleanup | US1 | OPEN | |
| 122 | 017 lowering-rule duplication deferred-in-place with a named re-trigger (third site) | cleanup | US1 | OPEN | |
| 123 | Update CLAUDE.md: feature 019 is merged, not "PLANNED, not yet implemented" | docs | US1 | OPEN | |
| 124 | Finalize 019 status lines: close-out says IN PROGRESS and contradicts itself; spec.md still Draft | docs | US1 | OPEN | |
| 125 | Dispose FR-016: the offload requirement was re-scoped to US9, and US9 was not built | docs | US1 | OPEN | |
| 126 | Give PERF_ANALYSIS.md the executed-disposition banner; three of its claims are now recorded false | docs | US1 | OPEN | |
| 127 | Fix the BENCH_REFINMENT.md filename typo | docs | US1 | OPEN | |
| 128 | bars.toml header still claims the dedup cell "carries NO bar" while the file defines one | docs | US1 | OPEN | |
| 129 | The 0.2→0.3 semver window: verified empty of queued API work; only the standing publish-time bump remains | docs | US1 | OPEN | |
| 130 | Lakehouse phase-2 cluster: 016's recorded doors and 018's deferred Iceberg 3-way cell share one re-trigger | feature | US1 | OPEN | |
| 131 | D18 open, and its trigger fired: file-dest still blocks the executor and buffers whole encoded files; 019 never dispositioned it | performance | US11 | OPEN | |
| 132 | Take or close the mimalloc/jemalloc follow-up — its recorded precondition (US4+US6 landed) is now met | performance | US11 | OPEN | |
| 133 | US2's unexplained ~4-point wall gap vs PERF_ANALYSIS's -18.3% is a recorded open question | performance | US11 | OPEN | |
| 134 | D-08 recorded prize: 41.6% of the COPY encoder is bytes plumbing (~5M instructions), declined under PI3 | performance | US11 | OPEN | |
| 135 | DuckDB full loads still write every row twice — deferral recorded only in a code comment, absent from 019's close-out | performance | US11 | OPEN | |
| 136 | D17 open: SPI and engine still ship duplicate byte-budget channel implementations | refactoring | US10 | OPEN | |
| 137 | D19 open and its trigger fired: config-plumbing trio still triplicated after US7's config change | refactoring | US10 | OPEN | |
| 138 | Pin the Polaris test image — the 017 D16 deferral ("a later increment") never happened | testing | US1 | OPEN | |
| 139 | Build the testkit container reaper/labeling convention recorded as follow-up in 017 D16 | testing | US1 | OPEN | |
| 140 | Crash sweep cannot reach the server-committed/client-unlearned state that produced D-23's real defect | testing | US1 | OPEN | |
| 141 | Queued operator work: a second recorded session to tighten two bars and decide the unbarred parquet cell | testing | US1 | OPEN | |
| 142 | Container-backed test flakiness under parallel load is a recorded, unowned gate weakness | testing | US1 | OPEN | |
| 143 | Record the equivalent/untestable residuals so future triage doesn't re-litigate them | docs | US1 | OPEN | |
| 144 | Re-run cargo-mutants: the committed run predates features 006-019 entirely | testing | US8 | OPEN | |
| 145 | Pin LoadItem::byte_size — the backpressure input has zero coverage and a closure comment falsely claims otherwise | testing | US8 | OPEN | |
| 146 | Pin WAL segment sequencing — the test named for it never asserts it | testing | US8 | OPEN | |
| 147 | Pin the EverySeconds commit-policy boundary (the one policy_triggers residual) | testing | US8 | OPEN | |
| 148 | Pin lower_batch under MIXED capabilities — each guard is only tested in isolation | testing | US8 | OPEN | |
| 149 | Pin lowered-field nullability rules in lower_batch | testing | US8 | OPEN | |
| 150 | Pin render_decimal boundary values: zero, and a length where minus differs from divide | testing | US8 | OPEN | |
| 151 | Assert ContractViolation from/to fields — scalar_of is entirely unpinned | testing | US8 | OPEN | |
| 152 | SchemaPolicy::freeze() is unused by the entire workspace and untested | testing | US8 | OPEN | |
| 153 | Make the saw_cancelled precedence arm deterministically testable | testing | US8 | OPEN | |
| 154 | Pin that a clean run removes the WAL directory | testing | US8 | OPEN | |
| 155 | Convert the 7 timeout-kills into fast assertion-kills | testing | US8 | OPEN | |
| 156 | Add a spec-parse pin for the hand-maintained File destination mirror (ParquetOptions path) | testing | US8 | OPEN | |
| 157 | Consider adding rdlt-connector-sqlcore to the mutation scope | testing | US8 | OPEN | |


### Recorded non-goals (AR2) — 18 refuted claims

| # | claim | refutation (NEXT_STEPS.md Appendix A) |
|---|---|---|
| 1 | Stop swallowing RunReport read/parse errors in the bench runner | The cited code exists exactly as claimed (runner.rs:254-258 collapses read/parse failures to None; runner.rs:350-355 reports "no RunReport captured"), but no concrete input/state on main can… |
| 2 | Staged part names can collide across (table, partition) pairs within one load | The collision requires a table name containing '-', which is unreachable through any production path. Every TableName the file destination sees is produced by normalize_ident (rdlt-core/src/… |
| 3 | OffsetLimit short-page heuristic overrides a declared total_count stop (silent data loss when the server clamps limit) | The claim fails on its own evidence. Both the short-page check (paginate.rs:223-224) and the total-count check (paginate.rs:226-227) return PageDecision::Done; total_count_reached (paginate.… |
| 4 | NextUrl/LinkHeader pagination follows cross-origin URLs with full credentials attached | The mechanical facts in the claim are accurate: driver.rs:248-256 accepts any absolute http(s):// next-URL with no origin check, fetch_page uses it verbatim (driver.rs:112), client/mod.rs:98… |
| 5 | PageDecision::NextUrl doc contradicts the code: relative next-URLs join base_url, not the current URL; protocol-relative URLs break | The line citations are accurate (paginate.rs:32 says "relative to the current URL"; driver.rs:248-253 joins onto config.base_url via trim/concat join_base_url at driver.rs:265-271), but the … |
| 6 | Guard credential-bearing keys in the iceberg catalog.props escape hatch | Every cited mechanic checks out (props is a plain-string verbatim passthrough at config.rs:102-104; catalog.rs:74-76 inserts it after the Secret reveal() entries so credential keys there wou… |
| 7 | Vended-credential expiry vs 401/403-fatal classification: code can contradict the recorded 'expiry = transient' posture | Refuted by tracing the actual error chain. The 401/403-fatal arm (errors.rs:34-38) fires only when status_from_context (errors.rs:89-97) decodes a `status:` context entry. The only code in t… |
| 8 | Clear Replace targets on the direct path even when the load delivers zero rows | The claimed cross-destination divergence is unreachable. The claim assumes the staged path clears a zero-row Replace table "unconditionally", but commit_script only iterates the session's re… |
| 9 | Route shred/passthrough errors through stream_task's close+join tail instead of `?` | The control-flow observation is accurate: the `?` exits at run.rs:535/537/562/564 do bypass the `input.close()` at :590 and the reader join at :592, and the comment at :585-590 overstates it… |
| 10 | Stream-task failure is not observed until the load channel fully drains | The factual mechanics are 100% confirmed on main: the drain loop (run.rs:619-637) selects only on load_rx.recv() and cancel; a failing stream_task merely returns Err and drops its ByteTx (ru… |
| 11 | WAL durability barrier omits directory fsync — commit-protocol comment overstates what is durable | The factual observation is accurate: no directory fsync exists anywhere in wal/ (verified by grep), sync_for_commit syncs only segment files (mod.rs:206-216) and the manifest handle (mod.rs:… |
| 12 | Decimal-to-Utf8 batch lowering hardcodes nullable=true, diverging from schema lowering | The textual divergence is real: lowering.rs:135-139 hardcodes nullable=true in lower_batch's Decimal128 arm while lower_schema (line 65) and the non-decimal batch arm (line 147) compute null… |
| 13 | Discarded items after the last commit never reach the destination's commit counters | The code observation is accurate: the Discarded arm (load/mod.rs:214-230) updates report and counters but never sets self.dirty (unlike lines 163/202/208), finish() gates on dirty||commit_se… |
| 14 | Align passthrough DiscardRow semantics and discard accounting with the shred path | The code-level facts are all correct (both Discard actions project the column on AddColumn; count is rows×dropped_columns including NULL cells; shred path drops rows/skips nulls; no passthro… |
| 15 | Replace the negative-scale clamp in lower_batch with a typed error | The code facts are accurately described: lowering.rs:130 does clamp `(*scale).max(0) as u8`, and render_decimal (lowering.rs:173-183) would render raw 5 at scale -2 as "5" instead of "500" I… |
| 16 | Commit trailing discard counters — LoadItem::Discarded never sets dirty | Mechanics verified accurate: load/mod.rs:214-230 Discarded arm sets no dirty flag (contrast lines 163/202/208), finish() at :249 skips the final commit when dirty=false && commit_seq>0, coun… |
| 17 | Distinguish manifest-open I/O errors from absence in WAL resume scan | The cited code exists (resume.rs:68-71 maps every File::open error to Scan::Nothing; run.rs:317 does nothing for Nothing), but the claimed defect — silent skip of crash recovery with zero si… |
| 18 | Do not let a swallowed mtime error disarm the same-size rewrite tripwire | Mechanics verified accurate (source/mod.rs:88-92 maps modified()/duration_since failure to None; cursor.rs:59 mtime arm needs (Some,Some); cursor.rs:124 skips complete files; tail-hash only … |

