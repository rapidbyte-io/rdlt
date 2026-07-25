# Implementation Plan: Performance Improvements — Measured Wins and the Serial-Path Ceiling

**Branch**: `019-performance-improvements` | **Date**: 2026-07-25 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/019-performance-improvements/spec.md`

## Summary

Execute the eight findings of `PERF_ANALYSIS.md` as nine independently
mergeable increments, each landing with a recorded before/after measurement.
The work splits into three kinds and they must not be confused with each other:

1. **A corrected claim, not an optimization.** The keep-in-sync benchmark cell
   delivers three source streams where it declares one, so it measures rdlt
   moving 3M rows against a competitor moving 1M. Correcting it turns the
   matrix's only recorded loss (0.85×) into a 2.6× win with byte-identical
   output — no engine change at all.
2. **Serial-path work, which converts to wall-clock.** The recovery log encodes
   every staged batch as Parquet on the loader's critical path (23.9% of
   processor time, measured −18%/−21% wall when replaced); the relational
   destination allocates a `Box` per cell (~12M per load) and frames its wire
   stream in 4 KiB units (113,552 blocking waits); full-refresh publishes copy
   every row into the target a second time (710 ms of a 1.97 s cell).
3. **Off-critical-path work, which buys headroom but not wall-clock yet.** The
   build profile (untuned: no LTO, 16 codegen units, −14% processor time
   available), the shred path's identity hashing and column-major batch
   assembly, and the per-row scratch allocations. `PERF_ANALYSIS.md` shows
   these do not move the clock while the pipeline spends 30–70% of its life
   blocked — they are taken because they compound once the ninth increment
   removes the blocking.

The ninth increment is the serial-path ceiling itself: every pipeline runs at
under one core on a 32-core machine, and eight concurrent pipelines reach 4.2×
the throughput of one. Spec decision D5 puts it fully in scope including a
breaking change to the destination session interface — which, because CI's
release-compatibility gate is blocking on `rdlt-connector`, means opening the
recorded 0.2 → 0.3 version window in the same change or not breaking at all.

**The owner directive for this planning round — do not hand-write what a crate
already does — is binding and is encoded as contract clause PI3.** It changed
the shape of the largest code increment: `PERF_ANALYSIS.md` proposed writing
the Postgres binary-COPY wire format by hand, and plan-time registry facts show
that is unnecessary. Three of the feature's headline changes turn out to cost
**zero new dependency-tree entries**, because what they need is already
present:

| change | what it needs | already in the tree? |
|---|---|---|
| Recovery-log segment format | `arrow::ipc` | **yes** — `arrow`'s default features include `ipc`; `arrow-ipc 58.3.0` is in `Cargo.lock` |
| Binary-COPY value encoding | `postgres_protocol::types::*_to_sql`, or `postgres_types::ToSql::to_sql` | **yes** — `postgres-protocol 0.6.11` is in the tree; `ToSql` is already imported by the crate |
| Default output compression `snappy` | parquet's `snap` codec | **yes** — parquet's default features include `snap`; `snap 1.1.2` is in `Cargo.lock` |

Only the *tuple framing* and *buffer sizing* of the COPY path remain ours, and
only because `BinaryCopyInWriter` hard-codes a 4096-byte flush — that is
protocol framing, not an algorithm, which PI3 admits explicitly. The one
hand-written algorithm this feature RETAINS is the `numeric` wire encoder,
justified by fact: `postgres-protocol` has no `numeric_to_sql`, and the
obvious crate substitute (`rust_decimal`, 96-bit mantissa) cannot represent the
38-digit `Decimal128` values the engine carries — adopting it would lose
precision.

Phases follow the spec's story order (P1 → P2 → P3), each mergeable with the
full gate green.

## Technical Context

**Language/Version**: Rust 1.96.0, edition 2024, pinned by `rust-toolchain.toml`;
MSRV floor `rust-version = "1.96"`. Workspace denies `unsafe_code` (sole
sanctioned exception: the CLI `mallopt` FFI, which increment 3 may be able to
delete outright).

**Primary Dependencies**: no new tree entries required for the three headline
changes (table above). Candidate additions, each already present in
`Cargo.lock` and each requiring a Principle I justification recorded in
`research.md`: `postgres-protocol = "0.6"` as a *direct* dependency of
`rdlt-connector-postgres` (currently transitive via `tokio-postgres`, not
re-exported by it — verified in both `lib.rs` files), and `uuid = "1"` for that
same crate (in the lock at 1.24.0 via `iceberg`→`apache-avro`, but not in the
postgres crate's tree; reachable instead through `tokio-postgres`'s
`with-uuid-1` feature). **No hashing dependency is proposed**: identity hashing
is `blake3` (already direct) and frozen by D6; the only new lookup is a
child-table memo whose key space is a handful of tables per stream, where a
`std` map is ample.

**Storage**: one authorised persisted-format change — the recovery-log segment
format and its `WAL_FORMAT_VERSION`, with refuse-and-degrade migration
(FR-014). Benchmark artifacts may need a `format_version` increment to carry
bytes-written per arm (FR-035). Everything else is frozen: `_rdlt_id` values,
binary wire bytes, golden statement text, `StateDoc`, receipts.

**Testing**: `cargo nextest run` (doc-tests `cargo test --doc`);
`make test TARGET=sweep` for the crash-point suite over 14 recorded points, ten
of which this feature touches; `crates/rdlt-connector-postgres/tests/golden_sql.rs`
for statement text; identity property tests for `_rdlt_id`; `make bench
TARGET=iai` for instruction counts against `benches/perf-baselines.json` at a
**3% tolerance**; `benches/check-cold-start.sh` at **≤ 40 ms**. `make check` =
lint + test + sweep + iai + cold start. The bars gate (`make bench
TARGET=gate`) is deliberately outside `make check`.

**Target Platform**: Linux. Measurements are taken on the recorded workstation
(32 cores, 62 GB) under the harness's quiet guard; the library is embeddable
and must stay so.

**Project Type**: Rust workspace — embeddable library crates behind the `rdlt`
facade, a thin CLI, and a dev-only benchmark harness (`rdlt-bench`,
`publish = false`).

**Performance Goals**: spec SC-001…SC-012. Headline floors: 1M-row relational
copy and relational-to-lake extract each ≥ 25% faster than the baseline of
record; peak memory on those two ≥ 15% lower; a single pipeline above one core
of utilisation and ≥ 50% more rows/s; nested-document processor time ≥ 10%
lower with byte-identical identities; no cell regresses.

**Constraints**: full gate green at every increment; cold start ≤ 40 ms
throughout; no `unsafe`; **blocking** `cargo semver-checks` gate on
`rdlt-core` and `rdlt-connector` against `main` (no `continue-on-error`), which
is what forces the version-window decision to be made at design time rather
than discovered in CI; instruction-count baselines re-recorded deliberately
when a known change shifts them, never absorbed by widening the tolerance.

**Scale/Scope**: 13 crates, ~54k LOC. Touched: `rdlt-engine` (WAL, shred,
runtime, loader), `rdlt-connector-postgres` (destination encode + publish,
source decode, config), `rdlt-connector-sqlcore` (publish plan),
`rdlt-connector-file` and `rdlt-connector-iceberg` (writer properties),
`rdlt-connector` (the session interface, if increment 9 breaks it),
`rdlt-cli` (build/allocator policy), `rdlt-bench` + `benches/` (cell
correction, artifact fields, bars, results). **8 implementors of
`LoadSession`** must move together if that interface changes: four bundled
destinations, the testkit's memory and crash destinations, and two test
implementors.

## Constitution Check

*Gate evaluated against constitution v1.1.0 pre-Phase-0; re-checked
post-Phase-1. Two Principle IX exercises are deliberate and tracked in
Complexity Tracking rather than treated as violations.*

| # | Principle | Verdict | Notes |
|---|---|---|---|
| I | Small Core, Verified Breadth | **PASS (with a standing gate)** | No new capability; this feature makes existing capability cheaper. Surface grows in exactly one place — output-format settings (FR-032), which closes a gap where users currently cannot ask for compressed output at all. New dependencies are gated by PI3: the three headline changes cost zero tree entries, and the two candidates are already in `Cargo.lock`. Each still needs its recorded justification. |
| II | Library-First, Thin CLI | **PASS** | Every optimization is in library crates. The CLI-only items are policy, not capability: the distribution build profile (FR-037) and the allocator tuning (FR-038). No capability becomes CLI-reachable-only. |
| III | One-Boundary Wrapping | **AT RISK — gated in Phase 1** | Increment 4 may take `postgres-protocol` as a direct dependency and `uuid` as a value type. Neither may cross `rdlt-connector-postgres`'s public surface; both must stay inside the `dest::encode` module where `tokio-postgres` types already live. **Note a pre-existing exception**: `tokio_postgres::Config` is public today as `rdlt_connector_postgres::tls::ParsedConn::pg` (`src/tls/connstring.rs:15`, re-exported at `tls/mod.rs:22`, module public at `lib.rs:25`), so the boundary is not currently pristine. This feature must not widen it; whether the existing leak is corrected here is out of scope (it is a 0.2 → 0.3 candidate if the window opens for increment 9 anyway). Phase 1 re-checks the crate's public API for new leakage before increment 4 is written. |
| IV | Exactly-Once Is Sacred | **PASS via PI5** | Increments 2, 5 and 9 touch recovery, publish atomicity and concurrency — the highest-risk cluster in the feature. Every one runs the crash sweep over the ten in-scope points with duplicate-free verification; increments that change what a point *means* update its comment; new failure windows get new points. |
| V | Typed Error Taxonomy | **PASS** | New rejections are configuration-time and typed: no-tables-and-no-queries (FR-011), contradictory output-format settings (FR-034), unrepresentable wire values naming the column (FR-021). Existing SQLSTATE-based classification is untouched. No clause IDs in any message. |
| VI | Self-Contained Code & Comments | **PASS** | No new `unsafe`. Increment 3 may *remove* the one sanctioned `unsafe` block if an off-the-shelf allocator proves better on both axes — a strict improvement. Comments that describe measured trade-offs (the allocator comment, currently contradicted by measurement) are corrected to state the rule and the number, with no spec references. |
| VII | Test-and-Verification Gate | **PASS** | Full gate at each of the nine merges; container-backed tests keep skip-not-fail; ≥ 80% coverage measured baseline-first at close-out; verification matrix with zero uncited claims. |
| VIII | Benchmark Governance | **PASS, and exercised** | Increment 1 corrects a cell and re-derives its bar measurement-first from a recorded session floor with a policy-log entry — exactly the machinery this principle exists for. Post-improvement bars for other cells follow the same rule: at most one per cell, below a recorded floor, each with an entry. |
| IX | Contracts and Persisted Formats Are Frozen | **DELIBERATE EXERCISE ×2 — see Complexity Tracking** | (a) The recovery-log segment format version is bumped under spec decision D3, with refuse-and-degrade migration. (b) Increment 9 may break the destination session interface, which under 0.x semver means opening the recorded 0.2 → 0.3 window. Both go through the recorded procedure; neither is silent. |

**Gate result**: PASS to proceed to Phase 0, with two tracked Principle IX
exercises and one Principle III risk to be closed in Phase 1.

## Project Structure

### Documentation (this feature)

```text
specs/019-performance-improvements/
├── plan.md              # This file
├── spec.md              # The specification (decisions D1–D6, FR-001…FR-045, SC-001…SC-012)
├── research.md          # Phase 0 output — the resolved design decisions
├── data-model.md        # Phase 1 output — formats, config vocabulary, interface shapes
├── quickstart.md        # Phase 1 output — how to take a measurement that counts
├── contracts/
│   └── performance-improvements.md   # Clauses PI1–PI8
├── checklists/
│   └── requirements.md  # Spec quality validation
└── tasks.md             # Phase 2 output (/speckit-tasks — NOT created here)
```

### Source Code (repository root)

```text
crates/
├── rdlt-core/                     # identity (frozen values), commit policy
│   └── src/identity.rs            #   RowIdBuilder — hashing stays blake3 (D6)
├── rdlt-connector/                # THE SPI — LoadSession; semver-gated against main
│   └── src/lib.rs                 #   write(&mut self) — increment 9 may break this
├── rdlt-engine/
│   ├── src/wal/{mod.rs,resume.rs} #   increment 2: segment format + version + off-thread write
│   ├── src/shred/                 #   increment 6: identity, canon, build_batch, memoization
│   ├── src/load/mod.rs            #   increments 2 + 9: the loader's serial path
│   └── src/runtime/{run.rs,channel.rs}  # increment 9: stage overlap
├── rdlt-connector-postgres/
│   ├── src/dest/{commit.rs,encode.rs}   # increments 4 + 5: encoder, framing, publish
│   ├── src/source/{copy_decode.rs,config.rs}  # increments 1 + 8: discovery scope, scratch reuse
│   └── tests/golden_sql.rs        #   re-pinned wherever statements change
├── rdlt-connector-sqlcore/
│   └── src/plan/                  #   increment 5: the publish plan; increment 8: dedup reuse
├── rdlt-connector-file/
│   └── src/dest/session.rs        #   increment 7: writer properties, snappy default
├── rdlt-connector-iceberg/
│   └── src/dest/writer.rs         #   increment 7: same vocabulary
├── rdlt-cli/
│   └── src/main.rs                #   increment 3: allocator policy (and possibly its deletion)
└── rdlt-bench/                    #   increments 1 + 7: stream-set validation, artifact fields

benches/
├── cells/{e2e.toml,pipelines/}    # increment 1: the corrected cell
├── bars.toml, RESULTS.md          # increments 1 + close-out: bars re-derived, policy log
├── perf-baselines.json            # increment 3: re-recorded under the new build profile
└── check-cold-start.sh            # guard, must stay green throughout

Cargo.toml                         # increment 3: [profile.release] + [profile.dist]
```

**Structure Decision**: no new crates and no restructuring. This feature edits
hot paths in place and deletes what it replaces (PI2). The only structural
question is whether the shared output-format vocabulary (increment 7) lives in
`rdlt-connector-file` and is re-used by `rdlt-connector-iceberg`, or is lifted
to a shared home — resolved in Phase 0 against the feature-016 precedent that
config *vocabulary* is shared while plumbing is not.

## Phase Sequencing

Increments map 1:1 onto the spec's user stories and merge in that order. The
ordering is not arbitrary — three dependencies are real:

- **Increment 1 first.** Every later measurement is compared against the
  matrix; the matrix must be telling the truth before it is a comparator.
- **Increments 4 and 5 measured after increment 2.** All three sit on the same
  serial path; measured out of order their effects confound.
- **Increment 9 designed after 2, 4 and 5.** Those shorten the serial path it
  must parallelise, so its design target comes from the post-improvement
  baseline. FR-039 additionally requires re-measuring the ceiling against a
  destination that does not itself saturate, because `PERF_ANALYSIS.md` §7
  flags that the 1.5M rows/s figure may belong to the benchmark's Postgres
  fixture rather than to the engine.

Increments 3, 6, 7 and 8 are independent of the others and may land in any
order, subject to increment 3 re-recording instruction-count baselines before
increments 4 and 6 measure against them.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| **Principle IX — recovery-log format version bump** (increment 2) | The segment encoder is 23.9% of processor time on a Postgres-to-Postgres pipeline, spent building dictionaries and statistics for files deleted seconds later. Replacing the container is worth a measured −18%/−21% wall and −28% processor time. | Keeping the current format and only disabling its dictionary/statistics options was measured too (8.0 vs 5.8 ms/batch) and captures ~85% of the win with no version bump — but D1 (greenfield) rejects carrying a columnar analytics format as a scratch buffer when a streaming container is strictly better, and PI2 forbids keeping both. The format is documented as "a replayable buffer, never the source of truth" and already carries a version field for exactly this. |
| ~~**Principle IX — opening the recorded 0.2 → 0.3 version window**~~ (increment 9) — **DOES NOT FIRE** | Phase 0 established that parallel staging lives entirely behind the existing `LoadSession::write(&mut self, …)` signature: the parallelism is inside the session, not across the interface. `cargo semver-checks` reports no break on `rdlt-connector`, and **the 0.2 → 0.3 window stays closed** — recorded as an outcome per PI8, exactly as the spec's Assumption anticipated. | The alternative shape was designed and then **rejected on a compile fact**: `Box<dyn TableWriter>` carries no lifetime tie to the session, yet every implementor's per-table work mutates session state (`FileSession.staged`, `IcebergSession.pending_files`), so `finish` would have to return a per-table accumulation for the session to absorb — putting a new vocabulary type on the semver-sacred surface for no measured gain. D5 permitted the break; Phase 0 found it unnecessary, which is the better outcome. |
| **Nine increments in one feature** | The findings share instruments, fixtures and a single baseline; measuring them under one protocol is what makes the deltas comparable and additive. Splitting them across features would re-measure the baseline repeatedly and lose the cross-increment ordering constraints above. | Each increment is independently mergeable with the full gate green (spec, "Each story is an independently mergeable increment"), so the size is a sequencing fact rather than a coupling. Feature 017 executed ~12 increments on the same discipline. |

## Phase 0 → research.md

Eight designs were produced and each was handed to an adversarial reviewer
briefed to refute it. Both halves are in [research.md](research.md). The
outcomes that change this plan:

**Dependencies — net negative, which is the right direction.**
`parquet` is **deleted** from `crates/rdlt-engine/Cargo.toml`. Exactly one
dependency is proposed (`smallvec`, zero new lock entries, already at 1.15.2 via
hyper/moka/idna) and it is **measurement-gated**: it lands only if
`shred_nested_10k`'s instruction count actually moves. Its justification is that
the hand-written alternative needs `MaybeUninit`, which `unsafe_code = "deny"`
forbids — so the choice is a crate or leaving ~2.2M malloc/free pairs in place.

**The owner directive cut both ways.** The review's most useful act was to
*reject* the proposed `uuid` dependency: it appears nowhere in the measured
profile, it would be a genuinely new dependency, and `Uuid::try_parse` accepts a
narrower set than both today's parser and PostgreSQL's own `uuid_in` — a silent
narrowing of semantics that Principle IV forbids and no requirement authorises.
The existing parser is kept and its real defect (it accepts a hyphen at a
position it should not) is fixed in place. Contract PI3 now carries both halves
of the rule: do not hand-write what a crate does, **and** do not take a crate for
code that works, is tested, and does not appear in the profile.

**One requirement was unsatisfiable and is corrected.** FR-029 asked for identity
hashing without materialising the canonical rendering. `RowIdBuilder::update_lp`
feeds that rendering's **length before its bytes**, so the whole rendering must
exist before hashing starts — verified independently. With identities frozen by
D6, only the *allocation* is recoverable, and FR-029 now says so.

**Two acceptance figures did not match the evidence** and are corrected: Story 2's
memory criterion split per cell (measured −19% and −9.5%, not one number), and
SC-004 with it.

**Three forks went to the owner and are settled** (research.md §9): SC-005
re-targets to the merge cell, because Story 5 removes the staging table that
Story 9's lever depends on; Story 7's defaults gain a reduced dictionary limit,
because snappy alone makes encoder CPU *rise*; and no `[profile.bench]` pin —
baselines shift once and are re-recorded deliberately, which PI1 permits and the
one-sided gate makes safe.

**The evidence for increment 1 was already committed.**
`benches/results/pg-to-pg-dedup-1m.json` records `rdlt.rows = 3000000` beside
`verify.actual_rows = 1000000`. Nothing compared the two numbers — which is
precisely what FR-010 makes the harness do.

### Constitution re-check, post-Phase-0

| # | Principle | Verdict after design |
|---|---|---|
| I | Small Core | **PASS, improved.** One dependency removed, one proposed under a measurement gate, one rejected on evidence. |
| III | One-Boundary Wrapping | **PASS.** `postgres-protocol` is *not* taken as a direct dependency — `ToSql` gets the same bytes with zero additions. The pre-existing `tokio_postgres::Config` leak in `tls::ParsedConn` is not widened. |
| IV | Exactly-Once | **PASS, with a sharpened obligation.** `crash_point!` expands to a `return` from the enclosing function, so no crash point may move inside a `spawn_blocking` closure — which forces `sync_for_commit` into two hops, not one. Crash points are renamed and re-scoped in increment 5 and re-pinned. |
| IX | Frozen Formats | **PASS.** One authorised bump (recovery log), full document-amendment inventory established including a migration note. **The version window stays closed.** |
