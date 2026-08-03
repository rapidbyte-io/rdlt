# 034 — OUTPUT PART SIZING

Owner goal: "I want ~128 MB Parquet files" — the canonical data-lake
requirement, which no existing knob can express.

## The gap, precisely

`batch_policy` (033) coalesces on the ARROW IN-MEMORY footprint. That
is a memory bound and a poor proxy for file size: snappy Parquet can
be 5-10x smaller than its Arrow form, and the ratio moves with the
data. A user set `every_bytes: 100000` and got 73 KB files — right
behaviour, wrong units for the question being asked.

Only the DESTINATION knows the encoded size, and only after encoding.
So part sizing belongs there, and `batch_policy` does NOT change.

## Scope — three destinations, not one

Checked, not assumed:

| destination | materialises files? | today's shape |
|---|---|---|
| file | YES — parts ARE the output | one part per `write` |
| iceberg | YES — a table is a set of data files | **writer already held across writes**, retired at publish |
| snowflake | YES — parquet staged, then COPY | one part per `write`, then PUT |
| postgres | no — rows into a table | n/a |
| duckdb | no — appender into tables | n/a |

Iceberg's own spec has `write.target-file-size-bytes`; Snowflake's
guidance is 100-250 MB compressed for load parallelism. The knob has a
real meaning in all three.

**postgres and duckdb must REFUSE the option, not ignore it.** Their
configs already `deny_unknown_fields`, so an unknown key fails at
parse — the requirement is to keep it that way. A silently-ignored
knob is the defect class this project has now hit five times.

## D1 — Shared vocabulary, ONE implementation

Not a file-connector field. Three connectors would mean three
spellings, three defaults and three rolling bugs, which is the exact
argument that put batching in the engine.

Precedent: `ParquetOptions` already lives in the SPI and iceberg
imports it directly. Same shape:

```rust
// rdlt-connector (SPI), beside ParquetOptions
pub struct PartOptions {
    /// Roll once the ENCODED part reaches this many bytes.
    pub target_bytes: Option<u64>,
    /// Roll after this long, measured at write time.
    pub roll_after_seconds: Option<u32>,
}
```

Same first-to-fire disjunction as `CommitPolicy`/`BatchPolicy`, so it
is one mental model in three places rather than three models.

```yaml
destination:
  iceberg:
    parts:
      target_bytes: 134217728    # ~128 MB files
      roll_after_seconds: 900    # … or every 15 min, whichever first
```

## D2 — Measure the ENCODED bytes, because we can

Verified in parquet 58.3: `ArrowWriter::bytes_written()` and
`in_progress_size()` both exist, so the encoded size of an open file
is observable mid-stream. JSONL is the buffer length. No estimation,
no compression-ratio guessing.

## D3 — Two shapes to adopt, not one

- **iceberg** already holds `state.writer` across writes and retires
  it at publish. Rolling is close-and-reopen when the threshold trips
  — the smallest change of the three.
- **file** and **snowflake** encode a complete part per `write` call.
  They must hold a writer open across calls, which is the real work.

An SDK helper owns the decision (`should_roll(encoded, elapsed)`) so
the rule lives once even though the plumbing differs.

## D4 — Invariants

- **A part NEVER spans a commit.** The publish protocols need whole
  files in a commit, so a commit closes the open part. This makes the
  commit cadence an upper bound on part size, exactly as it already
  is on batch size — one rule, applied consistently, and it must be
  documented as the trap it is.
- **A schema change rolls.** A parquet file has one schema.
- **Overshoot is expected.** Rolling happens AFTER crossing the
  target, since a batch is never split. A 128 MB target with large
  batches gives 128-140 MB files. Firehose behaves the same way.
- **`roll_after_seconds` fires only when data arrives.** There is no
  background timer in the write path; a quiet stream rolls at its
  next write or at commit. This is an approximation of Firehose's
  real timer and must be documented as one rather than implied.

## D5 — The knob count, and why it is defensible

Three thresholds in three places, answering three questions, of which
users normally touch ONE:

- `parts.target_bytes` — output file size. The one they want.
- `batch_policy` — memory and throughput. Leave alone.
- `commit_policy` — durability. Leave alone.

The status quo is worse: `batch_policy.every_bytes` LOOKS like it
controls file size and does not.

## Build order

1. SPI `PartOptions` + the SDK roll decision, with unit pins.
2. **file** destination — the provable one. Real run showing ~128 MB
   parquet parts from a source paging in hundreds of rows.
3. **iceberg** — smallest change, writer already long-lived.
4. **snowflake** — same restructure as file.
5. Refusal pins on postgres/duckdb.
6. Docs: examples README, and the `batch_policy` vs `parts` distinction
   stated where the last confusion happened.

Each stage gated before the next; no stage lands without a measured
proof, per the 033 lesson that a green check is not evidence until you
know what it measures.

---

## Build record

### Stage 1 — SPI `PartOptions` (`rdlt-connector`, beside `ParquetOptions`)

Three fields, not the two D1 sketched. The third was added on
measurement, not on design — see the ceiling below.

| field | default | question it answers |
|---|---|---|
| `target_bytes` | 128 MiB | how big is an output file |
| `roll_after_seconds` | off | how stale may an open file get |
| `max_open_bytes` | 512 MiB | how much RAM may open parts hold |

Constructors: `unbounded()` (never roll on size or time — note this is
one part per COMMIT, not per write) and `per_write()` (the pre-034
behaviour, spelled as a one-byte target).

Refusals are typed and named: zero for any of the three, plus
`max_open_bytes < target_bytes`, which is a contradiction that would
otherwise miss the target SILENTLY.

### D2 CORRECTED — the size is observable, but partly ESTIMATED

The plan claimed "No estimation, no compression-ratio guessing." That
is right for jsonl and for flushed parquet row groups, and WRONG for
the tail. `ArrowWriter::in_progress_size()` is documented as the
*anticipated* encoded size of pages not yet flushed, and it runs high.

MEASURED at a 4 KiB target, the worst case since nothing flushes:
parts of 3,616–3,845 bytes, i.e. 88–94% of target. The error is
bounded by one page per column, so it shrinks toward nothing as the
target grows. At 128 MiB it is under a percent — see below.

A related measurement, pinned in `stage.rs`: re-appending IDENTICAL
rows moved the reported size 4 → 37 → 37 bytes, because the dictionary
already held those values. The size is observable, not monotone per
append. Test data has to be distinct or the threshold is untestable.

### The measured proof — real 128 MiB parts

1.5M rows of incompressible hex (365 MB jsonl) → parquet, one commit,
`target_bytes: 134217728`:

| part | bytes | % of target |
|---|---|---|
| 0 | 141,482,718 | 105.4% |
| 1 | 141,331,126 | 105.3% |
| 2 | 126,427,992 | 94.2% |

The two rolled parts OVERSHOOT by ~5% — a batch is never split, so the
part closes after crossing. The last is UNDER because the commit
closed it, not the target. Both are D4 behaving as written.

### D4's trap, measured rather than asserted

The SAME run under the default commit policy produced **44 commits and
9.5 MB parts** — the 128 MiB target never came close to binding.
Commit cadence is an upper bound on part size, and at default cadence
it is the ONLY bound that matters. The examples README says so where
the previous confusion happened.

### The ceiling, and why it exists

An open part lives in RAM until it closes, and a partitioned
destination holds ONE PER PARTITION. Before 034 every write went
straight to staging, so RSS was O(one batch); now it is O(uncommitted
data), bounded by `partitions × target_bytes`.

Measured on the same 1.5M rows partitioned 97 ways in one commit:

| ceiling | peak RSS | parts written |
|---|---|---|
| 512 MiB (default) | 536 MB | 97 |
| 64 MiB | 252 MB | 353 |

So the valve works and its cost is fragmentation, which is the right
trade: undersized files beat an unbounded heap. The largest open part
is closed first, being nearest its target and so the least undersized
part available.

### Stage 2 gate

`rdlt-connector` + `rdlt-connector-file`: 166/166, 0 skipped.

One existing test changed, deliberately:
`final_names_independent_of_cross_table_arrival_order` pinned
one-part-per-write, which the default now coalesces. Its subject is
the per-table index arithmetic, so it takes `PartOptions::per_write()`
and tests the same thing.

New suite `tests/cases/test_parts.rs` (6 cells): default coalescing,
rolling with every part in band, commit-closes-the-part,
schema-change-rolls, the memory ceiling, and the four refusals.

### What the gate caught: the memory floor moved

`rdlt-connector-postgres::memory_bound` failed with **"memory
allocation of 6553600 bytes failed"** — a real OOM under the 256 MiB
`prlimit --data` ceiling it enforces. That test drives a PARQUET
destination, and 034 had just put up to 128 MiB of open part inside
its ceiling.

The consequence, stated plainly rather than papered over: **a
file-destination pipeline's memory floor rose by up to
`parts.target_bytes`.** Before 034 every write went straight to
staging; now a part accumulates in RAM until it closes. A deployment
under a tight cgroup must size `parts` to fit, and `max_open_bytes`
exists so that sizing is one number rather than an emergent property
of partition count.

The test itself was fixed by pinning `parts` to 8 MiB, since its
subject is the SOURCE/engine path — it says so in its own header — and
the default would have made it measure destination file sizing
instead. It also moved from the frozen path-only `parquet:` shorthand
to `file:`, the only spelling that can carry `parts`.

Worth recording for the next session: the failure MOVED when fixed —
from an allocation failure to `Disk quota exceeded` on `/tmp`, which
is a 32 GB tmpfs on this machine and had 9 GB free against a 6.9 GB
table. `TMPDIR` off the tmpfs is the fix; the two failures are
unrelated and only the first was 034's.

### Stage 2 gate of record

`cargo nextest run --workspace`: **1129/1129, 0 skipped**, with
`TMPDIR` on real disk. Clippy clean workspace-wide, all targets.

---

### Stage 3 — iceberg

D3 called this "the smallest change of the three" because the writer
is already long-lived. It turned out smaller still, and for a better
reason than the plan guessed: **iceberg-rust already has a rolling
file writer.** `Writer::open` was calling
`RollingFileWriterBuilder::new_with_default_file_size`, so this
destination has always rolled files — at the library's default, which
is the Iceberg spec's `write.target-file-size-bytes` of 512 MiB.

So `target_bytes` is FED to the library rather than reimplemented
above it: `RollingFileWriterBuilder::new(.., target_file_size, ..)`.
Reimplementing would have meant two rolling mechanisms disagreeing
about the same file.

This CHANGES the effective default from the library's 512 MiB to
rdlt's 128 MiB. Deliberate, and the reason is D1: one size across
every destination rdlt writes files from, or the shared vocabulary is
a fiction. Recorded here because it is a behaviour change nothing else
would announce.

**`roll_after_seconds` is applied by rdlt**, because the library's
rolling writer has a size trigger and no clock. It retires the whole
window writer, which is the exact move a mid-window schema change
already makes — the closed files park in `pending_files` and join the
window's publish, and the next write opens a fresh writer under a NEW
window prefix (reusing one would overwrite the retired writer's
files).

This forced a split in the SPI: `rolls_on_time` answers the time half
alone, because a destination that delegates SIZE elsewhere would
otherwise get the size question answered twice, once from each side,
with the two able to disagree.

**`max_open_bytes` is met trivially here** and says so in the config
doc. The library streams each file out rather than accumulating it, so
there is nothing for a memory ceiling to cap. That is different from
ignoring it, and the distinction is now written into `PartOptions`
itself: a behavioural promise must be honoured or REFUSED; a resource
bound with no resource to bound is satisfied.

#### Stage 3 proof — live, against Polaris

`test_parts.rs`: the same 16,000 rows through the engine into two
namespaces differing only in `parts.target_bytes`, with the file count
read off the CATALOG's own snapshot summary (`added-data-files`) —
an oracle independent of the code under test.

| target | data files |
|---|---|
| 64 KiB | 8 |
| 128 MiB (default) | 1 |

Both arms landed exactly 16,000 records.

#### Stage 3 gate

`rdlt-connector-iceberg`: **74/74, 0 skipped**, live cells included
(Polaris + RUSTFS). Clippy clean.

---

### Stage 4 — snowflake

The file-destination shape again: one part per write, PUT immediately.
Now an `OpenPart` per DESTINATION table (the same key `pending` uses,
so a part and the COPY that will name it cannot disagree about which
table they belong to), uploaded when it rolls or when the commit
closes it.

Three things the restructure forced:

**`execute_step` had to stop taking `&self`.** Holding `&Load` across
an await requires `Load: Sync`, and an open `ArrowWriter` is `Send`
but never `Sync`. It now takes `&mut self` — mutating nothing, and
reborrowing shared inside its own body — and builds its own guarded
executor rather than being handed one, which is what freed the three
call sites from holding a `DmlOnly` borrow across the call. The file
destination hit the identical wall in `commit_log`, resolved there by
making it a free function.

**`parts` is declared BEFORE the flattened `options`.** `serde(flatten)`
consumes whatever is left, so a field declared after it is never seen.

**A part is asked for its rowcount BEFORE it is finished.** An empty
part has no file worth finalising, and a zero-row part in `pending`
would make the COPY name a file the service has nothing to load from.

#### What the gate caught: fewer orphans

`test_reclaim::stale_parts_are_reclaimed_and_fresh_parts_survive`
failed at "the orphaned part is really there". Its setup writes three
rows and drops the session without committing, expecting remote debris
— but a write no longer uploads, so under the 128 MiB default there
was nothing remote to reclaim.

That is an IMPROVEMENT worth naming: **a load that crashes below the
target now orphans nothing remotely**, because a part is only uploaded
once it rolls or a commit closes it. The test now asks for the upload
it wants to reclaim (`target_bytes: 1`), and says why.

#### Stage 4 proof — live, against the account

`test_parts.rs`: the same 2,000 rows into two scratch schemas
differing only in `parts.target_bytes`, with the file count read from
the service's own `INFORMATION_SCHEMA.COPY_HISTORY` — one row per file
it loaded, an oracle independent of this crate.

| target | files staged | rows landed |
|---|---|---|
| 4 KiB | 8 | 2,000 |
| 128 MiB (default) | 1 | 2,000 |

The target decides the file count and never the row count.

### Stage 5 — the refusals

`postgres` and `duckdb` REFUSE `parts`: rows going into a table have
no file whose size it could describe. `deny_unknown_fields` was
already doing the refusing, so the work was to PIN it — one cell each,
asserting the error names the field — so that removing the attribute,
or adding a field that shadows it, fails a test instead of silently
making a meaningless setting look effective.

### Stage 6 — docs

`examples/README.md` now opens the grouping section with the three
questions and which knob answers each, since the confusion that
started this feature was picking `batch_policy` for a file-size
question. It carries the per-destination table, the two honest
caveats (overshoot, and no background timer), the memory ceiling, and
the measured commit-cadence trap.

## Gate of record — 034 complete

`make check` TWICE CLEAN on the pinned toolchain, `TMPDIR` off the
tmpfs:

| | run 1 | run 2 |
|---|---|---|
| suite | 1138/1138, 0 skipped | 1138/1138, 0 skipped |
| semver | no update required | no update required |
| benches | 6, 0 regressed | 6, 0 regressed |
| cold start | 25.0 ms | 24.7 ms (bar ≤ 40) |

Live Polaris, RUSTFS, Postgres, Oracle and Snowflake cells all ran;
the e2e and sweep targets are in the gate. Clippy clean
workspace-wide, all targets.

Counts by stage: 1129 (stages 1-2) → 1132 (stage 3) → 1136 (stage 4)
→ 1138 (stage 5).

### The one environment flake, recorded rather than re-rolled

Run 2 first died at
`test_merge_strategies::shredded_upsert_is_rejected_typed_at_ensure`
with `rootlessport listen tcp 0.0.0.0:46413: bind: address already in
use` — the recorded podman port flake, which has an inter-run residue
mechanism and an intra-run concurrency one.

The residue mechanism was DIAGNOSED and removed rather than waited
out: the cancelled first attempt had left 11 labelled postgres
fixtures behind, and `make reclaim` (which filters by
`label=rdlt-test=1`) cleared them. Volumes were at 176, far below the
2,048-lock ceiling that amplified this in 029, so that was not a
factor here. The rerun was clean.

Nothing in 034 touches container lifecycle; the failing cell has no
`parts` involvement at all.
