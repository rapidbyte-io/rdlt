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
