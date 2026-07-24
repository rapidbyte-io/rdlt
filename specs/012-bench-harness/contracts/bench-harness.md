# Contract: Benchmark Harness Rules (BH1–BH8)

| # | Rule |
|---|---|
| BH1 | Cells are data: adding a source×destination pairing that reuses existing fixtures touches only TOML (+ a pipeline YAML template). Unknown fields, duplicate ids, and gated-cell↔bar mismatches are load-time typed errors naming the offender. |
| BH2 | One protocol, executed by the harness for every cell: environment check (quiet-machine loadavg guard — gated runs REFUSE or loudly annotate on a loaded machine), fixture lifecycle + seeding with recorded dataset identity, declared warmups, N runs, medians/p95. The 004 measurement rules as code, not prose. |
| BH3 | Metric completeness: every artifact carries wall (runs + median + p95), rows/s + MB/s derived from RunReport's own row/byte totals (never estimated), CPU mean+peak and peak RSS (VmHWM / cgroup memory.peak) or an explicit `null` with reason, and — in library mode — per-stream attribution from the events seam. |
| BH4 | Apples-to-apples competitors: dlt variants report the same metric set (self-timed wall for continuity; cgroup v2 CPU/RSS). A missing/failed baseline is `status: "missing"` — visible in report output, and any ratio bar over it FAILS with "baseline missing"; it never becomes a silent green. |
| BH5 | Artifacts are versioned JSON (`format_version`) with the environment fingerprint (CPU, kernel, rustc, competitor pin, dataset hashes, load reading). Summary artifacts are committed; raw sampler streams are gitignored. |
| BH6 | `rdlt-bench gate` evaluates bars.toml against latest artifacts: violation (beyond recorded tolerance) → nonzero exit naming cell, bar, measured value. Only `class = "gated"` cells gate; only wall-median bars exist in this feature — CPU/RSS/throughput are recorded, not gated. |
| BH7 | `rdlt-bench report` regenerates RESULTS.md's number tables from artifacts between explicit generated-section markers; hand-written narrative outside the markers is preserved byte-for-byte. No hand-transcribed number may remain inside generated sections. |
| BH8 | Governance: rdlt-bench is `publish = false`, zero runtime-crate manifest changes, SPI frozen (semver-checks "no update required"), safe Rust only (procfs/cgroup text parsing, no libc FFI). Harness deps stay conservative: clap (dev-only, owner decision 2026-07-21) is the sole addition beyond the existing workspace tree. The iai gate (perf-baselines.json + compare-iai.sh), criterion shred, and the hyperfine cold-start protocol are retained unchanged. Migrated gated cells prove continuity per the R7 protocol: in-band, or a version-policy entry — never silent renumbering. |

---

**Amendment (feature 018, 2026)**: the gated/scoreboard cell
classification and the library/hyperfine run modes are RETIRED from the
harness vocabulary (BH1/BH2/BH3/BH6 wording amended accordingly). The
mechanisms those clauses protect — declarative cells, honest recorded
artifacts, loud skips, bars enforced only through `rdlt-bench gate` with
governance entries — are unchanged and remain binding. Enforcement
additionally requires a recorded session floor (constitution v1.1.0).
Continuity for bars retired by the matrix rebuild rides the RESULTS.md
policy log (BH8 spirit).
