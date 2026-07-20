# Evidence: Shred-Stage Attribution Profile (T006) + Candidate Ranking (T007)

**Date**: 2026-07-20 | **Identity**: per [environment.md](environment.md)
(blashyrkh, Ryzen AI MAX+ 395, kernel 7.0.12-201.fc44, rustc 1.96.0
ac68faa20, valgrind 3.27.1, perf from fc44, dataset sha256 `22bdb31b…9bfb`)

## Invocations

- Primary (gate units): `cargo bench -p rdlt-engine --bench iai_hotpath` →
  `callgrind_annotate --threshold=97 target/iai/…/callgrind.shred_nested_10k.rows_10k.out`
  (10k-row NDJSON through `fuzzing::bench_shred_bytes` — same bytes/tape path
  as the cell; verified against the 003 mislabeled-entry-point lesson).
- Secondary (wall truth): `perf stat -e cycles,instructions,…` and
  `perf record -F 997 --call-graph dwarf` on
  `target/release/examples/shred_only` over the 200k-row dataset (the actual
  cell binary).

Headline numbers: cell wall 0.5116 s median; 6.77 G instructions, 2.36 G
cycles (**IPC 2.87** — compute-bound), 4.85 M cache-misses (low), 1.11 M
branch-misses.

## ⚠ Lens divergence (finding in itself)

Callgrind runs blake3 on its **SSE4.1 fallback** (valgrind masks CPUID; the
hot symbol resolves to `blake3_compress_in_place_sse41` via `nm`), while bare
metal uses `_blake3_compress_in_place_avx512`. Instruction shares therefore
UNDERSTATE nothing but their ratios are skewed: blake3 is 17.8% of
instructions under callgrind but **28–33% of real wall time**. Per protocol
P4.2/R1, wall time decides; callgrind localizes. All rankings below use the
wall lens with callgrind as corroboration.

## Attribution — callgrind (10k rows, 364,856,676 Ir total)

| Cluster | Ir | Share |
|---|---|---|
| serde_json parse (skip_to_escape, deserialize_any×2, raw_value, parse_str, ignore_str, has_next_key, parse_integer) | 68.8 M | 18.9% |
| blake3 (compress_sse41 42.0 M + ChunkState 12.5 M + Hasher::update 10.2 M) | 64.8 M | 17.8% |
| allocator family (malloc/free/realloc/_int_*/finish_grow) | 42.8 M | 11.7% |
| canonicalization serialize (format_escaped_str 16.5 M + canonical_json_bytes 14.6 M) | 31.1 M | 8.5% |
| tape/drain (push_and_drain 14.5 M + get_top 4.6 M + memcmp 6.5 M) | 25.5 M | 7.0% |
| memcpy | 23.4 M | 6.4% |
| RowId::write_hex | 21.6 M | 5.9% |
| infer (ColState/ScalarState::observe) | 8.4 M | 2.3% |
| from_utf8 | 6.3 M | 1.7% |
| build (build_batch + arrow append) | 6.3 M | 1.7% |

## Attribution — perf wall (200k rows, 0.517 s; flat + DWARF call-graph runs)

| Cluster | Wall share | Attribution detail (DWARF) |
|---|---|---|
| **blake3 total** | **28–33%** | compress_avx512 19.8–22.5%, Hasher::update 3.8–5.5%, ChunkState buffer memmove 1.3%, Hasher::new ~1.4%, final_output ~1.6% |
| tape/arena memory (push_and_drain self ~4–5%, push_node 2.3%, Vec spec_extend/realloc-growth memmove ~3.5%) | ~10–11% | growth memmove: serde_json deserialize_any spec_extend 2.4%, finish_grow/realloc 1.1% |
| column lookup (get_top self 3.9% + its memcmp 5.8% + push_and_drain memcmp 1.1%) | ~10% | `DrainRow::get_top` → `obj_get(key)` = per-row LINEAR string-keyed scan over object entries |
| serde_json parse cluster | ~10–12% | skip_to_escape 2.9%, deserialize_any 2.8%, raw_value 2.0%, ignore_str 1.5%, fragments below limit |
| canonicalization serialize | ~5.3% | canonical_json_bytes 2.6%, format_escaped_str 1.6%, Number::serialize memmove 1.1% |
| from_utf8 | ~4.3% | flat, no dominant caller ≥ limit |
| build_scalar | ~3.6% | |
| write_hex (SchemaHash + RowId) | ~2–3% | |

## R3 arithmetic (restated)

20× vs frozen dlt 5.95 s ⇒ rdlt ≤ **297.5 ms**; from 511.6 ms ⇒ a **41.9%
cut (1.72×)**. In gate units ~365 M → ≤ 215 M with the IPC caveat recorded in
research R3.

## Candidate ranking (T007) — attempt order = descending wall share

| # | Candidate | Wall share | Classification |
|---|---|---|---|
| 1 | **C5′ — identity pipeline usage** (NOT the frozen T023 algorithm swap: one-shot `blake3::hash` vs per-row `Hasher::new/update/finalize` streaming overhead; hash tape-direct canonical form to shrink canon serialize; hex reduction) | 33–40% combined (blake3 28–33 + canon 5.3 + hex ~2) | **VIABLE — reopened** (see below) |
| 2 | **C6 — column lookup interning** (profile-discovered: resolve column keys to entry positions once per schema/slab instead of per-row linear obj_get scans) | ~10% | VIABLE |
| 3 | **C3 — arena/tape layout + growth** (buffer reuse, reserve sizing to kill spec_extend/realloc memmove; get_top D1-miss cluster from callgrind: 27% of all D1mr) | ~10–11% | VIABLE (two-lens rule binds) |
| 4 | **C1 — structural scan / parse** (serde_json tokenizer cluster; memchr-based stage-1 or trimming raw_value double-scan) | ~10–12% | VIABLE |
| 5 | **C2 — UTF-8 validate-once** (from_utf8 4.3%; safe APIs only, P4.4) | ~4.3% | VIABLE (small) |
| 6 | **C4 — scalar fast paths** (build_scalar 3.6%; datetime chrono cost did NOT surface ≥1% on this dataset — bench data has no datetime casts) | ~3.6% | MARGINAL |

**C5 reopen decision (R2 threshold)**: FIRES. The R2 threshold ("identity
hashing ≥ 25% of stage instructions") was written before the lens divergence
was known; under the deciding wall lens blake3 alone is 28–33% ≥ 25%.
Scope of the reopen is NARROW: T023's frozen conclusion (blake3 is not
swapped for another algorithm; the >30% e2e bar for a swap stands) is NOT
re-litigated. What reopens is the usage pattern: per-row streaming-hasher
overhead (`Hasher::new`+`update`+`finalize` per identity, visible as ~9–10%
non-compress blake3 overhead), the size of the hashed canonical byte string,
and double-hex-encoding costs.

**Ceiling math (honest)**: capturing HALF of C5′+C6+C3+C1+C2 ≈ 0.5 ×
(36+10+10+11+4) ≈ 35% → ~0.33 s ≈ 18×. Reaching 20× requires the C5′ bucket
to yield most of its share (hash fewer bytes, one-shot API) **plus** two of
the mid-tier candidates landing well. Genuinely open: the bar is in reach
only on the optimistic branch. Every A/B below will move this arithmetic
from estimate to measurement.

**Attempt order**: C5′ → C6 → C3 → C1 → C2 → C4 (per T007's rule this
overrides the T008–T011 numbering; C5′ and C6 get evidence files
`ab-c5-identity-pipeline.md` and `ab-c6-column-interning.md`, and both MUST
appear in the resolution record's candidates table).
