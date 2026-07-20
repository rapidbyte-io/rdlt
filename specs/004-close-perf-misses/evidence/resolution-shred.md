# Resolution: Shred-Only Cell (T012)

**Date**: 2026-07-20 | **Identity**: per [environment.md](environment.md)
(blashyrkh, Ryzen AI MAX+ 395, kernel 7.0.12-201.fc44, rustc 1.96.0
ac68faa20, valgrind 3.27.1, hyperfine 1.20.0, dataset sha256
`22bdb31b…9bfb` — regenerated and hash-verified this session)

## Outcome

**(b) re-baselined — by owner decision, recorded as such.**

The bar moves from **≥ 20×** to **≥ 10×** vs pinned dlt 1.29.0
normalize. No code changed; no candidate was attempted; the perf gate is
NOT re-recorded (P5/FR-007 — no accepted change, no re-record).

### P6 deviation, stated plainly

Protocol P6 defines leaf (b) as valid only when every viable candidate
carries a measured accept/reject. That condition is **not met here**: the
fresh profile (T006/T007) classified five candidates VIABLE with measured
shares, and none was A/B-attempted. The project owner decided on
2026-07-20 to close the cell at current measured performance and defer
the candidates to the backlog. Per the contract's own amendment rule
("future benchmark work inherits these rules unless a recorded policy
event changes them"), the version-policy entry for this adjustment
records the deviation. What this record therefore does NOT claim:

- It does NOT claim 11.4× is the measured ceiling. The profile's honest
  estimate is that the optimistic branch reaches ~18× and 20× is in reach
  only if the C5′ bucket yields most of its share plus two mid-tier
  candidates land ([profile-shred.md](profile-shred.md), ceiling math).
- The adjusted bar is set from the measured **current** value (what the
  code demonstrably does), never from the unmeasured estimate.

## Final measurement (protocol P2)

Quiet machine verified (load avg 0.13, no concurrent builds; builds
completed before measurement). `target/release/examples/shred_only`
(same bytes/tape path as the gate bench) over the hash-verified 200k-row
dataset, 1 warmup + 5 timed runs, two independent series; dlt
`normalize()` re-measured same-session under the frozen methodology
(unchanged `benches/baseline/normalize_only.py`, pin 1.29.0, 5 runs):

| Side | Runs (s) | Median |
|---|---|---|
| rdlt series 1 | 0.5239, 0.5174, 0.5275, 0.5256, 0.5291 | 0.5256 |
| rdlt series 2 (recorded) | 0.5221, 0.5238, 0.5271, 0.5220, 0.5209 | **0.5221** |
| dlt normalize (same session) | 5.7185, 5.7223, 5.7610, 5.7525, 5.7845 | **5.752** |

- rdlt shred: **0.522 s median** (all 10 runs within 0.517–0.529 s,
  spread ≤ 2.3%; consistent with T003's 0.5116 s reproduction — the
  ~2% drift is session wall-time jitter, matching the +0.66% callgrind
  jitter noted in [environment.md](environment.md))
- dlt 1.29.0 `normalize()`: **5.75 s** same-session median (the
  close-out session recorded 5.95 s from the identical frozen protocol
  — a ~3% between-sessions movement on the dlt side)
- Multiple, same-session pair: 5.752 / 0.5221 = **11.0× faster**
- Observed multiple across this feature's same-day sessions:
  **11.0–11.6×** (both sides move ±2–3% session-to-session).
- Adjusted bar **≥ 10×**: one integer step below the observed session
  floor. A ≥ 11× bar would sit 0.2% under the same-session measurement
  and flap on ordinary jitter — the spec's flap rule treats such a bar
  as mis-set. ≥ 10× holds with ~10% headroom against the observed
  worst case while still recording a stage advantage of an order of
  magnitude.

## Candidates table (every profiled candidate; T007 order)

| Candidate | Measured wall share | Attempted? | Disposition | Evidence |
|---|---|---|---|---|
| C5′ — identity pipeline usage (one-shot hash API, tape-direct canonical form, hex reduction; NARROW reopen — T023's algorithm freeze untouched) | 33–40% combined (blake3 28–33 + canon 5.3 + hex ~2) | No | **Viable, not attempted — owner decision; backlog** | [profile-shred.md](profile-shred.md) |
| C6 — column lookup interning (per-row linear `obj_get` scans → per-schema entry positions) | ~10% | No | Viable, not attempted — backlog | [profile-shred.md](profile-shred.md) |
| C3 — arena/tape layout + growth (spec_extend/realloc memmove; get_top D1-miss cluster) | ~10–11% | No | Viable, not attempted — backlog (two-lens rule binds if attempted) | [profile-shred.md](profile-shred.md) |
| C1 — structural scan / parse (serde_json tokenizer cluster) | ~10–12% | No | Viable, not attempted — backlog | [profile-shred.md](profile-shred.md) |
| C2 — UTF-8 validate-once (safe APIs only, P4.4) | ~4.3% | No | Viable (small), not attempted — backlog | [profile-shred.md](profile-shred.md) |
| C4 — scalar fast paths (build_scalar; datetime cost did not surface ≥1% on this dataset) | ~3.6% | No | Marginal, not attempted — backlog | [profile-shred.md](profile-shred.md) |
| C5 — blake3 algorithm swap (003 T023 freeze) | n/a | No | **Not reopened** — only the usage pattern (C5′) reopened; the >30% e2e bar for a swap stands | [profile-shred.md](profile-shred.md) §C5 reopen decision |

No candidate is without a row (data-model §2). T008–T011's A/B evidence
files (`ab-c1…` … `ab-c4…`, `ab-c5-identity-pipeline.md`,
`ab-c6-column-interning.md`) were **not created** because no A/B was run;
this table plus the policy entry is the traceable record of why.

## Evidence links

- [environment.md](environment.md) — T001–T003 identity + close-out
  reproduction (incl. gate pass at +0.66% shred drift, within ±3%).
- [profile-shred.md](profile-shred.md) — T006 two-lens attribution
  (callgrind + perf wall, lens-divergence finding) and T007 ranking with
  the R3 arithmetic and honest ceiling math.

## Policy entry reference

`benches/RESULTS.md` → "Baseline version policy" → entry
**2026-07-20 — shred-only bar adjusted ≥ 20× → ≥ 10× (owner decision)**.

Perf gate: `benches/perf-baselines.json` unchanged (no accepted A/B; the
gate stays armed at ±3% on the existing baselines, P5).
