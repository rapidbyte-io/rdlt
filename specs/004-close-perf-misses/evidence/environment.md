# Evidence: Environment Identity & Baseline Reproduction (T001–T003)

**Date**: 2026-07-20 | **Feature**: 004-close-perf-misses

This is the environment-header template (research R7): every evidence
artifact in this directory copies the Identity block below.

## Identity

| Field | Value |
|---|---|
| Machine | `blashyrkh` — the 003 matrix machine (confirmed) |
| CPU | AMD Ryzen AI MAX+ 395 w/ Radeon 8060S (32 hw threads) |
| RAM | 62 GiB |
| Kernel | 7.0.12-201.fc44.x86_64 (Fedora Atomic host) |
| Container | distrobox `my-distrobox`, fedora-toolbox:latest, podman 5.8.3 |
| rustc | 1.96.0 (ac68faa20 2026-05-25) — **matches `benches/perf-baselines.json` recorded toolchain**; cross-toolchain refusal does not fire |
| cargo | 1.96.0 (30a34c682 2026-05-25) |
| valgrind | 3.27.1 |
| hyperfine | 1.20.0 (installed this session via dnf) |
| Dataset | 200,000-row nested NDJSON via the `benches/run-e2e.sh` generator, 200000 lines, sha256 `22bdb31b5989ebc23ae16fd5e7c1f5d46ba2b6f7909f158fc6fb370e1a6e9bfb` |

## T001 — environment repair notes

The feature-start scare ("distrobox missing from PATH") was a
misdiagnosis: this session's shell runs INSIDE `my-distrobox` (confirmed via
`/run/.containerenv`), where the `distrobox` wrapper itself is rightly
absent. The toolchain was intact; the only repair needed was installing
`hyperfine` (dnf, 1.20.0). Note for T017: `podman` is not visible inside the
container — the dlt-side baseline containers need the host podman socket
(`systemctl --user start podman.socket` +
`DOCKER_HOST=unix:///run/user/1000/podman/podman.sock`, per 003 notes) or
`distrobox-host-exec`.

## T003 — close-out reproduction (quiet machine, all builds completed first)

| Cell | 003 close-out record | Reproduced now | Verdict |
|---|---|---|---|
| shred_only, 200k rows | 0.50 s | **0.5116 s median** (5 runs: 0.5077–0.5133, spread < 1.2%) | ✅ within 2% |
| cold start, one-row | "30 ms" | **22.5 ms ± 0.9** (hyperfine, 2 warmup + 10 runs, 21.2–24.1 ms) | ✅ see note |
| iai gate vs `perf-baselines.json` | all within ±3% | identity_keyed +0.00%, identity_keyless +0.00%, passthrough +0.00%, shred_nested **+0.66%** (364,856,676 vs 362,456,649) | ✅ gate passes |

**Cold-start note (measurement, not performance)**: the 003 record's "30 ms"
came from `/usr/bin/time -f %e`, which quantizes to 10 ms steps; the same
command today prints `0.02 s` on every run. hyperfine puts the true value at
22.5 ms ± 0.9. Same reality, coarser instrument then — NOT a deviation, and
independently useful for US2: at 22.5 ms, rdlt sits only ~8% above even the
retired ratio bar's implied 20.9 ms (418 ms / 20), so the composition profile
(T013) may find the old bar was nearly met all along.

**Shred instruction drift (+0.66%)**: within tolerance, consistent with the
known session-to-session callgrind jitter; recorded so later A/B deltas are
read against 364.86 M as the session-local reference, 362.46 M as the gate
baseline.

Conclusion: environment verified, close-out state reproduced — candidate work
may begin (protocol P2/P3 preconditions met).
