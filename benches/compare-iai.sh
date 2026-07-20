#!/usr/bin/env bash
# Perf gate G1 (feature 003): compare iai-callgrind instruction counts against
# the committed baselines in benches/perf-baselines.json.
#
#   compare-iai.sh            compare the latest run; fail on >3% regression
#   compare-iai.sh --record   (re)write the baseline file from the latest run
#
# Run AFTER `cargo bench -p rdlt-engine --bench iai_hotpath` (the Makefile's
# `TARGET=iai make bench` does both). Baselines change only deliberately,
# in-diff (lockfile discipline, G1.3).
set -euo pipefail
cd "$(dirname "$0")/.."

MODE="${1:-compare}"
python3 - "$MODE" <<'PY'
import json, pathlib, sys

TOLERANCE = 0.03
BASELINE_FILE = pathlib.Path("benches/perf-baselines.json")
IAI_DIR = pathlib.Path("target/iai")

def max_ir(doc):
    """Current-run 'Ir' from an iai-callgrind 0.16 summary: profiles[] ->
    summaries.parts[] -> metrics_summary.Callgrind.Ir.metrics.<variant>[0]
    (first element is the CURRENT run; the second, when present, the prior)."""
    best = 0
    for profile in doc.get("profiles", []):
        for part in profile.get("summaries", {}).get("parts", []):
            ir = part.get("metrics_summary", {}).get("Callgrind", {}).get("Ir")
            if not ir:
                continue
            for variant in ir.get("metrics", {}).values():
                if isinstance(variant, list) and variant and isinstance(variant[0], dict):
                    value = variant[0].get("Int", variant[0].get("Float", 0))
                    best = max(best, int(value))
    return best

measured = {}
for summary in IAI_DIR.rglob("summary.json"):
    doc = json.loads(summary.read_text())
    name = doc.get("function_name") or summary.parent.name.split(".")[0]
    ir = max_ir(doc)
    if ir:
        measured[name] = max(measured.get(name, 0), ir)

if not measured:
    sys.exit("no iai summaries under target/iai — run the bench with --save-summary first")

mode = sys.argv[1]
if mode == "--record":
    toolchain = ""
    if BASELINE_FILE.exists():
        toolchain = json.loads(BASELINE_FILE.read_text()).get("toolchain", "")
    BASELINE_FILE.write_text(json.dumps({
        "format_version": 1,
        "toolchain": toolchain,
        "benches": {name: {"instructions": ir} for name, ir in sorted(measured.items())},
    }, indent=2) + "\n")
    print(f"recorded {len(measured)} baselines -> {BASELINE_FILE}")
    sys.exit(0)

if not BASELINE_FILE.exists():
    sys.exit(f"{BASELINE_FILE} missing — record baselines first (compare-iai.sh --record)")
baselines = json.loads(BASELINE_FILE.read_text())["benches"]

failures = []
for name, entry in sorted(baselines.items()):
    base = entry["instructions"]
    now = measured.get(name)
    if now is None:
        failures.append(f"{name}: bench missing from this run (baseline {base})")
        continue
    delta = (now - base) / base
    marker = "REGRESSION" if delta > TOLERANCE else "ok"
    print(f"{name}: {base} -> {now} ({delta:+.2%}) {marker}")
    if delta > TOLERANCE:
        failures.append(f"{name}: {base} -> {now} ({delta:+.2%} > {TOLERANCE:.0%})")
for name in sorted(set(measured) - set(baselines)):
    failures.append(f"{name}: not in baselines — record it deliberately (--record)")

if failures:
    print("\nPERF GATE FAILED (G1):", *failures, sep="\n  ")
    sys.exit(1)
print("perf gate: all benches within tolerance")
PY
