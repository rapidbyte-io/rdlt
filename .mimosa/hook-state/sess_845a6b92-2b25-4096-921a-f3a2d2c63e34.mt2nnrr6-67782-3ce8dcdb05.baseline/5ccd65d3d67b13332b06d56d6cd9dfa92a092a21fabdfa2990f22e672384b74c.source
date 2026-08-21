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
cd "$(dirname "$0")/../.."

MODE="${1:-compare}"
python3 - "$MODE" <<'PY'
import json, pathlib, sys

TOLERANCE = 0.03
BASELINE_FILE = pathlib.Path("benches/perf-baselines.json")
IAI_DIR = pathlib.Path("target/iai")

def max_ir(doc):
    """Current-run 'Ir' from an iai-callgrind 0.16 summary: profiles[] ->
    summaries.parts[] -> metrics_summary.Callgrind.Ir.metrics, an
    either-or-both map: 'Both' -> [current, prior], 'Left' -> current only
    (every FIRST run over a fresh target/iai — always the case in CI),
    'Right' -> prior only (not this run; skipped)."""
    best = 0
    for profile in doc.get("profiles", []):
        for part in profile.get("summaries", {}).get("parts", []):
            ir = part.get("metrics_summary", {}).get("Callgrind", {}).get("Ir")
            if not ir:
                continue
            for key, variant in ir.get("metrics", {}).items():
                if key == "Right":
                    continue
                current = variant[0] if isinstance(variant, list) else variant
                if isinstance(current, dict):
                    value = current.get("Int", current.get("Float", 0))
                    best = max(best, int(value))
    return best

measured = {}
summary_files = 0
for summary in IAI_DIR.rglob("summary.json"):
    summary_files += 1
    doc = json.loads(summary.read_text())
    name = doc.get("function_name") or summary.parent.name.split(".")[0]
    ir = max_ir(doc)
    if ir:
        measured[name] = max(measured.get(name, 0), ir)

if not measured:
    if summary_files:
        sys.exit(
            f"{summary_files} summary.json file(s) under target/iai but no "
            "current-run Ir metrics parsed — summary schema drift? (expected "
            "iai-callgrind 0.16 Both/Left metric variants)"
        )
    sys.exit("no iai summaries under target/iai — run the bench with --save-summary first")

def codegen_signature():
    """The codegen settings these counts were produced under.

    Instruction counts are a property of the generated code, so they are only
    comparable within one codegen configuration — `lto` and `codegen-units`
    move them as surely as a compiler bump does. `[profile.bench]` inherits
    `[profile.release]`, so the release profile is what the bench binaries are
    actually built with; read it rather than trusting a profile NAME, which
    stays "release" no matter what the settings say.
    """
    import re
    manifest = pathlib.Path("Cargo.toml").read_text()
    block = re.search(r"^\[profile\.release\]\s*$(.*?)(?=^\[|\Z)", manifest, re.S | re.M)
    body = block.group(1) if block else ""
    def field(key, default):
        m = re.search(rf"^{key}\s*=\s*(.+?)\s*$", body, re.M)
        return m.group(1).strip().strip('"') if m else default
    # Cargo's own defaults when the profile is absent or silent.
    return (f"lto={field('lto', 'false')},"
            f"codegen-units={field('codegen-units', '16')},"
            f"opt-level={field('opt-level', '3')}")

# The postgres bench (iai_pg) moved to the rdlt-connectors repository with its
# crate at the 044 cut; its recorded baselines stay in the file as the dated
# reference and are neither compared nor re-recorded here — this repo can no
# longer run them.
MOVED = {"pg_copy_decode_10k", "pg_copy_encode_10k"}

mode = sys.argv[1]
if mode == "--record":
    import subprocess
    toolchain = subprocess.run(
        ["rustc", "--version"], capture_output=True, text=True
    ).stdout.strip()
    benches = {name: {"instructions": ir} for name, ir in sorted(measured.items())}
    # Carry the moved benches' recorded figures forward untouched — a
    # re-record must never silently delete a dated reference.
    if BASELINE_FILE.exists():
        prior = json.loads(BASELINE_FILE.read_text()).get("benches", {})
        for name in sorted(MOVED & set(prior)):
            benches.setdefault(name, prior[name])
    BASELINE_FILE.write_text(json.dumps({
        "format_version": 1,
        "toolchain": toolchain,
        "codegen": codegen_signature(),
        "benches": benches,
    }, indent=2) + "\n")
    print(f"recorded {len(measured)} baselines -> {BASELINE_FILE}")
    sys.exit(0)

if not BASELINE_FILE.exists():
    sys.exit(f"{BASELINE_FILE} missing — record baselines first (compare-iai.sh --record)")
doc = json.loads(BASELINE_FILE.read_text())
baselines = doc["benches"]

# Instruction counts are only comparable within one compiler: a rustc bump
# shifts codegen and would either flap the gate or silently absorb a real
# regression. Refuse to compare across toolchains; re-record deliberately.
import subprocess
current = subprocess.run(["rustc", "--version"], capture_output=True, text=True).stdout.strip()
recorded = doc.get("toolchain", "")
if recorded and current and recorded != current:
    sys.exit(
        f"PERF GATE: toolchain mismatch — baselines recorded with '{recorded}', "
        f"current is '{current}'. Re-record deliberately in a dedicated commit: "
        f"cargo bench -p rdlt-engine --bench iai_hotpath -- --save-summary=json "
        f"&& benches/harness/compare-iai.sh --record"
    )

# Same rule as the toolchain guard, for the same reason: a codegen change
# shifts every count, so comparing across one would either flap the gate or
# silently absorb a real regression.
recorded_codegen = doc.get("codegen", "")
current_codegen = codegen_signature()
if recorded_codegen and recorded_codegen != current_codegen:
    sys.exit(
        f"PERF GATE: codegen mismatch — baselines recorded under "
        f"'{recorded_codegen}', current is '{current_codegen}'. Re-record "
        f"deliberately in a dedicated commit: cargo bench -p rdlt-engine "
        f"--bench iai_hotpath -- --save-summary=json "
        f"&& benches/harness/compare-iai.sh --record"
    )

failures = []
for name, entry in sorted(baselines.items()):
    if name in MOVED:
        continue
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
