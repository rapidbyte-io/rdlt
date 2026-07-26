#!/usr/bin/env bash
# Record nextest's flaky classifications into a committed log.
#
# Why this exists: six container flakes are recorded across features 015-019
# (017 D-04/D-16, 019 D-04, 020 D-13), and every one was handled by re-running
# until green. That convention answers "did it pass eventually" but never "how
# often does it fail", which is the only number that separates a timing-sensitive
# fixture from a real intermittent bug. A test that flakes once a year and one
# that flakes every third run get the same shrug today.
#
# What it does: runs the suite under the `flake` nextest profile (retries on,
# JUnit out), then appends one line per test that PASSED ONLY AFTER A RETRY —
# nextest's own definition of flaky, not a guess of ours. Runs that are cleanly
# green append nothing, so the log stays a list of real observations.
#
# The log is committed. It is evidence, not a cache.
#
# Usage:
#   benches/record-flakes.sh                     # whole suite
#   benches/record-flakes.sh -E 'package(rdlt-connector-file)'   # a slice
#
# Anything after the script name is passed to `cargo nextest run` verbatim.
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

LOG="benches/flakes.log"
JUNIT="target/nextest/flake/junit.xml"

if ! command -v python3 >/dev/null 2>&1; then
    echo "record-flakes: python3 is required to read the JUnit report" >&2
    exit 1
fi

echo "record-flakes: running under the 'flake' profile (retries enabled)…"
cargo nextest run --profile flake "$@"
run_status=$?

if [ ! -f "$JUNIT" ]; then
    echo "record-flakes: no JUnit report at $JUNIT — nothing recorded" >&2
    exit "$run_status"
fi

# `flakyFailure` elements are nextest's own marking: the test failed, was
# retried, and then passed. A `failure` element is a hard failure and is NOT a
# flake — it belongs in the gate's output, not in this log.
python3 - "$JUNIT" "$LOG" "$run_status" <<'PY'
import sys
import subprocess
from datetime import datetime, timezone

# The report is written by nextest into our own `target/` directory, so it is
# not attacker-controlled — but parse it defensively anyway, because the cost is
# one import and the alternative is a script that becomes unsafe the moment
# someone points it at a downloaded artifact.
try:
    from defusedxml import ElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

junit, log, run_status = sys.argv[1], sys.argv[2], sys.argv[3]

tree = ET.parse(junit)
flaky = []
for case in tree.iter("testcase"):
    retries = case.findall("flakyFailure")
    if retries:
        name = f"{case.get('classname', '?')}::{case.get('name', '?')}"
        flaky.append((name, len(retries)))

if not flaky:
    print("record-flakes: no flaky classifications — nothing appended")
    sys.exit(int(run_status))

commit = subprocess.run(
    ["git", "rev-parse", "--short", "HEAD"],
    capture_output=True, text=True,
).stdout.strip() or "unknown"
stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

header = (
    "# Flaky-test observations. One line per test that FAILED then PASSED on\n"
    "# retry under the `flake` nextest profile. Appended by\n"
    "# benches/record-flakes.sh; never edited by hand, never truncated.\n"
    "#\n"
    "# stamp\tcommit\tretries\ttest\n"
)
try:
    existing = open(log).read()
except FileNotFoundError:
    existing = ""

with open(log, "a") as out:
    if not existing:
        out.write(header)
    for name, count in sorted(flaky):
        out.write(f"{stamp}\t{commit}\t{count}\t{name}\n")

print(f"record-flakes: appended {len(flaky)} observation(s) to {log}:")
for name, count in sorted(flaky):
    print(f"  {count} retry/retries: {name}")
sys.exit(int(run_status))
PY
