"""Runs the exit scripts beside this one many times over, and fails where one ends otherwise.

`python looped.py RUNS` runs each script `RUNS` times, eight at a time, each within a minute. It
says how each script's runs ended, and exits 1 where any ended with another status than its
script's own, by a signal, or past its minute. A run past its minute is aborted, so faulthandler
writes where it was: each thread's stack, or on a free-threaded build the aborted thread's alone.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

HERE = Path(__file__).resolve().parent
STATUSES = {"awaiting.py": 7, "flowing.py": 7, "ran.py": 0}
"""Each script run, and the status it exits with."""
AT_ONCE = 8
"""Runs at once: two for each of a runner's four cores, since a run waits on its engine more than
it computes."""
WITHIN = 60.0
"""Seconds a run may take."""
ABORTED = 10.0
"""Seconds an aborted run has to say where its threads were."""


def ended(script: str) -> tuple[str, str]:
    """How a run of `script` ended, as its status, a signal's name or `timeout`, and its stderr."""
    with subprocess.Popen(
        [sys.executable, str(HERE / script)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, "PYTHONFAULTHANDLER": "1"},
    ) as run:
        try:
            _, said = run.communicate(timeout=WITHIN)
        except subprocess.TimeoutExpired:
            run.send_signal(signal.SIGABRT)
            try:
                _, said = run.communicate(timeout=ABORTED)
            except subprocess.TimeoutExpired:
                run.kill()
                _, said = run.communicate()
            return "timeout", said
    if run.returncode < 0:
        return signal.Signals(-run.returncode).name, said
    return str(run.returncode), said


def main() -> int:
    runs = int(sys.argv[1])
    failed = False
    for script, status in STATUSES.items():
        with ThreadPoolExecutor(AT_ONCE) as pool:
            outcomes = list(pool.map(ended, [script] * runs))
        counts = Counter(how for how, _ in outcomes)
        sys.stdout.write(f"{script}: {runs} runs, ended {dict(sorted(counts.items()))}\n")
        unexpected = [said for how, said in outcomes if how != str(status)]
        if unexpected:
            failed = True
            sys.stdout.write(
                f"{script}: the first run that ended otherwise said:\n{unexpected[0]}\n"
            )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
