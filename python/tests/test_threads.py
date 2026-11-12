"""Which threads start rdlt's daemon threads, and a process exiting as they run."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

PYTHON = Path(__file__).resolve().parents[1]
"""The directory `tests` imports from, which a script appends to its path."""
EXITS = Path(__file__).resolve().parent / "exits"
"""Scripts that end as a user's do, run once here and many times over by the nightly workflow."""


def owned(names: list[str]) -> bool:
    """Whether each of `names` is a thread's that Python started and named, not one it was lent."""
    return all(name != "unknown" and not name.startswith("Dummy-") for name in names)


STARTS = """
import sys, threading

starters = []


def audited(event, arguments):
    if event == "_thread.start_new_thread" and arguments[0].__module__ == "rdlt._reading":
        known = vars(threading)["_active"].get(threading.get_ident())
        starters.append("unknown" if known is None else known.name)


sys.addaudithook(audited)
sys.path.append(sys.argv[1])

import rdlt
from rdlt.connector import stream
from tests.connectors import memory


@stream(name="rows")
def rows():
    for n in range(3000):
        yield {"id": n, "amount": n * 1.5}


@rdlt.transform()
def kept(batch):
    return batch


outcome = rdlt.pipeline(
    "owned",
    source=rows,
    destination=rdlt.local(memory.Memory, config={"store": "owned"}),
    streams=[rdlt.stream("rows", transform=[kept])],
    commit=rdlt.Commit(rows=100),
).build(rdlt.Host()).run()
print(" ".join(starters))
sys.exit(0 if outcome.report.status is rdlt.RunStatus.SUCCEEDED else 3)
"""


@pytest.mark.skipif(
    sys.version_info < (3, 12), reason="CPython 3.11 audits no thread `_thread` starts"
)
def test_every_thread_rdlt_starts_is_started_by_a_thread_python_owns() -> None:
    finished = subprocess.run(
        [sys.executable, "-c", STARTS, str(PYTHON)],
        timeout=60,
        check=False,
        capture_output=True,
        text=True,
    )
    assert finished.returncode == 0, finished.stderr
    starters = finished.stdout.split()
    assert starters, "the run started threads"
    assert owned(starters), f"threads started by threads Python did not start: {starters}"


def exited(script: str) -> subprocess.CompletedProcess[str]:
    """How the script `script` of `EXITS` ended, within a minute."""
    return subprocess.run(
        [sys.executable, str(EXITS / script)],
        timeout=60,
        check=False,
        capture_output=True,
        text=True,
    )


def test_a_process_exits_with_its_own_status_while_transforms_flow_into_python() -> None:
    finished = exited("flowing.py")
    assert finished.returncode == 7, finished.stderr


def test_a_process_exits_with_its_own_status_while_a_thread_waits_inside_rust() -> None:
    finished = exited("waiting.py")
    assert "held while finalizing: True" in finished.stderr, finished.stderr
    assert finished.returncode == 7, finished.stderr
