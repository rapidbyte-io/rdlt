"""Which threads start rdlt's daemon threads and answer its awaits, and exits as they run."""

from __future__ import annotations

import asyncio
import itertools
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from contextvars import Context
from pathlib import Path
from unittest import mock

import pytest

import rdlt
from rdlt.connector import stream
from tests.connectors import memory

PYTHON = Path(__file__).resolve().parents[1]
"""The directory `tests` imports from, which a script appends to its path."""
EXITS = Path(__file__).resolve().parent / "exits"
"""Scripts that end as a user's do, run once here and many times over by the nightly workflow."""


def owned(names: list[str]) -> bool:
    """Whether each of `names` is a thread's that Python started and named, not one it was lent."""
    return all(name != "unknown" and not name.startswith("Dummy-") for name in names)


STARTS = """
import _thread, sys, threading

starters = []


def started(function):
    if function.__module__ == "rdlt._reading":
        known = vars(threading)["_active"].get(threading.get_ident())
        starters.append("unknown" if known is None else known.name)


def audited(event, arguments):
    if event == "_thread.start_new_thread":
        started(arguments[0])


start_new_thread = _thread.start_new_thread


def recorded(function, *arguments):
    started(function)
    return start_new_thread(function, *arguments)


if sys.argv[2] == "audited":
    sys.addaudithook(audited)
else:
    _thread.start_new_thread = recorded
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


@pytest.mark.parametrize(
    "seen",
    [
        pytest.param(
            "audited",
            marks=pytest.mark.skipif(
                sys.version_info < (3, 12), reason="CPython 3.11 audits no thread `_thread` starts"
            ),
        ),
        "wrapped",
    ],
)
def test_every_thread_rdlt_starts_is_started_by_a_thread_python_owns(seen: str) -> None:
    finished = subprocess.run(
        [sys.executable, "-c", STARTS, str(PYTHON), seen],
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


def starter() -> str:
    """The name `threading` knows the calling thread by, read without making it known."""
    known = vars(threading)["_active"].get(threading.get_ident())
    return "unknown" if known is None else known.name


@stream(name="rows")
def rows() -> Iterator[object]:
    for n in range(100):
        yield {"id": n}


@stream(name="rows")
def endless() -> Iterator[object]:
    for n in itertools.count():
        yield {"id": n}
        time.sleep(0.001)


@pytest.mark.parametrize("end", ["finished", "cancelled"])
def test_an_await_of_a_run_is_answered_from_a_thread_python_owns(end: str) -> None:
    callers: list[str] = []

    async def main() -> None:
        loop = asyncio.get_running_loop()
        handed = loop.call_soon_threadsafe

        def recorded(
            callback: Callable[..., object], *arguments: object, context: Context | None = None
        ) -> asyncio.Handle:
            callers.append(starter())
            return handed(callback, *arguments, context=context)

        built = rdlt.pipeline(
            "awaited",
            source=rows if end == "finished" else endless,
            destination=rdlt.local(memory.Memory, config={"store": "awaited"}),
        ).build(rdlt.Host())
        with mock.patch.object(loop, "call_soon_threadsafe", side_effect=recorded):
            run = built.start()
            if end == "finished":
                outcome = await asyncio.wait_for(run.wait_async(), timeout=60)
                assert outcome.report.status is rdlt.RunStatus.SUCCEEDED
                return
            run.cancel()
            with pytest.raises(rdlt.StoppedError):
                await asyncio.wait_for(run.wait_async(), timeout=60)

    asyncio.run(main())
    assert callers, "the await's answer was handed to the event loop"
    assert owned(callers), f"handed to asyncio by threads Python did not start: {callers}"
