"""Which threads start rdlt's daemon threads and answer its awaits, and exits as they run."""

from __future__ import annotations

import asyncio
import itertools
import logging
import subprocess
import sys
import textwrap
import threading
import time
from collections.abc import Callable, Iterator
from contextvars import Context
from pathlib import Path
from typing import cast
from unittest import mock

import pyarrow as pa
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

# Each start of rdlt's, by the thread that made it and whether it was a `Thread`'s; and the threads
# Python started, as their starts answered them, with the main thread. None of them ends, so no
# thread of Rust's takes one's ident.
starts = []
started = {_thread.get_ident()}


def rdlt_s(function):
    target = getattr(getattr(function, "__self__", None), "_target", function)
    return getattr(target, "__module__", None) == "rdlt._reading"


def start(function):
    return _thread.get_ident(), isinstance(getattr(function, "__self__", None), threading.Thread)


def audited(event, arguments):
    if event in {"_thread.start_new_thread", "_thread.start_joinable_thread"}:
        if rdlt_s(arguments[0]):
            starts.append(start(arguments[0]))


def recording(starting, ident):
    def recorded(function, *arguments, **keywords):
        if sys.argv[2] == "wrapped" and rdlt_s(function):
            starts.append(start(function))
        begun = starting(function, *arguments, **keywords)
        started.add(ident(begun))
        return begun

    return recorded


if sys.argv[2] == "audited":
    sys.addaudithook(audited)
_thread.start_new_thread = recording(_thread.start_new_thread, int)
if sys.version_info >= (3, 13):
    threading._start_joinable_thread = recording(
        threading._start_joinable_thread, lambda handle: handle.ident
    )
else:
    threading._start_new_thread = recording(threading._start_new_thread, int)
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
dummy = vars(threading)["_DummyThread"]
print(
    sum(not of_threading for _, of_threading in starts),
    sum(of_threading for _, of_threading in starts),
    sum(starter not in started for starter, _ in starts),
    sum(isinstance(thread, dummy) for thread in threading.enumerate()),
)
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
    raw, threads, unowned, lent = (int(count) for count in finished.stdout.split())
    assert raw == 1, "a thread `_thread` started started the first daemon thread"
    assert threads >= 2, "`threading` started the first daemon thread and a spare"
    assert not unowned, f"{unowned} of rdlt's threads were started by threads Python did not start"
    assert not lent, f"`threading` made {lent} dummies of threads it did not start"


def test_calls_run_on_daemon_threads_threading_started_never_on_ones_it_was_lent() -> None:
    ran_on: list[threading.Thread] = []

    # `logging` asks `threading` which thread it runs on, as a user's function may at any moment.
    @stream(name="rows")
    def logging_rows() -> Iterator[object]:
        ran_on.append(threading.current_thread())
        logging.getLogger(__name__).warning("reading")
        for n in range(1000):
            yield {"id": n}

    @rdlt.transform()
    def kept(batch: pa.RecordBatch) -> pa.RecordBatch:
        ran_on.append(threading.current_thread())
        return batch

    outcome = (
        rdlt.pipeline(
            "lent",
            source=logging_rows,
            destination=rdlt.local(memory.Memory, config={"store": "lent"}),
            streams=[rdlt.stream("rows", transform=[kept])],
            commit=rdlt.Commit(rows=100),
        )
        .build(rdlt.Host())
        .run()
    )
    assert outcome.report.status is rdlt.RunStatus.SUCCEEDED
    dummy = cast("type[threading.Thread]", vars(threading)["_DummyThread"])
    assert ran_on, "the stream function and the transform ran"
    lent = [thread for thread in ran_on if isinstance(thread, dummy)]
    assert not lent, f"calls ran on threads threading was lent: {lent}"
    rdlt_s = [thread for thread in threading.enumerate() if thread.name.startswith("rdlt-")]
    assert rdlt_s, "threading holds rdlt's daemon threads"
    unowned = [thread for thread in rdlt_s if isinstance(thread, dummy) or not thread.daemon]
    assert not unowned, f"rdlt's threads threading did not start as daemons: {unowned}"


def test_a_run_whose_first_daemon_thread_does_not_start_fails_rather_than_waits() -> None:
    script = textwrap.dedent(
        f"""
        import sys, threading
        sys.path.append({str(PYTHON)!r})

        def refused(*_):
            raise RuntimeError("no threads left")

        threading.Thread.start = refused
        import rdlt
        from rdlt.connector import stream
        from tests.connectors import memory

        @stream(name="rows")
        def rows():
            yield {{"id": 1}}

        try:
            rdlt.pipeline(
                "refused",
                source=rows,
                destination=rdlt.local(memory.Memory, config={{"store": "refused"}}),
                retry=rdlt.Retry(max_attempts=1),
            ).build(rdlt.Host()).run()
        except rdlt.Error:
            sys.exit(0)
        sys.exit(3)
        """
    )
    finished = subprocess.run(
        [sys.executable, "-c", script], timeout=60, check=False, capture_output=True, text=True
    )
    assert finished.returncode == 0, finished.stderr


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


def waits_for_a_call(ident: int) -> bool:
    """Whether the daemon thread `ident` is back waiting for a call, within ten seconds."""
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        frame = sys._current_frames().get(ident)  # pyright: ignore[reportPrivateUsage]
        if frame is not None and frame.f_code.co_name == "_turn":
            return True
        time.sleep(0.01)
    return False


def endless_run() -> rdlt.Run:
    return (
        rdlt.pipeline(
            "awaited", source=endless, destination=rdlt.local(memory.Memory, config={"store": "a"})
        )
        .build(rdlt.Host())
        .start()
    )


def test_an_await_whose_loop_closed_before_its_answer_leaves_its_daemon_thread_waiting() -> None:
    run = endless_run()
    loop = asyncio.new_event_loop()

    async def begun() -> None:
        run._native.wait_async()  # pyright: ignore[reportPrivateUsage]

    loop.run_until_complete(begun())
    handed = loop.call_soon_threadsafe
    callers: list[int] = []

    def recorded(
        callback: Callable[..., object], *arguments: object, context: Context | None = None
    ) -> asyncio.Handle:
        callers.append(threading.get_ident())
        return handed(callback, *arguments, context=context)

    with mock.patch.object(loop, "call_soon_threadsafe", side_effect=recorded):
        loop.close()
        run.cancel()
        deadline = time.monotonic() + 30
        while not callers:
            assert time.monotonic() < deadline, "the await's answer was handed over"
            time.sleep(0.01)
    assert waits_for_a_call(callers[0]), "its daemon thread went back to waiting for a call"


@pytest.mark.parametrize("before", ["cancelled", "done"])
def test_an_await_done_before_its_answer_lands_takes_none_and_its_thread_waits_on(
    before: str,
) -> None:
    raised: list[dict[str, object]] = []
    callers: list[int] = []

    async def main() -> None:
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(lambda _, context: raised.append(context))
        run = endless_run()
        native = run._native  # pyright: ignore[reportPrivateUsage]
        awaited = cast("asyncio.Future[object]", native.wait_async())
        handed = loop.call_soon_threadsafe
        landed = threading.Event()

        def recorded(
            callback: Callable[..., object], *arguments: object, context: Context | None = None
        ) -> asyncio.Handle:
            callers.append(threading.get_ident())
            landed.set()
            return handed(callback, *arguments, context=context)

        with mock.patch.object(loop, "call_soon_threadsafe", side_effect=recorded):
            run.cancel()
            # The loop is held until the answer is on its way, so it lands on a future done.
            assert landed.wait(30), "the await's answer was handed over"
            if before == "cancelled":
                awaited.cancel()
            else:
                awaited.set_result("first")
            await asyncio.sleep(0.1)
        if before == "done":
            assert awaited.result() == "first"

    asyncio.run(main())
    assert not raised, f"the event loop met {raised}"
    assert waits_for_a_call(callers[0]), "its daemon thread went back to waiting for a call"
