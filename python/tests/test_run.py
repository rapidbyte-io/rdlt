"""Runs: their outcome, events, lag, stop modes, asyncio and Ctrl-C."""

from __future__ import annotations

import _thread
import asyncio
import contextlib
import queue
import sqlite3
import subprocess
import sys
import textwrap
import threading
import time
from collections.abc import Generator
from pathlib import Path

import pytest

import rdlt
from rdlt import Json
from tests.documents import generated


def host(tmp_path: Path, connectors: Path) -> rdlt.Host:
    return rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True)


def endless(tmp_path: Path) -> dict[str, Json]:
    document = generated("endless", 1_000_000_000_000, tmp_path / "out.db")
    document["commit"] = {"rows": 1000}
    return document


@contextlib.contextmanager
def held_back(database: Path) -> Generator[sqlite3.Connection]:
    """Holds the database's write lock: a run writing to it can commit nothing while it is held."""
    lock = sqlite3.connect(database, timeout=30, isolation_level=None, check_same_thread=False)
    try:
        lock.execute("BEGIN EXCLUSIVE")
        yield lock
    finally:
        lock.close()


def test_a_run_commits_every_row(tmp_path: Path, connectors: Path) -> None:
    pipeline = rdlt.Pipeline.from_document(generated("all", 1000, tmp_path / "out.db"))
    outcome = pipeline.build(host(tmp_path, connectors)).run()
    assert outcome.report.status is rdlt.RunStatus.SUCCEEDED
    assert outcome.report.rows == 1000
    assert outcome.error is None
    with sqlite3.connect(tmp_path / "out.db") as database:
        assert database.execute('SELECT count(*) FROM "rows"').fetchone() == (1000,)


def test_the_report_holds_the_facade_s_fields(tmp_path: Path, connectors: Path) -> None:
    pipeline = rdlt.Pipeline.from_document(generated("fields", 10, tmp_path / "out.db"))
    report = pipeline.build(host(tmp_path, connectors)).run().report
    expected = {"pipeline", "status", "attempted", "elapsed", "rows", "bytes", "commits"}
    assert expected | {"peak_memory", "streams"} <= set(report.raw)


def test_a_report_and_an_event_are_hashable(tmp_path: Path, connectors: Path) -> None:
    pipeline = rdlt.Pipeline.from_document(generated("hashed", 10, tmp_path / "out.db"))
    run = pipeline.build(host(tmp_path, connectors)).start()
    events = [event for event in run.events() if isinstance(event, rdlt.Event)]
    outcome = run.wait()
    assert hash(outcome.report) is not None
    assert hash(outcome) is not None
    assert hash(events[0]) is not None


def test_events_begin_at_the_first_and_end_with_the_run(tmp_path: Path, connectors: Path) -> None:
    pipeline = rdlt.Pipeline.from_document(generated("events", 2000, tmp_path / "out.db"))
    run = pipeline.build(host(tmp_path, connectors)).start()
    events = [event for event in run.events() if isinstance(event, rdlt.Event)]
    assert [event.seq for event in events] == list(range(1, len(events) + 1))
    assert events[0].kind == "attempt_started"
    assert events[-1].kind == "run_ended"
    assert run.wait().report.rows == 2000


def test_a_run_goes_on_while_only_its_events_are_held(tmp_path: Path, connectors: Path) -> None:
    pipeline = rdlt.Pipeline.from_document(generated("held", 500, tmp_path / "out.db"))
    kinds = [
        event.kind
        for event in pipeline.build(host(tmp_path, connectors)).start().events()
        if isinstance(event, rdlt.Event)
    ]
    assert kinds[-1] == "run_ended"
    with sqlite3.connect(tmp_path / "out.db") as database:
        assert database.execute('SELECT count(*) FROM "rows"').fetchone() == (500,)


def test_a_lag_is_told_as_how_many_events_were_missed() -> None:
    events = rdlt.Events(iter(['{"lagged": {"dropped": 3}}']), "t", None)  # pyright: ignore[reportArgumentType]
    assert next(events) == rdlt.Lagged(dropped=3)


def test_a_run_and_its_events_show_their_pipeline(tmp_path: Path, connectors: Path) -> None:
    pipeline = rdlt.Pipeline.from_document(generated("named", 10, tmp_path / "out.db"))
    run = pipeline.build(host(tmp_path, connectors)).start()
    events = run.events()
    assert repr(run) == "Run('named')"
    assert repr(events) == "Events('named')"
    run.wait()


def first_commit(run: rdlt.Run) -> None:
    for event in run.events():
        if isinstance(event, rdlt.Event) and event.kind == "committed":
            return
    pytest.fail("the run ended before it committed")


def test_a_stop_after_commit_ends_stopped(tmp_path: Path, connectors: Path) -> None:
    run = rdlt.Pipeline.from_document(endless(tmp_path)).build(host(tmp_path, connectors)).start()
    first_commit(run)
    run.stop(rdlt.StopMode.AFTER_COMMIT)
    outcome = run.wait()
    assert outcome.report.status is rdlt.RunStatus.STOPPED
    assert outcome.report.commits >= 1


def test_a_stop_now_ends_cancelled_within_seconds(tmp_path: Path, connectors: Path) -> None:
    run = rdlt.Pipeline.from_document(endless(tmp_path)).build(host(tmp_path, connectors)).start()
    first_commit(run)
    started = time.monotonic()
    run.stop("now")
    with pytest.raises(rdlt.StoppedError) as stopped:
        run.wait()
    assert time.monotonic() - started < 10
    assert stopped.value.outcome is not None
    assert stopped.value.outcome.report.status is rdlt.RunStatus.CANCELLED


def test_a_stop_refuses_a_mode_that_is_none_quoting_nothing(
    tmp_path: Path, connectors: Path
) -> None:
    run = rdlt.Pipeline.from_document(endless(tmp_path)).build(host(tmp_path, connectors)).start()
    with pytest.raises(ValueError, match="no such stop mode"):
        run.stop("sideways")  # type: ignore[arg-type]  # pyright: ignore[reportArgumentType]
    run.cancel()


def test_a_cancelled_run_raises_stopped(tmp_path: Path, connectors: Path) -> None:
    run = rdlt.Pipeline.from_document(endless(tmp_path)).build(host(tmp_path, connectors)).start()
    first_commit(run)
    run.cancel()
    with pytest.raises(rdlt.StoppedError):
        run.wait()
    with pytest.raises(rdlt.StoppedError):
        run.status()


def test_the_status_says_the_phase_and_the_run_so_far(tmp_path: Path, connectors: Path) -> None:
    run = rdlt.Pipeline.from_document(endless(tmp_path)).build(host(tmp_path, connectors)).start()
    first_commit(run)
    status = run.status()
    assert status.phase
    assert status.report.status is rdlt.RunStatus.RUNNING
    run.stop(rdlt.StopMode.AFTER_COMMIT)
    run.wait()


def test_a_reset_after_a_run_answers_what_it_reset(tmp_path: Path, connectors: Path) -> None:
    built = rdlt.Pipeline.from_document(generated("resetting", 10, tmp_path / "out.db")).build(
        host(tmp_path, connectors)
    )
    built.run()
    reset = built.reset("rows")
    assert reset["streams"] == ["rows"]
    assert reset["dropped"] == []
    assert isinstance(reset["epoch"], int)
    with sqlite3.connect(tmp_path / "out.db") as database:
        assert database.execute('SELECT count(*) FROM "rows"').fetchone() == (10,)


def test_a_reset_of_tables_drops_them_and_takes_an_identity(
    tmp_path: Path, connectors: Path
) -> None:
    built = rdlt.Pipeline.from_document(generated("dropping", 10, tmp_path / "out.db")).build(
        host(tmp_path, connectors)
    )
    built.run()
    reset = built.reset([rdlt.StreamName("rows")], tables=True)
    assert reset["streams"] == ["rows"]
    assert reset["dropped"]
    with sqlite3.connect(tmp_path / "out.db") as database:
        table = database.execute(
            "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'rows'"
        ).fetchone()
    assert table == (0,)


def test_a_run_and_its_events_await_in_asyncio(tmp_path: Path, connectors: Path) -> None:
    pipeline = rdlt.Pipeline.from_document(generated("async", 500, tmp_path / "out.db"))
    built = pipeline.build(host(tmp_path, connectors))

    async def main() -> tuple[int, list[str]]:
        run = built.start()
        kinds = [event.kind async for event in run.events() if isinstance(event, rdlt.Event)]
        outcome = await asyncio.wait_for(run.wait_async(), timeout=60)
        again = await asyncio.wait_for(built.run_async(), timeout=60)
        return outcome.report.rows + again.report.rows, kinds

    rows, kinds = asyncio.run(main())
    assert rows == 1000
    assert kinds[-1] == "run_ended"


def test_a_run_answers_a_second_event_loop_too(tmp_path: Path, connectors: Path) -> None:
    pipeline = rdlt.Pipeline.from_document(generated("two-loops", 10, tmp_path / "out.db"))
    run = pipeline.build(host(tmp_path, connectors)).start()
    first = asyncio.run(asyncio.wait_for(run.wait_async(), timeout=60))
    second = asyncio.run(asyncio.wait_for(run.wait_async(), timeout=60))
    assert first.report.rows == second.report.rows == 10


def test_a_first_ctrl_c_stops_a_blocking_run_after_its_commit(
    tmp_path: Path, connectors: Path
) -> None:
    built = rdlt.Pipeline.from_document(endless(tmp_path)).build(host(tmp_path, connectors))
    threading.Timer(1.0, _thread.interrupt_main).start()
    started = time.monotonic()
    outcome = built.run()
    assert outcome.report.status is rdlt.RunStatus.STOPPED
    assert time.monotonic() - started < 10


class Noted:
    """A standard error that tells `steps` when the first press's note is written to it."""

    def __init__(self, steps: queue.SimpleQueue[str]) -> None:
        self._steps = steps

    def write(self, text: str) -> int:
        """Tells of the first press's note; nothing is kept."""
        if "stopping" in text:
            self._steps.put("noted")
        return len(text)

    def flush(self) -> None:
        """Nothing is buffered."""


def test_a_second_ctrl_c_cancels_a_blocking_run_within_a_second(
    tmp_path: Path, connectors: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    built = rdlt.Pipeline.from_document(endless(tmp_path)).build(host(tmp_path, connectors))
    run = built.start()
    first_commit(run)
    steps: queue.SimpleQueue[str] = queue.SimpleQueue()
    noted = threading.Event()
    ended = threading.Event()
    second: list[float] = []
    monkeypatch.setattr(sys, "stderr", Noted(steps))

    def waiting() -> None:
        # A press can land before the native wait began, which raises here with no note written:
        # the wait is entered again and the press made again, wherever each one lands.
        while True:
            try:
                steps.put("entered")
                run.wait()
            except KeyboardInterrupt:
                if noted.is_set():
                    raise
            else:
                return

    with held_back(tmp_path / "out.db") as lock:
        # The run can commit nothing while the lock is held, so it cannot end before the second
        # press: each press follows what the wait has done, never a timing gap.
        def press() -> None:
            try:
                while steps.get(timeout=30) == "entered":
                    _thread.interrupt_main()
                noted.set()
                second.append(time.monotonic())
                _thread.interrupt_main()
                ended.wait(10)
            finally:
                # Where the second press cancels nothing, the run commits now, `wait` returns and
                # the test fails.
                lock.close()

        presser = threading.Thread(target=press, daemon=True)
        presser.start()
        with pytest.raises(KeyboardInterrupt):
            waiting()
        raised = time.monotonic()
        ended.set()
        presser.join(30)
    assert noted.is_set(), "the first press was not answered with its note"
    assert second, "the second press was not made"
    assert raised - second[0] < 1.5


def test_a_timeout_ends_the_wait_at_once(tmp_path: Path, connectors: Path) -> None:
    built = rdlt.Pipeline.from_document(endless(tmp_path)).build(host(tmp_path, connectors))

    async def timed_out(run: rdlt.Run) -> None:
        async with asyncio.timeout(0.5):
            await run.wait_async()

    async def main() -> float:
        run = built.start()
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            await timed_out(run)
        elapsed = time.monotonic() - started
        run.cancel()
        return elapsed

    assert asyncio.run(main()) < 5


def test_a_cancelled_await_raises_at_once_and_the_run_finishes_in_the_background(
    tmp_path: Path, connectors: Path
) -> None:
    built = rdlt.Pipeline.from_document(endless(tmp_path)).build(host(tmp_path, connectors))

    async def main() -> rdlt.Outcome:
        run = built.start()
        waiting = asyncio.ensure_future(run.wait_async())
        async for event in run.events():
            if isinstance(event, rdlt.Event) and event.kind == "committed":
                break
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        return await asyncio.wait_for(run.wait_async(), timeout=30)

    assert asyncio.run(main()).report.status is rdlt.RunStatus.STOPPED


def test_a_wait_cancelled_before_it_began_still_asks_for_the_stop(
    tmp_path: Path, connectors: Path
) -> None:
    built = rdlt.Pipeline.from_document(endless(tmp_path)).build(host(tmp_path, connectors))

    async def main() -> rdlt.Outcome:
        run = built.start()
        # A timeout of zero cancels the wait before its first step.
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(run.wait_async(), 0)
        return await asyncio.wait_for(run.wait_async(), timeout=60)

    assert asyncio.run(main()).report.status is rdlt.RunStatus.STOPPED


def test_a_cancelled_await_leaves_the_run_cancellable_by_its_handle(
    tmp_path: Path, connectors: Path
) -> None:
    built = rdlt.Pipeline.from_document(endless(tmp_path)).build(host(tmp_path, connectors))

    async def main() -> None:
        run = built.start()
        async for event in run.events():
            if isinstance(event, rdlt.Event) and event.kind == "committed":
                break
        # The run can commit nothing while the lock is held, so the stop the cancellation asks
        # for cannot end it before `cancel` does.
        with held_back(tmp_path / "out.db"):
            waiting = asyncio.ensure_future(run.wait_async())
            await asyncio.sleep(0.1)
            waiting.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiting
            run.cancel()
            with pytest.raises(rdlt.StoppedError):
                await asyncio.wait_for(run.wait_async(), timeout=30)

    asyncio.run(main())


def test_a_forked_child_may_not_stop_or_cancel_its_parent_s_run(
    tmp_path: Path, connectors: Path
) -> None:
    script = textwrap.dedent(
        f"""
        import os, pathlib, sys, warnings
        warnings.simplefilter("ignore")
        import rdlt
        sys.path.insert(0, {str(Path(__file__).parents[1])!r})
        from tests.documents import generated
        host = rdlt.Host(
            connectors={str(connectors)!r},
            writable=[{str(tmp_path)!r}],
            trust_binaries=True,
        )
        database = pathlib.Path({str(tmp_path / "out.db")!r})
        document = generated("forked", 1_000_000_000, database)
        run = rdlt.Pipeline.from_document(document).build(host).start()
        child = os.fork()
        if child == 0:
            for control in (run.stop, run.cancel):
                try:
                    control()
                except rdlt.InternalError:
                    continue
                os._exit(3)
            os._exit(0)
        _, status = os.waitpid(child, 0)
        run.cancel()
        sys.exit(os.waitstatus_to_exitcode(status))
        """
    )
    finished = subprocess.run(
        [sys.executable, "-c", script], timeout=60, check=False, capture_output=True
    )
    assert finished.returncode == 0, finished.stderr
