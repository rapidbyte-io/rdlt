"""Python functions as in-process sources."""

from __future__ import annotations

import asyncio
import contextlib
import gc
import json
import logging
import sqlite3
import subprocess
import sys
import textwrap
import threading
import time
import weakref
from collections.abc import AsyncIterator, Iterator, Sequence
from contextvars import ContextVar
from pathlib import Path
from typing import cast

import pyarrow as pa
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import rdlt
from rdlt import _reading


def host(tmp_path: Path, connectors: Path) -> rdlt.Host:
    return rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True)


def into_sqlite(tmp_path: Path) -> rdlt.Connector:
    return rdlt.Connector(
        "io.rapidbyte.sqlite",
        config={"path": str(tmp_path / "out.db")},
        access=rdlt.Access(write=[tmp_path]),
    )


def rows_of(tmp_path: Path, table: str) -> list[tuple[object, ...]]:
    with sqlite3.connect(tmp_path / "out.db") as database:
        columns = [
            row[1]
            for row in database.execute(f'PRAGMA table_info("{table}")')
            if not str(row[1]).startswith("_rdlt")
        ]
        selected = ", ".join(f'"{column}"' for column in sorted(columns))
        return sorted(database.execute(f'SELECT {selected} FROM "{table}"').fetchall(), key=repr)


def test_yielded_dicts_land_once(tmp_path: Path, connectors: Path) -> None:
    @rdlt.resource(primary_key="id")
    def users() -> Iterator[object]:
        yield [{"id": n, "name": f"user {n}"} for n in range(100)]
        yield {"id": 100, "name": "the last"}

    built = rdlt.pipeline("users", source=users, destination=into_sqlite(tmp_path))
    outcome = built.build(host(tmp_path, connectors)).run()
    assert outcome.report.rows == 101
    assert len(rows_of(tmp_path, "users")) == 101


def test_arrow_batches_and_tables_land(tmp_path: Path, connectors: Path) -> None:
    @rdlt.resource()
    def measured() -> Iterator[object]:
        yield pa.record_batch({"n": pa.array(range(10), pa.int64())})
        yield pa.table({"n": pa.array(range(10, 25), pa.int64())})

    rdlt.pipeline("measured", source=measured, destination=into_sqlite(tmp_path)).build(
        host(tmp_path, connectors)
    ).run()
    assert len(rows_of(tmp_path, "measured")) == 25


@pytest.mark.parametrize("library", ["polars", "duckdb", "pandas"])
def test_other_libraries_results_land(library: str, tmp_path: Path, connectors: Path) -> None:
    module = pytest.importorskip(library)

    @rdlt.resource(name="produced")
    def produced() -> Iterator[object]:
        if library == "polars":
            yield module.DataFrame({"n": list(range(7))})
        elif library == "duckdb":
            yield module.sql("SELECT * FROM range(7) t(n)")
        else:
            yield pa.Table.from_pandas(module.DataFrame({"n": list(range(7))}))

    rdlt.pipeline("produced", source=produced, destination=into_sqlite(tmp_path)).build(
        host(tmp_path, connectors)
    ).run()
    assert len(rows_of(tmp_path, "produced")) == 7


def test_a_transient_failure_retries_from_the_last_committed_checkpoint(
    tmp_path: Path, connectors: Path
) -> None:
    class Since(rdlt.Cursor):
        after: int = -1

    started_from: list[int] = []

    def checkpoint_committed() -> None:
        # A row past the first checkpoint carries it in its commit: failing sooner, the retry
        # rightly starts from the beginning.
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            with contextlib.suppress(sqlite3.OperationalError):
                if len(rows_of(tmp_path, "numbers")) > 10:
                    return
            time.sleep(0.01)
        raise AssertionError("no commit landed")

    @rdlt.resource(cursor=Since)
    def numbers(since: Since) -> Iterator[object]:
        started_from.append(since.after)
        for n in range(since.after + 1, 100):
            yield {"id": n}
            if n % 10 == 9:
                yield rdlt.checkpoint(Since(after=n))
            if n == 54 and len(started_from) == 1:
                checkpoint_committed()
                raise ConnectionResetError("the API went away")

    built = rdlt.pipeline(
        "retried",
        source=numbers,
        destination=into_sqlite(tmp_path),
        commit=rdlt.Commit(rows=1),
        retry=rdlt.Retry(max_attempts=3),
    )
    outcome = built.build(host(tmp_path, connectors)).run()
    assert outcome.report.status is rdlt.RunStatus.SUCCEEDED
    assert started_from[0] == -1
    assert started_from[1] in {9, 19, 29, 39, 49}
    assert len(rows_of(tmp_path, "numbers")) == 100


def test_an_exception_fails_the_run_with_its_type_message_and_traceback(
    tmp_path: Path, connectors: Path
) -> None:
    @rdlt.resource()
    def broken() -> Iterator[object]:
        yield {"n": 1}
        raise ValueError("page 2 is malformed")

    built = rdlt.pipeline(
        "broken", source=broken, destination=into_sqlite(tmp_path), retry=rdlt.Retry(max_attempts=1)
    )
    with pytest.raises(rdlt.SourceError) as raised:
        built.build(host(tmp_path, connectors)).run()
    report = raised.value.report()
    assert report.connector_code == "value_error"
    assert any("page 2 is malformed" in cause for cause in report.causes)
    assert any(cause.startswith("Traceback (most recent call last)") for cause in report.causes)
    assert raised.value.outcome is not None
    assert raised.value.outcome.report.status is rdlt.RunStatus.FAILED


JSON = st.recursive(
    st.none()
    | st.booleans()
    | st.integers(-(2**63), 2**63 - 1)
    | st.floats(allow_nan=False, allow_infinity=False)
    | st.text(st.characters(categories=("L", "M", "N", "P", "S", "Zs")), max_size=10),
    lambda children: (
        st.lists(children, max_size=3)
        | st.dictionaries(st.sampled_from(["x", "y"]), children, max_size=2)
    ),
    max_leaves=6,
)
ROWS = st.lists(
    st.dictionaries(st.sampled_from(["a", "b", "c"]), JSON, min_size=1), min_size=1, max_size=20
)


@settings(
    max_examples=20, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)
@given(rows=ROWS)
def test_dicts_load_as_the_rust_source_s_same_json_does(
    rows: list[dict[str, rdlt.Json]], tmp_path_factory: pytest.TempPathFactory, connectors: Path
) -> None:
    ours, theirs = tmp_path_factory.mktemp("ours"), tmp_path_factory.mktemp("theirs")

    @rdlt.resource(name="rows")
    def drawn() -> Iterator[object]:
        yield rows

    memory = rdlt.Connector("io.rapidbyte.memory", config={"streams": {"rows": list(rows)}})
    landed = [
        loaded("ours", drawn, ours, connectors),
        loaded("theirs", memory, theirs, connectors, streams=[rdlt.stream("rows")]),
    ]
    assert canonical(landed[0]) == canonical(landed[1])


def canonical(landed: object) -> object:
    """`landed` sorted with each JSON text's keys sorted, as a document's reader writes them."""
    if not isinstance(landed, list):
        return landed

    def cell_of(cell: object) -> object:
        return (
            json.dumps(json.loads(cell), sort_keys=True)
            if isinstance(cell, str) and cell[:1] in {"{", "["}
            else cell
        )

    rows = cast("list[tuple[object, ...]]", landed)
    return sorted([tuple(cell_of(cell) for cell in row) for row in rows], key=repr)


def loaded(
    name: str,
    source: rdlt.Resource | rdlt.Connector,
    into: Path,
    connectors: Path,
    *,
    streams: Sequence[rdlt.Stream] = (),
) -> object:
    """What landed from `source`: the rows, or the refusal both sides fail with alike."""
    built = rdlt.pipeline(name, source=source, destination=into_sqlite(into), streams=streams)
    try:
        built.build(rdlt.Host(connectors=connectors, writable=[into], trust_binaries=True)).run()
    except rdlt.DestinationError as refused:
        report = refused.report()
        return (type(refused), report.code, report.connector_code)
    return rows_of(into, "rows")


def test_stop_now_returns_while_a_resource_blocks(tmp_path: Path, connectors: Path) -> None:
    release = threading.Event()
    waiting = threading.Event()

    @rdlt.resource()
    def blocked() -> Iterator[object]:
        yield {"n": 1}
        waiting.set()
        release.wait(60)
        yield {"n": 2}

    run = (
        rdlt.pipeline("blocked", source=blocked, destination=into_sqlite(tmp_path))
        .build(host(tmp_path, connectors))
        .start()
    )
    assert waiting.wait(30)
    started = time.monotonic()
    run.stop(rdlt.StopMode.NOW)
    with pytest.raises(rdlt.StoppedError):
        run.wait()
    assert time.monotonic() - started < 10
    release.set()


def exits_with_seven(tmp_path: Path, connectors: Path, resource: str) -> int:
    """The status of a script that exits, with a run in flight, once `started` is set inside it."""
    out = str(tmp_path / "out.db")
    tmp = str(tmp_path)
    script = "\n".join(
        [
            "import itertools, sys, threading, time",
            "import rdlt",
            "",
            "started = threading.Event()",
            textwrap.dedent(resource),
            "",
            f"host = rdlt.Host(connectors={str(connectors)!r}, writable=[{tmp!r}],",
            "                 trust_binaries=True)",
            f'sqlite = rdlt.Connector("io.rapidbyte.sqlite", config={{"path": {out!r}}},',
            f"                        access=rdlt.Access(write=[{tmp!r}]))",
            'built = rdlt.pipeline("exiting", source=source, destination=sqlite,',
            "                      commit=rdlt.Commit(rows=100)).build(host)",
            "run = built.start()",
            "if not started.wait(30):",
            "    sys.exit(3)",
            "sys.exit(7)",
            "",
        ]
    )
    finished = subprocess.run([sys.executable, "-c", script], timeout=60, check=False)
    return finished.returncode


def test_a_process_exits_while_a_read_is_flowing(tmp_path: Path, connectors: Path) -> None:
    resource = """
    class Since(rdlt.Cursor):
        after: int = -1

    @rdlt.resource(cursor=Since)
    def source(since):
        for n in itertools.count(since.after + 1):
            yield {"n": n}
            yield rdlt.checkpoint(Since(after=n))
            started.set()
    """
    assert exits_with_seven(tmp_path, connectors, resource) == 7


def test_a_process_exits_while_a_resource_polls_in_python(tmp_path: Path, connectors: Path) -> None:
    resource = """
    @rdlt.resource()
    def source():
        yield {"n": 0}
        started.set()
        while True:
            time.sleep(0.003)
    """
    assert exits_with_seven(tmp_path, connectors, resource) == 7


def test_a_process_exits_while_a_resource_lists_its_partitions(
    tmp_path: Path, connectors: Path
) -> None:
    resource = """
    def partitions():
        started.set()
        while True:
            time.sleep(0.001)

    @rdlt.resource(partitions=partitions)
    def source(ctx):
        yield {"n": 0}
    """
    assert exits_with_seven(tmp_path, connectors, resource) == 7


class _Since(rdlt.Cursor):
    after: int = -1


@pytest.mark.parametrize("write", [None, rdlt.merge("id")], ids=["append", "merge"])
def test_a_resource_with_a_cursor_resumes_the_next_run_and_adds_no_duplicate(
    write: rdlt.WriteMode | None, tmp_path: Path, connectors: Path
) -> None:
    started_from: list[int] = []

    @rdlt.resource(primary_key="id", write=write, cursor=_Since)
    def users(since: _Since) -> Iterator[object]:
        started_from.append(since.after)
        for n in range(since.after + 1, 3):
            yield {"id": n}
        yield rdlt.checkpoint(_Since(after=2))

    built = rdlt.pipeline("resumed", source=users, destination=into_sqlite(tmp_path))
    every_host = host(tmp_path, connectors)
    built.build(every_host).run()
    built.build(every_host).run()
    assert started_from == [-1, 2]
    assert len(rows_of(tmp_path, "users")) == 3


@pytest.mark.parametrize("write", [None, rdlt.merge("id")], ids=["append", "merge"])
def test_a_read_that_ends_on_rows_resumes_from_its_last_checkpoint_and_loses_none(
    write: rdlt.WriteMode | None, tmp_path: Path, connectors: Path
) -> None:
    available = [7]
    finishing = [False]
    started_from: list[int] = []

    @rdlt.resource(primary_key="id", write=write, cursor=_Since)
    def users(since: _Since) -> Iterator[object]:
        started_from.append(since.after)
        for n in range(since.after + 1, available[0]):
            yield {"id": n}
            if n % 3 == 2:
                yield rdlt.checkpoint(_Since(after=n))
        if finishing[0]:
            yield rdlt.checkpoint(_Since(after=available[0] - 1))

    built = rdlt.pipeline("partial", source=users, destination=into_sqlite(tmp_path))
    every_host = host(tmp_path, connectors)

    built.build(every_host).run()
    assert rows_of(tmp_path, "users") == [(n,) for n in range(6)]

    available[0], finishing[0] = 10, True
    built.build(every_host).run()
    assert rows_of(tmp_path, "users") == [(n,) for n in range(10)]

    outcome = built.build(every_host).run()
    assert outcome.report.rows == 0
    assert len(rows_of(tmp_path, "users")) == 10
    assert started_from == [-1, 5, 9]


def test_a_resource_with_a_cursor_that_never_checkpoints_fails_naming_it(
    tmp_path: Path, connectors: Path
) -> None:
    @rdlt.resource(cursor=_Since)
    def users(since: _Since) -> Iterator[object]:
        del since
        yield {"id": 1}

    built = rdlt.pipeline(
        "unmarked",
        source=users,
        destination=into_sqlite(tmp_path),
        retry=rdlt.Retry(max_attempts=1),
    )
    with pytest.raises(rdlt.Error) as raised:
        built.build(host(tmp_path, connectors)).run()
    assert raised.value.report().code == "checkpoint_missing"


def test_a_resumed_read_that_yields_rows_and_never_checkpoints_fails(
    tmp_path: Path, connectors: Path
) -> None:
    checkpointing = [True]

    @rdlt.resource(cursor=_Since)
    def users(since: _Since) -> Iterator[object]:
        yield {"id": since.after + 1}
        if checkpointing[0]:
            yield rdlt.checkpoint(_Since(after=since.after + 1))

    built = rdlt.pipeline(
        "unmarked",
        source=users,
        destination=into_sqlite(tmp_path),
        retry=rdlt.Retry(max_attempts=1),
    )
    every_host = host(tmp_path, connectors)
    built.build(every_host).run()

    checkpointing[0] = False
    with pytest.raises(rdlt.Error) as raised:
        built.build(every_host).run()
    assert raised.value.report().code == "checkpoint_missing"


def test_a_stream_named_twice_is_refused_by_the_plan(tmp_path: Path, connectors: Path) -> None:
    @rdlt.resource()
    def users() -> Iterator[object]:
        yield {"id": 1}

    built = rdlt.pipeline(
        "twice",
        source=users,
        destination=into_sqlite(tmp_path),
        streams=[
            rdlt.stream("users", write=rdlt.append()),
            rdlt.stream("users", write=rdlt.replace()),
        ],
    )
    with pytest.raises(rdlt.Error) as refused:
        built.build(host(tmp_path, connectors)).run()
    assert refused.value.code == "plan_duplicate_stream"


def test_naming_a_resource_in_streams_keeps_what_it_does_not_change(
    tmp_path: Path, connectors: Path
) -> None:
    @rdlt.resource(primary_key="id", write=rdlt.merge("id"))
    def merged() -> Iterator[object]:
        yield [{"id": n} for n in range(3)]

    built = rdlt.pipeline(
        "merged",
        source=merged,
        destination=into_sqlite(tmp_path),
        streams=[rdlt.stream("merged", read="full")],
    )
    every_host = host(tmp_path, connectors)
    built.build(every_host).run()
    built.build(every_host).run()
    assert len(rows_of(tmp_path, "merged")) == 3


def test_an_entry_in_streams_overrides_what_it_sets(tmp_path: Path, connectors: Path) -> None:
    @rdlt.resource(primary_key="id", write=rdlt.merge("id"))
    def merged() -> Iterator[object]:
        yield [{"id": n} for n in range(3)]

    built = rdlt.pipeline(
        "overridden",
        source=merged,
        destination=into_sqlite(tmp_path),
        streams=[rdlt.stream("merged", write=rdlt.append())],
    )
    every_host = host(tmp_path, connectors)
    built.build(every_host).run()
    built.build(every_host).run()
    assert len(rows_of(tmp_path, "merged")) == 6


def test_an_object_with_only_aiter_is_read(tmp_path: Path, connectors: Path) -> None:
    class Numbers:
        def __aiter__(self) -> AsyncIterator[object]:
            return self._numbers()

        async def _numbers(self) -> AsyncIterator[object]:
            yield {"n": 1}
            yield {"n": 2}

    @rdlt.resource()
    def counted() -> Numbers:
        return Numbers()

    rdlt.pipeline("counted", source=counted, destination=into_sqlite(tmp_path)).build(
        host(tmp_path, connectors)
    ).run()
    assert len(rows_of(tmp_path, "counted")) == 2


def test_an_async_resource_is_read_in_one_task_of_a_running_loop(
    tmp_path: Path, connectors: Path
) -> None:
    held: ContextVar[int] = ContextVar("held")

    class Numbers:
        def __aiter__(self) -> AsyncIterator[object]:
            asyncio.get_running_loop()
            return self._numbers()

        async def _numbers(self) -> AsyncIterator[object]:
            token = held.set(7)
            try:
                async with asyncio.timeout(60):
                    yield {"n": held.get()}
                    yield {"n": held.get()}
            finally:
                held.reset(token)

    @rdlt.resource()
    def counted() -> Numbers:
        return Numbers()

    rdlt.pipeline(
        "counted",
        source=counted,
        destination=into_sqlite(tmp_path),
        retry=rdlt.Retry(max_attempts=1),
    ).build(host(tmp_path, connectors)).run()
    assert rows_of(tmp_path, "counted") == [(7,), (7,)]


def test_a_task_an_async_resource_leaves_running_is_cancelled_and_cleans_up(
    tmp_path: Path, connectors: Path
) -> None:
    cleaned: list[bool] = []

    async def forever() -> None:
        try:
            await asyncio.sleep(3600)
        finally:
            await asyncio.sleep(0)
            cleaned.append(True)

    @rdlt.resource()
    async def leaving() -> AsyncIterator[object]:
        left = asyncio.create_task(forever())
        await asyncio.sleep(0)
        yield {"done": left.done()}

    rdlt.pipeline("leaving", source=leaving, destination=into_sqlite(tmp_path)).build(
        host(tmp_path, connectors)
    ).run()
    assert cleaned == [True]


def test_an_async_read_leaves_its_event_loop_running_while_the_engine_is_full() -> None:
    class Full:
        room = False
        room_at_push: bool | None = None

        def has_room(self) -> bool:
            return self.room

        def push(self, _item: object) -> bool:
            self.room_at_push = self.room
            return True

    out = Full()

    async def opening() -> None:
        out.room = True

    async def reading() -> AsyncIterator[object]:
        opened = asyncio.create_task(opening())
        yield {"n": 1}
        await opened

    asyncio.run(_reading.pushed(reading(), cast("_reading.Out", out)))
    assert out.room_at_push is True


def test_a_string_is_not_a_list_of_partitions(tmp_path: Path, connectors: Path) -> None:
    def one_string() -> Sequence[str]:
        return "abc"

    @rdlt.resource(partitions=one_string)
    def split(ctx: rdlt.StreamContext) -> Iterator[object]:
        yield {"partition": ctx.partition}

    built = rdlt.pipeline("split", source=split, destination=into_sqlite(tmp_path))
    with pytest.raises(rdlt.SourceError) as raised:
        built.build(host(tmp_path, connectors)).run()
    assert raised.value.report().connector_code == "type_error"


def test_a_partitioned_resource_that_takes_no_ctx_is_refused() -> None:
    def naive() -> Iterator[object]:
        yield {"n": 1}

    with pytest.raises(TypeError, match="ctx"):
        rdlt.resource(partitions=lambda: ["a", "b"])(naive)


class Holder:
    """What a resource keeps, which holds the pipeline the resource serves."""

    pipeline: rdlt.Pipeline | None = None

    def read(self) -> Iterator[object]:
        """Yields one row that says what it holds."""
        yield {"held": self.pipeline is not None}


def test_a_resource_holding_its_own_pipeline_is_collected(tmp_path: Path, connectors: Path) -> None:
    holder = Holder()
    rows = rdlt.resource(name="rows")(holder.read)
    holder.pipeline = rdlt.pipeline("cycle", source=rows, destination=into_sqlite(tmp_path)).build(
        host(tmp_path, connectors)
    )
    collected = weakref.ref(holder)
    del holder, rows
    gc.collect()
    assert collected() is None


def test_a_built_pipeline_and_its_run_keep_its_resource(tmp_path: Path, connectors: Path) -> None:
    @rdlt.resource(name="rows")
    def rows() -> Iterator[object]:
        yield {"n": 1}

    built = rdlt.pipeline("kept", source=rows, destination=into_sqlite(tmp_path)).build(
        host(tmp_path, connectors)
    )
    kept = weakref.ref(rows)
    del rows
    gc.collect()
    assert kept() is not None, "a built pipeline reads with its resource"
    run = built.start()
    del built
    gc.collect()
    assert kept() is not None, "a run in progress reads with its resource"
    run.wait()


def test_a_source_s_failure_says_its_type_and_message_once(
    tmp_path: Path, connectors: Path
) -> None:
    @rdlt.resource(name="failing")
    def failing() -> Iterator[object]:
        yield {"n": 1}
        raise ConnectionRefusedError("the api is down")

    builder = rdlt.pipeline(
        "failing",
        source=failing,
        destination=into_sqlite(tmp_path),
        retry=rdlt.Retry(max_attempts=1),
    )
    with pytest.raises(rdlt.SourceError) as raised:
        builder.build(host(tmp_path, connectors)).run()
    assert str(raised.value).count("ConnectionRefusedError: the api is down") == 1


def ended_threads() -> list[threading.Thread]:
    """What `threading` holds of threads that have ended."""
    running = sys._current_frames()  # pyright: ignore[reportPrivateUsage]
    return [thread for thread in threading.enumerate() if thread.ident not in running]


def test_a_read_s_threads_leave_nothing_in_threading_once_they_end(
    tmp_path: Path, connectors: Path
) -> None:
    @rdlt.resource(name="parted", partitions=lambda: [str(n) for n in range(32)])
    def parted(ctx: rdlt.StreamContext) -> Iterator[object]:
        yield {"partition": ctx.partition}

    built = rdlt.pipeline("parted", source=parted, destination=into_sqlite(tmp_path))
    assert built.build(host(tmp_path, connectors)).run().report.rows == 32
    deadline = time.monotonic() + 30
    while ended := ended_threads():
        assert time.monotonic() < deadline, f"threading holds {len(ended)} threads that ended"
        time.sleep(0.05)


class Logged:
    """A partition's rows, which log as they are let go, as a client closing its connection does."""

    def __init__(self, partition: str | None) -> None:
        self._rows = iter([{"partition": partition}])

    def __iter__(self) -> Logged:
        return self

    def __next__(self) -> object:
        return next(self._rows)

    def __del__(self) -> None:
        logging.getLogger(__name__).warning("closed")


def test_rows_that_log_as_they_are_let_go_leave_nothing_in_threading(
    tmp_path: Path, connectors: Path
) -> None:
    @rdlt.resource(name="logged", partitions=lambda: [str(n) for n in range(8)])
    def logged(ctx: rdlt.StreamContext) -> Iterator[object]:
        return Logged(ctx.partition)

    built = rdlt.pipeline("logged", source=logged, destination=into_sqlite(tmp_path))
    assert built.build(host(tmp_path, connectors)).run().report.rows == 8
    deadline = time.monotonic() + 30
    while ended := ended_threads():
        assert time.monotonic() < deadline, f"threading holds {len(ended)} threads that ended"
        time.sleep(0.05)


def test_a_thread_of_a_forked_child_ends_though_threading_s_lock_was_held_at_the_fork() -> None:
    script = textwrap.dedent(
        """
        import os, signal, sys, threading, time, warnings
        warnings.simplefilter("ignore")
        from rdlt import _reading
        held, release = threading.Event(), threading.Event()
        def hold():
            with vars(threading)["_active_limbo_lock"]:
                held.set()
                release.wait()
        threading.Thread(target=hold, daemon=True).start()
        held.wait()
        child = os.fork()
        if child == 0:
            _reading._ended()
            os._exit(0)
        release.set()
        deadline = time.monotonic() + 10
        while not (ended := os.waitpid(child, os.WNOHANG))[0]:
            if time.monotonic() > deadline:
                os.kill(child, signal.SIGKILL)
                os.waitpid(child, 0)
                sys.exit("the child's thread never ended")
            time.sleep(0.05)
        sys.exit(os.waitstatus_to_exitcode(ended[1]))
        """
    )
    finished = subprocess.run(
        [sys.executable, "-c", script], timeout=30, check=False, capture_output=True
    )
    assert finished.returncode == 0, finished.stderr
