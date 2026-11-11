"""Functions `@rdlt.connector.stream` makes streams, read as a pipeline's in-process source."""

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
from collections.abc import AsyncIterator, Callable, Iterator, Sequence
from contextvars import ContextVar
from pathlib import Path
from typing import cast

import pyarrow as pa
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import rdlt
from rdlt import _reading
from rdlt.connector import (
    Config,
    Cursor,
    Partition,
    ReadStream,
    StreamContext,
    checkpoint,
    source,
    stream,
)
from rdlt.connector._streams import Stream


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


def test_a_stream_function_is_a_pipeline_s_source(tmp_path: Path, connectors: Path) -> None:
    @stream(primary_key="id")
    def users() -> Iterator[object]:
        yield [{"id": n} for n in range(3)]

    built = rdlt.pipeline("users", source=users, destination=into_sqlite(tmp_path))
    outcome = built.build(host(tmp_path, connectors)).run()
    assert outcome.report.rows == 3
    assert rows_of(tmp_path, "users") == [(0,), (1,), (2,)]


def test_a_sequence_of_stream_functions_is_a_pipeline_s_source(
    tmp_path: Path, connectors: Path
) -> None:
    @stream(primary_key="id")
    def users() -> Iterator[object]:
        yield {"id": 1}

    @stream(primary_key="id")
    def orders() -> Iterator[object]:
        yield [{"id": 1}, {"id": 2}]

    built = rdlt.pipeline("shop", source=[users, orders], destination=into_sqlite(tmp_path))
    built.build(host(tmp_path, connectors)).run()
    assert rows_of(tmp_path, "users") == [(1,)]
    assert rows_of(tmp_path, "orders") == [(1,), (2,)]


def _plain() -> Iterator[object]:
    yield {"id": 1}


@pytest.mark.parametrize("given", [_plain, [_plain]], ids=["function", "sequence"])
def test_a_plain_function_as_a_source_is_refused_naming_the_decorator(given: object) -> None:
    with pytest.raises(TypeError, match=r"@rdlt\.connector\.stream"):
        rdlt.pipeline(
            "plain",
            source=given,  # pyright: ignore[reportArgumentType]
            destination=rdlt.Connector("io.rapidbyte.sqlite"),
        )


class _Since(Cursor):
    after: int = -1


Started = list[tuple[int, bool]]
"""Where each read started, and whether it was given a cursor."""


def two_runs(
    tmp_path: Path, connectors: Path, placed: object, available: list[int]
) -> list[tuple[object, ...]]:
    """Runs `placed` twice, with three more rows available for the second run."""
    built = rdlt.pipeline(
        "moved",
        source=placed,  # pyright: ignore[reportArgumentType]
        destination=into_sqlite(tmp_path),
        streams=[rdlt.stream("users", write=rdlt.merge("id"))],
    )
    every_host = host(tmp_path, connectors)
    built.build(every_host).run()
    available[0] += 3
    built.build(every_host).run()
    return rows_of(tmp_path, "users")


def as_function(available: list[int], started: Started) -> object:
    @stream(primary_key="id", cursor=_Since)
    def users(since: _Since, ctx: StreamContext) -> Iterator[object]:
        started.append((since.after, ctx.resumes))
        for n in range(since.after + 1, available[0]):
            yield {"id": n}
            if n % 3 == 2 or n == available[0] - 1:
                yield checkpoint(_Since(after=n))

    return users


def as_method(available: list[int], started: Started) -> object:
    @source(id="io.example.moved", config=Config, version="1.0.0")
    class Moved:
        def __init__(self, config: Config) -> None:
            del config

        def check(self) -> None:
            """Reads need nothing checked first."""

        @stream(primary_key="id", cursor=_Since)
        def users(self, since: _Since, ctx: StreamContext) -> Iterator[object]:
            started.append((since.after, ctx.resumes))
            for n in range(since.after + 1, available[0]):
                yield {"id": n}
                if n % 3 == 2 or n == available[0] - 1:
                    yield checkpoint(_Since(after=n))

    return rdlt.local(Moved, config={})


Made = Callable[..., object]


@pytest.mark.parametrize("made", [as_function, as_method], ids=["function", "method"])
def test_a_stream_reads_the_same_as_a_function_and_as_a_source_class_s_method(
    made: Made, tmp_path: Path, connectors: Path
) -> None:
    available: list[int] = [7]
    started: Started = []
    placed = made(available, started)
    assert two_runs(tmp_path, connectors, placed, available) == [(n,) for n in range(10)]
    assert started == [(-1, False), (6, True)]


def test_a_stream_without_a_cursor_is_read_whole_every_run(
    tmp_path: Path, connectors: Path
) -> None:
    reads: list[int] = []

    @stream()
    def users() -> Iterator[object]:
        reads.append(1)
        yield [{"id": n} for n in range(3)]

    built = rdlt.pipeline("whole", source=users, destination=into_sqlite(tmp_path))
    every_host = host(tmp_path, connectors)
    assert built.build(every_host).run().report.rows == 3
    assert built.build(every_host).run().report.rows == 3
    assert len(reads) == 2
    assert len(rows_of(tmp_path, "users")) == 6


def test_a_partition_that_never_ends_is_read_again_from_its_last_checkpoint(
    tmp_path: Path, connectors: Path
) -> None:
    available: list[int] = [7]
    started: list[tuple[int, str]] = []

    @stream(primary_key="id", cursor=_Since, partitions=lambda: [Partition.single()])
    def users(since: _Since, ctx: StreamContext) -> Iterator[object]:
        started.append((since.after, ctx.partition.id))
        for n in range(since.after + 1, available[0]):
            yield {"id": n}
            if n % 3 == 2:
                yield checkpoint(_Since(after=n))

    assert two_runs(tmp_path, connectors, users, available) == [(n,) for n in range(9)]
    assert started == [(-1, "whole"), (5, "whole")]


def test_partitions_answer_ids_and_partitions_alike(tmp_path: Path, connectors: Path) -> None:
    @stream(primary_key="id", partitions=lambda: ["eu", Partition("us")])
    def users(ctx: StreamContext) -> Iterator[object]:
        yield {"id": ctx.partition.id}

    built = rdlt.pipeline("users", source=users, destination=into_sqlite(tmp_path))
    built.build(host(tmp_path, connectors)).run()
    assert rows_of(tmp_path, "users") == [("eu",), ("us",)]


def test_a_stream_that_is_not_split_is_told_it_reads_its_single_partition(
    tmp_path: Path, connectors: Path
) -> None:
    told: list[Partition] = []

    @stream()
    def whole(ctx: StreamContext) -> Iterator[object]:
        told.append(ctx.partition)
        yield {"id": 1}

    @stream(cursor=_Since)
    def resumed(since: _Since, ctx: StreamContext) -> Iterator[object]:
        told.append(ctx.partition)
        yield {"id": since.after + 1}
        yield checkpoint(_Since(after=since.after + 1))

    built = rdlt.pipeline("single", source=[whole, resumed], destination=into_sqlite(tmp_path))
    built.build(host(tmp_path, connectors)).run()
    assert told == [Partition.single(), Partition.single()]
    assert Partition.single() == Partition("whole", unbounded=True)


Told = list[tuple[str, int]]
"""The committed cursors a stream was told of: each partition, and the cursor's `after`."""


def told_of(told: Told) -> Callable[[Sequence[tuple[str, _Since]]], None]:
    """What a stream calls with its committed cursors, keeping each in `told`."""

    def committed(cursors: Sequence[tuple[str, _Since]]) -> None:
        told.extend((partition, cursor.after) for partition, cursor in cursors)

    return committed


def test_a_stream_function_is_told_which_cursors_are_committed(
    tmp_path: Path, connectors: Path
) -> None:
    told: Told = []

    @stream(cursor=_Since, committed=told_of(told))
    def users(since: _Since) -> Iterator[object]:
        for n in range(since.after + 1, 6):
            yield {"id": n}
            if n % 3 == 2:
                yield checkpoint(_Since(after=n))

    built = rdlt.pipeline(
        "told", source=users, destination=into_sqlite(tmp_path), commit=rdlt.Commit(rows=1)
    )
    built.build(host(tmp_path, connectors)).run()
    assert told, "the stream was told of its commits"
    assert told[-1] == ("whole", 5)
    assert {after for _, after in told} <= {2, 5}


def test_a_committed_start_is_told_as_the_cursor_a_read_starts_from() -> None:
    def read(since: _Since) -> Iterator[object]:
        del since
        yield from ()

    told: Told = []
    users = Stream(
        "users",
        read,
        None,
        _Since,
        None,
        committed=told_of(told),
        takes_context=False,
        replayable=True,
    )
    users.committed([("whole", None), ("whole", _Since(after=4).model_dump_json())])
    assert told == [("whole", -1), ("whole", 4)]


def test_a_source_class_s_stream_is_told_which_cursors_are_committed_through_a_method(
    tmp_path: Path, connectors: Path
) -> None:
    told: Told = []

    @source(id="io.example.told", config=Config, version="1.0.0")
    class Told:
        def __init__(self, config: Config) -> None:
            del config

        def check(self) -> None:
            """Reads need nothing checked first."""

        def settle(self, cursors: Sequence[tuple[str, _Since]]) -> None:
            told_of(told)(cursors)

        @stream(cursor=_Since, partitions=lambda _self: ["eu"], committed="settle")
        def users(self, since: _Since, ctx: StreamContext) -> Iterator[object]:
            del ctx
            for n in range(since.after + 1, 3):
                yield {"id": n}
            yield checkpoint(_Since(after=2))

    placed = rdlt.local(Told, config={})
    rdlt.pipeline("told", source=placed, destination=into_sqlite(tmp_path)).build(
        host(tmp_path, connectors)
    ).run()
    assert told == [("eu", 2)]


def test_a_stream_that_cannot_be_read_again_says_so_in_its_catalog(
    tmp_path: Path, connectors: Path
) -> None:
    @stream(cursor=_Since, replayable=False)
    def leased(since: _Since) -> Iterator[object]:
        del since
        yield {"id": 1}

    built = rdlt.pipeline("leased", source=leased, destination=into_sqlite(tmp_path))
    (found,) = built.build(host(tmp_path, connectors)).discover().streams
    assert found.raw["replayable"] is False


def test_yielded_dicts_land_once(tmp_path: Path, connectors: Path) -> None:
    @stream(primary_key="id")
    def users() -> Iterator[object]:
        yield [{"id": n, "name": f"user {n}"} for n in range(100)]
        yield {"id": 100, "name": "the last"}

    built = rdlt.pipeline("users", source=users, destination=into_sqlite(tmp_path))
    outcome = built.build(host(tmp_path, connectors)).run()
    assert outcome.report.rows == 101
    assert len(rows_of(tmp_path, "users")) == 101


def test_arrow_batches_and_tables_land(tmp_path: Path, connectors: Path) -> None:
    @stream()
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

    @stream(name="produced")
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
    class Since(Cursor):
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

    @stream(cursor=Since)
    def numbers(since: Since) -> Iterator[object]:
        started_from.append(since.after)
        for n in range(since.after + 1, 100):
            yield {"id": n}
            if n % 10 == 9:
                yield checkpoint(Since(after=n))
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
    @stream()
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

    @stream(name="rows")
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
    source: ReadStream | rdlt.Connector,
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


def test_stop_now_returns_while_a_stream_function_blocks(tmp_path: Path, connectors: Path) -> None:
    release = threading.Event()
    waiting = threading.Event()

    @stream()
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


def exits_with_seven(tmp_path: Path, connectors: Path, streamed: str) -> int:
    """The status of a script that exits, with a run in flight, once `started` is set inside it."""
    out = str(tmp_path / "out.db")
    tmp = str(tmp_path)
    script = "\n".join(
        [
            "import itertools, sys, threading, time",
            "import rdlt",
            "",
            "started = threading.Event()",
            textwrap.dedent(streamed),
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
    streamed = """
    class Since(rdlt.connector.Cursor):
        after: int = -1

    @rdlt.connector.stream(cursor=Since)
    def source(since):
        for n in itertools.count(since.after + 1):
            yield {"n": n}
            yield rdlt.connector.checkpoint(Since(after=n))
            started.set()
    """
    assert exits_with_seven(tmp_path, connectors, streamed) == 7


def test_a_process_exits_while_a_stream_function_polls_in_python(
    tmp_path: Path, connectors: Path
) -> None:
    streamed = """
    @rdlt.connector.stream()
    def source():
        yield {"n": 0}
        started.set()
        while True:
            time.sleep(0.003)
    """
    assert exits_with_seven(tmp_path, connectors, streamed) == 7


def test_a_process_exits_while_a_stream_function_lists_its_partitions(
    tmp_path: Path, connectors: Path
) -> None:
    streamed = """
    def partitions():
        started.set()
        while True:
            time.sleep(0.001)

    @rdlt.connector.stream(partitions=partitions)
    def source(ctx):
        yield {"n": 0}
    """
    assert exits_with_seven(tmp_path, connectors, streamed) == 7


@pytest.mark.parametrize("ends_on_checkpoint", [False, True], ids=["on-rows", "on-a-checkpoint"])
def test_an_incremental_read_after_a_full_one_resumes_only_where_it_ended_on_a_checkpoint(
    ends_on_checkpoint: bool,  # noqa: FBT001
    tmp_path: Path,
    connectors: Path,
) -> None:
    available = [4]
    started_from: list[int] = []

    @stream(primary_key="id", cursor=_Since)
    def users(since: _Since) -> Iterator[object]:
        started_from.append(since.after)
        for n in range(since.after + 1, available[0]):
            yield {"id": n}
            if n == 1:
                yield checkpoint(_Since(after=n))
        if ends_on_checkpoint:
            yield checkpoint(_Since(after=available[0] - 1))

    every_host = host(tmp_path, connectors)
    full = rdlt.pipeline(
        "switched",
        source=users,
        destination=into_sqlite(tmp_path),
        streams=[rdlt.stream("users", read="full")],
    )
    full.build(every_host).run()
    available[0] = 6
    incremental = rdlt.pipeline(
        "switched",
        source=users,
        destination=into_sqlite(tmp_path),
        streams=[rdlt.stream("users", read="incremental")],
        retry=rdlt.Retry(max_attempts=1),
    )
    if ends_on_checkpoint:
        incremental.build(every_host).run()
        assert started_from == [-1, 3]
        assert rows_of(tmp_path, "users") == [(n,) for n in range(6)]
        return
    # No cursor follows the rows the full read ended on: read again, they would land twice.
    with pytest.raises(rdlt.Error) as raised:
        incremental.build(every_host).run()
    assert raised.value.report().code == "cursor_missing"
    assert started_from == [-1]
    assert rows_of(tmp_path, "users") == [(n,) for n in range(4)]


def test_a_stream_named_twice_is_refused_by_the_plan(tmp_path: Path, connectors: Path) -> None:
    @stream()
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


def counted(started_from: list[int]) -> ReadStream:
    """A stream of three rows read from its cursor, which says where each read started."""

    @stream(primary_key="id", cursor=_Since)
    def merged(since: _Since) -> Iterator[object]:
        started_from.append(since.after)
        for n in range(since.after + 1, 3):
            yield {"id": n}
        yield checkpoint(_Since(after=2))

    return merged


def test_naming_a_stream_function_in_streams_keeps_what_it_does_not_change(
    tmp_path: Path, connectors: Path
) -> None:
    started_from: list[int] = []
    built = rdlt.pipeline(
        "merged",
        source=counted(started_from),
        destination=into_sqlite(tmp_path),
        streams=[rdlt.stream("merged", write=rdlt.merge("id"))],
    )
    every_host = host(tmp_path, connectors)
    built.build(every_host).run()
    built.build(every_host).run()
    assert started_from == [-1, 2]
    assert len(rows_of(tmp_path, "merged")) == 3


def test_an_entry_in_streams_overrides_what_it_sets(tmp_path: Path, connectors: Path) -> None:
    started_from: list[int] = []
    built = rdlt.pipeline(
        "overridden",
        source=counted(started_from),
        destination=into_sqlite(tmp_path),
        streams=[rdlt.stream("merged", read="full")],
    )
    every_host = host(tmp_path, connectors)
    built.build(every_host).run()
    built.build(every_host).run()
    assert started_from == [-1, -1]
    assert len(rows_of(tmp_path, "merged")) == 6


def test_an_object_with_only_aiter_is_read(tmp_path: Path, connectors: Path) -> None:
    class Numbers:
        def __aiter__(self) -> AsyncIterator[object]:
            return self._numbers()

        async def _numbers(self) -> AsyncIterator[object]:
            yield {"n": 1}
            yield {"n": 2}

    @stream()
    def counted() -> Numbers:
        return Numbers()

    rdlt.pipeline("counted", source=counted, destination=into_sqlite(tmp_path)).build(
        host(tmp_path, connectors)
    ).run()
    assert len(rows_of(tmp_path, "counted")) == 2


def test_an_async_stream_function_is_read_in_one_task_of_a_running_loop(
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

    @stream()
    def counted() -> Numbers:
        return Numbers()

    rdlt.pipeline(
        "counted",
        source=counted,
        destination=into_sqlite(tmp_path),
        retry=rdlt.Retry(max_attempts=1),
    ).build(host(tmp_path, connectors)).run()
    assert rows_of(tmp_path, "counted") == [(7,), (7,)]


def test_a_task_an_async_stream_function_leaves_running_is_cancelled_and_cleans_up(
    tmp_path: Path, connectors: Path
) -> None:
    cleaned: list[bool] = []

    async def forever() -> None:
        try:
            await asyncio.sleep(3600)
        finally:
            await asyncio.sleep(0)
            cleaned.append(True)

    @stream()
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

    @stream(partitions=one_string)
    def split(ctx: StreamContext) -> Iterator[object]:
        yield {"partition": ctx.partition.id}

    built = rdlt.pipeline("split", source=split, destination=into_sqlite(tmp_path))
    with pytest.raises(rdlt.SourceError) as raised:
        built.build(host(tmp_path, connectors)).run()
    assert raised.value.report().connector_code == "type_error"


class Holder:
    """What a stream function keeps, which holds the pipeline the function serves."""

    pipeline: rdlt.Pipeline | None = None

    def read(self) -> Iterator[object]:
        """Yields one row that says what it holds."""
        yield {"held": self.pipeline is not None}


def test_a_stream_function_holding_its_own_pipeline_is_collected(
    tmp_path: Path, connectors: Path
) -> None:
    holder = Holder()
    rows = stream(name="rows")(holder.read)
    holder.pipeline = rdlt.pipeline("cycle", source=rows, destination=into_sqlite(tmp_path)).build(
        host(tmp_path, connectors)
    )
    collected = weakref.ref(holder)
    del holder, rows
    gc.collect()
    assert collected() is None


def test_a_built_pipeline_and_its_run_keep_its_stream_function(
    tmp_path: Path, connectors: Path
) -> None:
    @stream(name="rows")
    def rows() -> Iterator[object]:
        yield {"n": 1}

    built = rdlt.pipeline("kept", source=rows, destination=into_sqlite(tmp_path)).build(
        host(tmp_path, connectors)
    )
    kept = weakref.ref(rows)
    del rows
    gc.collect()
    assert kept() is not None, "a built pipeline reads with its stream function"
    run = built.start()
    del built
    gc.collect()
    assert kept() is not None, "a run in progress reads with its stream function"
    run.wait()


def test_a_source_s_failure_says_its_type_and_message_once(
    tmp_path: Path, connectors: Path
) -> None:
    @stream(name="failing")
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
    @stream(name="parted", partitions=lambda: [str(n) for n in range(32)])
    def parted(ctx: StreamContext) -> Iterator[object]:
        yield {"partition": ctx.partition.id}

    built = rdlt.pipeline("parted", source=parted, destination=into_sqlite(tmp_path))
    assert built.build(host(tmp_path, connectors)).run().report.rows == 32
    deadline = time.monotonic() + 30
    while ended := ended_threads():
        assert time.monotonic() < deadline, f"threading holds {len(ended)} threads that ended"
        time.sleep(0.05)


class Logged:
    """A partition's rows, which log as they are let go, as a client closing its connection does."""

    def __init__(self, partition: str) -> None:
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
    @stream(name="logged", partitions=lambda: [str(n) for n in range(8)])
    def logged(ctx: StreamContext) -> Iterator[object]:
        return Logged(ctx.partition.id)

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
