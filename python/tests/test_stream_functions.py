"""Functions `@rdlt.connector.stream` makes streams, read as a pipeline's in-process source."""

from __future__ import annotations

import asyncio
import contextlib
import gc
import json
import logging
import socket
import sqlite3
import subprocess
import sys
import textwrap
import threading
import time
import weakref
from collections.abc import AsyncIterable, AsyncIterator, Callable, Iterator, Sequence
from contextvars import ContextVar
from datetime import timedelta
from pathlib import Path
from typing import cast

import pyarrow as pa
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import rdlt
from rdlt import _native, _reading
from rdlt.connector import (
    Config,
    Cursor,
    Exclusion,
    Headroom,
    Limits,
    Partition,
    PartitionState,
    ReadStream,
    StreamContext,
    StreamState,
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


def test_partitions_that_name_state_are_told_where_each_partition_stands(
    tmp_path: Path, connectors: Path
) -> None:
    told: list[StreamState] = []

    def planned(state: StreamState) -> list[str]:
        told.append(state)
        return ["eu", "us"]

    # "eu" ends on rows, which finishes it; "us" ends on a checkpoint, which leaves it there.
    @stream(primary_key="id", cursor=_Since, partitions=planned)
    def users(since: _Since, ctx: StreamContext) -> Iterator[object]:
        if since.after < 0:
            yield {"id": ctx.partition.id}
            if ctx.partition.id == "us":
                yield checkpoint(_Since(after=7))

    built = rdlt.pipeline("planned", source=users, destination=into_sqlite(tmp_path))
    built.build(host(tmp_path, connectors)).run()
    assert told, "the first run planned the stream"
    assert set(told) == {StreamState()}
    told.clear()
    built.build(host(tmp_path, connectors)).run()
    committed = StreamState(0, {"eu": PartitionState.DONE, "us": _Since(after=7)})
    assert told, "the second run planned the stream"
    assert all(state == committed for state in told), told


def test_a_plan_is_told_the_full_read_in_progress_and_the_full_reads_completed(
    tmp_path: Path, connectors: Path
) -> None:
    told: list[StreamState] = []
    committed = threading.Event()
    interrupted = [False]

    def planned(state: StreamState) -> list[str]:
        told.append(state)
        return ["p"]

    def acknowledged(cursors: object) -> None:
        del cursors
        committed.set()

    @stream(primary_key="id", cursor=_Since, partitions=planned, committed=acknowledged)
    def users(since: _Since, ctx: StreamContext) -> Iterator[object]:
        del ctx
        if since.after < 1:
            yield {"id": 1}
            yield checkpoint(_Since(after=1))
        if interrupted[0]:
            assert committed.wait(30), "the checkpoint was not committed within 30 s"
            message = "interrupted"
            raise RuntimeError(message)

    built = rdlt.pipeline(
        "generations",
        source=users,
        destination=into_sqlite(tmp_path),
        streams=[rdlt.stream("users", read="full", write=rdlt.replace())],
        commit=rdlt.Commit(rows=1),
        retry=rdlt.Retry(max_attempts=1),
    )
    built.build(host(tmp_path, connectors)).run()
    committed.clear()
    interrupted[0] = True
    with pytest.raises(rdlt.SourceError):
        built.build(host(tmp_path, connectors)).run()
    interrupted[0] = False
    built.build(host(tmp_path, connectors)).run()
    # The first two runs each began a full read; the third resumed the second's.
    first, second, resumed = told
    assert first == second == StreamState()
    assert resumed.partitions == {"p": _Since(after=1)}
    assert resumed.generation is not None
    assert len(resumed.completed) == 1, resumed
    assert resumed.generation not in resumed.completed
    assert rows_of(tmp_path, "users") == [(1,)]


def test_a_class_s_partitions_method_is_told_the_state_where_it_names_it(
    tmp_path: Path, connectors: Path
) -> None:
    told: list[StreamState] = []

    @source(id="io.example.planned", config=Config, version="1.0.0")
    class Planned:
        def __init__(self, config: Config) -> None:
            del config

        def check(self) -> None:
            """Reads need nothing checked first."""

        def regions(self, state: StreamState, /) -> list[str]:
            told.append(state)
            return ["eu"]

        @stream(cursor=_Since, partitions="regions")
        def users(self, since: _Since, ctx: StreamContext) -> Iterator[object]:
            del ctx
            yield {"id": since.after}
            yield checkpoint(_Since(after=3))

        @stream(cursor=_Since, partitions=lambda _self: ["eu"])
        def stateless(self, since: _Since, ctx: StreamContext) -> Iterator[object]:
            del ctx
            yield {"id": since.after}

    built = rdlt.pipeline(
        "planned", source=rdlt.local(Planned, config={}), destination=into_sqlite(tmp_path)
    )
    built.build(host(tmp_path, connectors)).run()
    assert told, "the stream's partitions were planned"
    assert set(told) == {StreamState()}
    told.clear()
    built.build(host(tmp_path, connectors)).run()
    assert told, "the second run planned the stream"
    assert set(told) == {StreamState(0, {"eu": _Since(after=3)})}


def test_a_read_is_told_the_columns_its_stream_excludes_and_the_limits_it_keeps_within(
    tmp_path: Path, connectors: Path
) -> None:
    told: list[tuple[Exclusion, Limits]] = []

    @stream(primary_key="id")
    def users(ctx: StreamContext) -> Iterator[object]:
        told.append((ctx.excluded, ctx.limits))
        row: dict[str, object] = {"id": 1, "name": "ada"}
        if not ctx.excluded.contains("phone"):
            row["phone"] = "555 0100"
        yield row

    excluding = rdlt.stream("users", exclude="phone")
    built = rdlt.pipeline(
        "excluded", source=users, destination=into_sqlite(tmp_path), streams=[excluding]
    )
    built.build(host(tmp_path, connectors)).run()
    ((excluded, limits),) = told
    assert excluded == Exclusion(("phone",))
    assert not excluded.is_empty()
    assert limits.json_push_bytes > 0
    assert limits.nesting_depth > 0
    assert rows_of(tmp_path, "users") == [(1, "ada")]
    whole = tmp_path / "whole"
    whole.mkdir()
    rdlt.pipeline("whole", source=users, destination=into_sqlite(whole)).build(
        host(tmp_path, connectors)
    ).run()
    assert told[-1][0].is_empty()
    assert told[-1][1] == limits


def test_a_following_read_that_waits_for_data_ends_once_it_is_asked_to_stop(
    tmp_path: Path, connectors: Path
) -> None:
    ended = threading.Event()

    @stream(primary_key="id", cursor=_Since)
    def users(since: _Since, ctx: StreamContext) -> Iterator[object]:
        yield {"id": since.after + 1}
        yield checkpoint(_Since(after=since.after + 1))
        while not ctx.stopped():
            time.sleep(0.005)
        ended.set()

    built = rdlt.pipeline(
        "following", source=users, destination=into_sqlite(tmp_path), until=timedelta(seconds=1)
    )
    built.build(host(tmp_path, connectors)).run()
    assert ended.wait(10), "the read's thread went on after its run"
    assert rows_of(tmp_path, "users") == [(0,)]


def test_an_async_read_s_lag_goes_through_its_router_and_is_totalled_in_the_report(
    tmp_path: Path, connectors: Path
) -> None:
    routed: list[bool] = []

    @stream(primary_key="id")
    async def users(ctx: StreamContext) -> AsyncIterator[object]:
        native = ctx._native  # pyright: ignore[reportPrivateUsage]
        routed.append(_reading.router(native) is not None)
        ctx.behind(5, Headroom(bytes=500))
        yield {"id": 1}

    built = rdlt.pipeline("routed", source=users, destination=into_sqlite(tmp_path))
    streams = built.build(host(tmp_path, connectors)).run().report.streams
    assert routed == [True]
    assert streams["users"].raw["behind"] == 5


def test_a_report_totals_how_far_behind_its_source_each_stream_s_reads_last_were(
    tmp_path: Path, connectors: Path
) -> None:
    behind = {"eu": 3, "us": 4}

    @stream(primary_key="id", cursor=_Since, partitions=lambda: ["eu", "us"])
    def users(since: _Since, ctx: StreamContext) -> Iterator[object]:
        records = behind[ctx.partition.id]
        ctx.behind(records, Headroom(bytes=records * 100, time=timedelta(seconds=records)))
        yield {"id": f"{ctx.partition.id}-{since.after}"}
        yield checkpoint(_Since(after=0))

    @stream(primary_key="id")
    def quiet() -> Iterator[object]:
        yield {"id": 1}

    built = rdlt.pipeline("lagged", source=[users, quiet], destination=into_sqlite(tmp_path))
    streams = built.build(host(tmp_path, connectors)).run().report.streams
    assert streams["users"].raw["behind"] == 7
    headroom = cast("dict[str, object]", streams["users"].raw["headroom"])
    assert headroom["bytes"] == 300
    assert headroom["time"] is not None
    # A stream whose source says nothing leaves it unknown.
    assert streams["quiet"].raw["behind"] is None


def test_a_following_run_plans_again_as_soon_as_a_read_says_its_partitions_changed(
    tmp_path: Path, connectors: Path
) -> None:
    grown = threading.Event()

    def planned() -> list[Partition]:
        return [Partition(name, unbounded=True) for name in ("a", "b")[: 1 + grown.is_set()]]

    # The run plans every minute: only the read's word starts the partition added in time.
    @stream(primary_key="id", cursor=_Since, partitions=planned)
    def users(since: _Since, ctx: StreamContext) -> Iterator[object]:
        if since.after < 0:
            yield {"id": ctx.partition.id}
            yield checkpoint(_Since(after=0))
        if not grown.is_set():
            grown.set()
            ctx.replan()
        while not ctx.stopped():
            time.sleep(0.005)

    built = rdlt.pipeline(
        "replanned", source=users, destination=into_sqlite(tmp_path), until=timedelta(seconds=3)
    )
    built.build(host(tmp_path, connectors)).run()
    assert sorted(set(rows_of(tmp_path, "users"))) == [("a",), ("b",)]


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
        stated=False,
        committed=told_of(told),
        acknowledged=None,
        cursor_version=1,
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


def test_a_stream_tells_where_it_stands_in_its_own_cursor_type_or_none() -> None:
    def read(since: _Since) -> Iterator[object]:
        del since
        yield from ()

    standing: dict[str, object] = {"eu": _Since(after=3), "us": None, "asia": {"after": 3}}
    users = Stream(
        "users",
        read,
        None,
        _Since,
        None,
        stated=False,
        committed=None,
        acknowledged=lambda partition: cast("_Since | None", standing[partition]),
        cursor_version=1,
        takes_context=False,
        replayable=True,
    )
    assert json.loads(users.acknowledged("eu") or "") == {"after": 3}
    assert users.acknowledged("us") is None
    with pytest.raises(TypeError, match="_Since"):
        users.acknowledged("asia")


def test_a_stream_without_a_cursor_cannot_say_where_it_stands() -> None:
    with pytest.raises(TypeError, match="acknowledged"):

        @stream(acknowledged=lambda partition: None)
        def rows() -> Iterator[object]:
            yield {"id": 1}


def _users_in_format(version: int) -> ReadStream:
    @stream(name="users", cursor=_Since, cursor_version=version)
    def users(since: _Since) -> Iterator[object]:
        for n in range(since.after + 1, 3):
            yield {"id": n}
        yield checkpoint(_Since(after=2))

    return cast("ReadStream", users)


def test_a_cursor_committed_in_another_format_fails_the_read_coded_cursor_version(
    tmp_path: Path, connectors: Path
) -> None:
    placed = host(tmp_path, connectors)
    first = rdlt.pipeline("formats", source=_users_in_format(1), destination=into_sqlite(tmp_path))
    first.build(placed).run()
    again = rdlt.pipeline("formats", source=_users_in_format(1), destination=into_sqlite(tmp_path))
    assert again.build(placed).run().report.rows == 0
    bumped = rdlt.pipeline("formats", source=_users_in_format(2), destination=into_sqlite(tmp_path))
    # A cursor in another format is the Rust SDK's `Config` failure.
    with pytest.raises(rdlt.ConfigError) as raised:
        bumped.build(placed).run()
    assert raised.value.report().connector_code == "cursor_version"
    assert rows_of(tmp_path, "users") == [(0,), (1,), (2,)]


@pytest.mark.parametrize("version", [-1, 2**16, True, 2.0, "2"])
def test_a_cursor_format_is_a_number_a_u16_holds(version: object) -> None:
    with pytest.raises(ValueError, match="cursor_version"):

        @stream(cursor=_Since, cursor_version=cast("int", version))
        def users(since: _Since) -> Iterator[object]:
            yield {"id": since.after}


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
    with pytest.raises(rdlt.ConfigError) as raised:
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


class _Notice:
    """What the native `Notice` makes, as the tests below read it."""

    @staticmethod
    def replan() -> tuple[object, ...]:
        return ("replan",)

    @staticmethod
    def behind(records: int, bytes: int | None, time: timedelta | None) -> tuple[object, ...]:  # noqa: A002 - the Rust SDK's `Headroom` field
        return ("behind", records, bytes, time)


class _Engine:
    """A read's out and its context at once, whose room a test gives.

    What finds no room waits for it: an item is kept and sent once room came, as the native side
    sends it, and a notice is not taken. Giving room makes the descriptor the read watches
    readable.
    """

    def __init__(self) -> None:
        self._room = False
        self.sent: list[object] = []
        self._kept: list[object] = []
        self._watched, self._rung = socket.socketpair()
        self._watched.settimeout(0)

    @property
    def room(self) -> bool:
        return self._room

    @room.setter
    def room(self, room: bool) -> None:
        self._room = room
        if room:
            self._rung.send(b"\0")

    def send(self, said: object) -> bool | None:
        if self._room:
            self.sent.append(said)
            return True
        # The tests' notices are what `_Notice` makes, tuples; anything else is an item.
        if not isinstance(said, tuple):
            self._kept.append(said)
        return None

    def resume(self) -> bool | None:
        if not self._room:
            return None
        self.sent.extend(self._kept)
        self._kept.clear()
        return True

    def watched(self) -> int:
        return self._watched.fileno()

    def woken(self) -> None:
        with contextlib.suppress(BlockingIOError):
            self._watched.recv(64)

    def close(self) -> None:
        self._watched.close()
        self._rung.close()


def _routed(reading: Callable[[StreamContext, _Engine], AsyncIterator[object]]) -> list[object]:
    """What the async read `reading` makes, given its context and the engine, sent in order."""
    engine = _Engine()
    ctx = StreamContext(cast("_native.StreamContext", engine))
    read = _reading.pushed(reading(ctx, engine), cast("_reading.Out", engine), engine)
    try:
        asyncio.run(asyncio.wait_for(read, timeout=30))
    finally:
        engine.close()
    return engine.sent


def test_an_async_read_leaves_its_event_loop_running_while_the_engine_is_full() -> None:
    async def reading(_ctx: StreamContext, engine: _Engine) -> AsyncIterator[object]:
        async def opening() -> None:
            engine.room = True

        opened = asyncio.create_task(opening())
        yield {"n": 1}
        await opened

    assert _routed(reading) == [{"n": 1}]


class _Gone(_Engine):
    """An engine that has stopped: nothing it is sent is taken."""

    def send(self, said: object) -> bool | None:
        return False


class _IterationError(Exception):
    """What a read's iteration raised in a test."""


class _Unopened:
    """An async iterable whose iteration cannot begin."""

    def __aiter__(self) -> AsyncIterator[object]:
        raise _IterationError


async def _cleaned_up_badly() -> AsyncIterator[object]:
    try:
        yield {"n": 1}
    finally:
        raise _IterationError


@pytest.mark.parametrize(
    "opened", [_cleaned_up_badly, _Unopened], ids=["its-cleanup-raises", "it-cannot-begin"]
)
def test_an_async_read_stops_watching_for_room_however_its_iteration_ends(
    opened: Callable[[], AsyncIterable[object]],
) -> None:
    unwatched: list[int] = []

    class Recording(asyncio.SelectorEventLoop):
        def remove_reader(self, fd: object) -> bool:  # pyright: ignore[reportIncompatibleMethodOverride]
            unwatched.append(cast("int", fd))
            return super().remove_reader(cast("int", fd))

    engine = _Gone()
    watched = engine.watched()
    read = _reading.pushed(opened(), cast("_reading.Out", engine), engine)
    try:
        with asyncio.Runner(loop_factory=Recording) as runner, pytest.raises(_IterationError):
            runner.run(read)
    finally:
        engine.close()
    assert unwatched == [watched]


def test_an_async_read_on_a_loop_that_cannot_watch_a_socket_fails_rather_than_waits() -> None:
    class Unwatching(asyncio.SelectorEventLoop):
        def add_reader(self, *_: object) -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
            raise NotImplementedError

    async def reading() -> AsyncIterator[object]:
        yield {"n": 1}

    engine = _Engine()
    read = _reading.pushed(reading(), cast("_reading.Out", engine), engine)
    try:
        with (
            asyncio.Runner(loop_factory=Unwatching) as runner,
            pytest.raises(RuntimeError) as raised,
        ):
            runner.run(read)
    finally:
        engine.close()
    assert isinstance(raised.value.__cause__, NotImplementedError)
    assert engine.sent == []


def test_an_async_read_s_notices_leave_its_event_loop_running_while_the_engine_is_full(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_native, "Notice", _Notice)
    ticks = 0

    async def ticking() -> None:
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0)

    async def reading(ctx: StreamContext, engine: _Engine) -> AsyncIterator[object]:
        ticker = asyncio.create_task(ticking())
        engine.room = True
        yield {"n": 1}
        engine.room = False
        ctx.behind(5, Headroom())
        ctx.behind(3, Headroom(bytes=10))
        ctx.replan()
        seen = ticks
        while ticks < seen + 3:
            await asyncio.sleep(0)
        assert engine.sent == [{"n": 1}], "a notice went before the engine had room"
        engine.room = True
        yield {"n": 2}
        ticker.cancel()

    # The second lag, said before the first was sent, replaced it.
    expected = [{"n": 1}, ("behind", 3, 10, None), ("replan",), {"n": 2}]
    assert _routed(reading) == expected


def test_an_async_read_s_notices_said_after_its_last_yield_are_sent_before_it_ends(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_native, "Notice", _Notice)
    held: list[asyncio.Task[None]] = []

    async def reading(ctx: StreamContext, engine: _Engine) -> AsyncIterator[object]:
        async def giving() -> None:
            for _ in range(3):
                await asyncio.sleep(0)
            engine.room = True

        engine.room = True
        yield {"n": 1}
        engine.room = False
        ctx.behind(5, Headroom())
        ctx.replan()
        # Room comes only once the read has ended.
        held.append(asyncio.create_task(giving()))

    assert _routed(reading) == [{"n": 1}, ("behind", 5, None, None), ("replan",)]


def test_an_async_read_s_alternating_notices_keep_one_of_each_kind_between_yields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_native, "Notice", _Notice)

    async def reading(ctx: StreamContext, engine: _Engine) -> AsyncIterator[object]:
        engine.room = True
        yield {"n": 1}
        engine.room = False
        for records in range(100):
            ctx.behind(records, Headroom())
            ctx.replan()
        engine.room = True
        yield {"n": 2}

    # A replan said again is said once, and a lag is its newest.
    expected = [{"n": 1}, ("behind", 99, None, None), ("replan",), {"n": 2}]
    assert _routed(reading) == expected


def test_a_sync_read_s_notices_are_sent_as_it_says_them(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_native, "Notice", _Notice)
    sent: list[object] = []

    class Engine:
        def notify(self, notice: object) -> None:
            sent.append(notice)

    ctx = StreamContext(cast("_native.StreamContext", Engine()))
    ctx.behind(5, Headroom())
    ctx.behind(3, Headroom(time=timedelta(seconds=1)))
    ctx.replan()
    assert sent == [
        ("behind", 5, None, None),
        ("behind", 3, None, timedelta(seconds=1)),
        ("replan",),
    ]


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


def reading_threads() -> list[threading.Thread]:
    """The daemon threads of rdlt's that `threading` holds."""
    return [thread for thread in threading.enumerate() if thread.name.startswith("rdlt-")]


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


@stream(name="parted", partitions=lambda: [str(n) for n in range(32)])
def parted(ctx: StreamContext) -> Iterator[object]:
    yield {"partition": ctx.partition.id}


@stream(name="logged", partitions=lambda: [str(n) for n in range(32)])
def logged(ctx: StreamContext) -> Iterator[object]:
    return Logged(ctx.partition.id)


@pytest.mark.parametrize("read", [parted, logged], ids=["rows", "rows_that_log_as_let_go"])
def test_a_run_s_reads_leave_threading_only_daemon_threads_that_wait_fewer_than_its_reads(
    tmp_path: Path, connectors: Path, read: ReadStream
) -> None:
    before = {thread.ident for thread in reading_threads()}
    built = rdlt.pipeline(
        "parted",
        source=read,
        destination=into_sqlite(tmp_path),
        resources=rdlt.Resources(partitions=2),
    )
    assert built.build(host(tmp_path, connectors)).run().report.rows == 32
    reading = f"rdlt-read-{read.name}"
    deadline = time.monotonic() + 30
    while (ended := ended_threads()) or any(t.name == reading for t in reading_threads()):
        assert time.monotonic() < deadline, f"threading holds {ended} and {reading_threads()}"
        time.sleep(0.05)
    started = [thread for thread in reading_threads() if thread.ident not in before]
    assert len(started) < 32, "a run starts daemon threads for what it reads at once"


def test_a_forked_child_s_first_run_is_answered_though_threading_s_lock_was_held_at_the_fork(
    tmp_path: Path,
) -> None:
    script = textwrap.dedent(
        f"""
        import os, signal, sys, threading, time, warnings
        warnings.simplefilter("ignore")
        sys.path.append({str(Path(__file__).parents[1])!r})
        import rdlt
        from rdlt.connector import stream
        from tests.connectors import memory

        @stream(name="rows")
        def rows():
            yield {{"id": 1}}

        @rdlt.transform()
        def kept(batch):
            return batch

        held, release = threading.Event(), threading.Event()
        def hold():
            with vars(threading)["_active_limbo_lock"]:
                held.set()
                release.wait()
        threading.Thread(target=hold, daemon=True).start()
        held.wait()
        child = os.fork()
        if child == 0:
            outcome = rdlt.pipeline(
                "forked",
                source=rows,
                destination=rdlt.local(memory.Memory, config={{"store": "forked"}}),
                streams=[rdlt.stream("rows", transform=[kept])],
            ).build(rdlt.Host()).run()
            os._exit(0 if outcome.report.status is rdlt.RunStatus.SUCCEEDED else 3)
        release.set()
        deadline = time.monotonic() + 30
        while not (ended := os.waitpid(child, os.WNOHANG))[0]:
            if time.monotonic() > deadline:
                os.kill(child, signal.SIGKILL)
                os.waitpid(child, 0)
                sys.exit("the child's run never ended")
            time.sleep(0.05)
        sys.exit(os.waitstatus_to_exitcode(ended[1]))
        """
    )
    finished = subprocess.run(
        [sys.executable, "-c", script], timeout=60, check=False, capture_output=True, cwd=tmp_path
    )
    assert finished.returncode == 0, finished.stderr
