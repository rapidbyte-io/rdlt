"""How a read of a stream with a cursor ends, by read mode, write mode and where the read stops.

Each case runs three times, on six rows, then nine, then the same nine. Each read yields the rows
after its cursor and checkpoints as its end says; the engine decides what lands, in process and in
a connector's own process alike.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pytest

import rdlt
from rdlt.connector import Config, Cursor, ReadStream, checkpoint, source, stream
from tests.launchers import numbers_launcher

AVAILABLE = (6, 9, 9)
"""How many rows each run finds: ids 0 to 5, then 0 to 8, twice."""

Read = Literal["full", "incremental"]

End = Literal["checkpoint", "rows", "never", "leading"]
"""Where a read stops: on a checkpoint after its last row, on rows after a checkpoint halfway,
having never checkpointed, or having checkpointed only before its first row."""

Write = Literal["append", "replace", "merge"]

WRITES: dict[Write, tuple[rdlt.WriteMode, rdlt.Json]] = {
    "append": (rdlt.append(), "append"),
    "replace": (rdlt.replace(), "replace"),
    "merge": (rdlt.merge("id"), {"merge": {"key": ["id"]}}),
}
"""Each write mode, as a pipeline's stream and as a document's entry say it."""

Form = Literal["function", "method", "document", "pattern"]
"""How the stream is written and named: a function, a source class's method placed in process,
a class a document names, or a class placed in process whose stream a pattern selects."""

Run = tuple[type[rdlt.Error] | None, str | None, list[int]]
"""What a run comes to: the error it fails with and its code, if it fails, and the ids its table
then holds."""


class Since(Cursor):
    """The last id a read checkpointed."""

    after: int = -1


@dataclass
class Reading:
    """What the next read finds, `available` rows, and where it stops; a test changes both."""

    available: int
    end: End

    def yielded(self, since: Since) -> Iterator[object]:
        """The rows after `since`, checkpointed as `end` says."""
        after = list(range(since.after + 1, self.available))
        halfway = (len(after) + 1) // 2 - 1
        if self.end == "leading":
            yield checkpoint(since)
        for position, n in enumerate(after):
            yield {"id": n}
            if (self.end in {"checkpoint", "rows"} and position == halfway) or (
                self.end == "checkpoint" and position == len(after) - 1
            ):
                yield checkpoint(Since(after=n))


def ids(count: int) -> list[int]:
    return list(range(count))


def as_function(reading: Reading) -> ReadStream:
    @stream(primary_key="id", cursor=Since)
    def users(since: Since) -> Iterator[object]:
        yield from reading.yielded(since)

    return users


def as_class(reading: Reading) -> type:
    @source(id="io.example.ends", config=Config, version="1.0.0")
    class Users:
        def __init__(self, config: Config) -> None:
            del config

        def check(self) -> None:
            """Reads need nothing checked first."""

        @stream(primary_key="id", cursor=Since)
        def users(self, since: Since) -> Iterator[object]:
            yield from reading.yielded(since)

    return Users


FULL_APPENDED: list[Run] = [
    (None, None, ids(6)),
    (None, None, sorted(ids(6) + ids(9))),
    (None, None, sorted(ids(6) + ids(9) + ids(9))),
]
FULL_WHOLE: list[Run] = [(None, None, ids(6)), (None, None, ids(9)), (None, None, ids(9))]
EVERY_ROW_ONCE: list[Run] = [(None, None, ids(6)), (None, None, ids(9)), (None, None, ids(9))]
UP_TO_THE_LAST_CHECKPOINT: list[Run] = [
    (None, None, ids(3)),
    (None, None, ids(6)),
    (None, None, ids(8)),
]
CHECKPOINT_MISSING: list[Run] = [(rdlt.SourceError, "checkpoint_missing", [])] * 3
PLAN_MODE_INVALID: list[Run] = [(rdlt.ConfigError, "plan_mode_invalid", [])] * 3

CASES: list[tuple[Read, End, Write, list[Run]]] = [
    ("full", "checkpoint", "append", FULL_APPENDED),
    ("full", "rows", "append", FULL_APPENDED),
    ("full", "never", "append", FULL_APPENDED),
    ("full", "leading", "append", FULL_APPENDED),
    ("full", "checkpoint", "replace", FULL_WHOLE),
    ("full", "rows", "replace", FULL_WHOLE),
    ("full", "never", "replace", FULL_WHOLE),
    ("full", "leading", "replace", FULL_WHOLE),
    ("full", "checkpoint", "merge", FULL_WHOLE),
    ("full", "rows", "merge", FULL_WHOLE),
    ("full", "never", "merge", FULL_WHOLE),
    ("full", "leading", "merge", FULL_WHOLE),
    ("incremental", "checkpoint", "append", EVERY_ROW_ONCE),
    ("incremental", "rows", "append", UP_TO_THE_LAST_CHECKPOINT),
    ("incremental", "never", "append", CHECKPOINT_MISSING),
    ("incremental", "leading", "append", CHECKPOINT_MISSING),
    ("incremental", "checkpoint", "replace", PLAN_MODE_INVALID),
    ("incremental", "rows", "replace", PLAN_MODE_INVALID),
    ("incremental", "never", "replace", PLAN_MODE_INVALID),
    ("incremental", "leading", "replace", PLAN_MODE_INVALID),
    ("incremental", "checkpoint", "merge", EVERY_ROW_ONCE),
    ("incremental", "rows", "merge", UP_TO_THE_LAST_CHECKPOINT),
    ("incremental", "never", "merge", CHECKPOINT_MISSING),
    ("incremental", "leading", "merge", CHECKPOINT_MISSING),
]
"""Each read mode, end and write mode, and what each of its three runs comes to."""

FORMS: dict[Read, tuple[Form, ...]] = {
    "full": ("function", "method", "document", "pattern"),
    "incremental": ("function", "method"),
}
"""The forms each read mode is read in: a document's entry and a pattern read in full."""


def into_sqlite(into: Path) -> rdlt.Connector:
    return rdlt.Connector(
        "io.rapidbyte.sqlite",
        config={"path": str(into / "out.db")},
        access=rdlt.Access(write=[into]),
    )


def built(
    form: Form, read: Read, write: Write, reading: Reading, *, into: Path, connectors: Path
) -> tuple[rdlt.PipelineBuilder, rdlt.Host]:
    """The pipeline `form` makes, read and written as said, into `into`, and the host it needs."""
    host = rdlt.Host(connectors=connectors, writable=[into], trust_binaries=True)
    mode, written = WRITES[write]
    if form == "document":
        document: dict[str, rdlt.Json] = {
            "version": 1,
            "pipeline": "ends",
            "source": {"connector": "io.example.ends"},
            "destination": {
                "connector": "io.rapidbyte.sqlite",
                "config": {"path": str(into / "out.db")},
                "access": {"write": [str(into)]},
            },
            "streams": [{"name": "users", "write": written}],
            "retry": {"max_attempts": 1},
        }
        return rdlt.Pipeline.from_document(document), host.source(as_class(reading))
    placed: ReadStream | rdlt.Local
    if form == "function":
        placed = as_function(reading)
    else:
        placed = rdlt.local(as_class(reading), config={})
    entry = (
        rdlt.stream("*", write=mode)
        if form == "pattern"
        else rdlt.stream("users", read=read, write=mode)
    )
    pipeline = rdlt.pipeline(
        "ends",
        source=placed,
        destination=into_sqlite(into),
        streams=[entry],
        retry=rdlt.Retry(max_attempts=1),
    )
    return pipeline, host


def landed(into: Path, table: str = "users") -> list[int]:
    """The ids `table` holds, each as often as it lands; none where no table was made."""
    with sqlite3.connect(into / "out.db") as database:
        made = database.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
        ).fetchone()
        if made is None:
            return []
        return sorted(row[0] for row in database.execute(f'SELECT "id" FROM "{table}"'))


@pytest.mark.parametrize(
    "case",
    [
        pytest.param((form, read, end, write, runs), id=f"{form}-{read}-{end}-{write}")
        for read, end, write, runs in CASES
        for form in FORMS[read]
    ],
)
def test_a_read_lands_what_its_read_mode_and_its_end_say_in_every_run(
    case: tuple[Form, Read, End, Write, list[Run]], tmp_path: Path, connectors: Path
) -> None:
    form, read, end, write, runs = case
    reading = Reading(AVAILABLE[0], end)
    pipeline, host = built(form, read, write, reading, into=tmp_path, connectors=connectors)
    found: list[Run] = []
    for rows in AVAILABLE:
        reading.available = rows
        try:
            pipeline.build(host).run()
        except rdlt.Error as failed:
            found.append((type(failed), failed.code, landed(tmp_path)))
        else:
            found.append((None, None, landed(tmp_path)))
    assert found == runs


@pytest.mark.parametrize("form", ["function", "method"])
def test_a_resumed_read_that_yields_rows_and_no_checkpoint_fails_and_lands_none_of_them(
    form: Form, tmp_path: Path, connectors: Path
) -> None:
    reading = Reading(AVAILABLE[0], "checkpoint")
    pipeline, host = built(
        form, "incremental", "append", reading, into=tmp_path, connectors=connectors
    )
    pipeline.build(host).run()
    reading.available, reading.end = AVAILABLE[1], "never"
    with pytest.raises(rdlt.SourceError) as raised:
        pipeline.build(host).run()
    assert raised.value.code == "checkpoint_missing"
    assert landed(tmp_path) == ids(6)


SPAWNED: list[tuple[Read, End, list[Run]]] = [
    ("full", "rows", FULL_APPENDED),
    ("full", "never", FULL_APPENDED),
    ("incremental", "rows", UP_TO_THE_LAST_CHECKPOINT),
    ("incremental", "never", CHECKPOINT_MISSING),
]
"""Reads of the numbers source, whose stream declares no partitions, through its launcher, each
appended, and what each of their three runs comes to."""


@pytest.fixture(scope="module")
def launchers(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A directory of its own holding the numbers source's launcher."""
    return numbers_launcher(tmp_path_factory.mktemp("launchers")).parent


@pytest.mark.parametrize(
    "case", [pytest.param(case, id=f"{case[0]}-{case[1]}") for case in SPAWNED]
)
def test_a_spawned_connector_s_read_lands_what_its_read_mode_and_its_end_say_in_every_run(
    case: tuple[Read, End, list[Run]], tmp_path: Path, connectors: Path, launchers: Path
) -> None:
    read, end, runs = case
    host = rdlt.Host(connectors=[connectors, launchers], writable=[tmp_path], trust_binaries=True)
    found: list[Run] = []
    for rows in AVAILABLE:
        pipeline = rdlt.pipeline(
            "spawned",
            source=rdlt.Connector("io.example.numbers", config={"rows": rows, "end": end}),
            destination=into_sqlite(tmp_path),
            streams=[rdlt.stream("numbers", read=read, write=rdlt.append())],
            retry=rdlt.Retry(max_attempts=1),
        )
        try:
            pipeline.build(host).run()
        except rdlt.Error as failed:
            found.append((type(failed), failed.code, landed(tmp_path, "numbers")))
        else:
            found.append((None, None, landed(tmp_path, "numbers")))
    assert found == runs
