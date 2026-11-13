"""The change batches a stream read as changes yields, in phases or not, sync or async."""

from __future__ import annotations

import sqlite3
from collections.abc import AsyncIterator, Callable, Iterator, Sequence
from contextlib import closing
from pathlib import Path
from typing import Any, NamedTuple

import pyarrow as pa
import pytest

import rdlt
from rdlt.connector import (
    OP_COLUMN,
    SEQ_COLUMN,
    UNCHANGED_COLUMN,
    ChangeOp,
    Partition,
    PartitionPlan,
    ReadStream,
    StreamContext,
    StreamState,
    changes,
    checkpoint,
    stream,
)
from tests.phased import Position, cdc, host, into_sqlite

CAPTURED = 100
"""The position a snapshot captured: its rows carry it, and its changes start after it."""


class Change(NamedTuple):
    """A change row: its op, position and key, its `value` and `n`, and whether it keeps `value`."""

    op: ChangeOp
    position: int
    key: int
    value: str | None
    n: int | None
    unchanged: bool


def table(tmp_path: Path) -> list[tuple[object, ...]]:
    with closing(sqlite3.connect(tmp_path / "out.db")) as database:
        return database.execute('SELECT id, value, n FROM "rows" ORDER BY id').fetchall()


def changed(rows: Sequence[Change]) -> pa.RecordBatch:
    """`rows` as a change batch, a row leaving `value` unchanged flagging it, field 1."""
    return pa.record_batch(
        {
            "id": pa.array([row.key for row in rows], pa.int64()),
            "value": pa.array([row.value for row in rows], pa.string()),
            "n": pa.array([row.n for row in rows], pa.int64()),
            OP_COLUMN: pa.array([int(row.op) for row in rows], pa.int8()),
            SEQ_COLUMN: pa.array([row.position.to_bytes(16, "big") for row in rows], pa.binary(16)),
            UNCHANGED_COLUMN: pa.array(
                [b"\x02" if row.unchanged else None for row in rows], pa.binary()
            ),
        }
    )


SNAPSHOT = [
    Change(ChangeOp.INSERT, CAPTURED, key, f"value {key}", 0, unchanged=False) for key in range(3)
]
"""A snapshot of three keys."""

CHANGES = [
    Change(ChangeOp.UPDATE, CAPTURED + 1, 0, "changed", 1, unchanged=False),
    Change(ChangeOp.UPDATE, CAPTURED + 2, 1, None, 2, unchanged=True),
    Change(ChangeOp.DELETE, CAPTURED + 3, 2, None, None, unchanged=False),
]
"""The changes after the snapshot: an update, an update leaving `value` unchanged, a delete."""

CHANGED = [(0, "changed", 1), (1, "value 1", 2)]
"""The table the snapshot and its changes leave."""


def phased(started: list[tuple[str, int]]) -> ReadStream:
    """A stream read as its snapshot and then its changes, saying where each read started."""

    def planned(state: StreamState) -> PartitionPlan:
        if state.phase == 1:
            return PartitionPlan([Partition("changes", unbounded=True)])
        snapshot = state.partitions.get("snapshot")
        if isinstance(snapshot, Position) and snapshot.next == len(SNAPSHOT):
            return PartitionPlan(
                [Partition("changes", unbounded=True)],
                phase=1,
                starts={"changes": Position(next=CAPTURED + 1)},
            )
        return PartitionPlan(["snapshot"])

    @stream(primary_key="id", cursor=Position, read_modes=["cdc"], plan=planned)
    def rows(start: Position, ctx: StreamContext) -> Iterator[object]:
        started.append((ctx.partition.id, start.next))
        if ctx.partition.id == "snapshot":
            yield changes(changed(SNAPSHOT[start.next :]))
            yield checkpoint(Position(next=len(SNAPSHOT)))
            return
        unread = [row for row in CHANGES if row.position >= start.next]
        if unread:
            yield changes(changed(unread))
            yield checkpoint(Position(next=unread[-1].position + 1))

    return rows


def test_a_stream_read_in_phases_reads_its_snapshot_then_its_changes_from_where_it_captured(
    tmp_path: Path, connectors: Path
) -> None:
    started: list[tuple[str, int]] = []
    built = cdc("phased", phased(started), tmp_path)
    every_host = host(tmp_path, connectors)
    built.build(every_host).run()
    assert started == [("snapshot", 0), ("changes", CAPTURED + 1)]
    assert table(tmp_path) == CHANGED
    built.build(every_host).run()
    assert started[2:] == [("changes", CAPTURED + 4)]
    assert table(tmp_path) == CHANGED


def _sync(batches: Sequence[object]) -> Callable[..., Iterator[object]]:
    def read(start: Position) -> Iterator[object]:
        del start
        for each in batches:
            yield changes(each)
        yield checkpoint(Position(next=CAPTURED + 4))

    return read


def _async(batches: Sequence[object]) -> Callable[..., AsyncIterator[object]]:
    async def read(start: Position) -> AsyncIterator[object]:
        del start
        for each in batches:
            yield changes(each)
        yield checkpoint(Position(next=CAPTURED + 4))

    return read


@pytest.mark.parametrize("made", [_sync, _async], ids=["sync", "async"])
def test_change_batches_and_tables_land_from_a_sync_or_an_async_read(
    made: Callable[[Sequence[object]], Callable[..., object]], tmp_path: Path, connectors: Path
) -> None:
    batches = [
        changed(SNAPSHOT),
        pa.Table.from_batches([changed(CHANGES[:1]), changed(CHANGES[1:])]),
    ]
    rows = stream("rows", primary_key="id", cursor=Position, read_modes=["cdc"])(made(batches))
    cdc("changed", rows, tmp_path).build(host(tmp_path, connectors)).run()
    assert table(tmp_path) == CHANGED


def test_an_empty_change_batch_is_skipped_unread(tmp_path: Path, connectors: Path) -> None:
    empty = pa.record_batch({"id": pa.array([], pa.int64())})
    rows = stream("rows", primary_key="id", cursor=Position, read_modes=["cdc"])(
        _sync([empty, changed(SNAPSHOT)])
    )
    cdc("empty", rows, tmp_path).build(host(tmp_path, connectors)).run()
    assert table(tmp_path) == [(key, f"value {key}", 0) for key in range(3)]


def _without(column: str) -> pa.RecordBatch:
    whole = changed(SNAPSHOT)
    return whole.drop_columns([column])


def _replaced(column: str, values: pa.Array[Any]) -> pa.RecordBatch:
    return _without(column).append_column(column, values)


@pytest.mark.parametrize(
    "refused",
    [
        lambda: _without("_rdlt_op"),
        lambda: _replaced("_rdlt_op", pa.array([0, 0, 0], pa.int32())),
        lambda: _replaced("_rdlt_op", pa.array([0, 9, 0], pa.int8())),
        lambda: _replaced("_rdlt_op", pa.array([0, None, 0], pa.int8())),
        lambda: _without("_rdlt_seq"),
        lambda: _replaced("_rdlt_seq", pa.array([b"\x00" * 8] * 3, pa.binary(8))),
        lambda: _replaced("_rdlt_seq", pa.array([bytes(16), None, bytes(16)], pa.binary(16))),
        lambda: _replaced("_rdlt_unchanged", pa.array(["", "", ""], pa.string())),
    ],
    ids=[
        "no-op",
        "op-int32",
        "op-unknown",
        "op-null",
        "no-seq",
        "seq-8-bytes",
        "seq-null",
        "unchanged-text",
    ],
)
def test_a_change_batch_without_its_columns_as_rust_reads_them_fails_coded_change_batch(
    refused: Callable[[], pa.RecordBatch], tmp_path: Path, connectors: Path
) -> None:
    rows = stream("rows", primary_key="id", cursor=Position, read_modes=["cdc"])(_sync([refused()]))
    with pytest.raises(rdlt.SourceError) as raised:
        cdc("refused", rows, tmp_path).build(host(tmp_path, connectors)).run()
    assert raised.value.report().connector_code == "change_batch"
    assert not raised.value.retryable


def _rows_read_as_changes(start: Position) -> Iterator[object]:
    del start
    yield {"id": 1}
    yield checkpoint(Position(next=1))


@pytest.mark.parametrize(
    ("declared", "read", "reading"),
    [
        ({}, "full", lambda: _sync([changed(SNAPSHOT)])),
        ({"read_modes": ["full", "cdc"]}, "full", lambda: _sync([changed(SNAPSHOT)])),
        ({"read_modes": ["cdc"]}, "cdc", lambda: _rows_read_as_changes),
    ],
    ids=["changes-undeclared", "changes-read-full", "rows-read-as-changes"],
)
def test_a_push_of_another_kind_than_the_read_mode_reads_fails_as_rust_s_does(
    declared: dict[str, object],
    read: str,
    reading: Callable[[], Callable[..., object]],
    tmp_path: Path,
    connectors: Path,
) -> None:
    rows = stream("rows", primary_key="id", cursor=Position, **declared)(reading())  # pyright: ignore[reportArgumentType]
    write = rdlt.merge("id") if read == "cdc" else rdlt.append()
    built = rdlt.pipeline(
        "unexpected",
        source=rows,
        destination=into_sqlite(tmp_path),
        streams=[rdlt.stream("rows", read=read, write=write)],  # pyright: ignore[reportArgumentType]
        retry=rdlt.Retry(max_attempts=1),
    )
    with pytest.raises(rdlt.SourceError) as raised:
        built.build(host(tmp_path, connectors)).run()
    assert raised.value.report().code == "push_unexpected"


def test_changes_take_arrow_alone() -> None:
    with pytest.raises(TypeError):
        changes({"id": 1})
