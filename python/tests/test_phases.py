"""How a stream says it is read and planned: its read modes, change time and plans in phases."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, cast

import pytest

import rdlt
from rdlt.connector import (
    Cursor,
    PartitionPlan,
    ReadStream,
    StreamContext,
    StreamState,
    checkpoint,
    stream,
)
from tests.failures import said
from tests.phased import Position, cdc, host, into_sqlite


def _catalog_stream(tmp_path: Path, connectors: Path, made: object) -> Any:  # noqa: ANN401 - the catalog's JSON
    built = rdlt.pipeline("declared", source=made, destination=into_sqlite(tmp_path))  # pyright: ignore[reportArgumentType]
    (found,) = built.build(host(tmp_path, connectors)).discover().streams
    return found.raw


@pytest.mark.parametrize(
    ("declared", "modes", "change_time"),
    [
        ({"cursor": Position}, ["full", "incremental"], None),
        ({}, ["full"], None),
        ({"cursor": Position, "read_modes": [rdlt.ReadMode.CDC]}, ["cdc"], None),
        (
            {"cursor": Position, "read_modes": ["full", "cdc"], "change_time": "changed_at"},
            ["full", "cdc"],
            ["changed_at"],
        ),
    ],
    ids=["cursor-default", "no-cursor-default", "cdc", "words-and-change-time"],
)
def test_a_stream_declares_its_read_modes_and_change_time_as_rust_s_spec_does(
    declared: dict[str, object],
    modes: list[str],
    change_time: list[str] | None,
    tmp_path: Path,
    connectors: Path,
) -> None:
    def rows(*arguments: object) -> Iterator[object]:
        del arguments
        yield {"id": 1, "changed_at": "2026-11-13T00:00:00Z"}

    made = stream("rows", primary_key="id", **declared)(rows)  # pyright: ignore[reportArgumentType]
    found = _catalog_stream(tmp_path, connectors, made)
    assert found["read_modes"] == modes
    assert found["change_time"] == change_time


@pytest.mark.parametrize("mode", [rdlt.ReadMode.INCREMENTAL, rdlt.ReadMode.CDC])
def test_a_stream_without_a_cursor_reads_neither_incrementally_nor_as_changes(
    mode: rdlt.ReadMode,
) -> None:
    with pytest.raises(TypeError, match="cursor"):

        @stream(read_modes=[mode])
        def rows() -> Iterator[object]:
            yield {"id": 1}


def test_a_plan_and_partitions_are_one_answer_said_once() -> None:
    with pytest.raises(TypeError, match=r"plan= and partitions="):

        @stream(cursor=Position, plan=lambda: PartitionPlan(["a"]), partitions=lambda: ["a"])
        def rows(start: Position, ctx: StreamContext) -> Iterator[object]:
            del start, ctx
            yield {"id": 1}


def test_a_plan_needs_a_read_told_which_partition_it_reads() -> None:
    with pytest.raises(TypeError, match="ctx"):

        @stream(cursor=Position, plan=lambda: PartitionPlan(["a"]))
        def rows(start: Position) -> Iterator[object]:
            del start
            yield {"id": 1}


def _planned_as(answer: Callable[[], PartitionPlan]) -> ReadStream:
    """A stream read as changes, planned as `answer` says, whose reads yield nothing."""

    @stream("rows", primary_key="id", cursor=Position, read_modes=["cdc"], plan=answer)
    def rows(start: Position, ctx: StreamContext) -> Iterator[object]:
        del start, ctx
        yield from ()

    return rows


@pytest.mark.parametrize(
    "answer",
    [
        lambda: PartitionPlan(["a", "b", "a"]),
        lambda: PartitionPlan(["a"], phase=1, starts={"b": Position()}),
    ],
    ids=["named-twice", "starts-unnamed"],
)
def test_a_plan_rust_refuses_fails_coded_plan_invalid(
    answer: Callable[[], PartitionPlan], tmp_path: Path, connectors: Path
) -> None:
    with pytest.raises(rdlt.SourceError) as raised:
        cdc("invalid", _planned_as(answer), tmp_path).build(host(tmp_path, connectors)).run()
    assert raised.value.report().code == "plan_invalid"


class _Elsewhere(Cursor):
    at: int = 0


@pytest.mark.parametrize(
    "start", [_Elsewhere(), {"next": 1}], ids=["another-cursor-type", "a-mapping"]
)
def test_a_plan_starts_a_partition_at_a_cursor_of_its_stream_s_type(
    start: object, tmp_path: Path, connectors: Path
) -> None:
    def answer() -> PartitionPlan:
        return PartitionPlan(["a"], phase=1, starts={"a": start})  # pyright: ignore[reportArgumentType]

    with pytest.raises(rdlt.SourceError) as raised:
        cdc("started", _planned_as(answer), tmp_path).build(host(tmp_path, connectors)).run()
    assert raised.value.report().connector_code == "type_error"
    assert "a start is the stream's cursor" in said(raised.value)


def test_a_phase_begins_with_its_partitions_read_from_where_its_plan_starts_them(
    tmp_path: Path, connectors: Path
) -> None:
    planned: list[tuple[int, dict[str, object]]] = []
    read: list[tuple[str, object]] = []

    def plan(state: StreamState) -> PartitionPlan:
        planned.append((state.phase, dict(state.partitions)))
        if state.phase == 1:
            return PartitionPlan(["b"])
        if state.partitions.get("a") != Position(next=1):
            return PartitionPlan(["a"])
        return PartitionPlan(["b"], phase=1, starts={"b": Position(next=5)})

    # Read as changes, as only a change stream reads in phases; each read checkpoints alone.
    @stream("rows", primary_key="id", cursor=Position, read_modes=["cdc"], plan=plan)
    def rows(start: Position, ctx: StreamContext) -> Iterator[object]:
        read.append((ctx.partition.id, start))
        yield checkpoint(Position(next=start.next + 1))

    built = cdc("phased", rows, tmp_path)
    every_host = host(tmp_path, connectors)
    built.build(every_host).run()
    assert read == [("a", Position()), ("b", Position(next=5))]
    assert all(type(start) is Position for _, start in read)
    # Planned as the run began, once "a" ended, and once "b" did, told the phase it began.
    assert [phase for phase, _ in planned] == [0, 0, 1]
    assert planned[1] == (0, {"a": Position(next=1)})
    built.build(every_host).run()
    assert planned[3] == (1, {"b": Position(next=6)})
    assert read[2:] == [("b", Position(next=6))]


def test_partitions_answered_as_one_string_fail_the_plan(tmp_path: Path, connectors: Path) -> None:
    @stream("rows", primary_key="id", cursor=Position, partitions=lambda: "ab")
    def rows(start: Position, ctx: StreamContext) -> Iterator[object]:
        del start, ctx
        yield from ()

    built = rdlt.pipeline(
        "one-string",
        source=rows,
        destination=into_sqlite(tmp_path),
        retry=rdlt.Retry(max_attempts=1),
    )
    with pytest.raises(rdlt.SourceError) as raised:
        built.build(host(tmp_path, connectors)).run()
    assert raised.value.report().connector_code == "type_error"
    assert "not one string" in said(raised.value)


@pytest.mark.parametrize("phase", [-1, 2**16, True, 1.0, "1"])
def test_a_phase_is_a_number_a_u16_holds(phase: object) -> None:
    with pytest.raises(ValueError, match="phase"):
        PartitionPlan(["a"], phase=cast("int", phase))
