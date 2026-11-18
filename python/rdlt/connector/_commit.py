"""What a commit publishes and what a destination acknowledges, as the Rust SDK's types say."""

from __future__ import annotations

import bisect
import itertools
import operator
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any, TypedDict, cast

from rdlt.model import Json


@dataclass(frozen=True, slots=True)
class SegmentRange:
    """An inclusive run of consecutive segment ids, as the Rust SDK's `SegmentRange` is."""

    first: int
    last: int


@dataclass(frozen=True, slots=True)
class SegmentSet:
    """A set of segment ids, as the Rust SDK's `SegmentSet` is: `segment in segments` asks.

    `ranges` are its runs of consecutive ids, ascending, disjoint and not adjacent, as a
    destination that keeps staging by segment selects it, `segment between first and last`.
    `len()` counts its ids and iterating walks them, ascending; `bool()` is whether it holds any.

    Raises:
        ValueError: the ranges are not ascending, disjoint and non-adjacent.
    """

    ranges: tuple[SegmentRange, ...] = ()

    def __post_init__(self) -> None:
        ranges = tuple(self.ranges)
        object.__setattr__(self, "ranges", ranges)
        ordered = all(each.first <= each.last for each in ranges) and all(
            before.last + 1 < after.first for before, after in itertools.pairwise(ranges)
        )
        if not ordered:
            message = "segment ranges must be ascending, disjoint and non-adjacent"
            raise ValueError(message)

    def __contains__(self, segment: object) -> bool:
        """Whether the set holds `segment`, any integer, as numpy's are.

        Raises:
            TypeError: `segment` is no segment id: a bool, or no integer, as the text of one is.
        """
        if isinstance(segment, bool):
            message = "a segment set holds segment ids, and a bool is none"
            raise TypeError(message)
        segment = operator.index(cast("Any", segment))
        index = bisect.bisect_left(self.ranges, segment, key=lambda each: each.last)
        return index < len(self.ranges) and self.ranges[index].first <= segment

    def __iter__(self) -> Iterator[int]:
        for each in self.ranges:
            yield from range(each.first, each.last + 1)

    def __len__(self) -> int:
        return sum(each.last - each.first + 1 for each in self.ranges)

    def __bool__(self) -> bool:
        return bool(self.ranges)


class CommitKey(TypedDict):
    """A commit's key, as the Rust SDK's `CommitKey` is.

    `epoch` is the epoch of the session that makes the commit, and `seq` its place among that
    session's commits, from 1.
    """

    epoch: int
    seq: int


class CommitMeta(TypedDict):
    """Everything one commit publishes, as the Rust SDK's `CommitMeta` is: what `commit` is told.

    `key` is the commit's key, and `after` the key of the commit the session expects landed, none
    before any did. `segments` are the sealed segments it publishes, and `abandoned` those of the
    load the engine abandoned since its last commit, whose staging it removes. `state_delta` holds
    its state changes, each `{"put": {"key": k, "value": v}}`, the value its JSON text, or
    `{"delete": k}`; `finish_generations` the replace generations it swaps in, each a table's path
    and its generation; `child_tables` the child tables of merge tables, each `{"table": name,
    "merge": key}`, which follow the root rows it publishes; `drop_tables` the tables it drops,
    each `{"path": path, "name": name}`. Its segment sets are no JSON: their `ranges` are.
    """

    key: CommitKey
    after: CommitKey | None
    segments: SegmentSet
    abandoned: SegmentSet
    state_delta: list[dict[str, Json]]
    finish_generations: list[list[Json]]
    child_tables: list[dict[str, Json]]
    drop_tables: list[dict[str, Json]]


class Receipt(TypedDict):
    """A destination's acknowledgment of a commit, as the Rust SDK's `Receipt` is.

    `key` is the commit's key, `committed_at` when the destination committed,
    `{"secs_since_epoch": s, "nanos_since_epoch": n}`, and `rows` and `bytes` what it published,
    the bytes as the destination measures them.
    """

    key: CommitKey
    committed_at: dict[str, int]
    rows: int
    bytes: int


def told(meta: dict[str, Json]) -> CommitMeta:
    """`meta`, a commit as the native side hands it over, as a destination's `commit` is told it."""
    sets = {name: _segments(meta[name]) for name in ("segments", "abandoned")}
    return cast("CommitMeta", {**meta, **sets})


def _segments(ranges: Json) -> SegmentSet:
    """The segment set `ranges` holds, as the Rust SDK writes one: its ranges."""
    spans = cast("list[Mapping[str, int]]", ranges)
    return SegmentSet(tuple(SegmentRange(span["first"], span["last"]) for span in spans))
