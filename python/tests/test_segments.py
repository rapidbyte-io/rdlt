"""A commit's segment sets, as the Rust SDK's `SegmentSet`: what `in`, `len` and iteration say."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from rdlt.connector import SegmentRange, SegmentSet


def runs(ids: set[int]) -> SegmentSet:
    """The segment set of `ids`, as the Rust SDK writes one: its runs of consecutive ids."""
    ranges: list[SegmentRange] = []
    for segment in sorted(ids):
        if ranges and ranges[-1].last + 1 == segment:
            ranges[-1] = SegmentRange(ranges[-1].first, segment)
        else:
            ranges.append(SegmentRange(segment, segment))
    return SegmentSet(tuple(ranges))


@given(ids=st.sets(st.integers(min_value=0, max_value=64)), asked=st.integers(-2, 66))
def test_a_segment_set_holds_its_ids_and_no_other(ids: set[int], asked: int) -> None:
    segments = runs(ids)
    assert (asked in segments) is (asked in ids)
    assert list(segments) == sorted(ids)
    assert len(segments) == len(ids)
    assert bool(segments) is bool(ids)


@pytest.mark.parametrize(
    "ranges",
    [
        (SegmentRange(3, 2),),
        (SegmentRange(4, 6), SegmentRange(1, 2)),
        (SegmentRange(1, 4), SegmentRange(3, 6)),
        (SegmentRange(1, 2), SegmentRange(3, 6)),
    ],
)
def test_ranges_not_ascending_disjoint_and_apart_are_no_segment_set(
    ranges: tuple[SegmentRange, ...],
) -> None:
    with pytest.raises(ValueError):  # noqa: PT011 - the refusal has no code
        SegmentSet(ranges)


@pytest.mark.parametrize("asked", ["1", 1.0, True, None])
def test_a_segment_set_refuses_to_say_whether_it_holds_what_is_no_segment_id(asked: object) -> None:
    with pytest.raises(TypeError):
        _ = asked in SegmentSet((SegmentRange(1, 1),))


class _Index:
    """An integer that is no `int`, as numpy's are."""

    def __init__(self, value: int) -> None:
        self.value = value

    def __index__(self) -> int:
        return self.value


def test_a_segment_set_takes_any_integer_as_a_segment_id() -> None:
    segments = SegmentSet((SegmentRange(1, 2),))
    assert _Index(2) in segments
    assert _Index(3) not in segments
