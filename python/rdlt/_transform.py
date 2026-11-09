"""Python transforms: a batch reshaped, or rows selected, inside the engine."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeAlias, cast

from rdlt import _native
from rdlt._weak import weakly_reached

if TYPE_CHECKING:
    import pyarrow as pa

Function: TypeAlias = "Callable[[pa.RecordBatch], object]"
"""What a transform's author writes: a batch in, its reshaped batch or its mask out."""


@dataclass(frozen=True, slots=True, weakref_slot=True)
class Transform:
    """A Python function the engine runs on each of a stream's batches, off its own threads.

    A transform answers the batch reshaped: the same rows in the same order, as anything with
    `__arrow_c_array__`. A filter answers one boolean a row; a row it does not keep is dropped,
    or, on a change stream merged by key, turned into a delete of that key. Its output schema is
    the one its function answers for an empty batch, unless `output` declares it.

    A function that never returns keeps its compute thread for as long as the process lives: a
    cancelled run ends without it, but once stuck calls reach a host's compute threads its other
    runs stop. A function that can block, on a network or a lock, needs a timeout of its own.
    """

    name: str
    function: Function
    filters: bool
    output: pa.Schema | None

    def _to_native(self) -> _native.Transform:
        """The transform as the native host runs it, reaching this object through a weak reference.

        What may still run it holds this object in the open.
        """
        reached = weakly_reached(self, f"transform {self.name!r}")

        def call(batch: pa.RecordBatch) -> object:
            return reached().function(batch)

        return _native.Transform(self.name, call, self.filters, self.output)


def transform(
    name: str | None = None, *, output: pa.Schema | None = None
) -> Callable[[Function], Transform]:
    """Makes a function a transform of the streams it is given to."""

    def made(function: Function) -> Transform:
        return Transform(name or function.__name__, function, filters=False, output=output)

    return made


def _mask(function: Function) -> Function:
    """`function` with what it answers made a boolean array, one value a row."""

    def masked(batch: pa.RecordBatch) -> object:
        import pyarrow as pa  # noqa: PLC0415

        answered: object = function(batch)
        if isinstance(answered, pa.ChunkedArray):
            answered = cast("pa.ChunkedArray[Any]", answered).combine_chunks()
        if isinstance(answered, pa.Array):
            array = cast("pa.Array[Any]", answered)
            if pa.types.is_null(array.type):
                return pa.nulls(len(array), pa.bool_())
            if array.type != pa.bool_():
                message = f"a filter answered an array of {array.type}, not of booleans"
                raise TypeError(message)
            return cast("object", array)
        return pa.array(cast("Iterable[Any]", answered), type=pa.bool_())

    return masked


def named_once(
    existing: tuple[tuple[str, Transform], ...], name: str, made: Transform
) -> tuple[tuple[str, Transform], ...]:
    """`existing` with `made` under `name`; refused where a different transform has that name."""
    if any(held != made for known, held in existing if known == name):
        message = f"two different transforms are named {name!r}"
        raise ValueError(message)
    return (*existing, (name, made))


def filter(name: str | None = None) -> Callable[[Function], Transform]:  # noqa: A001
    """Makes a function a filter of the streams it is given to: it answers which rows to keep."""

    def made(function: Function) -> Transform:
        return Transform(name or function.__name__, _mask(function), filters=True, output=None)

    return made
