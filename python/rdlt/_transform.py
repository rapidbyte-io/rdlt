"""Python transforms: a batch reshaped, or rows selected, inside the engine."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeAlias, cast

from rdlt import _native
from rdlt._weak import weakly_reached

if TYPE_CHECKING:
    import pyarrow as pa

Function: TypeAlias = "Callable[[pa.RecordBatch], object]"
"""What a transform's author writes: a batch in, its reshaped batch or its mask out."""

Passes: TypeAlias = "Mapping[str, str] | Iterable[str]"
"""The output columns a transform passes on as they arrived: each the input column of its name,
or, as a mapping from output column to input column, the one it names."""


@dataclass(frozen=True, slots=True, weakref_slot=True)
class Transform:
    """A Python function the engine runs on each of a stream's batches, off its own threads.

    A transform answers the batch reshaped: the same rows in the same order, as anything with
    `__arrow_c_array__`. A filter answers one boolean a row; a row it does not keep is dropped,
    or, on a change stream merged by key, turned into a delete of that key. Its output schema is
    the one its function answers for an empty batch, unless `output` declares it.

    `passes` names the output columns the transform passes on unchanged, each with the input
    column it is, as Rust's `Transform::passes` declares them. A column it names is that input
    column whatever the function returns for it, so a column the function changes is not one it
    names. It names the stream's keys and change time, each as itself, or the stream is refused,
    coded `transform_invalid`. On a change stream no transform computes from a delete or a
    truncate, which keeps its values in those columns, the stream's keys and its change time
    alone; a filter passes every column. A column it names that its input and output do not hold
    alike, of one type, refuses the stream's schema.

    A function that never returns keeps its compute thread for as long as the process lives: a
    cancelled run ends without it, but once stuck calls reach a host's compute threads its other
    runs stop. A function that can block, on a network or a lock, needs a timeout of its own.
    """

    name: str
    function: Function
    filters: bool
    output: pa.Schema | None
    passes: tuple[tuple[str, str], ...] | None = None

    def _to_native(self) -> _native.Transform:
        """The transform as the native host runs it, reaching this object through a weak reference.

        What may still run it holds this object in the open.
        """
        reached = weakly_reached(self, f"transform {self.name!r}")

        def call(batch: pa.RecordBatch) -> object:
            return reached().function(batch)

        return _native.Transform(self.name, call, self.filters, self.output, self.passes)


def transform(
    name: str | None = None, *, output: pa.Schema | None = None, passes: Passes | None = None
) -> Callable[[Function], Transform]:
    """Makes a function a transform of the streams it is given to, passing on `passes`."""
    passed = _passed(passes)

    def made(function: Function) -> Transform:
        return Transform(
            name or function.__name__, function, filters=False, output=output, passes=passed
        )

    return made


def _passed(passes: Passes | None) -> tuple[tuple[str, str], ...] | None:
    """`passes` as pairs of an output column and the input column it passes on."""
    if passes is None:
        return None
    if isinstance(passes, str):
        message = "passes names columns, as an iterable of names or a mapping, not one string"
        raise TypeError(message)
    if isinstance(passes, Mapping):
        mapped = cast("Mapping[str, str]", passes)
        return tuple((output, arrived) for output, arrived in mapped.items())
    return tuple((name, name) for name in passes)


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
