"""What an author's method answers: the answer its docs state, and nothing else read as one."""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterable
from typing import TypeVar, cast

T = TypeVar("T")


def answered(method: str, value: object, expected: type[T] | tuple[type, ...], stated: str) -> T:
    """`value`, what the author's `method` answered, where it is `stated`: an `expected`.

    rdlt calls each method on a thread of its own, where it may block, and reads what it answers
    as it returns, so nothing that runs later is an answer: a coroutine or any other awaitable,
    an async generator unless an async iterable is stated, or a generator unless an iterable is.
    Text is no iterable of answers. What was refused is closed where it can be, so its body
    never runs.

    Raises:
        TypeError: it answered anything else, naming `method` and what it answered.
    """
    wanted = expected if isinstance(expected, tuple) else (expected,)
    text = isinstance(value, str | bytes) and str not in wanted
    if isinstance(value, wanted) and not inspect.isawaitable(value) and not text:
        return cast("T", value)
    if inspect.iscoroutine(value) or inspect.isgenerator(value):
        value.close()
    raise TypeError(_refused(method, value, stated, asynchronous=AsyncIterable in wanted))


def nothing(method: str, value: object) -> None:
    """Refuses `value`, what the author's `method` answered, unless it is `None`.

    Raises:
        TypeError: it answered anything else, `False` included, naming `method` and its answer.
    """
    answered(method, value, type(None), "None, and raises where it fails")


def _refused(method: str, value: object, stated: str, *, asynchronous: bool) -> str:
    """Why `value` is not what `method` answers, `stated`, which may be an async iterable."""
    if inspect.iscoroutine(value) and asynchronous:
        return f"{method} is async and yields nothing: an async read is an async generator"
    if inspect.iscoroutine(value) or inspect.isasyncgen(value):
        return (
            f"{method} is async: rdlt calls it on a thread of its own, where it may block, so it "
            f"is a plain def answering {stated}"
        )
    if inspect.isgenerator(value):
        return f"{method} yields: it answers {stated} as it returns"
    if inspect.isawaitable(value):
        shown = type(value).__name__
        return f"{method} answered {shown}, which rdlt never awaits: it answers {stated}"
    shown = repr(value) if value is None or isinstance(value, bool) else type(value).__name__
    return f"{method} answered {shown}: it answers {stated}"
