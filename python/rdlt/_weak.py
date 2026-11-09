"""What the native side reaches of Python's objects, without keeping them alive."""

from __future__ import annotations

import weakref
from collections.abc import Callable
from typing import TypeVar

_T = TypeVar("_T")


def weakly_reached(owner: _T, what: str) -> Callable[[], _T]:
    """A function answering `owner`, which it holds only weakly.

    What the native side holds is opaque to Python's collector, so a strong reference from it to
    a function that reaches the pipeline it serves could never be collected, and the pipeline's
    connector processes would live on with it. The native side holds this instead, and what may
    still need `owner` holds it in the open, where the collector sees it.
    """
    held = weakref.ref(owner)

    def reached() -> _T:
        strong = held()
        if strong is None:
            message = f"what held {what} was dropped"
            raise ReferenceError(message)
        return strong

    return reached
