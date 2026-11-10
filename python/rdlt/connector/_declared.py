"""What the connector decorators share: one role a class, one class an id, and its placements."""

from __future__ import annotations

import functools
import weakref
from typing import Any, Generic, TypeAlias, TypeVar

Registered: TypeAlias = tuple[tuple[str, str, type], ...]
"""Python connector classes, each with the role it serves and the id it answers to."""

_D = TypeVar("_D")

_ROLES = {"source": "__rdlt_source__", "destination": "__rdlt_destination__"}
"""The attribute each decorator leaves on a class: a class it did not decorate has none of its
own."""


def declare(connector: type, role: str, declared: object) -> None:
    """Leaves `declared` on `connector` as what it declares as `role`.

    Raises:
        TypeError: the class declares the other role: a class declares one.
    """
    for other, attribute in _ROLES.items():
        if other != role and attribute in vars(connector):
            message = (
                f"{connector.__name__} is a {other} already: a class declares one role, "
                "@source or @destination"
            )
            raise TypeError(message)
    setattr(connector, _ROLES[role], declared)


def declared_as(connector: type, role: str) -> object | None:
    """What `connector` itself declares as `role`; none where it declares no such role."""
    return vars(connector).get(_ROLES[role])


class Placement(Generic[_D]):
    """One placement of a connector class: its declaration, and each connection's instance.

    The host or builder that placed the class holds it in the open, and the native side reaches
    it weakly, so the collector sees whatever an instance holds, its own pipeline included. The
    class lives as long as its module and holds nothing of a connection.
    """

    __slots__ = ("__weakref__", "connections", "declared")

    def __init__(self, declared: _D) -> None:
        self.declared = declared
        self.connections: weakref.WeakKeyDictionary[object, Any] = weakref.WeakKeyDictionary()
        """Each connection's instance, for as long as the native side holds that connection."""


@functools.cache
def package_version(top: str) -> str:
    """The version of the distribution that installed the top-level package `top`, else `0.0.0`.

    It is read once a package: finding the distribution reads every one installed.
    """
    import importlib.metadata  # noqa: PLC0415

    for distribution in importlib.metadata.packages_distributions().get(top, []):
        return importlib.metadata.version(distribution)
    return "0.0.0"


def registered_once(existing: Registered, role: str, answers: str, connector: type) -> Registered:
    """`existing` with `connector` as the `role` answering to `answers`, once a role and id.

    A different class in its place is refused. A source and a destination may answer to one id,
    as two classes of one connector serving both roles do.

    Raises:
        ValueError: another class serves the role under the id.
    """
    for held_role, known, held in existing:
        if (held_role, known) == (role, answers):
            if held is not connector:
                message = f"two different {role} classes answer to {answers!r}"
                raise ValueError(message)
            return existing
    return (*existing, (role, answers, connector))
