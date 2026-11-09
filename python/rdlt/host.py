# pyright: reportPrivateUsage=false
"""What a pipeline's operator allows, in the facade's words, as keyword arguments."""

from __future__ import annotations

import dataclasses
import os
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TypeAlias, cast

from rdlt import _native
from rdlt._weak import weakly_reached

StrPath: TypeAlias = "str | os.PathLike[str]"
"""A path, as text or as a `pathlib.Path`."""

_first_build = threading.Lock()
"""Held while a host publishes its native handle, so its first builds all share one."""


def _first_build_unheld() -> None:
    """Gives a forked child a lock of its own: one held at the fork is held for ever there."""
    global _first_build  # noqa: PLW0603
    _first_build = threading.Lock()


os.register_at_fork(after_in_child=_first_build_unheld)


def _paths(value: StrPath | Sequence[StrPath]) -> tuple[StrPath, ...]:
    if isinstance(value, (str, os.PathLike)):
        return (value,)
    return tuple(value)


def _names(value: str | Sequence[str]) -> tuple[str, ...]:
    return (value,) if isinstance(value, str) else tuple(value)


class _SecretStore:
    """A host's `secret_store`, which the native host reaches through a weak reference."""

    __slots__ = ("__weakref__", "_answer")

    def __init__(self, answer: Callable[[str], str | None]) -> None:
        self._answer = answer

    def weakly(self) -> Callable[[str], str | None]:
        """The store, for the native host: what the native host holds does not keep it alive."""
        reached = weakly_reached(self, "this host's secret store")

        def answer(name: str) -> str | None:
            return reached()._answer(name)

        return answer


@dataclass(frozen=True, slots=True, kw_only=True)
class Tls:
    """The host's identity for connectors reached over mutual TLS, and the CA theirs chain to."""

    cert: StrPath
    """The host's certificate, PEM."""
    key: StrPath
    """The host's private key, PEM."""
    ca: StrPath
    """The CA bundle a connector's certificate must chain to, PEM."""


@dataclass(frozen=True, slots=True, kw_only=True)
class Host:
    """What a pipeline's operator allows; nothing a pipeline says widens it.

    Local connectors run sandboxed unless `trust_binaries`; a connector is found only in
    `connectors`; a pipeline may let a connector write only beneath `writable`, read only beneath
    `readable` or `writable`, and reach the network only where `allow_network`; `${env:NAME}`
    resolves only for `allow_env`'s names, `${file:...}` only beneath `allow_files`, and
    `${secret:name}` only through `secret_store`.

    Each list also takes a single path or name.
    """

    connectors: StrPath | Sequence[StrPath] = ()
    """Where local connector binaries are found, and nowhere else."""
    writable: StrPath | Sequence[StrPath] = ()
    """Paths a pipeline may let a connector write beneath."""
    readable: StrPath | Sequence[StrPath] = ()
    """Paths a pipeline may let a connector read beneath."""
    always_readable: StrPath | Sequence[StrPath] = ()
    """Paths every connector reads."""
    allow_network: bool = False
    """Lets a pipeline let its sandboxed connectors share this host's network."""
    build_wait: timedelta | None = None
    """How long building a pipeline waits for each thing it asks of something else: a connector's
    placement, a mask key's secret; the facade's default where it says nothing."""
    allow_env: str | Sequence[str] = ()
    """The environment variables `${env:NAME}` may name."""
    allow_files: StrPath | Sequence[StrPath] = ()
    """The directories `${file:/dir/name}` may name files beneath."""
    secret_store: Callable[[str], str | None] | None = field(default=None, repr=False)
    """Answers `${secret:name}`: the secret's text, or `None` where none is set."""
    remote: Tls | None = None
    """The TLS that reaches remote connectors; without it a reference with an endpoint is
    refused."""
    trust_binaries: bool = False
    """Runs local connectors with the host's own access rather than in the sandbox."""
    _store: _SecretStore | None = field(default=None, init=False, repr=False, compare=False)
    _sources: tuple[_native.SourceFactory, ...] = field(default=(), repr=False)
    """Python functions and classes this host runs as sources, by the ids their factories name."""
    _held: tuple[object, ...] = field(default=(), repr=False)
    """What the native side reaches of Python's only weakly: this host holds it in the open."""
    _origin: Host | None = field(default=None, init=False, repr=False, compare=False)
    """The host this one was derived from, whose compute pool it shares."""
    _handle: _native.Host | None = field(default=None, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        for name in ("connectors", "writable", "readable", "always_readable", "allow_files"):
            object.__setattr__(self, name, _paths(getattr(self, name)))
        object.__setattr__(self, "allow_env", _names(self.allow_env))
        if self.secret_store is not None:
            object.__setattr__(self, "_store", _SecretStore(self.secret_store))

    @property
    def _weak_secret_store(self) -> Callable[[str], str | None] | None:
        return None if self._store is None else self._store.weakly()

    def _replaced(self, **changes: object) -> Host:
        """This host with `changes`, built on this host's compute pool."""
        replaced = dataclasses.replace(self, **changes)
        object.__setattr__(replaced, "_origin", self)
        return replaced

    def _native(self) -> _native.Host:
        if self._handle is None:
            origin = None if self._origin is None else self._origin._native()
            # Made outside the lock, since reading the settings can run a caller's `__fspath__`.
            made = _native.Host(cast("_native._HostSettings", self), origin)
            with _first_build:
                if self._handle is None:
                    object.__setattr__(self, "_handle", made)
        return cast("_native.Host", self._handle)
