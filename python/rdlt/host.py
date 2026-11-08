# pyright: reportPrivateUsage=false
"""What a pipeline's operator allows, in the facade's words, as keyword arguments."""

from __future__ import annotations

import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TypeAlias, cast

from rdlt import _native

StrPath: TypeAlias = "str | os.PathLike[str]"
"""A path, as text or as a `pathlib.Path`."""


def _paths(value: StrPath | Sequence[StrPath]) -> tuple[StrPath, ...]:
    if isinstance(value, (str, os.PathLike)):
        return (value,)
    return tuple(value)


def _names(value: str | Sequence[str]) -> tuple[str, ...]:
    return (value,) if isinstance(value, str) else tuple(value)


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
    """Answers `${secret:name}`: the secret's text, or `None` where none is set.

    Each pipeline built on the host holds this callable for as long as it lives: a bound method
    of an object that also holds such a pipeline is a cycle the collector cannot see, and leaks.
    """
    remote: Tls | None = None
    """The TLS that reaches remote connectors; without it a reference with an endpoint is
    refused."""
    trust_binaries: bool = False
    """Runs local connectors with the host's own access rather than in the sandbox."""
    _handle: _native.Host | None = field(default=None, init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        for name in ("connectors", "writable", "readable", "always_readable", "allow_files"):
            object.__setattr__(self, name, _paths(getattr(self, name)))
        object.__setattr__(self, "allow_env", _names(self.allow_env))

    def _native(self) -> _native.Host:
        if self._handle is None:
            object.__setattr__(self, "_handle", _native.Host(cast("_native._HostSettings", self)))
        return cast("_native.Host", self._handle)
