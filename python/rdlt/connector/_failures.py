"""What a connector raises to say what kind of failure it met; the host reports that kind.

These are a connector's own failures, apart from the `rdlt.*Error` exceptions a run raises.
"""

from __future__ import annotations

import re
from typing import ClassVar

from rdlt import _native

_CODE = re.compile(r"[a-z0-9_.-]+")
"""What a code may hold, as the protocol admits it: lower-case letters, digits, `_`, `.`, `-`."""


class ConnectorError(Exception):
    """A connector's failure of a declared kind, and the machine code the host keeps beside it.

    `code` is lower-case letters, digits, `_`, `.` and `-`, as `http_429`, and none of the codes the
    host keeps for its own findings, such as `transport`. By default it is the class's name in
    snake case, `transient` for `Transient`, qualified by its module where that name is one of the
    host's: `tickets.deadline_exceeded` for `tickets.DeadlineExceeded`.
    """

    kind: ClassVar[str] = "internal"

    def __init__(self, message: str, *, code: str | None = None) -> None:
        if code is not None and _CODE.fullmatch(code) is None:
            refused = f"a code is lower-case letters, digits, `_`, `.` and `-`, not {code!r}"
            raise ValueError(refused)
        if code in _native.HOST_CODES:
            refused = f"{code!r} is a code the host keeps for its own findings"
            raise ValueError(refused)
        super().__init__(message)
        self.code = code


class Transient(ConnectorError):
    """A failure a retry may mend: the engine tries again after its policy's delay."""

    kind = "transient"


class RateLimited(ConnectorError):
    """The remote system asked to slow down; `retry_after` seconds before a retry, where it said."""

    kind = "rate_limited"

    def __init__(
        self, message: str, *, retry_after: float | None = None, code: str | None = None
    ) -> None:
        super().__init__(message, code=code)
        self.retry_after = retry_after


class InvalidConfig(ConnectorError):
    """The configuration is invalid or names something that does not exist."""

    kind = "config"


class Unauthorized(ConnectorError):
    """Credentials are missing, wrong, or lack a permission."""

    kind = "auth"


class InvalidData(ConnectorError):
    """A value or a batch is malformed."""

    kind = "data"


class Unsupported(ConnectorError):
    """The request needs what the connector cannot do."""

    kind = "unsupported"


class Fenced(ConnectorError):
    """A newer session holds the destination, so this session may not commit.

    It is coded `commit_stale` where the commit expects another commit landed.
    """

    kind = "fenced"
