"""What a suite does where a test needs a sandbox and none can be made here."""

from __future__ import annotations

import os
from typing import NoReturn

import pytest

NO_SANDBOX = ("sandbox_missing", "sandbox_unavailable", "sandbox_unsupported")
"""The codes a build answers with where no sandbox can be made here."""


def without_sandbox(why: str) -> NoReturn:
    """Skips a test that needs a sandbox, or fails it where one is required: CI must not skip."""
    if os.environ.get("RDLT_REQUIRE_SANDBOX", "0") != "0":
        pytest.fail(f"a sandbox is required here, and none can be made: {why}")
    pytest.skip(f"no sandbox here: {why}")
