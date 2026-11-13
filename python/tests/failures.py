"""What a run's failure says, read as a test reads it."""

from __future__ import annotations

import rdlt


def said(error: rdlt.Error) -> str:
    """What `error` says and each of its causes, as one text."""
    report = error.report()
    return " ".join([report.message, *report.causes])
