"""Launchers of the test connectors, as an installed Python connector's launcher is."""

from __future__ import annotations

import stat
import sys
from pathlib import Path

TESTS = Path(__file__).resolve().parents[1]
"""The directory `tests` is imported from, appended to a child's path so the installed `rdlt` comes
first."""


def tickets_launcher(directory: Path) -> Path:
    """A launcher serving the tickets connector, in `directory`, a directory of its own."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "rdlt-connector-tickets"
    path.write_text(
        f"#!{sys.executable} -I\n"
        f"import sys\nsys.path.append({str(TESTS)!r})\n"
        "from rdlt.connector import serve\nfrom tests.connectors.tickets import Tickets\n"
        "raise SystemExit(serve(Tickets))\n"
    )
    path.chmod(stat.S_IRWXU)
    return path
