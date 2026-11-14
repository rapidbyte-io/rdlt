"""Launchers of the test connectors, as an installed Python connector's launcher is."""

from __future__ import annotations

import stat
import sys
from pathlib import Path

TESTS = Path(__file__).resolve().parents[1]
"""The directory `tests` is imported from, appended to a child's path so the installed `rdlt` comes
first."""


def launcher(directory: Path, name: str, connector: str) -> Path:
    """A launcher in `directory`, a directory of its own, serving a test connector.

    It is named `rdlt-connector-<name>` and serves what `connector` names of the module
    `tests.connectors.<name>`, a class or a tuple of a source and a destination, as the launcher
    `rdlt connector install` writes does, and refuses anything else as that one does.
    """
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"rdlt-connector-{name}"
    path.write_text(
        f"#!{sys.executable} -I\n"
        f"import sys\nsys.path.append({str(TESTS)!r})\n"
        f"from rdlt.connector._serve import installed\n"
        f"from tests.connectors.{name} import {connector}\n"
        f"raise SystemExit(installed({connector}))\n"
    )
    path.chmod(stat.S_IRWXU)
    return path


def tickets_launcher(directory: Path) -> Path:
    """A launcher serving the tickets source, in `directory`, a directory of its own."""
    return launcher(directory, "tickets", "Tickets")


def both_launcher(directory: Path) -> Path:
    """A launcher serving both roles of `io.example.both`, in `directory`, one of its own."""
    return launcher(directory, "both", "CONNECTORS")


def inbox_launcher(directory: Path) -> Path:
    """A launcher serving the inbox source, in `directory`, a directory of its own."""
    return launcher(directory, "inbox", "Inbox")


def numbers_launcher(directory: Path) -> Path:
    """A launcher serving the numbers source, in `directory`, a directory of its own."""
    return launcher(directory, "numbers", "Numbers")


def changelog_launcher(directory: Path) -> Path:
    """A launcher serving the change log source, in `directory`, a directory of its own."""
    return launcher(directory, "changelog", "Changelog")


def tables_launcher(directory: Path) -> Path:
    """A launcher serving the tables source, in `directory`, a directory of its own."""
    return launcher(directory, "tables", "Tables")


def pysqlite_launcher(directory: Path) -> Path:
    """A launcher serving the sqlite3 destination, in `directory`, a directory of its own."""
    return launcher(directory, "pysqlite", "PySqlite")
