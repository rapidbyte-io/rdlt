"""The run the release makes of each wheel it builds, made here of the package under test."""

from __future__ import annotations

from pathlib import Path

from tests import smoke


def test_the_release_s_run_of_a_wheel_commits_its_rows(connectors: Path) -> None:
    assert smoke.main(connectors).report.rows == smoke.ROWS
