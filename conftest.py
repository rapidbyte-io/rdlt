"""Fixtures the package's suites and the examples share: the reference connectors' binaries."""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import pytest

pytest_plugins = ["pytester"]

REPOSITORY = Path(__file__).resolve().parent

# Appended, so `tests` is importable and the installed `rdlt` still comes first on the path.
sys.path.append(str(REPOSITORY / "python"))

BUILT = Path(os.environ.get("RDLT_CONNECTORS_BUILT", str(REPOSITORY / "target" / "debug")))
REFERENCE = (
    "rdlt-connector-files",
    "rdlt-connector-generator",
    "rdlt-connector-memory",
    "rdlt-connector-sqlite",
)


@pytest.fixture(scope="session")
def connectors(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A directory holding a copy of each reference connector's binary, each with one name."""
    directory = tmp_path_factory.mktemp("connectors")
    for name in REFERENCE:
        built = BUILT / name
        if not built.is_file():
            pytest.fail(f"{built} is not built: run `just connectors-build`")
        shutil.copy2(built, directory / name)
    return directory
