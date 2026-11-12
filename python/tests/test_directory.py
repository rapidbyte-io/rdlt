"""The connector `rdlt connector new --lang python` scaffolds, certified as written and installed.

Installed as a directory connector, it is placed by a host, in the sandbox on Linux, runs a
pipeline, and is certified with its kill clause spawning the installed launcher.
"""

from __future__ import annotations

import hashlib
import importlib
import os
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import cast

import pytest

import rdlt
from rdlt.testing import certify_source
from tests.connectors import pysqlite

REPOSITORY = Path(__file__).resolve().parents[2]
TEMPLATE = REPOSITORY / "crates" / "rdlt-cli" / "templates" / "python"
"""The scaffold as written, which `rdlt connector new` renames."""
RDLT = REPOSITORY / "target" / "debug" / "rdlt"


def certified(project: Path) -> subprocess.CompletedProcess[str]:
    """The run of `project`'s certify test, as its author runs it, writing nothing beside it."""
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    command = [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", str(project)]
    return subprocess.run(  # noqa: PLW1510 - its status is asserted
        command, env=env, cwd=project, capture_output=True, text=True, timeout=600
    )


@pytest.mark.timeout(660)
def test_the_scaffold_s_certify_test_passes_as_written() -> None:
    finished = certified(TEMPLATE)
    assert finished.returncode == 0, finished.stdout + finished.stderr


def uv() -> str:
    """Where uv is, which builds wheels and exports the lock here."""
    found = shutil.which("uv")
    if found is None:
        pytest.fail("uv is not on the path")
    return found


def requirements(tmp_path: Path) -> Path:
    """The package's locked dependencies with their hashes, and the wheel of this tree's."""
    exported = subprocess.run(
        [
            uv(),
            "export",
            "--frozen",
            "--no-default-groups",
            "--no-emit-project",
            "--format",
            "requirements-txt",
        ],
        check=True,
        capture_output=True,
        text=True,
        cwd=REPOSITORY,
        timeout=60,
    ).stdout
    built = sorted((REPOSITORY / "target" / "wheels").glob("*/rdlt-*.whl"))
    if len(built) != 1:
        pytest.fail(
            f"target/wheels holds {len(built)} rdlt wheels, not one: "
            "run this test with `just python-network`, which builds the wheel"
        )
    (wheel,) = built
    digest = hashlib.sha256(wheel.read_bytes()).hexdigest()
    path = tmp_path / "requirements.txt"
    path.write_text(f"{exported}\nrdlt @ {wheel.as_uri()} --hash=sha256:{digest}\n")
    return path


@pytest.mark.network
@pytest.mark.timeout(1800)
def test_a_scaffolded_connector_installs_runs_and_certifies(
    tmp_path: Path, connectors: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rdlt_command = [RDLT, "--output", "json"]
    subprocess.run(
        [*rdlt_command, "connector", "new", "tickets", "--lang", "python", "--into", tmp_path],
        check=True,
        timeout=60,
    )
    project = tmp_path / "rdlt-connector-tickets"
    wheels = tmp_path / "wheels"
    subprocess.run(
        [uv(), "build", "--wheel", "--out-dir", wheels, project], check=True, timeout=600
    )
    (wheel,) = wheels.glob("rdlt_connector_tickets-*.whl")
    python = Path("/usr/bin/python3") if sys.platform == "linux" else Path(sys.executable)
    subprocess.run(
        [
            *rdlt_command,
            "--connectors",
            connectors,
            "connector",
            "install",
            wheel,
            "--python",
            python,
            "--requirements",
            requirements(tmp_path),
        ],
        check=True,
        timeout=900,
    )
    tree = connectors / "rdlt-connector-tickets"
    launcher = tree / "bin" / "rdlt-connector"
    assert launcher.is_file()
    sandboxed = sys.platform == "linux" and os.environ.get("RDLT_REQUIRE_SANDBOX") == "1"
    host = rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=not sandboxed)
    built = rdlt.pipeline(
        "installed",
        source=rdlt.Connector("io.example.tickets", config={"rows": 100}),
        destination=rdlt.Connector(
            "io.rapidbyte.sqlite",
            config={"path": str(tmp_path / "out.db")},
            access=rdlt.Access(write=[tmp_path]),
        ),
        streams=[rdlt.stream("numbers")],
    )
    assert built.build(host).run().report.rows == 100
    with sqlite3.connect(tmp_path / "out.db") as database:
        assert database.execute('SELECT count(*) FROM "numbers"').fetchone() == (100,)
    finished = certified(project)
    assert finished.returncode == 0, finished.stdout + finished.stderr
    monkeypatch.setattr(sys, "path", [str(project / "src"), *sys.path])
    tickets = cast("type", importlib.import_module("rdlt_connector_tickets").Tickets)
    # As the scaffold's own certification, enough rows that the kill comes before the read ends.
    killed = certify_source(tickets, config={"rows": 50_000}, launcher=launcher)
    assert killed.result("K-SOURCE").outcome == "passed"


BOTH = '''"""A connector of both roles under one id, which one install serves."""

from rdlt.connector import destination, source
from rdlt_connector_both.pysqlite import PySqlite, SqliteConfig
from rdlt_connector_both.tickets import Tickets, TicketsConfig


@source(id="io.example.both", config=TicketsConfig, version="1.0.0")
class BothSource(Tickets):
    """The tickets source."""


@destination(id="io.example.both", config=SqliteConfig, version="1.0.0")
class BothDestination(PySqlite):
    """The sqlite3 destination."""


CONNECTORS = (BothSource, BothDestination)
'''

BOTH_PROJECT = """[build-system]
requires = ["hatchling>=1.27"]
build-backend = "hatchling.build"

[project]
name = "rdlt-connector-both"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = ["rdlt"]

[project.entry-points."rdlt.connectors"]
"io.example.both" = "rdlt_connector_both:CONNECTORS"

[tool.hatch.build.targets.wheel]
packages = ["src/rdlt_connector_both"]
"""


@pytest.mark.network
@pytest.mark.timeout(1800)
def test_a_wheel_naming_a_source_and_a_destination_installs_one_connector_of_both_roles(
    tmp_path: Path, connectors: Path
) -> None:
    project = tmp_path / "rdlt-connector-both"
    package = project / "src" / "rdlt_connector_both"
    package.mkdir(parents=True)
    for module in ("tickets", "pysqlite"):
        shutil.copy2(Path(__file__).parent / "connectors" / f"{module}.py", package)
    (package / "__init__.py").write_text(BOTH)
    (project / "pyproject.toml").write_text(BOTH_PROJECT)
    wheels = tmp_path / "wheels"
    subprocess.run(
        [uv(), "build", "--wheel", "--out-dir", wheels, project], check=True, timeout=600
    )
    (wheel,) = wheels.glob("rdlt_connector_both-*.whl")
    python = Path("/usr/bin/python3") if sys.platform == "linux" else Path(sys.executable)
    install = ["--connectors", connectors, "connector", "install", wheel, "--python", python]
    requirements_file = requirements(tmp_path)
    subprocess.run(
        [RDLT, "--output", "json", *install, "--requirements", requirements_file],
        check=True,
        timeout=900,
    )
    sandboxed = sys.platform == "linux" and os.environ.get("RDLT_REQUIRE_SANDBOX") == "1"
    host = rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=not sandboxed)
    path = tmp_path / "both.db"
    built = rdlt.pipeline(
        "both",
        source=rdlt.Connector("io.example.both", config={"pages": 4, "token": "t0ken"}),
        destination=rdlt.Connector(
            "io.example.both", config={"path": str(path)}, access=rdlt.Access(write=[tmp_path])
        ),
        streams=[rdlt.stream("tickets", read="incremental")],
    )
    assert built.build(host).run().report.rows == 40
    assert pysqlite.published(str(path), "tickets") == 40
