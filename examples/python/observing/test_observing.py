"""The observing example tells its run as its README shows, and stops it cleanly.

The stop is asked from its code or at Ctrl-C, and a run after a stop reads on, landing every event
once.
"""

from __future__ import annotations

import importlib.util
import queue
import re
import runpy
import signal
import sqlite3
import subprocess
import sys
import threading
from contextlib import closing
from pathlib import Path
from types import ModuleType

import pytest

import rdlt
from tests.sandboxes import NO_SANDBOX, without_sandbox

EXAMPLE = Path(__file__).resolve().parent
PROGRAM = EXAMPLE / "main.py"
PATIENCE = 60.0
"""Seconds a test waits for the program to say something, or to end."""


def program() -> ModuleType:
    """The example's program, imported under a name of its own."""
    spec = importlib.util.spec_from_file_location("observing_main", PROGRAM)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def shown() -> tuple[list[str], str]:
    """The README's console block: its run's arguments after the two directories, and its output."""
    text = (EXAMPLE / "README.md").read_text(encoding="utf-8")
    block = text.split("```console\n", 1)[1].split("```", 1)[0]
    path = str(PROGRAM.relative_to(EXAMPLE.parents[2]))
    runs: list[tuple[list[str], str]] = []
    for line in block.splitlines(keepends=True):
        if line.startswith("$ "):
            command = line.split()[1:]
            if command[0].endswith("python"):
                assert command[1:2] == [path], command
                runs.append((command[4:], ""))
        elif runs:
            arguments, printed = runs[-1]
            runs[-1] = (arguments, printed + line)
    assert len(runs) == 1, runs
    return runs[0]


ASKED = "stopping after the next commit"
"""The line the program prints as it asks for the stop itself."""
ENDED = "run ended: stopped"
"""The line that tells a stopped run's end."""
VARYING = ("stopped: committed ",)
"""The starts of the lines whose numbers vary from run to run, besides the commits': the
report's."""
COMMIT = re.compile(r"commit (\d+): (\d+) rows in \d+ ms")
"""A commit's line, its number and its rows."""


def checked(printed: str, stop_after: int) -> tuple[list[str], int]:
    """A run stopped after `stop_after` commits as `printed` tells it, checked, and its rows.

    Each commit line has its form and comes in turn: `stop_after` of them before the stop is
    asked, and any others between that and the run's end, as many as the run read meanwhile. The
    commits' rows add up to the report's, and their count to the commits it tells. Answers the
    lines but the commits', those whose numbers vary as their form.
    """
    lines = printed.splitlines()
    assert ASKED in lines, printed
    assert ENDED in lines, printed
    asked, ended = lines.index(ASKED), lines.index(ENDED)
    rows: list[int] = []
    others: list[str] = []
    for at, line in enumerate(lines):
        told = COMMIT.fullmatch(line)
        if told is not None:
            assert int(told[1]) == len(rows) + 1, f"commit {told[1]} in turn: {printed}"
            assert at < asked or asked < at < ended, f"commit {told[1]} in its place: {printed}"
            rows.append(int(told[2]))
        else:
            assert not line.startswith("commit "), f"a commit's form: {line}"
            varies = line.startswith(VARYING)
            others.append(re.sub(r"\d+", "#", line) if varies else line)
    before = COMMIT.fullmatch(lines[asked - 1]) if asked > 0 else None
    assert before is not None, printed
    assert int(before[1]) == stop_after, printed
    report = f"stopped: committed {sum(rows)} rows in {len(rows)} commits"
    assert report in lines, f"{report}: {printed}"
    return others, sum(rows)


def commits(printed: str) -> list[int]:
    """The rows of each commit `printed` tells, in order."""
    found = (COMMIT.fullmatch(line) for line in printed.splitlines())
    return [int(told[2]) for told in found if told is not None]


def held(data: Path) -> tuple[int, int]:
    """The events the table holds, and those of them that are distinct."""
    with closing(sqlite3.connect(data / "observing.db")) as database:
        counted = 'SELECT count(*), count(DISTINCT "id") FROM "events"'
        found: tuple[int, int] = database.execute(counted).fetchone()
    return found


def test_a_run_stopped_from_code_prints_what_the_readme_shows(
    connectors: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    arguments, printed_there = shown()
    assert len(arguments) == 1, arguments
    stop_after = int(arguments[0])
    # The README's block holds as the program's output must, its own numbers adding up.
    expected, _ = checked(printed_there, stop_after)
    monkeypatch.setattr(sys, "argv", [str(PROGRAM), str(connectors), str(tmp_path), *arguments])
    try:
        runpy.run_path(str(PROGRAM), run_name="__main__")
    except rdlt.ConfigError as error:
        if error.code not in NO_SANDBOX:
            raise
        without_sandbox(str(error))
    printed = capsys.readouterr().out
    lines, total = checked(printed, stop_after)
    # Each line but the commits' and the report's is the README's, digits and all; how many
    # commits a run makes, and what it lands, depend on how fast it reads.
    assert lines == expected, printed
    assert held(tmp_path) == (total, total)


def test_a_run_after_a_stop_reads_on_and_lands_every_event_once(
    connectors: Path, tmp_path: Path
) -> None:
    loaded = program()
    try:
        stopped: rdlt.Outcome = loaded.main(connectors, tmp_path, 3)
    except rdlt.ConfigError as error:
        if error.code not in NO_SANDBOX:
            raise
        without_sandbox(str(error))
    assert stopped.report.status == rdlt.RunStatus.STOPPED
    finished: rdlt.Outcome = loaded.main(connectors, tmp_path)
    assert finished.report.status == rdlt.RunStatus.SUCCEEDED
    assert held(tmp_path) == (1_000_000, 1_000_000)
    # Read to their end, the pages hold nothing more.
    assert loaded.main(connectors, tmp_path).report.rows == 0


def test_ctrl_c_stops_the_run_after_its_next_commit(connectors: Path, tmp_path: Path) -> None:
    # A pipeline that cannot be built here, for want of a sandbox, is skipped before the program
    # is spawned, by its code.
    try:
        program().pipeline(connectors, tmp_path)
    except rdlt.ConfigError as error:
        if error.code not in NO_SANDBOX:
            raise
        without_sandbox(str(error))
    child = subprocess.Popen(
        [sys.executable, "-u", str(PROGRAM), str(connectors), str(tmp_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert child.stdout is not None
    assert child.stderr is not None
    lines: queue.Queue[str | None] = queue.Queue()

    def read() -> None:
        for line in child.stdout or ():
            lines.put(line)
        lines.put(None)

    reading = threading.Thread(target=read, name="observing-stdout", daemon=True)
    reading.start()
    printed: list[str] = []
    try:
        # Interrupted once a commit has landed, as a terminal's Ctrl-C interrupts it.
        while not printed or not printed[-1].startswith("commit "):
            line = lines.get(timeout=PATIENCE)
            if line is None:
                pytest.fail(f"the program ended before a commit: {child.stderr.read()}")
            printed.append(line)
        child.send_signal(signal.SIGINT)
        child.wait(timeout=PATIENCE)
    finally:
        if child.poll() is None:
            child.kill()
    reading.join(timeout=PATIENCE)
    while (line := lines.get_nowait()) is not None:
        printed.append(line)
    said = child.stderr.read()
    # The package's wait answers the stopped run's outcome, so the program ends as it does.
    assert child.returncode == 0, said
    assert "stopping after the next commit; press Ctrl-C again to stop now" in said
    assert "run ended: stopped\n" in printed, printed
    # What the run committed is in the table, each event once.
    total = sum(commits("".join(printed)))
    assert held(tmp_path) == (total, total), printed
