"""The pytest plugin: one test per clause, a failure naming its clause."""

from __future__ import annotations

import gc
import textwrap
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.compute as pc
import pytest

import rdlt
from rdlt.connector import Config, Cursor, checkpoint, source, stream
from rdlt.testing import certify_source, certify_transform
from tests.clauses import KILLED_PAGES, SOURCE_PASSED, SOURCE_UNTOLD, outcomes
from tests.connectors.inbox import Inbox
from tests.connectors.tickets import Tickets
from tests.launchers import inbox_launcher, tickets_launcher

TESTS = Path(__file__).resolve().parents[1]

CERTIFIED = """
import sys
sys.path.append({tests!r})
from rdlt.testing import certify_source
from tests.connectors.tickets import Tickets

test_certify = certify_source(
    Tickets, config={{"pages": {pages}, "token": "t0ken"}}, launcher={launcher!r}
)
"""

FORGETFUL = """
import sys
sys.path.append({tests!r})
from rdlt.connector import Config, Cursor, Secret, checkpoint, source, stream
from rdlt.testing import certify_source

class Since(Cursor):
    after: int = -1

class Settings(Config):
    pages: int

@source(id="io.example.forgetful", config=Settings, version="1.0.0")
class Forgetful:
    def __init__(self, config: Settings) -> None:
        self.pages = config.pages

    def check(self) -> None:
        pass

    @stream(primary_key=["id"], cursor=Since)
    def pages_read(self, since: Since):
        for page in range(self.pages):  # ignores where it resumes
            yield [{{"id": page * 10 + n}} for n in range(10)]
            yield checkpoint(Since(after=page))

test_certify = certify_source(Forgetful, config={{"pages": 6}})
"""

ROWLESS = """
import pyarrow as pa

import rdlt
from rdlt.testing import certify_transform

@rdlt.transform()
def unchanged(batch):
    return batch

empty = pa.record_batch({"n": pa.array([], pa.int64())})
test_certify = certify_transform(unchanged, inputs=[empty])
"""


def test_each_clause_is_a_test_of_its_own(pytester: pytest.Pytester) -> None:
    launcher = tickets_launcher(pytester.path / "launchers")
    certified = CERTIFIED.format(tests=str(TESTS), launcher=str(launcher), pages=KILLED_PAGES)
    pytester.makepyfile(test_tickets=textwrap.dedent(certified))
    result = pytester.runpytest_subprocess("-v")
    result.stdout.fnmatch_lines(["*test_certify[[]S-RESUME[]] PASSED*"])
    result.stdout.fnmatch_lines(["*test_certify[[]K-SOURCE[]] PASSED*"])
    # The tickets connector tells nothing of where it stands outside the engine.
    result.stdout.fnmatch_lines(["*test_certify[[]S-ACK[]] SKIPPED*"])
    assert result.ret == pytest.ExitCode.OK


def test_a_broken_clause_fails_naming_itself(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(test_forgetful=textwrap.dedent(FORGETFUL.format(tests=str(TESTS))))
    result = pytester.runpytest_subprocess("-v")
    result.stdout.fnmatch_lines(["*test_certify[[]S-RESUME[]] FAILED*", "*S-RESUME*"])
    assert result.ret == pytest.ExitCode.TESTS_FAILED


def test_a_launcher_that_cannot_start_fails_the_kill_clause_alone(tmp_path: Path) -> None:
    certification = certify_source(
        Tickets, config={"pages": 6, "token": "t0ken"}, launcher=tmp_path / "absent"
    )
    assert certification.result("S-RESUME").outcome == "passed"
    assert certification.result("K-SOURCE").outcome == "failed"


def test_an_unobserved_clause_fails_saying_it_went_unobserved(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(test_rowless=textwrap.dedent(ROWLESS))
    result = pytester.runpytest_subprocess("-v")
    result.stdout.fnmatch_lines(["*test_certify[[]T-REPLAY[]] FAILED*"])
    # A transform with no keys to pass is not held to the clause about them.
    result.stdout.fnmatch_lines(["*test_certify[[]T-KEYS[]] SKIPPED*"])
    result.stdout.fnmatch_lines(["*T-REPLAY: *", "*went unobserved: no input holds a row*"])
    assert result.ret == pytest.ExitCode.TESTS_FAILED


def test_a_launcher_s_kill_clause_kills_the_installed_connector(tmp_path: Path) -> None:
    certification = certify_source(
        Tickets,
        config={"pages": KILLED_PAGES, "token": "t0ken"},
        launcher=tickets_launcher(tmp_path / "launchers"),
    )
    killed = certification.result("K-SOURCE")
    assert killed.outcome == "passed", killed
    assert killed.note is not None
    assert killed.note.startswith("killed: "), killed
    assert outcomes(certification) == {"passed": SOURCE_PASSED, "inapplicable": SOURCE_UNTOLD}


def test_a_source_that_tells_where_it_stands_passes_every_source_clause(tmp_path: Path) -> None:
    certification = certify_source(
        Inbox,
        config={"messages": KILLED_PAGES * 10, "offsets": str(tmp_path / "offsets.db")},
        launcher=inbox_launcher(tmp_path / "launchers"),
    )
    assert outcomes(certification) == {"passed": [*SOURCE_PASSED[:-1], "S-ACK", "K-SOURCE"]}


class _Offset(Cursor):
    next: int = 0


class _Pages(Config):
    pages: int


@source(id="io.example.stuck", config=_Pages, version="1.0.0", acknowledged=True)
class _Stuck:
    """A source that says it stands at its start whatever it is told is committed."""

    def __init__(self, config: _Pages) -> None:
        self.pages = config.pages

    def check(self) -> None:
        """Nothing to check."""

    @stream(primary_key=["id"], cursor=_Offset, acknowledged="standing")
    def rows(self, start: _Offset) -> Iterator[object]:
        for page in range(start.next, self.pages):
            yield [{"id": page}]
            yield checkpoint(_Offset(next=page + 1))

    def standing(self, partition: str) -> _Offset:
        del partition
        return _Offset()


def test_a_source_whose_position_does_not_move_where_it_is_told_fails_s_ack() -> None:
    certification = certify_source(_Stuck, config={"pages": 6})
    assert certification.result("S-ACK").outcome == "failed"


def test_a_certification_keeps_the_connector_it_certifies() -> None:
    class PagesConfig(Config):
        pages: int

    @source(id="io.example.unheld", config=PagesConfig, version="1.0.0")
    class Unheld:
        def __init__(self, config: PagesConfig) -> None:
            self.pages = config.pages

        def check(self) -> None:
            """Reads need nothing checked first."""

        @stream(primary_key=["id"])
        def rows(self) -> Iterator[object]:
            yield {"id": self.pages}

    certification = certify_source(Unheld, config={"pages": 1})
    del Unheld
    gc.collect()
    assert certification.result("P-HANDSHAKE").outcome == "passed"


def test_a_transform_s_clauses_pass() -> None:
    @rdlt.transform()
    def doubled(batch: pa.RecordBatch) -> pa.RecordBatch:
        column = cast("pa.Array[Any]", batch.column("n"))
        return batch.append_column("twice", pc.multiply(column, pa.scalar(2)))

    inputs = [pa.record_batch({"id": [1, 2, 3], "n": [4, 5, 6]})]
    certification = certify_transform(doubled, inputs=inputs)
    outcomes = {clause: certification.result(clause).outcome for clause, _ in certification.clauses}
    assert "failed" not in outcomes.values(), outcomes
    assert outcomes["T-ROWS"] == "passed"


def test_a_clause_the_certification_does_not_hold_is_refused_naming_it() -> None:
    @rdlt.transform()
    def unchanged(batch: pa.RecordBatch) -> pa.RecordBatch:
        return batch

    certification = certify_transform(unchanged, inputs=[pa.record_batch({"n": [1]})])
    with pytest.raises(KeyError) as refused:
        certification.result("S-RESUME")
    assert "S-RESUME" in str(refused.value)
    assert certification.result("T-ROWS").clause == "T-ROWS"
