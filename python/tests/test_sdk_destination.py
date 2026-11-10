"""A destination written with the SDK: loaded in process, and certified clause by clause."""

from __future__ import annotations

import gc
import textwrap
import threading
import time
import weakref
from collections.abc import Iterator, Mapping
from pathlib import Path

import pyarrow as pa
import pytest

import rdlt
from rdlt.connector import (
    Config,
    Opened,
    Transient,
    answer,
    destination,
    minimal_capabilities,
    serve,
    source,
    stream,
)
from rdlt.model import Json
from rdlt.testing import certify_destination
from tests.connectors import memory
from tests.connectors.pysqlite import PySqlite
from tests.launchers import pysqlite_launcher
from tests.processes import children, ended

TESTS = Path(__file__).resolve().parents[1]


def generated(rows: int) -> rdlt.Connector:
    return rdlt.Connector(
        "io.rapidbyte.generator", config={"seed": 1, "streams": [{"name": "rows", "rows": rows}]}
    )


def test_rows_load_into_a_python_destination_once(connectors: Path) -> None:
    host = rdlt.Host(connectors=connectors, trust_binaries=True)
    built = rdlt.pipeline(
        "into-memory",
        source=generated(500),
        destination=rdlt.local(memory.Memory, config={"store": "loaded"}),
        streams=[rdlt.stream("rows")],
        commit=rdlt.Commit(rows=50),
    )
    assert built.build(host).run().report.rows == 500
    assert memory.published("loaded", "rows") == 500


def test_a_document_names_a_destination_the_host_registered(connectors: Path) -> None:
    host = rdlt.Host(connectors=connectors, trust_binaries=True).destination(memory.Memory)
    document: dict[str, rdlt.Json] = {
        "version": 1,
        "pipeline": "named-memory",
        "source": {
            "connector": "io.rapidbyte.generator",
            "config": {"seed": 2, "streams": [{"name": "rows", "rows": 30}]},
        },
        "destination": {"connector": "io.example.memory", "config": {"store": "named"}},
        "streams": [{"name": "rows"}],
    }
    rdlt.Pipeline.from_document(document).build(host).run()
    assert memory.published("named", "rows") == 30


def test_the_destination_clauses_pass_clause_by_clause(pytester: pytest.Pytester) -> None:
    pytester.makepyfile(
        test_memory=textwrap.dedent(
            f"""
            import sys
            sys.path.append({str(TESTS)!r})
            from rdlt.testing import certify_destination
            from tests.connectors.memory import Memory

            test_certify = certify_destination(Memory, config={{"store": "certified"}})
            """
        )
    )
    result = pytester.runpytest_subprocess("-v", "-rP")
    result.stdout.fnmatch_lines(
        [
            "*test_certify[[]P-HANDSHAKE[]] PASSED*",
            "*test_certify[[]D-EPOCH[]] PASSED*",
            "*test_certify[[]D-LANDED[]] PASSED*",
            "*test_certify[[]D-SCHEMA[]] PASSED*",
            "*test_certify[[]D-MERGE[]] SKIPPED*",
            "*test_certify[[]K-DESTINATION[]] PASSED*",
        ]
    )
    # Served in process, a kill cuts the connection, and the passed clause's note says so.
    result.stdout.fnmatch_lines(["*Captured note call*", "cut: *"])
    assert result.ret == pytest.ExitCode.OK


def test_a_launcher_s_kill_clause_kills_the_installed_destination(
    tmp_path: Path, tmp_path_factory: pytest.TempPathFactory
) -> None:
    certification = certify_destination(
        PySqlite,
        config={"path": str(tmp_path / "certified.db")},
        launcher=pysqlite_launcher(tmp_path_factory.mktemp("launchers")),
    )
    failed = [
        clause
        for clause, _ in certification.clauses
        if certification.result(clause).outcome == "failed"
    ]
    assert failed == []
    killed = certification.result("K-DESTINATION")
    assert killed.outcome == "passed", killed
    assert killed.note is not None
    assert killed.note.startswith("killed: "), killed


def test_a_launcher_that_cannot_start_fails_the_kill_clause_alone(tmp_path: Path) -> None:
    certification = certify_destination(
        memory.Memory, config={"store": "unlaunched"}, launcher=tmp_path / "absent"
    )
    assert certification.result("D-COMMIT").outcome == "passed"
    killed = certification.result("K-DESTINATION")
    assert killed.outcome == "failed"
    assert killed.reason is not None
    assert killed.reason.startswith("what the launcher published is read back in this process: ")


@destination(
    id="io.example.unread",
    config=memory.MemoryConfig,
    capabilities=minimal_capabilities(),
    version="1.0.0",
)
class Unread:
    """The memory destination, which cannot read back what it published."""

    def __init__(self, config: memory.MemoryConfig) -> None:
        self.memory = memory.Memory(config)

    def check(self) -> None:
        """Memory is always there."""

    def open(self, pipeline: str) -> Opened:
        """A session of the memory destination."""
        return self.memory.open(pipeline)


def test_a_destination_that_reads_nothing_back_leaves_those_clauses_unobserved() -> None:
    certification = certify_destination(Unread, config={"store": "unread"})
    assert certification.result("D-EPOCH").outcome == "passed"
    for clause in ("D-COMMIT", "K-DESTINATION"):
        assert certification.result(clause).outcome == "unobserved", clause


@pytest.mark.parametrize(
    ("landed", "key", "after", "decided"),
    [
        (None, {"epoch": 2, "seq": 1}, None, "apply"),
        ({"key": {"epoch": 2, "seq": 1}}, {"epoch": 2, "seq": 1}, None, "landed"),
        ({"key": {"epoch": 1, "seq": 4}}, {"epoch": 1, "seq": 4}, None, "landed"),
        (None, {"epoch": 1, "seq": 1}, None, "fenced"),
        ({"key": {"epoch": 2, "seq": 1}}, {"epoch": 2, "seq": 3}, {"epoch": 2, "seq": 2}, "stale"),
        (None, {"epoch": 2, "seq": 2}, {"epoch": 2, "seq": 1}, "stale"),
        ({"key": {"epoch": 2, "seq": 1}}, {"epoch": 2, "seq": 2}, {"epoch": 2, "seq": 1}, "apply"),
    ],
)
def test_a_commit_is_answered_as_the_rust_sdk_answers_it(
    landed: Mapping[str, Json] | None,
    key: Mapping[str, Json],
    after: Mapping[str, Json] | None,
    decided: str,
) -> None:
    assert answer(2, landed, key, after) == decided


def test_a_class_without_open_is_refused_where_it_is_declared() -> None:
    with pytest.raises(TypeError, match="open"):

        @destination(id="io.example.unopened", config=Config, capabilities=minimal_capabilities())
        class Unopened:
            def __init__(self, config: Config) -> None:
                del config

            def check(self) -> None:
                """Nothing to check."""


@destination(id="io.example.unknowing", config=Config, capabilities={"append": True})
class Unknowing:
    """A destination whose capabilities are not the Rust SDK's."""

    def __init__(self, config: Config) -> None:
        del config

    def check(self) -> None:
        """Nothing to check."""

    def open(self, pipeline: str) -> Opened:
        """Never reached: it is refused before it is placed."""
        raise NotImplementedError(pipeline)


def test_capabilities_rdlt_cannot_read_are_refused_where_they_are_placed() -> None:
    with pytest.raises(ValueError, match="capabilities"):
        rdlt.Host().destination(Unknowing)


def test_capabilities_rdlt_cannot_read_are_said_and_fail_the_launcher(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert serve(Unknowing, ["--rdlt-fd", "3"]) == 1
    said = capsys.readouterr().err
    assert "the capabilities are not rdlt's" in said, said
    assert "Traceback" not in said, said


class _Flaky(Config):
    store: str
    failures: int


_FAILED: dict[str, int] = {}


@destination(
    id="io.example.flaky", config=_Flaky, capabilities=minimal_capabilities(), version="1.0.0"
)
class Flaky(memory.Memory):
    """The memory destination whose first opens fail as transient, as a restarting service's do."""

    def __init__(self, config: _Flaky) -> None:
        super().__init__(memory.MemoryConfig(store=config.store))
        self.failures = config.failures
        self.name = config.store

    def open(self, pipeline: str) -> Opened:
        """Fails as transient for the first opens its configuration says."""
        failed = _FAILED.setdefault(self.name, 0)
        if failed < self.failures:
            _FAILED[self.name] = failed + 1
            raise Transient("the service is restarting")
        return super().open(pipeline)


def test_a_failure_s_kind_and_code_reach_the_run(connectors: Path) -> None:
    host = rdlt.Host(connectors=connectors, trust_binaries=True)
    built = rdlt.pipeline(
        "flaky",
        source=generated(10),
        destination=rdlt.local(Flaky, config={"store": "flaky", "failures": 1}),
        streams=[rdlt.stream("rows")],
    )
    assert built.build(host).run().report.rows == 10
    assert memory.published("flaky", "rows") == 10
    refused = rdlt.pipeline(
        "refused",
        source=generated(10),
        destination=rdlt.local(Flaky, config={"store": "refused", "failures": 9}),
        streams=[rdlt.stream("rows")],
        retry=rdlt.Retry(max_attempts=1),
    )
    with pytest.raises(rdlt.DestinationError) as raised:
        refused.build(host).run()
    assert raised.value.report().connector_code == "transient"


class _Holder:
    """What an instance of `Held` keeps: the pipeline that placed it, once it is built."""

    pipeline: rdlt.Pipeline | None = None


_HOLDERS: dict[str, _Holder] = {}


@destination(
    id="io.example.held",
    config=memory.MemoryConfig,
    capabilities=minimal_capabilities(),
    version="1.0.0",
)
class Held(memory.Memory):
    """The memory destination, whose instance keeps the holder its store names.

    An author's instance may keep what reaches its own pipeline, as this one does.
    """

    def __init__(self, config: memory.MemoryConfig) -> None:
        super().__init__(config)
        self.holder = _HOLDERS.get(config.store)


def test_a_destination_reaching_its_pipeline_is_collected_with_its_connector_processes(
    connectors: Path,
) -> None:
    gc.collect()
    before = children()
    holder = _HOLDERS.setdefault("cycle", _Holder())
    host = rdlt.Host(connectors=connectors, trust_binaries=True)
    holder.pipeline = rdlt.pipeline(
        "cycle",
        source=generated(3),
        destination=rdlt.local(Held, config={"store": "cycle"}),
        streams=[rdlt.stream("rows")],
    ).build(host)
    holder.pipeline.run()
    spawned = children() - before
    del _HOLDERS["cycle"]
    collected = weakref.ref(holder)
    del holder, host
    gc.collect()
    assert (collected() is None, ended(spawned)) == (True, True)


def test_one_class_declares_one_role() -> None:
    @source(id="io.example.both", config=Config, version="1.0.0")
    class Read:
        def __init__(self, config: Config) -> None:
            del config

        def check(self) -> None:
            """Reads need nothing checked first."""

        def open(self, pipeline: str) -> Opened:
            raise NotImplementedError(pipeline)

    as_destination = destination(
        id="io.example.both", config=Config, capabilities=minimal_capabilities()
    )
    with pytest.raises(TypeError, match=r"Read.*@source.*@destination"):
        as_destination(Read)

    @destination(id="io.example.both", config=Config, capabilities=minimal_capabilities())
    class Written:
        def __init__(self, config: Config) -> None:
            del config

        def check(self) -> None:
            """Nothing to check."""

        def open(self, pipeline: str) -> Opened:
            raise NotImplementedError(pipeline)

    with pytest.raises(TypeError, match=r"Written.*@source.*@destination"):
        source(id="io.example.both", config=Config)(Written)


def test_serving_a_class_neither_decorator_made_names_both() -> None:
    class Neither:
        """No connector."""

    with pytest.raises(TypeError, match=r"@source or @destination"):
        serve(Neither, ["--no-such-flag"])


_ORDER: list[str] = []
"""What `Ordered`'s session and table writer were called to do, in order."""

_WRITING = threading.Event()


class _SlowWriter:
    def write(self, segment: int, batch: pa.RecordBatch) -> None:
        """Takes a second, during which the source fails the attempt."""
        del segment, batch
        _ORDER.append("write begins")
        _WRITING.set()
        time.sleep(1)
        _ORDER.append("write ends")

    def flush(self) -> tuple[int, int]:
        """Stages nothing."""
        return 0, 0


class _OrderedSession:
    def apply_schema(self, change: Mapping[str, Json]) -> None:
        """Keeps no schema."""

    def writer(self, table: Mapping[str, Json]) -> _SlowWriter:
        """A writer that takes a second to write."""
        del table
        return _SlowWriter()

    def discard_staged(self) -> None:
        """Stages nothing."""

    def commit(self, meta: Mapping[str, Json]) -> Mapping[str, Json]:
        """Publishes nothing."""
        at: dict[str, Json] = {"secs_since_epoch": 0, "nanos_since_epoch": 0}
        return {"key": meta["key"], "committed_at": at, "rows": 0, "bytes": 0}

    def close(self) -> None:
        """Says it closed."""
        _ORDER.append("close")


@destination(
    id="io.example.ordered", config=Config, capabilities=minimal_capabilities(), version="1.0.0"
)
class Ordered:
    """A destination whose session records the order of its calls."""

    def __init__(self, config: Config) -> None:
        del config

    def check(self) -> None:
        """Nothing to check."""

    def open(self, pipeline: str) -> Opened:
        """A session at the first epoch: a run of one attempt opens once."""
        del pipeline
        return Opened(_OrderedSession(), 1, None, [])


@source(id="io.example.failing", config=Config, version="1.0.0")
class Failing:
    """A source that fails while its first batch is being written."""

    def __init__(self, config: Config) -> None:
        del config

    def check(self) -> None:
        """Reads need nothing checked first."""

    @stream(primary_key=["id"])
    def rows(self) -> Iterator[object]:
        """One row, then a failure once its write has begun."""
        yield [{"id": 1}]
        _WRITING.wait(30)
        time.sleep(0.2)
        raise RuntimeError("the source fails mid-write")


def test_a_failed_attempt_closes_its_session_once_its_calls_returned() -> None:
    built = rdlt.pipeline(
        "ordered",
        source=rdlt.local(Failing, config={}),
        destination=rdlt.local(Ordered, config={}),
        retry=rdlt.Retry(max_attempts=1),
    )
    with pytest.raises(rdlt.SourceError):
        built.build(rdlt.Host()).run()
    deadline = time.monotonic() + 30
    while not {"write ends", "close"} <= set(_ORDER) and time.monotonic() < deadline:
        time.sleep(0.05)
    assert _ORDER == ["write begins", "write ends", "close"]


def test_a_source_and_a_destination_may_answer_to_one_id(connectors: Path) -> None:
    @source(id="io.example.memory", config=Config, version="1.0.0")
    class Rows:
        def __init__(self, config: Config) -> None:
            del config

        def check(self) -> None:
            """Reads need nothing checked first."""

        @stream(primary_key=["id"])
        def rows(self) -> Iterator[object]:
            yield pa.record_batch({"id": [1, 2, 3]})

    host = rdlt.Host(connectors=connectors).source(Rows).destination(memory.Memory)
    built = rdlt.pipeline(
        "one-id",
        source=rdlt.local(Rows, config={}),
        destination=rdlt.local(memory.Memory, config={"store": "one-id"}),
    )
    built.build(host).run()
    assert memory.published("one-id", "rows") == 3


def test_two_destination_classes_under_one_id_are_refused() -> None:
    @destination(
        id="io.example.memory", config=memory.MemoryConfig, capabilities=minimal_capabilities()
    )
    class Twin(memory.Memory):
        """Another class answering to the memory destination's id."""

    host = rdlt.Host().destination(memory.Memory)
    with pytest.raises(ValueError, match=r"io\.example\.memory"):
        host.destination(Twin)
