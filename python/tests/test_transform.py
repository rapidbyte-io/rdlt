"""Python transforms on a pipeline's streams."""

from __future__ import annotations

import _thread
import asyncio
import gc
import os
import queue
import sqlite3
import subprocess
import sys
import textwrap
import threading
import time
import weakref
from collections.abc import Iterator
from pathlib import Path
from typing import Any, NamedTuple, cast

import pyarrow as pa
import pyarrow.compute as pc
import pytest

import rdlt
from tests.documents import generated


def host(tmp_path: Path, connectors: Path) -> rdlt.Host:
    return rdlt.Host(connectors=connectors, writable=[tmp_path], trust_binaries=True)


def into_sqlite(tmp_path: Path) -> rdlt.Connector:
    return rdlt.Connector(
        "io.rapidbyte.sqlite",
        config={"path": str(tmp_path / "out.db")},
        access=rdlt.Access(write=[tmp_path]),
    )


@rdlt.resource(name="orders")
def orders() -> Iterator[object]:
    yield pa.record_batch({"id": list(range(10)), "amount": [n * 1.5 for n in range(10)]})


@rdlt.transform()
def cents(batch: pa.RecordBatch) -> pa.RecordBatch:
    amount = cast("pa.Array[Any]", batch.column("amount"))
    return batch.append_column("cents", pc.multiply(amount, pa.scalar(100.0)))


@rdlt.filter()
def large(batch: pa.RecordBatch) -> object:
    amount = cast("pa.Array[Any]", batch.column("amount"))
    return pc.greater(amount, pa.scalar(6.0))  # pyright: ignore[reportUnknownMemberType]


def selected(tmp_path: Path, query: str) -> list[tuple[object, ...]]:
    with sqlite3.connect(tmp_path / "out.db") as database:
        return database.execute(query).fetchall()


def landed(tmp_path: Path) -> int:
    try:
        return len(selected(tmp_path, 'SELECT id FROM "orders"'))
    except sqlite3.OperationalError:
        return 0


def test_a_transform_and_a_filter_reshape_and_select_a_stream(
    tmp_path: Path, connectors: Path
) -> None:
    rdlt.pipeline(
        "transformed",
        source=orders,
        destination=into_sqlite(tmp_path),
        streams=[rdlt.stream("orders", transform=[large, cents])],
    ).build(host(tmp_path, connectors)).run()
    assert selected(tmp_path, 'SELECT id, cents FROM "orders" ORDER BY id') == [
        (n, n * 150.0) for n in range(5, 10)
    ]


def marked(batch: pa.RecordBatch) -> pa.RecordBatch:
    return batch.append_column("marked", pa.array([1] * batch.num_rows, pa.int64()))


def test_a_document_names_a_transform_the_host_registered(tmp_path: Path, connectors: Path) -> None:
    document = generated("named", 10, tmp_path / "out.db")
    document["streams"] = [{"name": "rows", "transform": [{"code": "marked"}]}]
    with pytest.raises(rdlt.ConfigError):
        rdlt.Pipeline.from_document(document).build(host(tmp_path, connectors))
    registered = host(tmp_path, connectors).transform("marked", marked)
    rdlt.Pipeline.from_document(document).build(registered).run()
    assert selected(tmp_path, 'SELECT count(marked) FROM "rows"') == [(10,)]


def test_a_filter_on_a_merge_fed_by_full_reads_is_refused_at_build(
    tmp_path: Path, connectors: Path
) -> None:
    builder = rdlt.pipeline(
        "refused",
        source=orders,
        destination=into_sqlite(tmp_path),
        streams=[rdlt.stream("orders", write=rdlt.merge("id"), transform=[large])],
    )
    with pytest.raises(rdlt.ConfigError):
        builder.build(host(tmp_path, connectors))


def test_a_transform_s_exception_fails_the_run_with_its_traceback(
    tmp_path: Path, connectors: Path
) -> None:
    @rdlt.transform(output=pa.schema([("id", pa.int64())]))
    def broken(batch: pa.RecordBatch) -> pa.RecordBatch:
        raise ArithmeticError("the rate is unknown")

    builder = rdlt.pipeline(
        "broken",
        source=orders,
        destination=into_sqlite(tmp_path),
        streams=[rdlt.stream("orders", transform=[broken])],
        retry=rdlt.Retry(max_attempts=1),
    )
    with pytest.raises(rdlt.TransformError) as raised:
        builder.build(host(tmp_path, connectors)).run()
    report = raised.value.report()
    assert report.code == "transform_failed"
    assert any("ArithmeticError" in cause for cause in report.causes)
    assert str(raised.value).count("ArithmeticError: the rate is unknown") == 1
    assert any(cause.startswith("Traceback") for cause in report.causes)


def test_two_different_transforms_may_not_share_a_name(tmp_path: Path, connectors: Path) -> None:
    @rdlt.transform(name="cents")
    def other(batch: pa.RecordBatch) -> pa.RecordBatch:
        return batch

    with pytest.raises(ValueError, match="cents"):
        rdlt.pipeline(
            "clashing",
            source=orders,
            destination=into_sqlite(tmp_path),
            streams=[rdlt.stream("orders", transform=[cents, other])],
        )


def test_a_host_refuses_a_second_transform_of_one_name(tmp_path: Path, connectors: Path) -> None:
    def other(batch: pa.RecordBatch) -> pa.RecordBatch:
        return batch

    registered = host(tmp_path, connectors).transform("cents", cents)
    with pytest.raises(ValueError, match="cents"):
        registered.transform("cents", other)


def test_a_host_and_a_pipeline_may_not_name_different_transforms_alike(
    tmp_path: Path, connectors: Path
) -> None:
    @rdlt.transform(name="cents")
    def other(batch: pa.RecordBatch) -> pa.RecordBatch:
        return batch

    registered = host(tmp_path, connectors).transform("cents", cents)
    builder = rdlt.pipeline(
        "clashing",
        source=orders,
        destination=into_sqlite(tmp_path),
        streams=[rdlt.stream("orders", transform=[other])],
    )
    with pytest.raises(ValueError, match="cents"):
        builder.build(registered)


def test_a_pipeline_may_use_a_transform_its_host_registered(
    tmp_path: Path, connectors: Path
) -> None:
    registered = host(tmp_path, connectors).transform("cents", cents)
    rdlt.pipeline(
        "shared",
        source=orders,
        destination=into_sqlite(tmp_path),
        streams=[rdlt.stream("orders", transform=[cents])],
    ).build(registered).run()
    assert len(selected(tmp_path, 'SELECT cents FROM "orders"')) == 10


@pytest.mark.parametrize(
    "mask", [pa.array([1] * 10, pa.int64()), pa.array([1.5] * 10)], ids=["int64", "double"]
)
def test_a_filter_answering_an_array_of_another_type_is_refused(
    tmp_path: Path, connectors: Path, mask: object
) -> None:
    @rdlt.filter()
    def not_a_mask(batch: pa.RecordBatch) -> object:
        return mask

    builder = rdlt.pipeline(
        "unmasked",
        source=orders,
        destination=into_sqlite(tmp_path),
        streams=[rdlt.stream("orders", transform=[not_a_mask])],
        retry=rdlt.Retry(max_attempts=1),
    )
    with pytest.raises(rdlt.TransformError) as raised:
        builder.build(host(tmp_path, connectors)).run()
    assert "TypeError" in raised.value.report().causes[0]
    assert landed(tmp_path) == 0


@pytest.mark.parametrize("typed", [False, True], ids=["a_list_of_none", "an_array_of_null_type"])
def test_a_filter_answering_only_nulls_keeps_nothing(
    tmp_path: Path,
    connectors: Path,
    typed: bool,  # noqa: FBT001
) -> None:
    @rdlt.filter()
    def nothing(batch: pa.RecordBatch) -> object:
        return pa.array([None] * batch.num_rows) if typed else [None] * batch.num_rows

    outcome = (
        rdlt.pipeline(
            "nothing",
            source=orders,
            destination=into_sqlite(tmp_path),
            streams=[rdlt.stream("orders", transform=[nothing])],
        )
        .build(host(tmp_path, connectors))
        .run()
    )
    assert outcome.report.status is rdlt.RunStatus.SUCCEEDED
    assert landed(tmp_path) == 0


def test_a_filter_may_answer_a_list_of_booleans(tmp_path: Path, connectors: Path) -> None:
    @rdlt.filter()
    def first_half(batch: pa.RecordBatch) -> object:
        return [index < batch.num_rows / 2 for index in range(batch.num_rows)]

    rdlt.pipeline(
        "halved",
        source=orders,
        destination=into_sqlite(tmp_path),
        streams=[rdlt.stream("orders", transform=[first_half])],
    ).build(host(tmp_path, connectors)).run()
    assert landed(tmp_path) == 5


def test_one_transform_may_be_used_twice(tmp_path: Path, connectors: Path) -> None:
    rdlt.pipeline(
        "twice",
        source=orders,
        destination=into_sqlite(tmp_path),
        streams=[rdlt.stream("orders", transform=[large, large])],
    ).build(host(tmp_path, connectors)).run()
    assert len(selected(tmp_path, 'SELECT id FROM "orders"')) == 5


class Slow(NamedTuple):
    """A built run whose commit waits on `transform` until `release` is set or 60 s pass."""

    built: rdlt.Pipeline
    entered: threading.Event
    release: threading.Event
    transform: rdlt.Transform
    resource: rdlt.Resource


def slow_run(tmp_path: Path, connectors: Path) -> Slow:
    entered = threading.Event()
    release = threading.Event()

    @rdlt.resource(name="orders")
    def rows() -> Iterator[object]:
        yield pa.record_batch({"id": list(range(10)), "amount": [n * 1.5 for n in range(10)]})

    @rdlt.transform()
    def waits(batch: pa.RecordBatch) -> pa.RecordBatch:
        entered.set()
        release.wait(60)
        return batch

    built = rdlt.pipeline(
        "slow",
        source=rows,
        destination=into_sqlite(tmp_path),
        streams=[rdlt.stream("orders", transform=[waits])],
    ).build(host(tmp_path, connectors))
    return Slow(built, entered, release, waits, rows)


async def entered_within(entered: threading.Event) -> None:
    """Waits for `entered` without blocking the event loop, for at most a minute."""
    deadline = time.monotonic() + 60
    while not entered.is_set():
        assert time.monotonic() < deadline, "the transform never started"
        await asyncio.sleep(0.01)


class Noted:
    """A standard error that tells `steps` when the first press's note is written to it."""

    def __init__(self, steps: queue.SimpleQueue[str]) -> None:
        self._steps = steps

    def write(self, text: str) -> int:
        """Tells of the first press's note; nothing is kept."""
        if "stopping" in text:
            self._steps.put("noted")
        return len(text)

    def flush(self) -> None:
        """Nothing is buffered."""


def test_a_second_ctrl_c_cancels_a_run_whose_commit_waits_on_a_transform(
    tmp_path: Path, connectors: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    slow = slow_run(tmp_path, connectors)
    built, entered, release = slow.built, slow.entered, slow.release
    steps: queue.SimpleQueue[str] = queue.SimpleQueue()
    monkeypatch.setattr(sys, "stderr", Noted(steps))

    second: list[float] = []

    def press() -> None:
        assert entered.wait(30), "the transform started"
        _thread.interrupt_main()
        # The second press follows the first's note: the wait has answered it.
        assert steps.get(timeout=30) == "noted"
        second.append(time.monotonic())
        _thread.interrupt_main()

    presser = threading.Thread(target=press, daemon=True)
    presser.start()
    with pytest.raises(KeyboardInterrupt):
        built.run()
    raised = time.monotonic()
    release.set()
    presser.join(30)
    assert second, "the second press was not made"
    assert raised - second[0] < 10


def test_a_cancelled_await_raises_at_once_while_a_transform_holds_the_commit(
    tmp_path: Path, connectors: Path
) -> None:
    slow = slow_run(tmp_path, connectors)
    built, entered, release = slow.built, slow.entered, slow.release

    async def main() -> rdlt.Outcome:
        run = built.start()
        waiting = asyncio.ensure_future(run.wait_async())
        await entered_within(entered)
        # The cancellation is seen while the transform still holds the commit back.
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(waiting, timeout=10)
        assert not release.is_set()
        release.set()
        return await asyncio.wait_for(run.wait_async(), timeout=30)

    assert asyncio.run(main()).report.status is rdlt.RunStatus.STOPPED


def test_a_detached_run_holds_what_it_runs_until_it_ends(tmp_path: Path, connectors: Path) -> None:
    slow = slow_run(tmp_path, connectors)

    async def detach(built: rdlt.Pipeline, entered: threading.Event) -> None:
        run = built.start()
        waiting = asyncio.ensure_future(run.wait_async())
        await entered_within(entered)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(waiting, timeout=10)

    asyncio.run(detach(slow.built, slow.entered))
    release = slow.release
    kept_transform, kept_resource = weakref.ref(slow.transform), weakref.ref(slow.resource)
    del slow
    gc.collect()
    assert kept_transform() is not None, "a detached run runs with its transform"
    assert kept_resource() is not None, "a detached run reads with its resource"
    release.set()
    deadline = time.monotonic() + 30
    while landed(tmp_path) < 10 and time.monotonic() < deadline:
        time.sleep(0.05)
    assert landed(tmp_path) == 10, "the detached run finished though nothing else holds it"


class Holder:
    """What a transform keeps, which holds the pipeline the transform serves."""

    pipeline: rdlt.Pipeline | None = None

    def marks(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """Adds a column saying whether it holds a pipeline."""
        return batch.append_column("held", pa.array([self.pipeline is None] * batch.num_rows))


def test_a_transform_holding_its_own_pipeline_is_collected(
    tmp_path: Path, connectors: Path
) -> None:
    holder = Holder()
    marks = rdlt.transform(name="marks")(holder.marks)
    holder.pipeline = rdlt.pipeline(
        "cycle",
        source=orders,
        destination=into_sqlite(tmp_path),
        streams=[rdlt.stream("orders", transform=[marks])],
    ).build(host(tmp_path, connectors))
    collected = weakref.ref(holder)
    del holder, marks
    gc.collect()
    assert collected() is None


def test_a_transform_a_host_registers_holding_its_own_pipeline_is_collected(
    tmp_path: Path, connectors: Path
) -> None:
    holder = Holder()
    document = generated("cycle", 1, tmp_path / "out.db")
    document["streams"] = [{"name": "rows", "transform": [{"code": "marks"}]}]
    registered = host(tmp_path, connectors).transform("marks", holder.marks)
    holder.pipeline = rdlt.Pipeline.from_document(document).build(registered)
    collected = weakref.ref(holder)
    del holder, registered
    gc.collect()
    assert collected() is None


def test_a_built_pipeline_and_its_run_keep_its_transform(tmp_path: Path, connectors: Path) -> None:
    @rdlt.transform()
    def passes(batch: pa.RecordBatch) -> pa.RecordBatch:
        return batch

    built = rdlt.pipeline(
        "kept",
        source=orders,
        destination=into_sqlite(tmp_path),
        streams=[rdlt.stream("orders", transform=[passes])],
    ).build(host(tmp_path, connectors))
    kept = weakref.ref(passes)
    del passes
    gc.collect()
    assert kept() is not None, "a built pipeline runs with its transform"
    run = built.start()
    del built
    gc.collect()
    assert kept() is not None, "a run in progress runs with its transform"
    run.wait()


EXITING_SCRIPT = """
import sys, threading
import rdlt

started = threading.Event()
@@BODY@@

@rdlt.resource(name="orders")
def orders():
    for n in range(1_000_000):
        yield {"id": n, "amount": n * 1.5}

built = rdlt.pipeline(
    "exiting",
    source=orders,
    destination=rdlt.Connector(
        "io.rapidbyte.sqlite",
        config={"path": @@OUT@@},
        access=rdlt.Access(write=[@@TMP@@]),
    ),
    streams=[rdlt.stream("orders", transform=[slow])],
    commit=rdlt.Commit(rows=100),
).build(rdlt.Host(connectors=@@CONNECTORS@@, writable=[@@TMP@@], trust_binaries=True))
run = built.start()
if not started.wait(30):
    sys.exit(3)
sys.exit(7)
"""


def exiting_script(body: str, tmp_path: Path, connectors: Path) -> str:
    """A script that runs a pipeline through the transform `body` defines and exits with 7."""
    return (
        EXITING_SCRIPT.replace("@@BODY@@", body)
        .replace("@@OUT@@", repr(str(tmp_path / "out.db")))
        .replace("@@TMP@@", repr(str(tmp_path)))
        .replace("@@CONNECTORS@@", repr(str(connectors)))
    )


STUCK_SCRIPT = """
import _thread, itertools, os, sys, threading

import pyarrow as pa
import rdlt

# Ends the process, with a status the test reads, if anything below hangs.
watchdog = threading.Timer(90, os._exit, (4,))
watchdog.daemon = True
watchdog.start()

tmp, connectors = sys.argv[1], sys.argv[2]
entered = threading.Event()
release = threading.Event()


def into(name):
    return rdlt.Connector(
        "io.rapidbyte.sqlite",
        config={"path": f"{tmp}/{name}.db"},
        access=rdlt.Access(write=[tmp]),
    )


def host():
    return rdlt.Host(connectors=connectors, writable=[tmp], trust_binaries=True)


@rdlt.resource(name="orders")
def orders():
    yield {"id": 1}


@rdlt.transform(output=pa.schema([("id", pa.int64())]))
def stuck(batch):
    entered.set()
    release.wait(600)
    return batch


flowing = threading.Event()


@rdlt.resource(name="endless")
def endless():
    for n in itertools.count():
        yield {"id": n}
        flowing.set()


built = rdlt.pipeline(
    "stuck",
    source=orders,
    destination=into("stuck"),
    streams=[rdlt.stream("orders", transform=[stuck])],
).build(host())
run = built.start()
if not entered.wait(60):
    sys.exit(3)
run.cancel()

# With one runtime worker, a cancel that waited there would stop this run from finishing.
other = rdlt.pipeline("other", source=orders, destination=into("other")).build(host()).run()
if other.report.status is not rdlt.RunStatus.SUCCEEDED:
    sys.exit(5)

third = rdlt.pipeline("third", source=endless, destination=into("third")).build(host()).start()
if not flowing.wait(60):
    sys.exit(6)
threading.Timer(0.5, _thread.interrupt_main).start()
try:
    third.wait()
except KeyboardInterrupt:
    pass

release.set()
sys.exit(7)
"""


def test_a_cancelled_run_stuck_in_a_transform_holds_no_runtime_worker(
    tmp_path: Path, connectors: Path
) -> None:
    script = tmp_path / "stuck.py"
    script.write_text(STUCK_SCRIPT)
    environment = {**os.environ, "TOKIO_WORKER_THREADS": "1"}
    done = subprocess.run(
        [sys.executable, str(script), str(tmp_path), str(connectors)],
        check=False,
        timeout=180,
        env=environment,
    )
    assert done.returncode == 7


def test_a_process_exits_while_a_transform_runs(tmp_path: Path, connectors: Path) -> None:
    body = textwrap.dedent(
        """
        import time
        @rdlt.transform()
        def slow(batch):
            started.set()
            time.sleep(600)
            return batch
        """
    )
    script = exiting_script(body, tmp_path, connectors)
    done = subprocess.run([sys.executable, "-c", script], check=False, timeout=60)
    assert done.returncode == 7


def test_a_process_exits_while_transforms_flow(tmp_path: Path, connectors: Path) -> None:
    body = textwrap.dedent(
        """
        @rdlt.transform()
        def slow(batch):
            started.set()
            return batch
        """
    )
    script = exiting_script(body, tmp_path, connectors)
    done = subprocess.run([sys.executable, "-c", script], check=False, timeout=60)
    assert done.returncode == 7
