"""A bulk load watched as it runs, and stopped after a commit.

Each of its events is told as it happens and its report once it has ended; the stop is asked at
Ctrl-C, or once the run has committed a given number of times.
"""

from __future__ import annotations

import sys
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import cast

import rdlt
from rdlt.connector import Cursor, checkpoint, stream
from rdlt.model import Json

PAGES = 1000
"""Pages of the load, each of `PAGE` events."""
PAGE = 1000


class Read(Cursor):
    """Where `events` resumes: the last page committed."""

    page: int = -1


@stream(cursor=Read)
def events(read: Read) -> Iterator[object]:
    """A thousand pages of a thousand events, each followed by the checkpoint after it."""
    for page in range(read.page + 1, PAGES):
        first = page * PAGE
        yield [{"id": n, "value": f"event {n}"} for n in range(first, first + PAGE)]
        yield checkpoint(Read(page=page))


def milliseconds(span: Json) -> int:
    """The milliseconds of `span`, a duration as the facade writes it."""
    held = cast("dict[str, int]", span)
    return held["secs"] * 1000 + held["nanos"] // 1_000_000


def told(event: rdlt.Event) -> str:
    """What `event` tells, as the run does it."""
    detail = event.detail
    match event.kind:
        case "attempt_started":
            return f"attempt {event.attempt} started"
        case "schema_changed":
            return f"table {detail['table']} at schema version {detail['version']}"
        case "committed":
            took = milliseconds(detail["took"])
            return f"commit {detail['commit']}: {detail['rows']} rows in {took} ms"
        case "retrying":
            error = cast("dict[str, Json]", detail["error"])
            return f"retrying in {milliseconds(detail['retry_in'])} ms: {error['message']}"
        case "run_ended":
            return f"run ended: {detail['status']}"
        case kind:
            return f"{kind}: {detail}"


def watch(run: rdlt.Run, stop_after: int | None) -> None:
    """Prints each of `run`'s events as it comes, and stops `run` after `stop_after` commits.

    Each line is flushed as it is printed, so a reader through a pipe sees it as it happens.
    """
    committed = 0
    for event in run.events():
        if isinstance(event, rdlt.Lagged):
            print(f"missed {event.dropped} events", flush=True)
            continue
        print(told(event), flush=True)
        if event.kind == "committed":
            committed += 1
            if committed == stop_after:
                print("stopping after the next commit", flush=True)
                run.stop()


def pipeline(connectors: Path, data: Path) -> rdlt.Pipeline:
    """`events` loaded into `observing.db` in `data`, built, the SQLite connector in `connectors`.

    It reads on from where the last run stopped, and commits every 50,000 rows.
    """
    # Sandboxed on Linux, where the connector may write in `data` alone; macOS has no sandbox.
    host = rdlt.Host(connectors=connectors, writable=data, trust_binaries=sys.platform == "darwin")
    sqlite = rdlt.Connector(
        "io.rapidbyte.sqlite",
        config={"path": str(data / "observing.db")},
        access=rdlt.Access(write=data),
    )
    built = rdlt.pipeline(
        "observing",
        source=events,
        destination=sqlite,
        streams=[rdlt.stream("events", read="incremental")],
        commit=rdlt.Commit(rows=50_000),
    )
    return built.build(host)


def main(connectors: Path, data: Path, stop_after: int | None = None) -> rdlt.Outcome:
    """Runs the pipeline, stopping it after its next commit once `stop_after` commits have landed.

    It stops so at Ctrl-C too.
    """
    run = pipeline(connectors, data).start()
    watching = threading.Thread(target=watch, args=(run, stop_after))
    watching.start()
    try:
        # A first Ctrl-C stops the run after its next commit, as the command line does, and a
        # second cancels it and raises `KeyboardInterrupt`.
        return run.wait()
    finally:
        watching.join()


if __name__ == "__main__":
    counted = [int(count) for count in sys.argv[3:]]
    outcome = main(Path(sys.argv[1]).resolve(), Path(sys.argv[2]).resolve(), *counted)
    report = outcome.report
    print(f"{report.status.value}: committed {report.rows} rows in {report.commits} commits")
