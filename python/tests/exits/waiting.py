"""Exits with 7 while a thread of its own waits on a run inside Rust, finalization held open.

The wait takes the GIL back every tenth of a second, inside Rust; the interpreter then finalizes
for a second with the GIL released, so the wait takes it back while the interpreter finalizes.
"""

from __future__ import annotations

import _thread
import builtins
import itertools
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path

# Appended, so the installed `rdlt` still comes first on the path.
sys.path.append(str(Path(__file__).resolve().parents[2]))

import rdlt
from rdlt.connector import stream
from tests.connectors import memory


class _Held:
    """Holds finalization open for a second, the GIL released, once module teardown reaches it."""

    def __del__(self) -> None:
        sys.stderr.write(f"held while finalizing: {sys.is_finalizing()}\n")
        sys.stderr.flush()
        time.sleep(1.0)


@stream(name="rows")
def rows() -> Iterator[object]:
    for n in itertools.count():
        yield {"id": n}
        time.sleep(0.001)


run = (
    rdlt.pipeline(
        "waiting",
        source=rows,
        destination=rdlt.local(memory.Memory, config={"store": "waiting"}),
        streams=[rdlt.stream("rows")],
    )
    .build(rdlt.Host())
    .start()
)
waiting = threading.Event()


def waited() -> None:
    waiting.set()
    run.wait()


_thread.start_new_thread(waited, ())
if not waiting.wait(30):
    sys.exit(3)
time.sleep(0.3)
# Module teardown reaches what `builtins` holds after `__main__`, with the interpreter finalizing.
builtins.__dict__["_rdlt_held"] = _Held()
sys.exit(7)
