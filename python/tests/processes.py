"""This process's child processes, which a test reads to see a connector process end."""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path


def children() -> set[int]:
    """The live child processes of this one, as Linux's `/proc` lists them; none elsewhere."""
    if sys.platform != "linux":
        return set()
    found: set[int] = set()
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            stat = (entry / "stat").read_text()
        except OSError:
            continue
        state, parent = stat.rsplit(")", 1)[1].split()[:2]
        if state != "Z" and int(parent) == os.getpid():
            found.add(int(entry.name))
    return found


def ended(processes: set[int], within: float = 30.0) -> bool:
    """Whether each of `processes` ended within `within` seconds."""
    deadline = time.monotonic() + within
    while processes & children():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.05)
    return True
