"""The destination example's modules, importable in its tests as its program imports them."""

from __future__ import annotations

import sys
from pathlib import Path

# Appended, so the installed `rdlt` still comes first on the path.
sys.path.append(str(Path(__file__).resolve().parent))
