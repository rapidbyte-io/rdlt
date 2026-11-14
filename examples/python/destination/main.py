"""Merges the reference generator's rows into the warehouse destination, in process."""

from __future__ import annotations

import sys
from pathlib import Path

import rdlt
from warehouse import Warehouse, published


def main(connectors: Path, data: Path) -> rdlt.Outcome:
    """Merges 1,000 generated rows into `warehouse.db` in `data`, the generator in `connectors`."""
    # The generator is sandboxed on Linux, with no access; macOS has no sandbox.
    host = rdlt.Host(connectors=connectors, trust_binaries=sys.platform == "darwin")
    generator = rdlt.Connector(
        "io.rapidbyte.generator", config={"seed": 5, "streams": [{"name": "rows", "rows": 1000}]}
    )
    warehouse = rdlt.local(Warehouse, config={"path": str(data / "warehouse.db")})
    # Merged by key, so a row read again replaces the one it lands over.
    merged = rdlt.stream("rows", write=rdlt.merge("id"))
    pipeline = rdlt.pipeline(
        "warehouse",
        source=generator,
        destination=warehouse,
        streams=[merged],
        commit=rdlt.Commit(rows=250),
    )
    return pipeline.build(host).run()


if __name__ == "__main__":
    data = Path(sys.argv[2]).resolve()
    outcome = main(Path(sys.argv[1]).resolve(), data)
    held = published(str(data / "warehouse.db"), "rows").num_rows
    print(f"{outcome.report.status.value}: committed {outcome.report.rows} rows, holding {held}")
