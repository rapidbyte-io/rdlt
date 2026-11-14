"""Orders reshaped before they land in SQLite, by ops, a Python filter and a Python transform.

A column is renamed, test orders are filtered out, quantities are cast and totals computed, emails
are masked and a column is added; then a load whose transform refuses an order.
"""

from __future__ import annotations

import sqlite3
import sys
from contextlib import closing
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.compute as pc

import rdlt
from rdlt.model import Json

ORDERS: list[Json] = [
    {"id": 1, "email": "buyer1@example.com", "qty": "2", "price_cents": 1250, "status": "paid"},
    {"id": 2, "email": "buyer2@example.com", "qty": "1", "price_cents": 725, "status": "test"},
    {"id": 3, "email": "buyer3@example.com", "qty": "4", "price_cents": 1000, "status": "paid"},
    {"id": 4, "email": "buyer4@example.com", "qty": "1", "price_cents": 310, "status": "refunded"},
]
"""The orders a shop sends, each quantity as text, a test order among them."""

LATE: Json = {
    "id": 5,
    "email": "buyer5@example.com",
    "qty": "two",
    "price_cents": 450,
    "status": "paid",
}
"""A later order, whose quantity is no number."""


@rdlt.filter()
def real(batch: pa.RecordBatch) -> pa.Array[Any]:
    """Keeps each order that is not a test."""
    status = cast("pa.Array[Any]", batch.column("status"))
    return pc.not_equal(status, pa.scalar("test"))  # pyright: ignore[reportUnknownMemberType]


@rdlt.transform()
def priced(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Casts each quantity, refusing one that is no number, and prices each order."""
    quantity = cast("pa.Array[Any]", batch.column("quantity")).cast(pa.int64())
    total = pc.multiply(quantity, cast("pa.Array[Any]", batch.column("price_cents")))
    at = batch.schema.get_field_index("quantity")
    replaced = batch.set_column(at, "quantity", quantity)  # pyright: ignore[reportUnknownMemberType]
    return replaced.append_column("total_cents", total)


def load(host: rdlt.Host, data: Path, orders: list[Json]) -> rdlt.Outcome:
    """Loads `orders` into `orders.db` in `data`, reshaped on their way, replacing its table."""
    memory = rdlt.Connector("io.rapidbyte.memory", config={"streams": {"orders": orders}})
    sqlite = rdlt.Connector(
        "io.rapidbyte.sqlite",
        config={"path": str(data / "orders.db")},
        access=rdlt.Access(write=data),
    )
    # Each step reshapes what the step before it leaves. The mask's key is a secret reference
    # the host resolves, never the key itself.
    reshaped = rdlt.stream(
        "orders",
        write=rdlt.replace(),
        transform=[
            {"rename": {"qty": "quantity"}},
            real,
            priced,
            {"mask": {"columns": ["email"], "with": "hmac_sha256", "key": "${env:ORDERS_KEY}"}},
            {"constant": {"currency": "EUR"}},
        ],
    )
    pipeline = rdlt.pipeline("orders", source=memory, destination=sqlite, streams=[reshaped])
    return pipeline.build(host).run()


def print_orders(data: Path) -> None:
    """Prints each order `orders.db` in `data` holds."""
    query = "SELECT id, quantity, total_cents, status, currency, email FROM orders ORDER BY id"
    with closing(sqlite3.connect(data / "orders.db")) as database:
        rows = database.execute(query).fetchall()
    print("id  quantity  total_cents  status    currency  email")
    for order, quantity, total, status, currency, email in rows:
        print(f"{order:<3} {quantity:<9} {total:<12} {status:<9} {currency:<9} {email}")


def main(connectors: Path, data: Path) -> None:
    """Loads the orders, then the orders with one more, the SQLite connector in `connectors`."""
    # Sandboxed on Linux, where the SQLite connector may write in `data` alone; macOS has no
    # sandbox. The mask's key comes from the environment variable the host allows by name.
    host = rdlt.Host(
        connectors=connectors,
        writable=data,
        allow_env="ORDERS_KEY",
        trust_binaries=sys.platform == "darwin",
    )
    report = load(host, data, ORDERS).report
    filtered = cast("dict[str, int]", report.streams["orders"].raw["filtered"])["dropped"]
    print(f"{report.status.value}: committed {report.rows} rows, {filtered} filtered out")
    print_orders(data)
    # A later load, with an order whose quantity is no number: the transform refuses it, the load
    # fails, and the table keeps what the first load replaced it with.
    try:
        load(host, data, [*ORDERS, LATE])
    except rdlt.TransformError as error:
        print(f"failed: {error.code}, {error}")
    print_orders(data)


if __name__ == "__main__":
    main(Path(sys.argv[1]).resolve(), Path(sys.argv[2]).resolve())
