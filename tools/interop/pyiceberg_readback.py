#!/usr/bin/env python3
"""Independent read-back oracle (feature 016 T013, contract ID8).

Reads an rdlt-written table through the SAME Iceberg REST catalog with
pyiceberg — a second, independent implementation — and prints a JSON
document the Rust cell asserts against:

    {"count": …, "columns": [{"name": …, "type": …}, …],
     "partition_fields": […], "snapshots": …,
     "snapshot_props": [{"rdlt.load-id": …, …}, …]}

Verified facts baked in (research T001 addendum): pyiceberg 0.11.1
(0.11.0 rejects Polaris's config response), and the delegation header
must be blanked against stsUnavailable catalogs.
"""

import argparse
import json
import sys

from pyiceberg.catalog import load_catalog


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", required=True)
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--credential", required=True, help="client_id:client_secret")
    parser.add_argument("--scope", default="PRINCIPAL_ROLE:ALL")
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--table", required=True)
    args = parser.parse_args()

    catalog = load_catalog(
        "rdlt",
        **{
            "type": "rest",
            "uri": args.uri,
            "warehouse": args.warehouse,
            "credential": args.credential,
            "scope": args.scope,
            # stsUnavailable catalogs 400 on the delegation header.
            "header.X-Iceberg-Access-Delegation": "",
        },
    )
    table = catalog.load_table(f"{args.namespace}.{args.table}")
    rows = table.scan().to_arrow()
    schema = table.schema()
    spec = table.spec()
    snapshots = sorted(table.metadata.snapshots, key=lambda s: s.timestamp_ms)
    result = {
        "count": rows.num_rows,
        "columns": [
            {"name": field.name, "type": str(field.field_type)} for field in schema.fields
        ],
        "partition_fields": [field.name for field in spec.fields],
        "snapshots": len(snapshots),
        "snapshot_props": [
            {
                key: value
                for key, value in (
                    snapshot.summary.additional_properties if snapshot.summary else {}
                ).items()
                if key.startswith("rdlt.")
            }
            for snapshot in snapshots
        ],
    }
    json.dump(result, sys.stdout)


if __name__ == "__main__":
    main()
