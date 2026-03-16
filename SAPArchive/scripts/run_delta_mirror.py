"""Run an Open Mirror export for a given table folder.

This script is a lightweight wrapper around `sap_archive.queries.run_open_mirror`.
It expects a per-table query folder under `queries/`, e.g.:

  queries/RSBREQUESTMESS/
    delta.sql
    state.json

It writes sequential parquet files into `parquet/<table>/0000000001.parquet`.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from sap_archive.hana import HanaFetcher
from sap_archive.queries import run_open_mirror


def main() -> int:
    parser = argparse.ArgumentParser(description="Run an Open Mirror export for a table folder.")
    parser.add_argument(
        "--table",
        required=True,
        help="Folder name under queries/ that contains the mirror configuration.",
    )
    parser.add_argument(
        "--out",
        help="Output parquet root directory (default: parquet)",
        default="parquet",
    )
    args = parser.parse_args()

    fetcher = HanaFetcher()
    queries_root = Path("queries") / args.table
    out_root = Path(args.out)

    results = run_open_mirror(queries_root, fetcher, out_root)

    any_failed = any(not r.succeeded() for r in results)
    for r in results:
        if r.succeeded():
            print(f"{r.name}: OK — {r.rows if r.rows is not None else 'unknown'} rows written ({r.elapsed:.2f}s)")
        else:
            print(f"{r.name}: ERROR — {r.error}")

    return 1 if any_failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
