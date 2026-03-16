#!/usr/bin/env python3
"""Lightweight helper to run an ad‑hoc HANA query and dump the results.

The script is independent of the rest of the project; it builds a connection
string from the same environment variables used by ``sap_archive`` and can
accept SQL text on the command line or from a file.  Use ``--top`` to display
only the first N rows (it injects a ``SELECT TOP`` clause).

Example:

```powershell
$env:SAPARCHIVE_HANA_DSN = "MYHANA"
$env:SAPARCHIVE_HANA_UID = "user"
$env:SAPARCHIVE_HANA_PWD = "secret"

python .\scripts\query_runner.py -t 10 "SELECT * FROM SAPBI.\"/1FB/MD___M80009\""
```"""
import argparse
import os
import sys

import pyodbc


def _make_conn_str() -> str:
    # mimic the logic used in sap_archive.hana; prefer the same
    # environment variables (`SAP_HANA_*`) but fall back to the older
    # `SAPARCHIVE_HANA_*` names that this standalone script historically
    # used.  This keeps behaviour consistent with the rest of the
    # package.
    dsn = os.environ.get("SAP_HANA_DSN") or os.environ.get("SAPARCHIVE_HANA_DSN", "HANA")
    uid = os.environ.get("SAP_HANA_USER") or os.environ.get("SAPARCHIVE_HANA_UID", "")
    pwd = os.environ.get("SAP_HANA_PASSWORD") or os.environ.get("SAPARCHIVE_HANA_PWD", "")
    return f"DSN={dsn};UID={uid};PWD={pwd}"


def run(sql: str, conn_str: str, top: int | None = None) -> None:
    if top is not None:
        s = sql.lstrip()
        if s[:6].lower() == "select":
            sql = "SELECT TOP %d %s" % (top, s[6:])
        else:
            sql = f"SELECT TOP {top} * FROM (\n{sql}\n) sub"

    conn = pyodbc.connect(conn_str)
    cur = conn.cursor()
    cur.execute(sql)

    cols = [d[0] for d in cur.description or []]
    if cols:
        print("\t".join(cols))
    for row in cur.fetchall():
        print("\t".join(str(x) for x in row))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run a HANA query from a string or file and display the results"
    )
    parser.add_argument(
        "--conn",
        help="ODBC connection string (built from SAPARCHIVE_HANA_* env vars if omitted)",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--file",
        "-f",
        help="path to a .sql file containing the query",
    )
    group.add_argument(
        "query",
        nargs="?",
        help="SQL query text (enclose in quotes)",
    )
    parser.add_argument("--top", "-t", type=int, help="return only the first N rows")
    args = parser.parse_args()

    if args.file:
        with open(args.file, "r", encoding="utf-8") as fp:
            sql_text = fp.read()
    else:
        sql_text = args.query or ""

    if not sql_text.strip():
        parser.error("no SQL supplied")

    conn_str = args.conn or _make_conn_str()
    run(sql_text, conn_str, args.top)


if __name__ == "__main__":
    main()
