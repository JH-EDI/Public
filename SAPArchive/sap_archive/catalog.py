"""HANA catalog utilities: enumerate schemas/tables/views and dump column metadata.

This module provides a programmatic API used by the CLI.
"""
from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pyodbc
from sap_archive.hana import HanaFetcher


def _build_conn_str(f: HanaFetcher) -> str:
    dsn = f.dsn
    user = f.user
    pwd = f.password
    if not dsn:
        raise RuntimeError("No SAP_HANA_DSN configured")
    if "=" in dsn:
        return dsn
    if ":" in dsn:
        host, port = dsn.split(":", 1)
        driver = os.getenv("ODBC_DRIVER", "HDBODBC")
        return f"DRIVER={{{driver}}};SERVERNODE={host}:{port};UID={user};PWD={pwd}"
    return f"DSN={dsn};UID={user};PWD={pwd}"


def _map_to_pyarrow_type(col_meta: Dict[str, Any]) -> str:
    dt = (col_meta.get("DATA_TYPE") or "").upper()
    prec = col_meta.get("NUMERIC_PRECISION")
    scale = col_meta.get("NUMERIC_SCALE")
    char_len = col_meta.get("CHARACTER_MAXIMUM_LENGTH")

    if "DECIMAL" in dt or dt.startswith("NUMERIC"):
        p = int(prec) if prec else 38
        s = int(scale) if scale else 0
        return f"decimal128({p},{s})"
    if any(k in dt for k in ("INT", "INTEGER", "BIGINT", "SMALLINT")):
        return "int64"
    if any(k in dt for k in ("DOUBLE", "REAL", "FLOAT")):
        return "float64"
    if "TIMESTAMP" in dt or "DATE" in dt:
        return "timestamp"
    if any(k in dt for k in ("BINARY", "BLOB")):
        return "binary"
    if char_len and isinstance(char_len, int) and char_len < 1024:
        return "string"
    return "large_string"


def _quote_ident(name: str) -> str:
    return f'"{name.replace("\"", "\"\"")}"'


def _object_ref(schema: str, name: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(name)}"


def _safe_filename(schema: str, name: str) -> str:
    safe = f"{schema}__{name}"
    for ch in ("/", "\\", ":", "\"", "*", "?", "<", ">", "|"):
        safe = safe.replace(ch, "_")
    return safe


def _connect(conn_str: str) -> pyodbc.Connection:
    return pyodbc.connect(conn_str, timeout=10)


def _count_table(conn_str: str, schema: str, table: str) -> Optional[int]:
    sql = f"SELECT COUNT(*) FROM {_object_ref(schema, table)}"
    try:
        conn = _connect(conn_str)
        try:
            cur = conn.cursor()
            cur.execute(sql)
            row = cur.fetchone()
            if not row:
                return None
            return int(row[0])
        finally:
            conn.close()
    except Exception:
        return None


def _access_check(conn_str: str, schema: str, name: str) -> bool:
    sql = f"SELECT 1 FROM {_object_ref(schema, name)} WHERE 1=0"
    try:
        conn = _connect(conn_str)
        try:
            cur = conn.cursor()
            cur.execute(sql)
            return True
        finally:
            conn.close()
    except Exception:
        return False


def _hana_cast_type(col_meta: Dict[str, Any]) -> str:
    # The catalog stores whatever field we were able to fetch from the
    # metadata query.  Historically we shoved the value into the `DATA_TYPE`
    # key, but HANA actually returns its type in a column called
    # SQL_TYPE_NAME (some versions also expose DATA_TYPE_NAME, TYPE_NAME, etc).
    # Make sure we look at all of them so casting works regardless of which one
    # was present.
    dt = (
        col_meta.get("SQL_TYPE_NAME")
        or col_meta.get("DATA_TYPE")
        or col_meta.get("DATA_TYPE_NAME")
        or col_meta.get("TYPE_NAME")
        or ""
    ).upper()
    prec = col_meta.get("NUMERIC_PRECISION")
    scale = col_meta.get("NUMERIC_SCALE")
    char_len = col_meta.get("CHARACTER_MAXIMUM_LENGTH")

    if "DECIMAL" in dt or dt.startswith("NUMERIC"):
        p = int(prec) if prec else 38
        s = int(scale) if scale else 0
        return f"DECIMAL({p},{s})"
    if "BIGINT" in dt:
        return "BIGINT"
    if "SMALLINT" in dt:
        return "SMALLINT"
    if "TINYINT" in dt:
        return "TINYINT"
    if "INT" in dt or "INTEGER" in dt:
        return "INTEGER"
    if any(k in dt for k in ("DOUBLE", "REAL", "FLOAT")):
        return "DOUBLE"
    if "SECONDDATE" in dt or "TIMESTAMP" in dt:
        return "TIMESTAMP"
    if "DATE" in dt:
        return "DATE"
    if any(k in dt for k in ("BINARY", "BLOB")):
        return "BLOB"

    if "NVARCHAR" in dt:
        length = int(char_len) if char_len else 5000
        return f"NVARCHAR({length})"
    if "VARCHAR" in dt:
        length = int(char_len) if char_len else 5000
        return f"VARCHAR({length})"
    if "CHAR" in dt:
        length = int(char_len) if char_len else 1
        return f"CHAR({length})"
    # fallback when we couldn't determine a specific type from the
    # catalog metadata.  HANA supports up to NVARCHAR(5000), but using a
    # plain VARCHAR(8000) here gives a little more breathing room and
    # avoids the very common NVARCHAR(5000) cast that was previously
    # emitted for every unknown column.  Parquet will simply treat this
    # as a UTF8 string/large_string anyway.
    return "VARCHAR(8000)"


def _build_select_template(schema: str, name: str, cols: List[Dict[str, Any]]) -> str:
    select_parts: List[str] = []
    for col in cols:
        col_name = col.get("COLUMN_NAME")
        if not col_name:
            continue
        cast_type = _hana_cast_type(col)
        select_parts.append(f"CAST({_quote_ident(col_name)} AS {cast_type}) AS {_quote_ident(col_name)}")
    if not select_parts:
        # No column metadata available in the catalog — do NOT emit a raw
        # executable SELECT *. Instead emit a commented warning block so an
        # editor user sees the issue and can re-run catalog to populate types.
        warning = [
            "-- WARNING: no column metadata available in catalog for this object",
            f"-- schema: {schema}",
            f"-- object: {name}",
            "-- Please re-run catalog to populate column metadata before using this query",
            f"-- Suggested fallback (commented):",
            f"-- SELECT *",
            f"-- FROM {_object_ref(schema, name)}",
        ]
        return "\n".join(warning)
    select_list = ",\n    ".join(select_parts)
    return f"SELECT\n    {select_list}\nFROM {_object_ref(schema, name)}"


def generate_queries_from_catalog(catalog_path: str = "metadata/hana_catalog.json", queries_output_dir: str = "metadata/queries") -> None:
    """Generate SQL query files from an existing catalog JSON file.

    This avoids connecting to HANA again; the function reads the catalog
    produced earlier and writes `queries/*.sql` files with a small header
    containing the recorded `row_count`.
    """
    import json as _json
    import os as _os

    with open(catalog_path, "r", encoding="utf-8") as f:
        cat = _json.load(f)

    generated_at = cat.get("generated_at")
    os.makedirs(queries_output_dir, exist_ok=True)

    for schema, sentry in cat.get("schemas", {}).items():
        tables = sentry.get("tables", {})
        for tbl, entry in tables.items():
            cols = entry.get("columns", []) or []
            row_count = entry.get("row_count")
            query_text = _build_select_template(schema, tbl, cols)
            header_lines = [
                "-- sap-archive-catalog-metadata-start",
                f"-- schema: {schema}",
                f"-- table: {tbl}",
                f"-- row_count: {row_count if row_count is not None else 'unknown'}",
                f"-- generated_at: {generated_at}",
                "-- sap-archive-catalog-metadata-end",
                "",
            ]
            header = "\n".join(header_lines)
            fname = _os.path.join(queries_output_dir, f"{_safe_filename(schema, tbl)}.sql")
            with open(fname, "w", encoding="utf-8") as f_out:
                f_out.write(header + query_text)

        # Also handle views if present
        views = sentry.get("views", {})
        for vw, entry in views.items():
            cols = entry.get("columns", []) or []
            row_count = entry.get("row_count")
            query_text = _build_select_template(schema, vw, cols)
            header_lines = [
                "-- sap-archive-catalog-metadata-start",
                f"-- schema: {schema}",
                f"-- view: {vw}",
                f"-- row_count: {row_count if row_count is not None else 'unknown'}",
                f"-- generated_at: {generated_at}",
                "-- sap-archive-catalog-metadata-end",
                "",
            ]
            header = "\n".join(header_lines)
            fname = _os.path.join(queries_output_dir, f"{_safe_filename(schema, vw)}.sql")
            with open(fname, "w", encoding="utf-8") as f_out:
                f_out.write(header + query_text)


def _list_schemas(conn: pyodbc.Connection) -> List[str]:
    cur = conn.cursor()
    cur.execute('SELECT SCHEMA_NAME FROM "SYS"."SCHEMAS" ORDER BY SCHEMA_NAME')
    rows = [r[0] for r in cur.fetchall()]
    return [s for s in rows if not (s.upper().startswith("SYS") or s.upper() in ("SYSTEM", "PUBLIC"))]


def _list_tables(conn: pyodbc.Connection, schema: str) -> List[str]:
    cur = conn.cursor()
    cur.execute('SELECT TABLE_NAME FROM "SYS"."TABLES" WHERE SCHEMA_NAME = ? ORDER BY TABLE_NAME', (schema,))
    return [r[0] for r in cur.fetchall()]


def _list_views(conn: pyodbc.Connection, schema: str) -> List[str]:
    cur = conn.cursor()
    try:
        cur.execute('SELECT VIEW_NAME FROM "SYS"."VIEWS" WHERE SCHEMA_NAME = ? ORDER BY VIEW_NAME', (schema,))
        return [r[0] for r in cur.fetchall()]
    except Exception:
        return []


def _get_table_columns(
    conn: pyodbc.Connection, schema: str, table: str, *, verbose: bool = False
) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    # Try table columns first (covers base tables and some variants).
    cur.execute(
        'SELECT * FROM "SYS"."TABLE_COLUMNS" WHERE SCHEMA_NAME = ? AND TABLE_NAME = ? ORDER BY POSITION',
        (schema, table),
    )
    rows = cur.fetchall()
    cols = [c[0] for c in cur.description] if cur.description is not None else []

    # If no rows found in TABLE_COLUMNS, the object may be a view; try VIEW_COLUMNS.
    if not rows:
        try:
            cur.execute(
                'SELECT * FROM "SYS"."VIEW_COLUMNS" WHERE SCHEMA_NAME = ? AND VIEW_NAME = ? ORDER BY POSITION',
                (schema, table),
            )
            rows = cur.fetchall()
            cols = [c[0] for c in cur.description] if cur.description is not None else []
        except Exception:
            # Fall through with empty rows/cols
            rows = []

    out: List[Dict[str, Any]] = []
    for r in rows:
        row = dict(zip(cols, r))
        mapped = {
            "COLUMN_NAME": row.get("COLUMN_NAME") or row.get("COLUMN") or row.get("FIELD_NAME"),
            # capture whichever field actually holds the type; prefer SQL_TYPE_NAME
            "DATA_TYPE": (
                row.get("SQL_TYPE_NAME")
                or row.get("DATA_TYPE")
                or row.get("DATA_TYPE_NAME")
                or row.get("TYPE_NAME")
            ),
            "CHARACTER_MAXIMUM_LENGTH": row.get("LENGTH"),
            "NUMERIC_PRECISION": row.get("PRECISION"),
            "NUMERIC_SCALE": row.get("SCALE"),
            "IS_NULLABLE": row.get("IS_NULLABLE")
            if row.get("IS_NULLABLE") is not None
            else ("YES" if row.get("NULLABLE") else "NO"),
            "ORDINAL_POSITION": row.get("POSITION"),
        }
        out.append(mapped)
    return out


def dump_hana_catalog(
    schemas: Iterable[str] | None = None,
    *,
    all_schemas: bool = False,
    include_views: bool = False,
    map_types: bool = False,
    count_tables: bool = True,
    generate_queries: bool = True,
    queries_output_dir: str = "metadata/queries",
    max_workers: int = 4,
    output: str = "metadata/hana_catalog.json",
    verbose: bool = False,
) -> Dict[str, Any]:
    """Dump HANA catalog to a JSON file and return the catalog dict.

    - Defaults to schema `SAPBI` and writes to `metadata/hana_catalog.json`.
    - Tables that report zero rows are omitted entirely; only objects with
      at least one row are kept (views are always included since we cannot
      determine their row count cheaply).
    - When `verbose=True` the function prints progress messages so the
      caller (CLI or scripts) can show activity while the catalog is built.
    """
    if verbose:
        print("Connecting to HANA...")
    f = HanaFetcher()
    conn_str = _build_conn_str(f)
    conn = pyodbc.connect(conn_str, timeout=10)
    worker_count = max(2, min(max_workers, 4))
    queries_dir = os.path.abspath(queries_output_dir)
    if generate_queries:
        # start with a clean slate so leftover SQL files from previous runs
        # (especially those for zero‑row tables) are removed.
        if os.path.isdir(queries_dir):
            for fname in os.listdir(queries_dir):
                path = os.path.join(queries_dir, fname)
                try:
                    os.remove(path)
                except Exception:
                    pass
        else:
            os.makedirs(queries_dir, exist_ok=True)

    try:
        if all_schemas:
            if verbose:
                print("Enumerating schemas (all non-system)...")
            schema_list = _list_schemas(conn)
        elif schemas:
            schema_list = [s for s in schemas]
            if verbose:
                print(f"Using requested schemas: {', '.join(schema_list)}")
        else:
            schema_list = ["SAPBI"]
            if verbose:
                print("Defaulting to schema: SAPBI")

        # Use local, timezone-naive timestamps for on-premises usage
        catalog: Dict[str, Any] = {"generated_at": datetime.now().isoformat(), "schemas": {}}

        for schema in schema_list:
            if verbose:
                print(f"Scanning schema: {schema}...")
            schema_entry: Dict[str, Any] = {"tables": {}, "views": {}} if include_views else {"tables": {}}

            try:
                tables = _list_tables(conn, schema)
            except Exception:
                tables = []
            if verbose:
                print(f"  found {len(tables)} tables")

            table_counts: Dict[str, int] = {}
            accessible_tables: List[str] = []
            if count_tables and tables:
                if verbose:
                    print(f"  counting tables (workers={worker_count})...")
                with ThreadPoolExecutor(max_workers=worker_count) as executor:
                    futures = {executor.submit(_count_table, conn_str, schema, tbl): tbl for tbl in tables}
                    for fut in as_completed(futures):
                        tbl = futures[fut]
                        count = fut.result()
                        if count is not None:
                            table_counts[tbl] = count
                            # only include tables with at least one row
                            if count > 0:
                                accessible_tables.append(tbl)
                if verbose:
                    print(f"  accessible tables with rows: {len(accessible_tables)}/{len(tables)}")
            else:
                # when not counting we don't know which tables have rows, so include
                # them all; caller may later filter or regenerated catalog should
                # run with count_tables=True for the desired behaviour.
                accessible_tables = list(tables)

            for i, tbl in enumerate(accessible_tables, start=1):
                if verbose and (i % 10 == 0 or i == 1):
                    print(f"    processing table {i}/{len(accessible_tables)}: {tbl}")
                try:
                    cols = _get_table_columns(conn, schema, tbl, verbose=verbose)
                except Exception:
                    cols = []
                entry: Dict[str, Any] = {
                    "columns": cols,
                    "row_count": table_counts.get(tbl),
                    "access_ok": True,
                }
                if map_types:
                    entry["pyarrow_cols"] = [{"name": c.get("COLUMN_NAME"), "type": _map_to_pyarrow_type(c)} for c in cols]
                if generate_queries:
                    query_text = _build_select_template(schema, tbl, cols)
                    # Prepend a small metadata header with the catalog row count so
                    # editors and reviewers can see expected sizes immediately.
                    header_lines = [
                        "-- sap-archive-catalog-metadata-start",
                        f"-- schema: {schema}",
                        f"-- table: {tbl}",
                        f"-- row_count: {table_counts.get(tbl) if table_counts.get(tbl) is not None else 'unknown'}",
                        f"-- generated_at: {catalog.get('generated_at')}",
                        "-- sap-archive-catalog-metadata-end",
                        "",
                    ]
                    header = "\n".join(header_lines)
                    query_file = os.path.join(queries_dir, f"{_safe_filename(schema, tbl)}.sql")
                    with open(query_file, "w", encoding="utf-8") as f_out:
                        f_out.write(header + query_text)
                    entry["query_file"] = os.path.relpath(query_file, os.getcwd())
                schema_entry["tables"][tbl] = entry

            if include_views:
                try:
                    views = _list_views(conn, schema)
                except Exception:
                    views = []
                if verbose:
                    print(f"  found {len(views)} views")

                accessible_views: List[str] = []
                if views:
                    if verbose:
                        print(f"  checking view access (workers={worker_count})...")
                    with ThreadPoolExecutor(max_workers=worker_count) as executor:
                        futures = {executor.submit(_access_check, conn_str, schema, vw): vw for vw in views}
                        for fut in as_completed(futures):
                            vw = futures[fut]
                            ok = fut.result()
                            if ok:
                                accessible_views.append(vw)
                    if verbose:
                        print(f"  accessible views: {len(accessible_views)}/{len(views)}")

                for vw in accessible_views:
                    if verbose:
                        print(f"    processing view: {vw}")
                    try:
                        cols = _get_table_columns(conn, schema, vw, verbose=verbose)
                    except Exception:
                        cols = []
                    entry: Dict[str, Any] = {
                        "columns": cols,
                        "row_count": None,
                        "access_ok": True,
                    }
                    if map_types:
                        entry["pyarrow_cols"] = [{"name": c.get("COLUMN_NAME"), "type": _map_to_pyarrow_type(c)} for c in cols]
                    if generate_queries:
                        query_text = _build_select_template(schema, vw, cols)
                        query_text = _build_select_template(schema, vw, cols)
                        header_lines = [
                            "-- sap-archive-catalog-metadata-start",
                            f"-- schema: {schema}",
                            f"-- view: {vw}",
                            f"-- row_count: unknown",
                            f"-- generated_at: {catalog.get('generated_at')}",
                            "-- sap-archive-catalog-metadata-end",
                            "",
                        ]
                        header = "\n".join(header_lines)
                        query_file = os.path.join(queries_dir, f"{_safe_filename(schema, vw)}.sql")
                        with open(query_file, "w", encoding="utf-8") as f_out:
                            f_out.write(header + query_text)
                        entry["query_file"] = os.path.relpath(query_file, os.getcwd())
                    schema_entry["views"][vw] = entry

            catalog["schemas"][schema] = schema_entry

        out_path = os.path.abspath(output)
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        if verbose:
            print(f"Writing catalog to {out_path}...")
        with open(out_path, "w", encoding="utf-8") as f_out:
            json.dump(catalog, f_out, indent=2, default=str)

        if verbose:
            print("Catalog generation complete.")
        return catalog

    finally:
        try:
            conn.close()
        except Exception:
            pass
