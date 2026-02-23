"""Discover and run SQL files from a `queries/` folder.

Behavior:
- non-recursive discovery of top-level `*.sql` files (sorted)
- ignores files starting with `_` or ending with `~`
- validates single-statement SQL (very small heuristic)
- orchestrates execution via a provided `HanaFetcher` and writes outputs
  using the project's parquet helpers.
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterator, List, Optional
import re
import time
import json
from datetime import datetime

from .hana import HanaFetcher
from .parquet import dataframes_to_single_parquet


SQL_STATEMENT_SPLIT_RE = re.compile(r";\s*(?:--.*)?$", re.MULTILINE)


def iter_sql_files(path: Path) -> Iterator[Path]:
    """Yield top-level .sql files in `path`, sorted deterministically."""
    path = Path(path)
    if not path.exists():
        return
    for p in sorted(path.iterdir()):
        if not p.is_file():
            continue
        if p.suffix.lower() != ".sql":
            continue
        name = p.name
        if name.startswith("_") or name.endswith("~"):
            continue
        yield p


def read_sql(path: Path) -> str:
    """Return SQL text (trimmed)."""
    text = path.read_text(encoding="utf-8").strip()
    return text


def _format_sql_literal(value: object) -> str:
    """Format a Python value as a SQL literal for safe textual substitution.

    - strings are single-quoted with embedded single quotes escaped
    - datetimes/timestamps are ISO-formatted and quoted
    - None -> NULL
    - numbers -> plain str()
    """
    if value is None:
        return "NULL"
    # pandas/pyarrow types sometimes appear — coerce when possible
    try:
        import pandas as pd

        if isinstance(value, pd.Timestamp):
            return f"'{value.isoformat()}'"
    except Exception:
        pass

    if isinstance(value, str):
        esc = value.replace("'", "''")
        return f"'{esc}'"
    if isinstance(value, (int, float)):
        return str(value)
    # fallback to quoted string
    esc = str(value).replace("'", "''")
    return f"'{esc}'"


def substitute_wm_placeholders(sql: str, wm_cols: list[str], wm_values: tuple | None) -> str:
    """Substitute {WM1}, {WM2}... placeholders in `sql` with literal values.

    If `wm_values` is None the placeholders are left unchanged.
    """
    if wm_values is None:
        return sql
    out = sql
    for i, col in enumerate(wm_cols, start=1):
        placeholder = f"{{WM{i}}}"
        try:
            val = wm_values[i - 1]
        except Exception:
            val = None
        literal = _format_sql_literal(val)
        out = out.replace(placeholder, literal)
    return out


def _looks_like_single_statement(sql: str) -> bool:
    """Heuristic: count semicolons ignoring those inside strings is hard — use
    a pragmatic check: require at most one trailing semicolon and not multiple
    non-empty statement fragments.
    """
    if not sql:
        return False
    # remove trailing semicolon(s)
    stripped = sql.strip()
    # split on semicolon and remove empty parts
    parts = [p for p in stripped.split(";") if p.strip()]
    return len(parts) == 1


# Public alias so callers/tests can use the check without referencing a
# private symbol. Keeps internal implementation name unchanged.
def looks_like_single_statement(sql: str) -> bool:
    return _looks_like_single_statement(sql)


class QueryResult:
    def __init__(self, name: str, rows: int = 0, elapsed: float = 0.0, error: Optional[str] = None) -> None:
        self.name = name
        self.rows = rows
        self.elapsed = elapsed
        self.error = error

    def succeeded(self) -> bool:
        return self.error is None


def run_queries_folder(folder: Path, fetcher: HanaFetcher, out_dir: Path, chunk_size: int = 50_000, files: Optional[List[Path]] = None) -> List[QueryResult]:
    """Execute each SQL file in `folder` and write a single Parquet file per SQL file.

    Returns a list of QueryResult (one per processed file, including failures).
    """
    folder = Path(folder)
    out_dir = Path(out_dir)
    results: List[QueryResult] = []

    file_iter: Iterator[Path]
    if files:
        # use the explicit file list provided by the caller
        file_iter = (Path(p) for p in files)
    else:
        file_iter = iter_sql_files(folder)

    for p in file_iter:
        name = p.stem
        res = QueryResult(name=name)
        start = time.perf_counter()
        try:
            sql = read_sql(p)
            if not _looks_like_single_statement(sql):
                raise ValueError("SQL file must contain a single statement")

            target = out_dir / f"{name}.parquet"

            # Detect WM columns from existing Parquet (if present)
            wm_cols: list[str] = []
            if target.exists():
                try:
                    from .parquet import get_wm_columns_from_parquet, compute_max_wm_tuple

                    wm_cols = get_wm_columns_from_parquet(target)
                except Exception:
                    wm_cols = []

            # If we have WM columns, compute the last-known watermark tuple
            last_wm = None
            if wm_cols:
                try:
                    from .parquet import compute_max_wm_tuple

                    last_wm = compute_max_wm_tuple(target, wm_cols)
                except Exception:
                    last_wm = None

            # perform placeholder substitution for {WM1}, {WM2}, ... if present
            sql_to_run = substitute_wm_placeholders(sql, wm_cols, last_wm)

            # Wrap the fetch generator so we can detect duplicate PKs during streaming.
            # validated_gen will discover PK columns from the first non-empty chunk
            # if `pk_cols` is None.
            def validated_gen(orig_gen, pk_cols: list[str] | None = None):
                seen = set()
                dup_examples: list[tuple] = []
                first = True
                for df in orig_gen:
                    if df is None or len(df) == 0:
                        continue
                    # Discover PK columns from the first non-empty chunk if not provided
                    if first and (not pk_cols):
                        discovered = [c for c in df.columns if c.upper().startswith("PK")]
                        pk_cols = discovered or None
                        first = False
                    if pk_cols:
                        # ensure PK cols exist
                        for c in pk_cols:
                            if c not in df.columns:
                                raise ValueError(f"Missing PK column '{c}' in query result")
                        # iterate PK tuples and detect duplicates
                        for pk in df[list(pk_cols)].itertuples(index=False, name=None):
                            if pk in seen:
                                if len(dup_examples) < 5:
                                    dup_examples.append(pk)
                                raise ValueError(f"Duplicate PK values detected: {dup_examples}")
                            seen.add(pk)
                    yield df

            # run query and stream-validate into Parquet writer
            gen = fetcher.fetch_in_chunks(sql_to_run, chunk_size=chunk_size)
            wrapped_gen = validated_gen(gen, pk_cols=None)

            # write streaming single-file Parquet (will be atomic)
            dataframes_to_single_parquet(wrapped_gen, target, compression="snappy", row_group_size=chunk_size)

            # If no file was created, assume zero rows returned
            if not target.exists():
                res.rows = 0
            else:
                # best-effort: count row-groups / rows by reading metadata (avoid full read)
                try:
                    import pyarrow.parquet as pq

                    pf = pq.ParquetFile(str(target))
                    res.rows = pf.metadata.num_rows
                except Exception:
                    # fallback: set -1 to indicate unknown
                    res.rows = -1

            # Write per-query metadata JSON next to the Parquet file so callers/CI can
            # programmatically inspect results without loading Parquet files.
            try:
                meta = {
                    "name": name,
                    "rows": res.rows,
                    "elapsed_seconds": round(time.perf_counter() - start, 6),
                    "error": res.error,
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                }
                meta_path = out_dir / f"{name}.metadata.json"
                meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception:
                # non-fatal; metadata writing should not stop other queries
                pass

            # Update the SQL file with a small metadata comment block at the top so
            # editors see the last run row-count and timing. We embed the block
            # between sentinel lines so it can be updated cleanly.
            try:
                sql_text = p.read_text(encoding="utf-8")
                start_tag = "-- sap-archive-metadata-start"
                end_tag = "-- sap-archive-metadata-end"
                meta_lines = [
                    start_tag,
                    f"-- rows: {res.rows}",
                    f"-- elapsed_seconds: {round(time.perf_counter() - start, 6)}",
                    f"-- last_run_utc: {datetime.utcnow().isoformat()}Z",
                ]
                if res.error:
                    meta_lines.append(f"-- error: {res.error}")
                meta_lines.append(end_tag)
                meta_block = "\n".join(meta_lines) + "\n\n"

                if sql_text.lstrip().startswith(start_tag):
                    # replace existing block
                    # find end_tag position
                    idx = sql_text.find(end_tag)
                    if idx != -1:
                        # find position after end_tag line
                        rest = sql_text[idx + len(end_tag):]
                        new_sql = meta_block + rest.lstrip("\n")
                    else:
                        # malformed existing block — just prepend new block
                        new_sql = meta_block + sql_text
                else:
                    new_sql = meta_block + sql_text

                p.write_text(new_sql, encoding="utf-8")
            except Exception:
                # non-fatal; don't interrupt processing
                pass
        except Exception as exc:
            res.error = str(exc)
        finally:
            res.elapsed = time.perf_counter() - start
            results.append(res)

    return results
