"""Helpers for writing DataFrame -> Parquet safely and efficiently."""
from __future__ import annotations

from pathlib import Path
import os
from typing import Iterable, Optional, Any

import pandas as pd


def dataframe_to_parquet(df: pd.DataFrame, path: Path | str, compression: str = "snappy") -> None:
    path = Path(path)
    # Pandas/pyarrow will handle schema conversion; ensure parent exists
    path.parent.mkdir(parents=True, exist_ok=True)
    # Use pyarrow via pandas for compatibility
    df.to_parquet(path, index=False, compression=compression, engine="pyarrow")


# ---------------------------------------------------------------------------
# Streaming single-file writer (writes each incoming DataFrame as a
# row-group). This keeps memory bounded and produces a single parquet file
# for an entire generator of DataFrames.
# ---------------------------------------------------------------------------

def _df_to_pa_table(df: pd.DataFrame, schema: Optional[Any] = None) -> Any:
    """Convert pandas DataFrame to pyarrow Table using pandas/pyarrow bridge.

    The caller is responsible for ensuring per-chunk compatibility with
    an existing schema.
    """
    import pyarrow as pa

    # Let pyarrow infer schema for the chunk; preserve tz-aware datetimes when possible
    table = pa.Table.from_pandas(df, preserve_index=False)
    if schema is not None:
        # Validate compatibility
        if not table.schema.equals(schema):
            # Allow certain safe upcasts (integers -> larger integers, etc.) are
            # handled by pa.Table.from_pandas when types are compatible. For
            # complex mismatches, let ParquetWriter raise.
            pass
    return table


def get_wm_columns_from_parquet(path: Path | str) -> list[str]:
    """Return a sorted list of WM columns present in the Parquet file (e.g. ['WM1','WM2']).

    Returns empty list when no WM columns are present or the file doesn't exist.
    """
    p = Path(path)
    if not p.exists():
        return []
    try:
        import pyarrow.parquet as pq

        pf = pq.ParquetFile(str(p))
        names = [n for n in pf.schema.names if n.upper().startswith("WM")]
        # sort by numeric suffix when present (WM1, WM2, ...)
        def keyfn(n: str) -> int:
            s = n[2:]
            try:
                return int(s)
            except Exception:
                return 0

        return sorted(names, key=keyfn)
    except Exception:
        return []


def compute_max_wm_tuple(path: Path | str, wm_cols: list[str]) -> tuple | None:
    """Compute the lexicographic-max WM tuple from `path` using only `wm_cols`.

    Returns a tuple of values (WM1_value, WM2_value, ...) or None when not
    found or when file is missing / columns absent.
    """
    p = Path(path)
    if not p.exists() or not wm_cols:
        return None
    # read only the watermark columns into memory (expected to be small)
    try:
        import pandas as pd

        df = pd.read_parquet(p, columns=wm_cols)
    except Exception:
        return None
    if df.empty:
        return None
    # ensure columns exist
    for c in wm_cols:
        if c not in df.columns:
            return None
    # compute lexicographic max row based on wm_cols
    # convert to list of tuples and find max (Python compares tuples lexicographically)
    tuples = [tuple(row) for row in df[wm_cols].itertuples(index=False, name=None)]
    if not tuples:
        return None
    return max(tuples)



def dataframes_to_single_parquet(
    df_iter: Iterable[Optional[pd.DataFrame]],
    path: Path | str,
    *,
    compression: str = "snappy",
    row_group_size: int | None = None,
    tmp_suffix: str = ".part",
    coerce_schema: bool = False,
) -> None:
    """Stream an iterable of DataFrames into a single Parquet file.

    - Writes each incoming DataFrame as one or more row-groups using
      pyarrow.ParquetWriter.
    - Writes atomically by first writing to `path + tmp_suffix` and then
      os.replace()ing into place.
    - If the generator yields no rows, no file is created.
    """
    import pyarrow.parquet as pq

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + tmp_suffix)

    writer: pq.ParquetWriter | None = None
    schema = None
    total_rows = 0
    created = False

    try:
        for df in df_iter:
            if df is None:
                continue
            if len(df) == 0:
                continue

            # initialize writer from first non-empty chunk
            if writer is None:
                table = _df_to_pa_table(df)
                schema = table.schema
                writer = pq.ParquetWriter(str(tmp_path), schema, compression=compression)
                # determine effective row_group_size default
                if row_group_size is None:
                    # Default to the incoming chunk size or a sensible fallback
                    row_group_size = max(1, len(df))

            # possibly slice large df into multiple row-groups
            if row_group_size and len(df) > row_group_size:
                for start in range(0, len(df), row_group_size):
                    sub = df.iloc[start : start + row_group_size]
                    tbl = _df_to_pa_table(sub, schema=schema)
                    writer.write_table(tbl)
                    total_rows += len(sub)
                    created = True
            else:
                tbl = _df_to_pa_table(df, schema=schema)
                writer.write_table(tbl)
                total_rows += len(df)
                created = True

        if writer is not None:
            writer.close()
            writer = None

        if created:
            # atomic move into place
            os.replace(tmp_path, path)
    except Exception:
        # cleanup temp file on error
        try:
            if writer is not None:
                writer.close()
        except Exception:
            pass
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except Exception:
                pass
        raise
    finally:
        # ensure writer closed
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass
