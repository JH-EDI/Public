"""HANA data fetch utilities.

- Provides a small abstraction that uses a real HANA client when available
  (hdbcli) and a deterministic dev-mode generator for local testing.
- Users must supply credentials via env vars or CLI options.

Note: the real `hdbcli` package is optional and is listed as an extra (`[hana]`).
"""
from __future__ import annotations

import os
from typing import Generator, Optional, Any
from pathlib import Path

import pandas as pd

DEV_ROW_COUNT = 12_345


class HanaFetcher:
    def __init__(self, dsn: Optional[str] = None, user: Optional[str] = None, password: Optional[str] = None, dev_mode: bool = False) -> None:
        self.dsn = dsn or os.getenv("SAP_HANA_DSN")
        self.user = user or os.getenv("SAP_HANA_USER")
        self.password = password or os.getenv("SAP_HANA_PASSWORD")
        self.dev_mode = dev_mode

        # Prefer pyodbc (ODBC) as the runtime connector
        self._has_pyodbc = False
        self._pyodbc = None
        if not dev_mode:
            try:
                import pyodbc as _pyodbc

                self._pyodbc = _pyodbc
                self._has_pyodbc = True
            except Exception:
                self._has_pyodbc = False

    def _raise_no_client(self) -> None:
        raise RuntimeError(
            "No supported Python HANA client found in the environment. Install 'pyodbc' (recommended) or the SAP 'hdbcli' package, or run with --dev."
        )

    def fetch_in_chunks(
        self,
        sql: str,
        chunk_size: int = 50_000,
        params: Optional[object] = None,
    ) -> Generator[pd.DataFrame, None, None]:
        """Yield pandas.DataFrame objects for each chunk of the query result.

        Parameters
        - sql: SQL text to execute.
        - chunk_size: number of rows per chunk.
        - params: optional parameter binding. In dev-mode a mapping like
          `{"WM1": <value>}` is understood and will be applied as a
          watermark filter. In production a sequence of parameters is
          forwarded to the DB driver's execute call.
        """
        if self.dev_mode:
            # dev generator can honor simple params (watermarks)
            yield from _dev_generator(chunk_size, params=params)
            return

        if not getattr(self, "_has_pyodbc", False) or self._pyodbc is None:
            self._raise_no_client()

        # Build an ODBC connection string. Support these `SAP_HANA_DSN` forms:
        #  - host:port
        #  - DSN=name
        #  - full conn-string (contains '=')
        dsn = self.dsn
        if not dsn:
            raise ValueError("No DSN provided (SAP_HANA_DSN or dsn argument)")

        if "=" in dsn:
            conn_str = dsn
        elif ":" in dsn:
            host, port = dsn.split(":", 1)
            driver = os.getenv("ODBC_DRIVER", "HDBODBC")
            conn_str = f"DRIVER={{{driver}}};SERVERNODE={host}:{port};UID={self.user};PWD={self.password}"
        else:
            # assume an ODBC DSN name
            conn_str = f"DSN={dsn};UID={self.user};PWD={self.password}"

        pyodbc = self._pyodbc
        if pyodbc is None:
            self._raise_no_client()
        assert pyodbc is not None

        conn = pyodbc.connect(conn_str, timeout=10)
        try:
            cur = conn.cursor()
            # forward sequence params to the DBAPI; named mappings are not
            # expanded here (templating should occur at the query layer).
            if isinstance(params, (list, tuple)):
                cur.execute(sql, params)
            else:
                cur.execute(sql)

            cols = [d[0] for d in cur.description]
            while True:
                rows = cur.fetchmany(chunk_size)
                if not rows:
                    break
                # pyodbc returns tuples — construct DataFrame with column names
                df = pd.DataFrame.from_records(rows, columns=cols)
                yield df
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def fetch_to_parquet(self, sql: str, path: str | Path, *, single_file: bool = True, chunk_size: int | None = None, params: Optional[object] = None, **parquet_kwargs: Any) -> None:
        """Convenience: fetch results for `sql` and write to a parquet `path`.

        - Defaults to single_file=True for callers that want one file per query.
        - Uses streaming writer under the hood; does not accumulate the whole
          result in memory.
        """
        chunk_size = chunk_size or 50_000
        gen = self.fetch_in_chunks(sql, chunk_size=chunk_size, params=params)
        from .parquet import dataframes_to_single_parquet

        if single_file:
            dataframes_to_single_parquet(gen, path, row_group_size=chunk_size, **parquet_kwargs)
        else:
            # legacy behaviour: write one file per chunk
            from pathlib import Path

            out = Path(path)
            out.parent.mkdir(parents=True, exist_ok=True)
            for i, df in enumerate(gen, start=1):
                if df.empty:
                    continue
                target = out.parent / f"{out.stem}_part_{i:03d}.parquet"
                from .parquet import dataframe_to_parquet

                dataframe_to_parquet(df, target, compression=parquet_kwargs.get("compression", "snappy"))

def _dev_generator(chunk_size: int = 1000, params: Optional[object] = None) -> Generator[pd.DataFrame, None, None]:
    """Deterministic fake data for local dev/testing.

    Supports a simple `params` mapping for watermark simulation: if
    `params` contains `WM1` (numeric), the generator yields only rows with
    `id > WM1`.
    """
    import numpy as np

    # support `params` being a mapping or other truthy object
    wm_threshold = None
    if isinstance(params, dict) and "WM1" in params:
        try:
            wm_threshold = int(params["WM1"])
        except Exception:
            wm_threshold = None

    n = DEV_ROW_COUNT
    # start index (0-based) — if wm_threshold given, skip up to that id
    idx = 0 if wm_threshold is None else min(max(0, wm_threshold), n)
    while idx < n:
        take = min(chunk_size, n - idx)
        # ids are 1-based
        start_id = idx + 1
        df: pd.DataFrame = pd.DataFrame({
            "id": range(start_id, start_id + take),
            "value": (np.arange(take) + idx) % 100,
            "text": [f"row-{i}" for i in range(start_id, start_id + take)],
        })
        # if wm_threshold is set, filter rows with id > wm_threshold
        if wm_threshold is not None:
            df = df[df["id"] > wm_threshold]
            # if entire chunk filtered out, advance and continue
            if df.empty:
                idx += take
                continue
        yield df
        idx += take
