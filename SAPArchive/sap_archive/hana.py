"""HANA data fetch utilities.

- Provides a small abstraction that uses a real HANA client when available
    (hdbcli) to execute queries and stream results to pandas.
- Users must supply credentials via env vars or CLI options.

Note: the real `hdbcli` package is optional and is listed as an extra (`[hana]`).
"""
from __future__ import annotations

import os
from typing import Generator, Optional, Any
from pathlib import Path

import pandas as pd

class HanaFetcher:
    def __init__(self, dsn: Optional[str] = None, user: Optional[str] = None, password: Optional[str] = None) -> None:
        self.dsn = dsn or os.getenv("SAP_HANA_DSN")
        self.user = user or os.getenv("SAP_HANA_USER")
        self.password = password or os.getenv("SAP_HANA_PASSWORD")

        # Prefer pyodbc (ODBC) as the runtime connector
        self._has_pyodbc = False
        self._pyodbc = None
        try:
            import pyodbc as _pyodbc

            self._pyodbc = _pyodbc
            self._has_pyodbc = True
        except Exception:
            self._has_pyodbc = False

    def _raise_no_client(self) -> None:
        raise RuntimeError(
            "No supported Python HANA client found in the environment. Install 'pyodbc' (recommended) or the SAP 'hdbcli' package."
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
        - params: optional parameter binding. A sequence of parameters is
          forwarded to the DB driver's execute call; named mappings are not
          used by this layer (placeholder substitution occurs at the query
          level).
        """
        # Production path: require a DB client
        # (no local fake-data generator)

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
