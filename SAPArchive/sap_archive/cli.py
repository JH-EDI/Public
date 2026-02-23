"""Command-line interface for sap-archive.

This CLI intentionally contains safe defaults and a development-mode data generator
so you can run and test without a live HANA instance.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Optional

import click

from .hana import HanaFetcher
from .queries import run_queries_folder


@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx: click.Context) -> None:
    """sap-archive: run SQL files from ./queries and export results to Parquet

    Running `sap-archive` with no arguments will execute the `export`
    subcommand (default workspace behavior)."""
    if ctx.invoked_subcommand is None:
        ctx.invoke(export)


@cli.command("export")
@click.option("--query", "query", help="SQL query name to run (omit to run all queries in ./queries)")
def export(query: str | None) -> None:
    """Run SQL file(s) in `./queries/` and write Parquet outputs to `./parquet/`.

    - Only `--query` is accepted (optional). Output path is `parquet/<query>.parquet`.
    - Dev mode is auto-detected when `SAP_HANA_DSN` is not set.
    """
    out = Path("parquet")
    out.mkdir(parents=True, exist_ok=True)

    runtime_dsn = os.getenv("SAP_HANA_DSN")
    runtime_user = os.getenv("SAP_HANA_USER")
    runtime_password = os.getenv("SAP_HANA_PASSWORD")
    dev_mode = not bool(runtime_dsn)

    fetcher = HanaFetcher(dsn=runtime_dsn, user=runtime_user, password=runtime_password, dev_mode=dev_mode)

    queries_dir = Path.cwd() / "queries"
    if query:
        qfile = queries_dir / (query if query.endswith(".sql") else f"{query}.sql")
        if not qfile.exists():
            click.echo(f"Query '{query}' not found in queries/", err=True)
            raise SystemExit(2)
        results = run_queries_folder(queries_dir, fetcher, out, chunk_size=50_000, files=[qfile])
    else:
        if not queries_dir.exists():
            click.echo("No 'queries/' folder found; create top-level 'queries/*.sql' files and re-run.", err=True)
            raise SystemExit(2)
        results = run_queries_folder(queries_dir, fetcher, out, chunk_size=50_000)

    any_failed = any(not r.succeeded() for r in results)
    for r in results:
        if r.succeeded():
            if r.rows == 0:
                click.echo(f"{r.name}: no rows returned; no file created")
            elif r.rows > 0:
                click.echo(f"{r.name}: OK — {r.rows:,} rows written ({r.elapsed:.2f}s)")
            else:
                click.echo(f"{r.name}: OK — rows written (count unknown) ({r.elapsed:.2f}s)")
        else:
            click.echo(f"{r.name}: ERROR — {r.error}", err=True)

    if any_failed:
        raise SystemExit(3)


@cli.command("catalog")
def catalog_cmd() -> None:
    """Generate HANA catalog and write to `metadata/hana_catalog.json`.

    This command is intentionally parameterless — it writes the catalog to
    `metadata/hana_catalog.json` and prints progress output while running so
    you can see activity. Advanced cataloging is available via the
    `sap_archive.catalog` API if needed.
    """
    from .catalog import dump_hana_catalog

    out = "metadata/hana_catalog.json"
    click.echo("Starting catalog generation — writing to metadata/hana_catalog.json")
    try:
        catalog = dump_hana_catalog(None, all_schemas=False, include_views=True, map_types=False, output=out, verbose=True)
        total_tables = sum(len(s.get("tables", {})) for s in catalog["schemas"].values())
        total_views = sum(len(s.get("views", {})) for s in catalog["schemas"].values()) if any("views" in s for s in catalog["schemas"].values()) else 0
        click.echo(f"Done — wrote {os.path.abspath(out)} (schemas={len(catalog['schemas'])}, tables={total_tables}, views={total_views})")
    except Exception as exc:
        click.echo(f"ERROR: {exc}", err=True)
        raise SystemExit(2)


@cli.command("regen-queries")
@click.option("--catalog", "catalog", default="metadata/hana_catalog.json", help="Path to existing catalog JSON")
@click.option("--out", "out_dir", default="metadata/queries", help="Output directory for generated queries")
def regen_queries_cmd(catalog: str, out_dir: str) -> None:
    """Generate SQL query files from an existing `metadata/hana_catalog.json`.

    This command does not connect to HANA; it uses the on-disk catalog JSON
    to regenerate `metadata/queries/*.sql` files with header metadata.
    """
    from .catalog import generate_queries_from_catalog

    try:
        generate_queries_from_catalog(catalog, out_dir)
        click.echo(f"Regenerated queries into: {out_dir}")
    except Exception as exc:
        click.echo(f"ERROR: {exc}", err=True)
        raise SystemExit(2)

@cli.command("version")
def version_cmd() -> None:
    """Show package version"""
    try:
        from importlib.metadata import version

        click.echo(version("sap-archive"))
    except Exception:
        click.echo("(development)")


def main(argv: Optional[list[str]] = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    return cli.main(args=argv, prog_name="sap-archive")


if __name__ == "__main__":
    raise SystemExit(main())
