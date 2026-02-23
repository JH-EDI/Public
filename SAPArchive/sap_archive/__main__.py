"""Module entrypoint for `python -m sap_archive`"""
from .cli import main


if __name__ == "__main__":
    raise SystemExit(main())
