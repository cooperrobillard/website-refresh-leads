"""Compatibility wrapper for the deterministic prefilter runner."""

from __future__ import annotations

from collections import Counter

from app.scoring.deterministic.prefilter import run_prefilter
from app.schema import ensure_database_schema


def main() -> None:
    ensure_database_schema()
    run_prefilter()


if __name__ == "__main__":
    main()
