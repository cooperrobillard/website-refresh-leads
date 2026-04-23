"""Compatibility wrapper for the preserved deterministic scoring runner."""

from __future__ import annotations

from collections import Counter

from app.scoring.deterministic.runner import run_deterministic_scoring as run_scoring
from app.schema import ensure_database_schema

def main() -> None:
    ensure_database_schema()
    run_scoring()


if __name__ == "__main__":
    main()
