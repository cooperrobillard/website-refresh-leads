"""Placeholder CSV export helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def export_leads_csv(rows: list[dict[str, Any]], output_path: Path) -> Path:
    """Return the intended CSV export path without writing output yet."""
    # TODO: Implement CSV export logic in a later phase.
    return output_path
