"""Compatibility alias for preserved deterministic scoring rubric."""

from __future__ import annotations

import sys

from app.scoring.deterministic import rubric as _rubric

sys.modules[__name__] = _rubric
