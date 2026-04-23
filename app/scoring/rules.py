"""Compatibility alias for preserved deterministic prefilter rules."""

from __future__ import annotations

import sys

from app.scoring.deterministic import rules as _rules

sys.modules[__name__] = _rules
