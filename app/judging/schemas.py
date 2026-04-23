"""Lightweight schemas for model-judgment scaffolding."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class BusinessJudgingPackage:
    """Compact evidence package assembled for one business judgment."""

    business_id: int
    pipeline_run_id: int
    business_name: str
    website: str | None
    canonical_url: str | None
    niche: str | None
    query_used: str | None
    location: str | None
    primary_type: str | None
    google_rating: float | None
    google_review_count: int | None
    browser_homepage_signals: dict[str, bool] = field(default_factory=dict)
    page_load_map: dict[str, bool] = field(default_factory=dict)
    pages_found: dict[str, str | None] = field(default_factory=dict)
    pages_captured_count: int = 0
    screenshots_captured_count: int = 0
    text_excerpts: dict[str, str | None] = field(default_factory=dict)
    screenshot_paths: dict[str, str | None] = field(default_factory=dict)
    evidence_summary: dict[str, Any] = field(default_factory=dict)
    diagnostics: dict[str, Any] = field(default_factory=dict)


@dataclass
class PromptBundle:
    """Prompt payload prepared for a future model client."""

    system_prompt: str
    user_prompt: str
    prompt_version: str


@dataclass
class ModelJudgeOutcome:
    """Normalized judgment record for persistence and downstream comparison."""

    model_name: str
    prompt_version: str
    response_id: str | None
    judgment_mode: str
    fit_status: str
    confidence: str
    evidence_quality: str
    business_legitimacy: str
    website_weakness: str
    outreach_story_strength: str
    recommended_action: str
    top_issues: list[str] = field(default_factory=list)
    short_teardown_angle: str | None = None
    short_reasoning: str | None = None
    raw_json: dict[str, Any] = field(default_factory=dict)
