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
    niche: str | None
    query_used: str | None
    website: str | None
    prefilter_status: str | None
    prefilter_reason: str | None
    location: str | None
    review_count: int | None
    rating: float | None
    page_snapshots: list[dict[str, str | None]] = field(default_factory=list)
    screenshot_paths: dict[str, str | None] = field(default_factory=dict)
    browser_report: dict[str, Any] = field(default_factory=dict)


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

