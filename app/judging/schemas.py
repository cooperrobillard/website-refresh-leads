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
    business_legitimacy: int
    website_weakness: int
    outreach_story_strength: int
    recommended_action: str
    top_issues: list[str] = field(default_factory=list)
    short_teardown_angle: str | None = None
    short_reasoning: str | None = None
    evidence_warnings: list[str] = field(default_factory=list)
    positive_signals: list[str] = field(default_factory=list)
    raw_json: dict[str, Any] = field(default_factory=dict)


def model_judgment_json_schema() -> dict[str, Any]:
    """Return the strict JSON schema used for model-judge structured output."""
    return {
        "type": "json_schema",
        "name": "website_refresh_model_judgment",
        "strict": True,
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "fit_status": {
                    "type": "string",
                    "enum": ["strong", "maybe", "skip"],
                },
                "confidence": {
                    "type": "string",
                    "enum": ["high", "medium", "low"],
                },
                "evidence_quality": {
                    "type": "string",
                    "enum": ["strong", "medium", "sparse", "minimal"],
                },
                "business_legitimacy": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 10,
                },
                "website_weakness": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 10,
                },
                "outreach_story_strength": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 10,
                },
                "recommended_action": {
                    "type": "string",
                    "enum": [
                        "review_for_outreach",
                        "low_priority_review",
                        "skip",
                    ],
                },
                "top_issues": {
                    "type": "array",
                    "items": {"type": "string"},
                    "maxItems": 3,
                },
                "short_teardown_angle": {
                    "type": "string",
                },
                "short_reasoning": {
                    "type": "string",
                },
                "evidence_warnings": {
                    "type": "array",
                    "items": {"type": "string"},
                    "maxItems": 5,
                },
                "positive_signals": {
                    "type": "array",
                    "items": {"type": "string"},
                    "maxItems": 5,
                },
            },
            "required": [
                "fit_status",
                "confidence",
                "evidence_quality",
                "business_legitimacy",
                "website_weakness",
                "outreach_story_strength",
                "recommended_action",
                "top_issues",
                "short_teardown_angle",
                "short_reasoning",
                "evidence_warnings",
                "positive_signals",
            ],
        },
    }
