"""Prompt builders for the future model-judge path."""

from __future__ import annotations

import json

from app.judging.schemas import BusinessJudgingPackage, PromptBundle


PROMPT_VERSION = "judge_scaffold_v1"


def build_prompt(package: BusinessJudgingPackage) -> PromptBundle:
    """Build a compact prompt bundle around the evidence package."""
    system_prompt = (
        "You judge whether a local service business is a good website refresh lead. "
        "Prefer concise, evidence-backed outputs."
    )
    user_prompt = json.dumps(
        {
            "business_id": package.business_id,
            "pipeline_run_id": package.pipeline_run_id,
            "business_name": package.business_name,
            "niche": package.niche,
            "query_used": package.query_used,
            "website": package.website,
            "prefilter_status": package.prefilter_status,
            "prefilter_reason": package.prefilter_reason,
            "location": package.location,
            "review_count": package.review_count,
            "rating": package.rating,
            "page_snapshots": package.page_snapshots,
            "browser_report": package.browser_report,
        },
        indent=2,
    )
    return PromptBundle(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        prompt_version=PROMPT_VERSION,
    )

