"""Persistence helpers for model-judgment rows."""

from __future__ import annotations

import json

from sqlalchemy.orm import Session

from app.judging.schemas import ModelJudgeOutcome
from app.models import ModelJudgment


def upsert_model_judgment(
    session: Session,
    *,
    business_id: int,
    pipeline_run_id: int,
    outcome: ModelJudgeOutcome,
) -> ModelJudgment:
    """Create or replace the stored judgment for one business/run/mode tuple."""
    row = (
        session.query(ModelJudgment)
        .filter(ModelJudgment.business_id == business_id)
        .filter(ModelJudgment.pipeline_run_id == pipeline_run_id)
        .filter(ModelJudgment.judgment_mode == outcome.judgment_mode)
        .first()
    )
    if row is None:
        row = ModelJudgment(
            business_id=business_id,
            pipeline_run_id=pipeline_run_id,
            judgment_mode=outcome.judgment_mode,
        )
        session.add(row)

    row.model_name = outcome.model_name
    row.prompt_version = outcome.prompt_version
    row.response_id = outcome.response_id
    row.fit_status = outcome.fit_status
    row.confidence = outcome.confidence
    row.evidence_quality = outcome.evidence_quality
    row.business_legitimacy = outcome.business_legitimacy
    row.website_weakness = outcome.website_weakness
    row.outreach_story_strength = outcome.outreach_story_strength
    row.recommended_action = outcome.recommended_action
    row.top_issues = "\n".join(outcome.top_issues) if outcome.top_issues else None
    row.short_teardown_angle = outcome.short_teardown_angle
    row.short_reasoning = outcome.short_reasoning
    row.raw_json = json.dumps(outcome.raw_json, indent=2) if outcome.raw_json else None
    return row

