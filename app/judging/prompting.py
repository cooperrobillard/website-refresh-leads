"""Prompt builders for the future model-judge path."""

from __future__ import annotations

from dataclasses import asdict
import json

from app.judging.schemas import BusinessJudgingPackage, PromptBundle


PROMPT_VERSION = "judge_scaffold_v1"


def build_prompt(package: BusinessJudgingPackage) -> PromptBundle:
    """Build a compact prompt bundle around the evidence package."""
    system_prompt = (
        "You judge whether a local service business is a good website refresh lead. "
        "Prefer concise, evidence-backed outputs."
    )
    user_prompt = json.dumps(asdict(package), indent=2)
    return PromptBundle(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        prompt_version=PROMPT_VERSION,
    )
