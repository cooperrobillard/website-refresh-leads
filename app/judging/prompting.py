"""Prompt builders for the future model-judge path."""

from __future__ import annotations

from dataclasses import asdict
import json

from app.judging.schemas import BusinessJudgingPackage, PromptBundle


PROMPT_VERSION = "model_judge_v1"


def build_prompt(package: BusinessJudgingPackage) -> PromptBundle:
    """Build a compact prompt bundle around the evidence package."""
    system_prompt = (
        "You are judging whether a local business is a good fit for a Website Rescue / website refresh offer. "
        "Judge fairly from the provided evidence only. "
        "Reward clear, owner-facing outreach stories that a small business owner could reasonably care about. "
        "Do not overclaim from missing evidence. "
        "Give credit when the site already has meaningful basics in place. "
        "Usually reject franchise, chain, or corporate landing-page style sites. "
        "Keep website weakness separate from outreach-story strength. "
        "Lower confidence when evidence is sparse, partial, or contradictory. "
        "Return only the requested structured result."
    )
    user_prompt = json.dumps(asdict(package), indent=2)
    return PromptBundle(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        prompt_version=PROMPT_VERSION,
    )
