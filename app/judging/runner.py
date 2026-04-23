"""Hybrid final-judgment runners and scoring-mode dispatch."""

from __future__ import annotations

from collections import Counter

from sqlalchemy import case

from app.db import SessionLocal
from app.judging.client import ModelJudgeClient
from app.judging.package_builder import build_business_judging_package
from app.judging.persistence import upsert_model_judgment
from app.judging.prompting import PROMPT_VERSION, build_prompt
from app.judging.schemas import ModelJudgeOutcome
from app.lead_selection import dedupe_businesses_by_website, normalized_website_key
from app.models import Business, Score
from app.pipeline_runs import businesses_for_run_query, resolve_pipeline_run
from app.scoring.deterministic.runner import run_deterministic_scoring
from app.scoring.deterministic.rubric import ScoringResult, evaluate_business, upsert_score_and_note


def _prefilter_status_order():
    """Return a stable prefilter-first ordering for downstream stages."""
    return case(
        (Business.prefilter_status == "strong", 0),
        (Business.prefilter_status == "maybe", 1),
        else_=2,
    )


def _zero_duplicate_score(session, business: Business, canonical_name: str) -> None:
    """Persist an explicit duplicate skip so downstream exports stay deterministic."""
    business.fit_status = "skip"
    business.skip_reason = f"Duplicate website of canonical business: {canonical_name}"

    score_row = session.query(Score).filter(Score.business_id == business.id).first()
    if score_row is None:
        return

    score_row.business_legitimacy = 0
    score_row.website_weakness = 0
    score_row.conversion_opportunity = 0
    score_row.trust_packaging = 0
    score_row.complexity_fit = 0
    score_row.outreach_viability = 0
    score_row.outreach_story_strength = 0
    score_row.raw_total_score = 0
    score_row.evidence_tier = "minimal"
    score_row.evidence_cap = 0
    score_row.total_score = 0
    score_row.fit_status = "skip"
    score_row.confidence = "low"


def _label_strength(score_value: int | None) -> str:
    """Map deterministic numeric sub-scores into compact qualitative labels."""
    value = score_value or 0
    if value >= 10:
        return "high"
    if value >= 5:
        return "medium"
    if value >= 1:
        return "low"
    return "minimal"


def _fallback_outcome_from_deterministic(
    *,
    result: ScoringResult,
    judgment_mode: str,
) -> ModelJudgeOutcome:
    """Translate deterministic rubric output into a temporary model-judgment record."""
    return ModelJudgeOutcome(
        model_name="deterministic-fallback",
        prompt_version=PROMPT_VERSION,
        response_id=None,
        judgment_mode=judgment_mode,
        fit_status=result.fit_status,
        confidence=result.confidence,
        evidence_quality=result.evidence_tier,
        business_legitimacy=_label_strength(result.scores.get("business_legitimacy")),
        website_weakness=_label_strength(result.scores.get("website_weakness")),
        outreach_story_strength=_label_strength(result.scores.get("outreach_story_strength")),
        recommended_action="review_now" if result.fit_status in {"strong", "maybe"} else "skip",
        top_issues=result.top_issues[:3],
        short_teardown_angle=result.teardown_angle,
        short_reasoning=result.quick_summary,
        raw_json={
            "source": "deterministic_fallback",
            "fit_status": result.fit_status,
            "confidence": result.confidence,
            "evidence_tier": result.evidence_tier,
            "scores": result.scores,
            "skip_reason": result.skip_reason,
        },
    )


def run_model_judging(
    run_id: int | None = None,
    *,
    finalize_business: bool = True,
    judgment_mode: str = "primary",
) -> Counter[str]:
    """Run the scaffolded model-judge path with deterministic fallback output."""
    client = ModelJudgeClient()

    with SessionLocal() as session:
        current_run_id, allow_revisit = resolve_pipeline_run(session, run_id)
        queried_businesses = (
            businesses_for_run_query(session, current_run_id, allow_revisit)
            .filter(Business.prefilter_status.in_(["strong", "maybe"]))
            .order_by(_prefilter_status_order(), Business.review_count.desc(), Business.name.asc())
            .all()
        )
        businesses, duplicate_count = dedupe_businesses_by_website(queried_businesses)
        canonical_ids = {business.id for business in businesses}
        canonical_names_by_key = {
            business.canonical_key or normalized_website_key(business.website) or f"business:{business.id}": business.name
            for business in businesses
        }

        label = "model-judge" if judgment_mode == "primary" else f"model-judge ({judgment_mode})"
        print(f"Run {current_run_id}: {label} processing {len(businesses)} businesses")
        if duplicate_count:
            print(f"Skipped {duplicate_count} duplicate website entr{'y' if duplicate_count == 1 else 'ies'}")

        counts: Counter[str] = Counter()

        for business in queried_businesses:
            if business.id in canonical_ids:
                continue
            if finalize_business:
                website_key = business.canonical_key or normalized_website_key(business.website) or f"business:{business.id}"
                _zero_duplicate_score(
                    session,
                    business,
                    canonical_names_by_key.get(website_key, "another lead"),
                )

        for business in businesses:
            package = build_business_judging_package(
                session,
                business=business,
                pipeline_run_id=current_run_id,
            )
            prompt = build_prompt(package)
            outcome = client.judge(package, prompt, judgment_mode=judgment_mode)
            deterministic_result = evaluate_business(session, business)

            if outcome is None:
                # Keep the new judgment path runnable before a live model client lands.
                outcome = _fallback_outcome_from_deterministic(
                    result=deterministic_result,
                    judgment_mode=judgment_mode,
                )

            upsert_model_judgment(
                session,
                business_id=business.id,
                pipeline_run_id=current_run_id,
                outcome=outcome,
            )

            if finalize_business:
                upsert_score_and_note(session, business, deterministic_result)

            counts[deterministic_result.fit_status] += 1
            print(
                f"{business.name} | "
                f"prefilter={business.prefilter_status} | "
                f"final={deterministic_result.fit_status} | "
                f"source={outcome.model_name}"
            )

        session.commit()

        print("\nDone.")
        print(f"Strong: {counts['strong']}")
        print(f"Maybe: {counts['maybe']}")
        print(f"Skip: {counts['skip']}")
        return counts


def run_compare_mode(run_id: int | None = None) -> Counter[str]:
    """Run deterministic scoring for exports, then persist shadow model judgments."""
    print("Compare mode: deterministic scoring remains authoritative for exports.")
    counts = run_deterministic_scoring(run_id=run_id)
    run_model_judging(run_id=run_id, finalize_business=False, judgment_mode="shadow")
    return counts


def run_final_judgment(run_id: int | None = None, *, scoring_mode: str = "model_judge") -> Counter[str]:
    """Dispatch the run's final-judgment stage."""
    if scoring_mode == "deterministic":
        return run_deterministic_scoring(run_id=run_id)
    if scoring_mode == "compare":
        return run_compare_mode(run_id=run_id)
    return run_model_judging(run_id=run_id, finalize_business=True, judgment_mode="primary")
