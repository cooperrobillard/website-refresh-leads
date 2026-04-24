"""Hybrid final-judgment runners and scoring-mode dispatch."""

from __future__ import annotations

from collections import Counter

from sqlalchemy import case

from app.db import SessionLocal
from app.judging.client import ModelJudgeClient
from app.judging.package_builder import build_business_judging_package, post_browser_evidence_gate
from app.judging.persistence import upsert_model_judgment
from app.judging.prompting import build_prompt
from app.judging.schemas import ModelJudgeOutcome
from app.lead_selection import dedupe_businesses_by_website, normalized_website_key
from app.models import Business
from app.pipeline_runs import businesses_for_run_query, resolve_pipeline_run
from app.scoring.deterministic.runner import run_deterministic_scoring


def _prefilter_status_order():
    """Return a stable prefilter-first ordering for downstream stages."""
    return case(
        (Business.prefilter_status == "strong", 0),
        (Business.prefilter_status == "maybe", 1),
        else_=2,
    )


def _zero_duplicate_score(session, business: Business, canonical_name: str) -> None:
    """Persist an explicit duplicate skip so downstream exports stay deterministic."""
    _apply_business_judgment(
        business,
        fit_status="skip",
        skip_reason=f"Duplicate website of canonical business: {canonical_name}",
    )


def _apply_business_judgment(
    business: Business,
    *,
    fit_status: str,
    skip_reason: str | None = None,
) -> None:
    """Update the business row to reflect the currently selected final judgment path."""
    business.fit_status = fit_status
    business.skip_reason = skip_reason if fit_status == "skip" else None


def _evidence_gate_outcome(
    *,
    judgment_mode: str,
    reason: str,
) -> ModelJudgeOutcome:
    """Build a stored judgment row for late hard-stop evidence failures."""
    return ModelJudgeOutcome(
        model_name="evidence-gate",
        prompt_version="evidence_gate_v1",
        response_id=None,
        judgment_mode=judgment_mode,
        fit_status="skip",
        confidence="high",
        evidence_quality="minimal",
        business_legitimacy=0,
        website_weakness=0,
        outreach_story_strength=0,
        recommended_action="skip",
        top_issues=[reason],
        short_teardown_angle="Do not send to the model until evidence collection succeeds.",
        short_reasoning=reason,
        evidence_warnings=[reason],
        positive_signals=[],
        raw_json={
            "source": "evidence_gate",
            "reason": reason,
        },
    )


def run_model_judging(
    run_id: int | None = None,
    *,
    finalize_business: bool = True,
    judgment_mode: str = "model_judge",
) -> Counter[str]:
    """Run the OpenAI-backed model-judge path across evidence-ready businesses."""
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

        label = "model-judge" if judgment_mode == "model_judge" else f"model-judge ({judgment_mode})"
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
            evidence_gate_reason = post_browser_evidence_gate(package)
            if evidence_gate_reason:
                outcome = _evidence_gate_outcome(
                    judgment_mode=judgment_mode,
                    reason=evidence_gate_reason,
                )
                upsert_model_judgment(
                    session,
                    business_id=business.id,
                    pipeline_run_id=current_run_id,
                    outcome=outcome,
                )
                if finalize_business:
                    _apply_business_judgment(
                        business,
                        fit_status="skip",
                        skip_reason=evidence_gate_reason,
                    )
                counts["skip"] += 1
                print(
                    f"{business.name} | "
                    f"prefilter={business.prefilter_status} | "
                    f"final=skip | "
                    f"source=evidence-gate"
                )
                continue

            prompt = build_prompt(package)
            outcome = client.judge(package, prompt, judgment_mode=judgment_mode)
            upsert_model_judgment(
                session,
                business_id=business.id,
                pipeline_run_id=current_run_id,
                outcome=outcome,
            )

            if finalize_business:
                skip_reason = None
                if outcome.fit_status == "skip":
                    skip_reason = (
                        outcome.top_issues[0]
                        if outcome.top_issues
                        else outcome.short_reasoning or "Model judgment marked this lead as skip."
                    )
                _apply_business_judgment(
                    business,
                    fit_status=outcome.fit_status,
                    skip_reason=skip_reason,
                )

            counts[outcome.fit_status] += 1
            print(
                f"{business.name} | "
                f"fit={outcome.fit_status} | "
                f"confidence={outcome.confidence} | "
                f"evidence={outcome.evidence_quality} | "
                f"weakness={outcome.website_weakness}/10 | "
                f"story={outcome.outreach_story_strength}/10"
            )

        session.commit()

        print("\nDone.")
        print(f"Strong: {counts['strong']}")
        print(f"Maybe: {counts['maybe']}")
        print(f"Skip: {counts['skip']}")
        return counts


def run_compare_mode(run_id: int | None = None) -> Counter[str]:
    """Run deterministic scoring and model judging in one compare-mode pass."""
    print("Compare mode: deterministic scoring is preserved, but model judgment remains primary for export.")
    run_deterministic_scoring(run_id=run_id)
    return run_model_judging(run_id=run_id, finalize_business=True, judgment_mode="compare")


def run_final_judgment(run_id: int | None = None, *, scoring_mode: str = "model_judge") -> Counter[str]:
    """Dispatch the run's final-judgment stage."""
    if scoring_mode == "deterministic":
        return run_deterministic_scoring(run_id=run_id)
    if scoring_mode == "compare":
        return run_compare_mode(run_id=run_id)
    return run_model_judging(run_id=run_id, finalize_business=True, judgment_mode="model_judge")
