"""Run the preserved deterministic scoring rubric across crawled businesses."""

from __future__ import annotations

from collections import Counter

from sqlalchemy import case

from app.db import SessionLocal
from app.lead_selection import dedupe_businesses_by_website, normalized_website_key
from app.models import Business, Score
from app.pipeline_runs import businesses_for_run_query, resolve_pipeline_run
from app.scoring.deterministic.rubric import evaluate_business, upsert_score_and_note


def run_deterministic_scoring(run_id: int | None = None) -> Counter[str]:
    """Run deterministic scoring for businesses that passed prefiltering."""
    status_order = case(
        (Business.prefilter_status == "strong", 0),
        (Business.prefilter_status == "maybe", 1),
        else_=2,
    )

    with SessionLocal() as session:
        current_run_id, allow_revisit = resolve_pipeline_run(session, run_id)
        queried_businesses = (
            businesses_for_run_query(session, current_run_id, allow_revisit)
            .filter(Business.prefilter_status.in_(["strong", "maybe"]))
            .order_by(status_order, Business.review_count.desc(), Business.name.asc())
            .all()
        )
        businesses, duplicate_count = dedupe_businesses_by_website(queried_businesses)
        canonical_ids = {business.id for business in businesses}
        canonical_names_by_key = {
            business.canonical_key or normalized_website_key(business.website) or f"business:{business.id}": business.name
            for business in businesses
        }

        print(f"Run {current_run_id}: scoring {len(businesses)} businesses")
        if duplicate_count:
            print(f"Skipped {duplicate_count} duplicate website entr{'y' if duplicate_count == 1 else 'ies'}")
        counts: Counter[str] = Counter()

        for business in queried_businesses:
            if business.id in canonical_ids:
                continue

            business.fit_status = "skip"
            website_key = business.canonical_key or normalized_website_key(business.website) or f"business:{business.id}"
            business.skip_reason = (
                f"Duplicate website of canonical business: "
                f"{canonical_names_by_key.get(website_key, 'another lead')}"
            )

            score_row = session.query(Score).filter(Score.business_id == business.id).first()
            if score_row:
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

        for business in businesses:
            result = evaluate_business(session, business)
            upsert_score_and_note(session, business, result)
            counts[result.fit_status] += 1

            print(
                f"{business.name} | "
                f"status={result.fit_status} | "
                f"score={result.total_score} raw={result.raw_total_score} | "
                f"confidence={result.confidence} | "
                f"evidence={result.evidence_tier} cap={result.evidence_cap}"
            )
            print(
                f"  leg={result.scores['business_legitimacy']} "
                f"weak={result.scores['website_weakness']} "
                f"conv={result.scores['conversion_opportunity']} "
                f"trust={result.scores['trust_packaging']} "
                f"fit={result.scores['complexity_fit']} "
                f"outreach={result.scores['outreach_viability']} "
                f"story={result.scores['outreach_story_strength']}"
            )
            if result.skip_reason:
                print(f"  reason={result.skip_reason}")

        session.commit()

        print("\nDone.")
        print(f"Strong: {counts['strong']}")
        print(f"Maybe: {counts['maybe']}")
        print(f"Skip: {counts['skip']}")

        return counts


def run_scoring(run_id: int | None = None) -> Counter[str]:
    """Backwards-compatible alias for deterministic scoring."""
    return run_deterministic_scoring(run_id=run_id)
