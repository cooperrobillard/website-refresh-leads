"""Run the preserved deterministic prefilter across saved businesses."""

from __future__ import annotations

from collections import Counter

from app.db import SessionLocal
from app.lead_selection import normalized_website_key
from app.models import Business
from app.pipeline_runs import businesses_for_run_query, resolve_pipeline_run
from app.scoring.deterministic.rules import FilterResult, passes_basic_filters


def run_prefilter(run_id: int | None = None) -> Counter[str]:
    """Run the deterministic admission gate across the current run scope."""
    with SessionLocal() as session:
        current_run_id, allow_revisit = resolve_pipeline_run(session, run_id)
        businesses = (
            businesses_for_run_query(session, current_run_id, allow_revisit)
            .order_by(Business.id.asc())
            .all()
        )
        counts: Counter[str] = Counter()
        canonical_names_by_key: dict[str, str] = {}

        for business in businesses:
            result = passes_basic_filters(business)
            website_key = business.canonical_key or normalized_website_key(business.website)

            if result.fit_status != "skip" and website_key:
                canonical_name = canonical_names_by_key.get(website_key)
                if canonical_name is not None:
                    result = FilterResult(
                        "skip",
                        f"Duplicate website of canonical business: {canonical_name}",
                    )
                else:
                    canonical_names_by_key[website_key] = business.name

            business.prefilter_status = result.fit_status
            business.prefilter_reason = result.reason
            counts[result.fit_status] += 1

        session.commit()

        print(f"Run {current_run_id}: processed {len(businesses)} businesses")
        print(f"Strong: {counts['strong']}")
        print(f"Maybe: {counts['maybe']}")
        print(f"Skip: {counts['skip']}")

        return counts
