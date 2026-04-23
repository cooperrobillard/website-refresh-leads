"""Run the preserved deterministic prefilter across saved businesses."""

from __future__ import annotations

from collections import Counter

from app.db import SessionLocal
from app.pipeline_runs import businesses_for_run_query, resolve_pipeline_run
from app.scoring.deterministic.rules import passes_basic_filters


def run_prefilter(run_id: int | None = None) -> Counter[str]:
    """Run the deterministic admission gate across the current run scope."""
    with SessionLocal() as session:
        current_run_id, allow_revisit = resolve_pipeline_run(session, run_id)
        businesses = businesses_for_run_query(session, current_run_id, allow_revisit).all()
        counts: Counter[str] = Counter()

        for business in businesses:
            result = passes_basic_filters(business)

            business.prefilter_status = result.fit_status
            business.prefilter_reason = result.reason
            counts[result.fit_status] += 1

        session.commit()

        print(f"Run {current_run_id}: processed {len(businesses)} businesses")
        print(f"Strong: {counts['strong']}")
        print(f"Maybe: {counts['maybe']}")
        print(f"Skip: {counts['skip']}")

        return counts
