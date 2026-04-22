"""Run the Phase 4 deterministic prefilter across saved businesses."""

from __future__ import annotations

from collections import Counter

from app.db import SessionLocal
from app.models import Business
from app.scoring.rules import passes_basic_filters


def main() -> None:
    with SessionLocal() as session:
        businesses = session.query(Business).all()
        counts: Counter[str] = Counter()

        for business in businesses:
            result = passes_basic_filters(business)

            business.fit_status = result.fit_status
            business.skip_reason = result.reason
            counts[result.fit_status] += 1

        session.commit()

        print(f"Processed {len(businesses)} businesses")
        print(f"Strong: {counts['strong']}")
        print(f"Maybe: {counts['maybe']}")
        print(f"Skip: {counts['skip']}")


if __name__ == "__main__":
    main()
