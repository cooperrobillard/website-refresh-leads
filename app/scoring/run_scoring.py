"""Run the Phase 7 scoring rubric across crawled businesses."""

from __future__ import annotations

from collections import Counter

from sqlalchemy import desc

from app.db import SessionLocal
from app.models import Business, Page
from app.scoring.rubric import evaluate_business, upsert_score_and_note


def main() -> None:
    with SessionLocal() as session:
        businesses = (
            session.query(Business)
            .join(Page, Page.business_id == Business.id)
            .filter(Page.page_type == "home")
            .distinct()
            .order_by(desc(Business.review_count), Business.name.asc())
            .all()
        )

        print(f"Scoring {len(businesses)} businesses")
        counts: Counter[str] = Counter()

        for business in businesses:
            result = evaluate_business(session, business)
            upsert_score_and_note(session, business, result)
            counts[result.fit_status] += 1

            print(
                f"{business.name} | "
                f"status={result.fit_status} | "
                f"score={result.total_score} | "
                f"confidence={result.confidence}"
            )

        session.commit()

        print("\nDone.")
        print(f"Strong: {counts['strong']}")
        print(f"Maybe: {counts['maybe']}")
        print(f"Skip: {counts['skip']}")


if __name__ == "__main__":
    main()
