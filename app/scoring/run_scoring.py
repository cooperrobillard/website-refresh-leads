"""Run the Phase 7 scoring rubric across crawled businesses."""

from __future__ import annotations

from collections import Counter

from sqlalchemy import case

from app.db import SessionLocal
from app.models import Business
from app.scoring.rubric import evaluate_business, upsert_score_and_note


def run_scoring() -> Counter[str]:
    """Run the scoring rubric across businesses currently marked strong or maybe."""
    status_order = case(
        (Business.fit_status == "strong", 0),
        (Business.fit_status == "maybe", 1),
        else_=2,
    )

    with SessionLocal() as session:
        businesses = (
            session.query(Business)
            .filter(Business.fit_status.in_(["strong", "maybe"]))
            .order_by(status_order, Business.review_count.desc(), Business.name.asc())
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

        return counts


def main() -> None:
    run_scoring()


if __name__ == "__main__":
    main()
