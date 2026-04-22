"""Review stored prefilter classifications for saved businesses."""

from __future__ import annotations

from sqlalchemy import case

from app.db import SessionLocal
from app.models import Business


def main() -> None:
    status_order = case(
        (Business.fit_status == "strong", 0),
        (Business.fit_status == "maybe", 1),
        (Business.fit_status == "skip", 2),
        else_=3,
    )

    with SessionLocal() as session:
        businesses = (
            session.query(Business)
            .order_by(status_order, Business.review_count.desc(), Business.name.asc())
            .all()
        )

        for business in businesses:
            review_count = business.review_count if business.review_count is not None else "n/a"
            primary_type = business.primary_type or "n/a"
            fit_status = business.fit_status or "unclassified"
            skip_reason = business.skip_reason or "-"

            print(
                f"{business.name} | "
                f"type={primary_type} | "
                f"reviews={review_count} | "
                f"status={fit_status} | "
                f"reason={skip_reason}"
            )


if __name__ == "__main__":
    main()
