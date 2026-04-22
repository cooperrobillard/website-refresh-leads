"""Run the Phase 5 crawl for businesses that passed the prefilter."""

from __future__ import annotations

from sqlalchemy import case

from app.db import SessionLocal
from app.models import Business
from app.crawl.crawler import crawl_business_site


def main() -> None:
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

        print(f"Found {len(businesses)} businesses to crawl")

        for business in businesses:
            print(f"\nCrawling: {business.name} | {business.website}")

            result = crawl_business_site(session, business)

            if result["success"]:
                print(f"  Success | pages fetched: {result['pages_fetched']}")
                print(f"  Selected pages: {result['pages_selected']}")
            else:
                print(f"  Failed | reason: {result['reason']}")


if __name__ == "__main__":
    main()
