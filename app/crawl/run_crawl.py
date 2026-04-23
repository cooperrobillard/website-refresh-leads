"""Run the Phase 5 crawl for businesses that passed the prefilter."""

from __future__ import annotations

from sqlalchemy import case

from app.db import SessionLocal
from app.lead_selection import dedupe_businesses_by_website
from app.models import Business
from app.pipeline_runs import businesses_for_run_query, resolve_pipeline_run
from app.schema import ensure_database_schema
from app.crawl.crawler import crawl_business_site


def run_crawl(run_id: int | None = None) -> dict[str, int]:
    """Run the crawl phase for current-run businesses that passed the prefilter."""
    status_order = case(
        (Business.fit_status == "strong", 0),
        (Business.fit_status == "maybe", 1),
        else_=2,
    )

    with SessionLocal() as session:
        current_run_id, allow_revisit = resolve_pipeline_run(session, run_id)
        success_count = 0
        failure_count = 0
        queried_businesses = (
            businesses_for_run_query(session, current_run_id, allow_revisit)
            .filter(Business.fit_status.in_(["strong", "maybe"]))
            .order_by(status_order, Business.review_count.desc(), Business.name.asc())
            .all()
        )
        businesses, duplicate_count = dedupe_businesses_by_website(queried_businesses)

        print(f"Run {current_run_id}: found {len(businesses)} businesses to crawl")
        if duplicate_count:
            print(f"Skipped {duplicate_count} duplicate website entr{'y' if duplicate_count == 1 else 'ies'}")

        for business in businesses:
            print(f"\nCrawling: {business.name} | {business.website}")

            result = crawl_business_site(session, business)

            if result["success"]:
                success_count += 1
                print(f"  Success | pages fetched: {result['pages_fetched']}")
                print(f"  Selected pages: {result['pages_selected']}")
            else:
                failure_count += 1
                print(f"  Failed | reason: {result['reason']}")

        print(f"\nDone.")
        print(f"Succeeded: {success_count}")
        print(f"Failed: {failure_count}")

        return {"success": success_count, "failed": failure_count, "total": len(businesses)}


def main() -> None:
    ensure_database_schema()
    run_crawl()


if __name__ == "__main__":
    main()
