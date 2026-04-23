"""Run screenshot capture and browser checks for prefiltered businesses."""

from __future__ import annotations

from collections import Counter

from sqlalchemy import case

from app.browser.checks import run_browser_checks as run_business_browser_checks
from app.db import SessionLocal
from app.lead_selection import dedupe_businesses_by_website
from app.models import Business
from app.pipeline_runs import businesses_for_run_query, resolve_pipeline_run
from app.schema import ensure_database_schema
from app.browser.screenshots import capture_homepage_screenshots


def run_browser_validation(run_id: int | None = None) -> Counter[str]:
    """Run screenshot capture and browser checks for current-run businesses."""
    status_order = case(
        (Business.prefilter_status == "strong", 0),
        (Business.prefilter_status == "maybe", 1),
        else_=2,
    )

    with SessionLocal() as session:
        current_run_id, allow_revisit = resolve_pipeline_run(session, run_id)
        counts: Counter[str] = Counter()
        queried_businesses = (
            businesses_for_run_query(session, current_run_id, allow_revisit)
            .filter(Business.prefilter_status.in_(["strong", "maybe"]))
            .order_by(status_order, Business.review_count.desc(), Business.name.asc())
            .all()
        )
        businesses, duplicate_count = dedupe_businesses_by_website(queried_businesses)

        print(f"Run {current_run_id}: found {len(businesses)} businesses for browser validation")
        if duplicate_count:
            print(f"Skipped {duplicate_count} duplicate website entr{'y' if duplicate_count == 1 else 'ies'}")

        for business in businesses:
            print(f"\nChecking: {business.name}")

            screenshot_result = capture_homepage_screenshots(session, business)
            print(
                f"  Screenshots | desktop={screenshot_result.get('desktop_ok')} "
                f"mobile={screenshot_result.get('mobile_ok')} "
                f"url={screenshot_result.get('homepage_url')}"
            )

            check_result = run_business_browser_checks(session, business)
            if not check_result["success"]:
                counts["failed"] += 1
                print(f"  Failed | reason={check_result['reason']}")
                continue

            signals = check_result["homepage_signals"]
            counts["success"] += 1
            print(
                f"  Homepage signals | "
                f"loaded={signals['homepage_loaded']} "
                f"phone_visible={signals['phone_visible']} "
                f"tel_link_present={signals['tel_link_present']} "
                f"cta_visible_near_top={signals['cta_visible_near_top']}"
            )

            print(f"  Page loads | {check_result['page_loads']}")
            print(f"  Report saved to: {check_result['report_path']}")

        print("\nDone.")
        print(f"Succeeded: {counts['success']}")
        print(f"Failed: {counts['failed']}")

        return counts


def main() -> None:
    ensure_database_schema()
    run_browser_validation()


if __name__ == "__main__":
    main()
