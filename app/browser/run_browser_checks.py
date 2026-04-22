"""Run screenshot capture and browser checks for prefiltered businesses."""

from __future__ import annotations

from collections import Counter

from sqlalchemy import case

from app.browser.checks import run_browser_checks as run_business_browser_checks
from app.db import SessionLocal
from app.models import Business
from app.browser.screenshots import capture_homepage_screenshots


def run_browser_validation() -> Counter[str]:
    """Run screenshot capture and browser checks for prefiltered businesses."""
    status_order = case(
        (Business.fit_status == "strong", 0),
        (Business.fit_status == "maybe", 1),
        else_=2,
    )

    with SessionLocal() as session:
        counts: Counter[str] = Counter()
        businesses = (
            session.query(Business)
            .filter(Business.fit_status.in_(["strong", "maybe"]))
            .order_by(status_order, Business.review_count.desc(), Business.name.asc())
            .all()
        )

        print(f"Found {len(businesses)} businesses for browser validation")

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
    run_browser_validation()


if __name__ == "__main__":
    main()
