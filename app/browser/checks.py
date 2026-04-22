"""Playwright-based browser checks for homepage quality signals."""

from __future__ import annotations

import json
import re
from pathlib import Path

from playwright.sync_api import sync_playwright
from sqlalchemy.orm import Session

from app.models import Business, Page


PHONE_PATTERN = re.compile(r"(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})")

CTA_KEYWORDS = [
    "contact",
    "call",
    "quote",
    "estimate",
    "request estimate",
    "free estimate",
    "book",
    "get started",
    "schedule",
]


def slugify(value: str) -> str:
    """Create a filesystem-friendly slug for a business name."""
    slug = "".join(c.lower() if c.isalnum() else "-" for c in value)
    parts = [part for part in slug.split("-") if part]
    return "-".join(parts) or "business"


def default_homepage_signals() -> dict[str, bool]:
    """Return the default homepage signal state."""
    return {
        "homepage_loaded": False,
        "phone_visible": False,
        "tel_link_present": False,
        "cta_visible_near_top": False,
    }


def get_selected_pages(session: Session, business: Business) -> dict[str, str]:
    """Return one saved URL per page type for a business."""
    rows = (
        session.query(Page)
        .filter(Page.business_id == business.id)
        .order_by(Page.id.asc())
        .all()
    )

    selected: dict[str, str] = {}
    for row in rows:
        if row.page_type and row.url and row.page_type not in selected:
            selected[row.page_type] = row.url

    return selected


def check_homepage_signals(home_url: str) -> dict[str, bool]:
    """Check a few simple homepage signals in a desktop browser."""
    result = default_homepage_signals()

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                context = browser.new_context(viewport={"width": 1440, "height": 900})
                try:
                    page = context.new_page()

                    response = page.goto(home_url, wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_timeout(1500)
                    result["homepage_loaded"] = bool(response and response.ok)

                    try:
                        body_text = page.locator("body").inner_text(timeout=3000)
                        result["phone_visible"] = bool(PHONE_PATTERN.search(body_text or ""))
                    except Exception:
                        pass

                    try:
                        tel_count = page.locator('a[href^="tel:"]').count()
                        result["tel_link_present"] = tel_count > 0
                    except Exception:
                        pass

                    try:
                        locators = page.locator("a, button")
                        count = min(locators.count(), 60)

                        for i in range(count):
                            item = locators.nth(i)
                            try:
                                if not item.is_visible():
                                    continue

                                text = (item.inner_text(timeout=1000) or "").strip().lower()
                                if not text:
                                    continue

                                if not any(keyword in text for keyword in CTA_KEYWORDS):
                                    continue

                                box = item.bounding_box()
                                if box and box["y"] < 950:
                                    result["cta_visible_near_top"] = True
                                    break
                            except Exception:
                                continue
                    except Exception:
                        pass
                finally:
                    context.close()
            finally:
                browser.close()

    except Exception:
        pass

    return result


def check_page_loads(page_map: dict[str, str]) -> dict[str, bool]:
    """Check whether each selected page loads successfully."""
    statuses: dict[str, bool] = {}

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                context = browser.new_context(viewport={"width": 1280, "height": 800})
                try:
                    page = context.new_page()

                    for page_type, url in page_map.items():
                        try:
                            response = page.goto(url, wait_until="domcontentloaded", timeout=25000)
                            page.wait_for_timeout(800)
                            statuses[page_type] = bool(response and response.ok)
                        except Exception:
                            statuses[page_type] = False
                finally:
                    context.close()
            finally:
                browser.close()

    except Exception:
        for page_type in page_map:
            statuses[page_type] = False

    return statuses


def save_browser_check_report(business: Business, report: dict[str, object]) -> str:
    """Write a JSON browser-check report to disk and return its path."""
    business_slug = slugify(business.name)
    folder = Path("data/browser_checks")
    folder.mkdir(parents=True, exist_ok=True)

    path = folder / f"{business_slug}.json"
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    return path.as_posix()


def run_browser_checks(session: Session, business: Business) -> dict[str, object]:
    """Run homepage/browser checks and persist a JSON report."""
    page_map = get_selected_pages(session, business)
    home_url = page_map.get("home") or business.website

    if not home_url:
        report = {
            "business_name": business.name,
            "business_id": business.id,
            "homepage_url": None,
            "homepage_signals": default_homepage_signals(),
            "page_loads": {},
            "success": False,
            "reason": "No homepage URL",
        }
        report_path = save_browser_check_report(business, report)
        report["report_path"] = report_path
        return report

    if "home" not in page_map:
        page_map = {"home": home_url, **page_map}

    homepage_signals = check_homepage_signals(home_url)
    page_loads = check_page_loads(page_map)

    report = {
        "business_name": business.name,
        "business_id": business.id,
        "homepage_url": home_url,
        "homepage_signals": homepage_signals,
        "page_loads": page_loads,
        "success": True,
        "reason": None,
    }

    report_path = save_browser_check_report(business, report)
    report["report_path"] = report_path

    return report
