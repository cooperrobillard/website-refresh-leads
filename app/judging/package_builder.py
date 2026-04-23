"""Helpers for building compact evidence packages for future model judgment."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from app.crawl.page_selector import normalize_url
from app.judging.schemas import BusinessJudgingPackage
from app.models import Artifact, Business, Page
from app.scoring.deterministic.rules import franchise_or_corporate_reason, primary_type_blocked, website_looks_blocked


KNOWN_PAGE_TYPES = ("home", "services", "about", "contact", "gallery", "faq")
SCREENSHOT_TYPE_MAP = {
    "desktop": "desktop_home_screenshot",
    "mobile": "mobile_home_screenshot",
}
SIGNAL_KEYS = ("homepage_loaded", "phone_visible", "tel_link_present", "cta_visible_near_top")


def _slugify(value: str) -> str:
    """Create a filesystem-friendly slug for a business name."""
    slug = "".join(c.lower() if c.isalnum() else "-" for c in value)
    parts = [part for part in slug.split("-") if part]
    return "-".join(parts) or "business"


def _compact_text(value: str | None, *, limit: int = 360) -> str | None:
    """Trim raw page text into a short, debug-friendly snapshot."""
    if not value:
        return None
    compact = " ".join(value.split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3].rstrip() + "..."


def _load_browser_report(business: Business) -> dict[str, object]:
    """Load the saved browser report directly from disk when present."""
    candidate = Path("data/browser_checks") / f"{_slugify(business.name)}.json"
    if not candidate.exists():
        return {}

    try:
        return json.loads(candidate.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _page_map(page_rows: list[Page]) -> dict[str, Page]:
    """Return the first saved page row for each known page type."""
    page_map: dict[str, Page] = {}
    for row in page_rows:
        if row.page_type and row.page_type not in page_map:
            page_map[row.page_type] = row
    return page_map


def _screenshot_paths(artifact_rows: list[Artifact]) -> dict[str, str | None]:
    """Return stable desktop/mobile screenshot paths when the files still exist."""
    screenshot_paths: dict[str, str | None] = {"desktop": None, "mobile": None}
    artifact_by_type = {
        row.artifact_type: row.file_path
        for row in artifact_rows
        if row.artifact_type and row.file_path
    }

    for variant, artifact_type in SCREENSHOT_TYPE_MAP.items():
        candidate = artifact_by_type.get(artifact_type)
        if candidate and Path(candidate).exists():
            screenshot_paths[variant] = candidate

    return screenshot_paths


def build_evidence_summary(
    *,
    page_map: dict[str, Page],
    text_excerpts: dict[str, str | None],
    screenshot_paths: dict[str, str | None],
) -> dict[str, bool | int]:
    """Compute a compact, inspectable evidence summary for one business."""
    return {
        "has_desktop_screenshot": bool(screenshot_paths.get("desktop")),
        "has_mobile_screenshot": bool(screenshot_paths.get("mobile")),
        "has_home_page": "home" in page_map and bool(page_map["home"].url),
        "captured_page_count": sum(1 for row in page_map.values() if row.url),
        "has_home_text": bool(text_excerpts.get("home")),
        "has_service_text": bool(text_excerpts.get("services")),
    }


def post_browser_evidence_gate(package: BusinessJudgingPackage) -> str | None:
    """Return a late hard-stop reason when the collected evidence is unusable."""
    has_homepage_signal = bool(
        package.browser_homepage_signals.get("homepage_loaded")
        or package.page_load_map.get("home")
        or package.evidence_summary.get("has_home_page")
        or package.evidence_summary.get("has_home_text")
        or package.evidence_summary.get("has_desktop_screenshot")
        or package.evidence_summary.get("has_mobile_screenshot")
    )
    if not has_homepage_signal:
        return "Homepage could not be loaded from crawl, browser checks, or screenshots"

    has_usable_crawl = bool(
        package.evidence_summary.get("has_home_text")
        or package.evidence_summary.get("has_service_text")
        or package.pages_captured_count > 0
    )
    has_usable_screenshots = bool(
        package.evidence_summary.get("has_desktop_screenshot")
        or package.evidence_summary.get("has_mobile_screenshot")
    )
    if not has_usable_crawl and not has_usable_screenshots:
        return "No usable crawl evidence or screenshot evidence"

    return None


def _browser_homepage_signals(browser_report: dict[str, object]) -> dict[str, bool]:
    """Extract only the homepage signal booleans needed for judging."""
    raw_signals = browser_report.get("homepage_signals", {})
    if not isinstance(raw_signals, dict):
        return {key: False for key in SIGNAL_KEYS}
    return {key: bool(raw_signals.get(key)) for key in SIGNAL_KEYS}


def _page_load_map(browser_report: dict[str, object]) -> dict[str, bool]:
    """Extract the compact browser page-load map."""
    raw_page_loads = browser_report.get("page_loads", {})
    if not isinstance(raw_page_loads, dict):
        return {}
    return {str(key): bool(value) for key, value in raw_page_loads.items()}


def build_business_judging_package(
    session: Session,
    *,
    business: Business,
    pipeline_run_id: int,
) -> BusinessJudgingPackage:
    """Assemble a compact structured judging package from collected evidence."""
    page_rows = (
        session.query(Page)
        .filter(Page.business_id == business.id)
        .order_by(Page.id.asc())
        .all()
    )
    artifact_rows = (
        session.query(Artifact)
        .filter(Artifact.business_id == business.id)
        .order_by(Artifact.id.asc())
        .all()
    )
    page_map = _page_map(page_rows)
    browser_report = _load_browser_report(business)
    browser_homepage_signals = _browser_homepage_signals(browser_report)
    page_load_map = _page_load_map(browser_report)
    screenshot_paths = _screenshot_paths(artifact_rows)
    text_excerpts = {
        "home": _compact_text(page_map.get("home").raw_text if page_map.get("home") else None),
        "services": _compact_text(page_map.get("services").raw_text if page_map.get("services") else None),
        "about": _compact_text(page_map.get("about").raw_text if page_map.get("about") else None),
        "contact": _compact_text(page_map.get("contact").raw_text if page_map.get("contact") else None),
    }
    pages_found = {
        page_type: normalize_url(page_map[page_type].url) if page_type in page_map and page_map[page_type].url else None
        for page_type in KNOWN_PAGE_TYPES
    }
    pages_captured_count = sum(1 for page_type in KNOWN_PAGE_TYPES if pages_found.get(page_type))
    screenshots_captured_count = sum(1 for path in screenshot_paths.values() if path)
    evidence_summary = build_evidence_summary(
        page_map=page_map,
        text_excerpts=text_excerpts,
        screenshot_paths=screenshot_paths,
    )
    franchise_reason = franchise_or_corporate_reason(
        business_name=business.name,
        website=business.website,
    )
    diagnostics: dict[str, Any] = {
        "prefilter_status": business.prefilter_status,
        "prefilter_reason": business.prefilter_reason,
        "pages_captured": pages_captured_count,
        "screenshots_captured": screenshots_captured_count,
        "homepage_loaded": bool(browser_homepage_signals.get("homepage_loaded") or page_load_map.get("home")),
        "phone_visible": browser_homepage_signals.get("phone_visible", False),
        "tel_link_present": browser_homepage_signals.get("tel_link_present", False),
        "cta_visible_near_top": browser_homepage_signals.get("cta_visible_near_top", False),
        "franchise_flag": bool(franchise_reason),
        "franchise_reason": franchise_reason,
        "ecommerce_flag": website_looks_blocked(business.website),
        "blocked_type_flag": primary_type_blocked(business.primary_type),
    }

    return BusinessJudgingPackage(
        business_id=business.id,
        pipeline_run_id=pipeline_run_id,
        business_name=business.name,
        website=normalize_url(business.website) if business.website else None,
        canonical_url=normalize_url(business.canonical_url) if business.canonical_url else None,
        niche=business.niche,
        query_used=business.query_used,
        location=business.address,
        primary_type=business.primary_type,
        google_rating=business.rating,
        google_review_count=business.review_count,
        browser_homepage_signals=browser_homepage_signals,
        page_load_map=page_load_map,
        pages_found=pages_found,
        pages_captured_count=pages_captured_count,
        screenshots_captured_count=screenshots_captured_count,
        text_excerpts=text_excerpts,
        screenshot_paths=screenshot_paths,
        evidence_summary=evidence_summary,
        diagnostics=diagnostics,
    )
