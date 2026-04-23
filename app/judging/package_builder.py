"""Helpers for building compact evidence packages for future model judgment."""

from __future__ import annotations

import json
from pathlib import Path

from sqlalchemy.orm import Session

from app.models import Artifact, Business, Page
from app.judging.schemas import BusinessJudgingPackage


def _compact_text(value: str | None, *, limit: int = 600) -> str | None:
    """Trim raw page text into a short, debug-friendly snapshot."""
    if not value:
        return None
    compact = " ".join(value.split())
    return compact[:limit]


def _load_browser_report(business: Business) -> dict[str, object]:
    """Load the saved browser report directly from disk when present."""
    candidate = Path("data/browser_checks") / f"{''.join(c.lower() if c.isalnum() else '-' for c in business.name).strip('-') or 'business'}.json"
    if not candidate.exists():
        return {}

    try:
        return json.loads(candidate.read_text(encoding="utf-8"))
    except Exception:
        return {}


def build_business_judging_package(
    session: Session,
    *,
    business: Business,
    pipeline_run_id: int,
) -> BusinessJudgingPackage:
    """Assemble a minimal, explicit evidence package for a business."""
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

    page_snapshots = [
        {
            "page_type": row.page_type,
            "url": row.url,
            "title": row.title,
            "text_excerpt": _compact_text(row.raw_text),
        }
        for row in page_rows[:6]
    ]
    screenshot_paths = {
        row.artifact_type: row.file_path
        for row in artifact_rows
        if row.artifact_type and row.file_path
    }

    return BusinessJudgingPackage(
        business_id=business.id,
        pipeline_run_id=pipeline_run_id,
        business_name=business.name,
        niche=business.niche,
        query_used=business.query_used,
        website=business.website,
        prefilter_status=business.prefilter_status,
        prefilter_reason=business.prefilter_reason,
        location=business.address,
        review_count=business.review_count,
        rating=business.rating,
        page_snapshots=page_snapshots,
        screenshot_paths=screenshot_paths,
        browser_report=_load_browser_report(business),
    )

