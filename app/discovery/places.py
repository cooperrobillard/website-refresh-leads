"""Google Places Text Search helpers for lead discovery."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import requests
from sqlalchemy.orm import Session

from app.canonical_sites import canonical_website_key, canonical_website_url
from app.config import GOOGLE_PLACES_API_KEY
from app.lead_selection import normalize_website_url
from app.models import Business, PipelineRun

TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"

FIELD_MASK = ",".join(
    [
        "places.id",
        "places.displayName",
        "places.formattedAddress",
        "places.websiteUri",
        "places.rating",
        "places.userRatingCount",
        "places.primaryType",
        "nextPageToken",
    ]
)


def search_places_text(query: str, page_size: int = 10, page_token: str | None = None) -> dict[str, Any]:
    """Run a Places Text Search request and return the API response."""
    if not GOOGLE_PLACES_API_KEY:
        raise ValueError("Missing GOOGLE_PLACES_API_KEY in .env")

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
        "X-Goog-FieldMask": FIELD_MASK,
    }

    body: dict[str, Any] = {
        "textQuery": query,
        "pageSize": page_size,
    }

    if page_token:
        body["pageToken"] = page_token

    response = requests.post(TEXT_SEARCH_URL, headers=headers, json=body, timeout=30)
    response.raise_for_status()
    return response.json()


def normalize_place(place: dict[str, Any], niche: str, query_used: str) -> dict[str, Any]:
    """Normalize a Places API result into Business model fields."""
    display_name = place.get("displayName", {})
    if isinstance(display_name, dict):
        name = display_name.get("text")
    else:
        name = display_name

    return {
        "place_id": place.get("id"),
        "name": name or "Unknown",
        "niche": niche,
        "query_used": query_used,
        "website": normalize_website_url(place.get("websiteUri")),
        "address": place.get("formattedAddress"),
        "primary_type": place.get("primaryType"),
        "rating": place.get("rating"),
        "review_count": place.get("userRatingCount"),
    }


def _apply_discovery_metadata(
    business: Business,
    *,
    row: dict[str, Any],
    canonical_url: str | None,
    current_run: PipelineRun,
) -> bool:
    """Apply lightweight metadata updates when a business is seen again."""
    changed = False

    if row["place_id"] and not business.place_id:
        business.place_id = row["place_id"]
        changed = True

    if canonical_url != business.canonical_url:
        business.canonical_url = canonical_url
        changed = True

    field_updates = {
        "name": row["name"],
        "niche": row["niche"],
        "query_used": row["query_used"],
        "website": row["website"],
        "address": row["address"],
        "primary_type": row["primary_type"],
        "rating": row["rating"],
        "review_count": row["review_count"],
    }

    for field_name, value in field_updates.items():
        if getattr(business, field_name) != value:
            setattr(business, field_name, value)
            changed = True

    seen_at = datetime.utcnow()
    business.last_seen_at = seen_at
    business.last_seen_run_id = current_run.id
    if business.first_seen_at is None:
        business.first_seen_at = seen_at

    return changed


def upsert_businesses(
    session: Session,
    places: list[dict[str, Any]],
    niche: str,
    query_used: str,
    current_run: PipelineRun,
) -> dict[str, int]:
    """Insert brand-new canonical sites and skip already-processed sites by default."""
    if not places:
        return {
            "inserted": 0,
            "updated_metadata": 0,
            "skipped_existing_processed": 0,
        }

    counts = {
        "inserted": 0,
        "updated_metadata": 0,
        "skipped_existing_processed": 0,
    }

    rows = [normalize_place(place, niche=niche, query_used=query_used) for place in places]
    place_ids = [row["place_id"] for row in rows if row["place_id"]]
    website_keys = {
        canonical_key
        for row in rows
        for canonical_key in [canonical_website_key(row["website"])]
        if canonical_key
    }

    existing_by_place_id = {
        business.place_id: business
        for business in session.query(Business).filter(Business.place_id.in_(place_ids)).all()
        if business.place_id
    }
    existing_by_website_key: dict[str, Business] = {}
    if website_keys:
        for business in session.query(Business).filter(Business.canonical_key.in_(website_keys)).all():
            if business.canonical_key and business.canonical_key not in existing_by_website_key:
                existing_by_website_key[business.canonical_key] = business

    for row in rows:
        canonical_key = canonical_website_key(row["website"])
        canonical_url = canonical_website_url(row["website"])
        existing_by_place = existing_by_place_id.get(row["place_id"])
        existing_by_canonical = existing_by_website_key.get(canonical_key) if canonical_key else None
        existing = existing_by_canonical or existing_by_place

        if existing:
            if canonical_key and not existing.canonical_key and canonical_key not in existing_by_website_key:
                existing.canonical_key = canonical_key
                existing_by_website_key[canonical_key] = existing

            metadata_changed = _apply_discovery_metadata(
                existing,
                row=row,
                canonical_url=canonical_url,
                current_run=current_run,
            )

            if existing.discovery_run_id == current_run.id:
                if metadata_changed:
                    counts["updated_metadata"] += 1
                continue

            if current_run.allow_revisit and existing.eligible_for_revisit:
                if metadata_changed:
                    counts["updated_metadata"] += 1
                continue

            counts["skipped_existing_processed"] += 1
            continue

        seen_at = datetime.utcnow()
        business = Business(
            place_id=row["place_id"],
            name=row["name"],
            niche=row["niche"],
            query_used=row["query_used"],
            website=row["website"],
            canonical_key=canonical_key,
            canonical_url=canonical_url,
            address=row["address"],
            primary_type=row["primary_type"],
            rating=row["rating"],
            review_count=row["review_count"],
            discovery_run_id=current_run.id,
            first_seen_at=seen_at,
            last_seen_at=seen_at,
            last_seen_run_id=current_run.id,
        )
        session.add(business)
        if row["place_id"]:
            existing_by_place_id[row["place_id"]] = business
        if canonical_key:
            existing_by_website_key[canonical_key] = business
        counts["inserted"] += 1

    session.commit()
    return counts
