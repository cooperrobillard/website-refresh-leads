"""Google Places Text Search helpers for lead discovery."""

from __future__ import annotations

from typing import Any

import requests
from sqlalchemy.orm import Session

from app.config import GOOGLE_PLACES_API_KEY
from app.lead_selection import normalized_website_key, normalize_website_url
from app.models import Business

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


def upsert_businesses(session: Session, places: list[dict[str, Any]], niche: str, query_used: str) -> tuple[int, int]:
    """Insert new businesses and update existing rows by place_id."""
    if not places:
        return 0, 0

    inserted = 0
    updated = 0

    rows = [normalize_place(place, niche=niche, query_used=query_used) for place in places]
    place_ids = [row["place_id"] for row in rows if row["place_id"]]
    website_keys = {
        normalized_website_key(row["website"])
        for row in rows
        if row["website"]
    }

    existing_by_place_id = {
        business.place_id: business
        for business in session.query(Business).filter(Business.place_id.in_(place_ids)).all()
        if business.place_id
    }
    existing_by_website_key: dict[str, Business] = {}
    if website_keys:
        for business in session.query(Business).filter(Business.website.isnot(None)).all():
            website_key = normalized_website_key(business.website)
            if website_key and website_key in website_keys and website_key not in existing_by_website_key:
                existing_by_website_key[website_key] = business

    for row in rows:
        website_key = normalized_website_key(row["website"])
        existing = existing_by_place_id.get(row["place_id"])
        if not existing and website_key:
            existing = existing_by_website_key.get(website_key)

        if existing:
            if row["place_id"] and not existing.place_id:
                existing.place_id = row["place_id"]
            existing.name = row["name"]
            existing.niche = row["niche"]
            existing.query_used = row["query_used"]
            existing.website = row["website"]
            existing.address = row["address"]
            existing.primary_type = row["primary_type"]
            existing.rating = row["rating"]
            existing.review_count = row["review_count"]
            updated += 1
        else:
            business = Business(
                place_id=row["place_id"],
                name=row["name"],
                niche=row["niche"],
                query_used=row["query_used"],
                website=row["website"],
                address=row["address"],
                primary_type=row["primary_type"],
                rating=row["rating"],
                review_count=row["review_count"],
            )
            session.add(business)
            if row["place_id"]:
                existing_by_place_id[row["place_id"]] = business
            if website_key:
                existing_by_website_key[website_key] = business
            inserted += 1

    session.commit()
    return inserted, updated
