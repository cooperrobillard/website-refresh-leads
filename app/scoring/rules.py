"""Deterministic prefilter rules for early lead qualification."""

from __future__ import annotations

from dataclasses import dataclass

from app.models import Business


ALLOWED_PRIMARY_TYPES = {
    "painter",
    "painting_contractor",
    "contractor",
    "general_contractor",
    "flooring_contractor",
    "landscaper",
    "pressure_washing_service",
    "house_cleaning_service",
    "cleaning_service",
    "junk_removal_service",
    "window_cleaning_service",
}

BLOCKED_PRIMARY_TYPES = {
    "restaurant",
    "cafe",
    "bakery",
    "bar",
    "hotel",
    "event_venue",
    "clothing_store",
    "furniture_store",
    "grocery_store",
    "supermarket",
    "florist",
    "car_dealer",
    "ecommerce_store",
}

BLOCKED_WEBSITE_HINTS = [
    "shopify",
    "/products",
    "/product",
    "/collections",
    "/cart",
    "/checkout",
    "/menu",
    "/order-online",
    "/reservations",
    "/events",
    "/news",
    "/blog",
]


@dataclass
class FilterResult:
    """Result of applying the Phase 4 prefilter rules to one business."""

    fit_status: str
    reason: str | None


def normalize_text(value: str | None) -> str:
    """Normalize optional string values for rule comparisons."""
    return (value or "").strip().lower()


def website_looks_blocked(website: str | None) -> bool:
    """Return True when the website URL suggests a poor-fit business model."""
    site = normalize_text(website)
    return any(hint in site for hint in BLOCKED_WEBSITE_HINTS)


def primary_type_allowed(primary_type: str | None) -> bool:
    """Return True when the primary type is in the current allowlist."""
    pt = normalize_text(primary_type)
    return pt in ALLOWED_PRIMARY_TYPES


def primary_type_blocked(primary_type: str | None) -> bool:
    """Return True when the primary type is in the explicit blocklist."""
    pt = normalize_text(primary_type)
    return pt in BLOCKED_PRIMARY_TYPES


def passes_basic_filters(business: Business) -> FilterResult:
    """Classify a business as strong, maybe, or skip using simple rules."""
    website = normalize_text(business.website)
    primary_type = normalize_text(business.primary_type)
    review_count = business.review_count or 0
    rating = business.rating or 0.0

    # 1. Must have a website
    if not website:
        return FilterResult("skip", "No website")

    # 2. Obvious blocked business categories
    if primary_type_blocked(primary_type):
        return FilterResult("skip", f"Blocked business type: {primary_type}")

    # 3. Website hints that suggest ecommerce or high-update models
    if website_looks_blocked(website):
        return FilterResult("skip", "Website pattern suggests ecommerce or high-update business")

    # 4. If we know the type and it is not in our allowlist, skip for now
    if primary_type and not primary_type_allowed(primary_type):
        return FilterResult("skip", f"Primary type not in allowed list: {primary_type}")

    # 5. Very low review count = weak legitimacy / low confidence
    if review_count <= 2:
        return FilterResult("skip", f"Too few reviews: {review_count}")

    # 6. Low but not terrible review count = maybe
    if 3 <= review_count <= 7:
        return FilterResult("maybe", f"Low review count: {review_count}")

    # 7. Extremely poor rating can be a weak signal; do not hard skip yet
    if review_count >= 8 and rating > 0 and rating < 3.5:
        return FilterResult("maybe", f"Low rating: {rating}")

    # 8. Missing primary type is uncertain, but not necessarily a skip
    if not primary_type:
        return FilterResult("maybe", "Missing primary type")

    return FilterResult("strong", None)
