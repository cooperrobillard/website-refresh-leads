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

FRANCHISE_BRAND_KEYWORDS = [
    "certapro",
    "servpro",
    "molly maid",
    "mr handyman",
    "mr. handyman",
    "mr rooter",
    "mr. rooter",
    "the grounds guys",
    "grounds guys",
    "zerorez",
    "junk king",
    "1-800-got-junk",
    "window genie",
]

CORPORATE_DOMAIN_HINTS = [
    "certapro.com",
    "servpro.com",
    "mollymaid.com",
    "mrhandyman.com",
    "mrrooter.com",
    "groundsguys.com",
    "zerorez.com",
    "junk-king.com",
    "1800gotjunk.com",
    "windowgenie.com",
]

CORPORATE_PATH_HINTS = [
    "/landing-page",
    "gbp-landing",
    "/locations/",
    "/location/",
    "/franchise",
    "franchise-opportunities",
]

CORPORATE_COPY_HINTS = [
    "independently owned and operated",
    "franchise opportunities",
    "find a location",
    "locations nationwide",
    "national brand",
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


def franchise_or_corporate_reason(
    *,
    business_name: str | None,
    website: str | None,
    extra_text: str | None = None,
) -> str | None:
    """Return a skip reason when the lead looks like a franchise or corporate page."""
    name_text = normalize_text(business_name)
    website_text = normalize_text(website)
    extra = normalize_text(extra_text)
    combined = " ".join(part for part in [name_text, website_text, extra] if part)

    for domain_hint in CORPORATE_DOMAIN_HINTS:
        if domain_hint not in website_text:
            continue
        if any(path_hint in website_text for path_hint in CORPORATE_PATH_HINTS):
            return f"Franchise or corporate landing page: {domain_hint}"
        return f"Franchise or chain website: {domain_hint}"

    for brand_keyword in FRANCHISE_BRAND_KEYWORDS:
        if brand_keyword in name_text or brand_keyword in website_text:
            return f"Franchise or chain brand: {brand_keyword}"

    if extra and any(copy_hint in combined for copy_hint in CORPORATE_COPY_HINTS):
        return "Franchise or corporate website markers detected"

    return None


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

    franchise_reason = franchise_or_corporate_reason(
        business_name=business.name,
        website=business.website,
    )
    if franchise_reason:
        return FilterResult("skip", franchise_reason)

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
