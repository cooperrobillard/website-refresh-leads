"""Super-light deterministic prefilter rules for early lead qualification."""

from __future__ import annotations

from dataclasses import dataclass

from app.models import Business


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
    "/shop",
    "/store",
    "/menu",
    "/order-online",
    "woocommerce",
    "bigcommerce",
    "squarespace.com/commerce",
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
    """Return True when the website URL suggests an obvious store/catalog flow."""
    site = normalize_text(website)
    return any(hint in site for hint in BLOCKED_WEBSITE_HINTS)


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
    """Classify a business with only high-confidence deterministic exclusions."""
    website = normalize_text(business.website)
    primary_type = normalize_text(business.primary_type)

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

    return FilterResult("strong", None)
