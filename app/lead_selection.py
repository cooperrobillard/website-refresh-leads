"""Helpers for durable website keys and simple business deduplication."""

from __future__ import annotations

from app.canonical_sites import canonical_website_key, normalize_website_url as canonical_normalize_website_url

from app.models import Business


def normalize_website_url(url: str | None) -> str | None:
    """Backwards-compatible wrapper around canonical URL normalization."""
    return canonical_normalize_website_url(url)


def normalized_website_key(url: str | None) -> str | None:
    """Backwards-compatible wrapper around canonical website key generation."""
    return canonical_website_key(url)


def dedupe_businesses_by_website(businesses: list[Business]) -> tuple[list[Business], int]:
    """Keep the first business seen for each normalized website key."""
    seen_keys: set[str] = set()
    deduped: list[Business] = []

    for business in businesses:
        website_key = business.canonical_key or normalized_website_key(business.website) or f"business:{business.id}"
        if website_key in seen_keys:
            continue

        seen_keys.add(website_key)
        deduped.append(business)

    return deduped, len(businesses) - len(deduped)
