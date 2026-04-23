"""Helpers for canonical website keys and simple business deduplication."""

from __future__ import annotations

from urllib.parse import urlparse, urlunparse

from app.models import Business

MULTI_TENANT_HOSTS = {
    "sites.google.com",
    "certapro.com",
}


def normalize_website_url(url: str | None) -> str | None:
    """Normalize a website URL for consistent comparison and storage."""
    if not url:
        return None

    raw_url = url.strip()
    if not raw_url:
        return None

    if "://" not in raw_url:
        raw_url = f"https://{raw_url}"

    parsed = urlparse(raw_url)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc.lower()
    path = parsed.path.rstrip("/") or "/"

    if netloc.startswith("www."):
        netloc = netloc[4:]

    normalized = parsed._replace(
        scheme=scheme,
        netloc=netloc,
        path=path,
        params="",
        query="",
        fragment="",
    )
    return urlunparse(normalized)


def normalized_website_key(url: str | None) -> str | None:
    """Return a practical dedupe key for a business website."""
    normalized = normalize_website_url(url)
    if not normalized:
        return None

    parsed = urlparse(normalized)
    netloc = parsed.netloc
    path = parsed.path.rstrip("/") or "/"

    if netloc in MULTI_TENANT_HOSTS:
        return f"{netloc}{path}"

    return netloc


def dedupe_businesses_by_website(businesses: list[Business]) -> tuple[list[Business], int]:
    """Keep the first business seen for each normalized website key."""
    seen_keys: set[str] = set()
    deduped: list[Business] = []

    for business in businesses:
        website_key = normalized_website_key(business.website) or f"business:{business.id}"
        if website_key in seen_keys:
            continue

        seen_keys.add(website_key)
        deduped.append(business)

    return deduped, len(businesses) - len(deduped)
