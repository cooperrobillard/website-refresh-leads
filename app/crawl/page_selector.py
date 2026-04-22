"""Helpers for picking a small set of priority pages from a business website."""

from __future__ import annotations

from urllib.parse import urljoin, urlparse, urlunparse


PAGE_KEYWORDS = {
    "about": ["about", "our-story", "who-we-are", "company"],
    "services": ["services", "service", "what-we-do"],
    "contact": ["contact", "estimate", "quote", "get-in-touch", "request-estimate"],
    "gallery": ["gallery", "projects", "project", "portfolio", "work", "before-after"],
    "faq": ["faq", "faqs", "questions"],
}

BLOCKED_PATH_HINTS = {
    "privacy",
    "privacy-policy",
    "terms",
    "terms-of-service",
    "terms-conditions",
    "legal",
    "cookies",
    "cookie-policy",
    "accessibility",
    "sitemap",
}

BLOCKED_EXTENSIONS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".svg",
    ".webp",
    ".pdf",
    ".doc",
    ".docx",
    ".xls",
    ".xlsx",
    ".zip",
}

PAGE_PRIORITY_ORDER = ["about", "services", "contact", "gallery", "faq"]


def normalize_url(url: str) -> str:
    """Normalize a URL for consistent comparison and storage."""
    parsed = urlparse(url)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc.lower()
    path = parsed.path.rstrip("/") or "/"
    normalized = parsed._replace(scheme=scheme, netloc=netloc, path=path, params="", query="", fragment="")
    return urlunparse(normalized)


def same_domain(url_a: str, url_b: str) -> bool:
    """Return True when both URLs belong to the same root host."""
    a = urlparse(url_a).netloc.lower().replace("www.", "")
    b = urlparse(url_b).netloc.lower().replace("www.", "")
    return a == b


def should_skip_link(url: str) -> bool:
    """Skip obvious non-content or utility URLs."""
    path = urlparse(url).path.lower()

    if any(path.endswith(ext) for ext in BLOCKED_EXTENSIONS):
        return True

    return any(hint in path for hint in BLOCKED_PATH_HINTS)


def classify_page_type(url: str, link_text: str = "") -> str | None:
    """Classify a page into one of the small MVP page types."""
    if should_skip_link(url):
        return None

    haystack = f"{url} {link_text}".lower()

    for page_type, keywords in PAGE_KEYWORDS.items():
        for keyword in keywords:
            if keyword in haystack:
                return page_type

    return None


def extract_internal_candidate_links(base_url: str, hrefs: list[tuple[str, str]]) -> list[dict[str, str | None]]:
    """Extract normalized same-domain candidate links from homepage anchors."""
    candidates: list[dict[str, str | None]] = []

    for href, link_text in hrefs:
        if not href:
            continue
        if href.startswith("mailto:") or href.startswith("tel:") or href.startswith("#"):
            continue

        absolute = urljoin(base_url, href)

        if not same_domain(base_url, absolute):
            continue

        normalized = normalize_url(absolute)
        if should_skip_link(normalized):
            continue

        page_type = classify_page_type(normalized, link_text)

        candidates.append(
            {
                "url": normalized,
                "link_text": (link_text or "").strip(),
                "page_type": page_type,
            }
        )

    return candidates


def pick_priority_pages(base_url: str, candidates: list[dict[str, str | None]]) -> dict[str, str]:
    """Return one best URL per priority page type when available."""
    selected: dict[str, str] = {"home": normalize_url(base_url)}

    seen_urls = set()
    deduped: list[dict[str, str | None]] = []
    for item in candidates:
        url = item["url"]
        if url in seen_urls:
            continue
        seen_urls.add(url)
        deduped.append(item)

    for page_type in PAGE_PRIORITY_ORDER:
        matches = [x for x in deduped if x["page_type"] == page_type]
        if matches:
            selected_url = matches[0]["url"]
            if isinstance(selected_url, str):
                selected[page_type] = selected_url

    return selected
