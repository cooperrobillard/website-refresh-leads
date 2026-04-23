"""Helpers for durable canonical website normalization."""

from __future__ import annotations

from urllib.parse import urlparse, urlunparse

# Most sites are identified well enough by hostname alone, but some multi-tenant
# hosts encode the tenant/site identity in the first path segment(s).
PATH_BASED_HOST_SEGMENTS = {
    "certapro.com": 1,
    "sites.google.com": 2,
}


def _prepare_url(url: str | None) -> str | None:
    """Return a stripped URL string with a default scheme when needed."""
    if not url:
        return None

    raw_url = url.strip()
    if not raw_url:
        return None

    if "://" not in raw_url:
        raw_url = f"https://{raw_url}"

    return raw_url


def _normalize_host(raw_url: str) -> str | None:
    """Return the lowercase host with common presentation noise removed."""
    parsed = urlparse(raw_url)
    host = (parsed.hostname or "").strip().lower().rstrip(".")
    if not host:
        return None

    if host.startswith("www."):
        host = host[4:]

    return host or None


def _normalize_path(path: str) -> str:
    """Collapse repeated slashes and trim trailing slashes."""
    segments = [segment for segment in path.split("/") if segment]
    if not segments:
        return "/"
    return "/" + "/".join(segments)


def _canonical_path(host: str, path: str) -> str:
    """Return the path fragment that defines canonical site identity."""
    segments = [segment for segment in path.split("/") if segment]
    prefix_segment_count = PATH_BASED_HOST_SEGMENTS.get(host, 0)

    if prefix_segment_count <= 0 or not segments:
        return "/"

    return "/" + "/".join(segments[:prefix_segment_count])


def normalize_website_url(url: str | None) -> str | None:
    """Normalize a website URL for consistent storage and comparisons."""
    raw_url = _prepare_url(url)
    if not raw_url:
        return None

    parsed = urlparse(raw_url)
    host = _normalize_host(raw_url)
    if not host:
        return None

    normalized = parsed._replace(
        scheme="https",
        netloc=host,
        path=_normalize_path(parsed.path),
        params="",
        query="",
        fragment="",
    )
    return urlunparse(normalized)


def canonical_website_key(url: str | None) -> str | None:
    """Return the durable canonical key for a business website."""
    normalized = normalize_website_url(url)
    if not normalized:
        return None

    parsed = urlparse(normalized)
    host = parsed.netloc
    path = _canonical_path(host, parsed.path)

    if path == "/":
        return host

    return f"{host}{path}"


def canonical_website_url(url: str | None) -> str | None:
    """Return the normalized canonical URL that matches the canonical key."""
    normalized = normalize_website_url(url)
    if not normalized:
        return None

    parsed = urlparse(normalized)
    canonical_url = parsed._replace(path=_canonical_path(parsed.netloc, parsed.path))
    return urlunparse(canonical_url)
