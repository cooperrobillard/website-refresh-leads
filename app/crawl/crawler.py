"""Lightweight crawl helpers for fetching and saving a few priority pages."""

from __future__ import annotations

from pathlib import Path

import requests
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session

from app.crawl.page_selector import extract_internal_candidate_links, normalize_url, pick_priority_pages
from app.models import Business, Page


HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; WebsiteRefreshLeadBot/0.1)"
}


def slugify(value: str) -> str:
    """Create a filesystem-friendly slug for a business name."""
    slug = "".join(c.lower() if c.isalnum() else "-" for c in value)
    parts = [part for part in slug.split("-") if part]
    return "-".join(parts) or "business"


def fetch_html(url: str, timeout: int = 20) -> str | None:
    """Fetch a page and return HTML content when the response is HTML."""
    try:
        response = requests.get(url, headers=HEADERS, timeout=timeout)
        response.raise_for_status()
        if "text/html" not in response.headers.get("Content-Type", ""):
            return None
        return response.text
    except Exception:
        return None


def parse_links(html: str) -> list[tuple[str, str]]:
    """Extract raw href/text pairs from HTML anchor tags."""
    soup = BeautifulSoup(html, "html.parser")
    links = []

    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        text = a.get_text(" ", strip=True)
        links.append((href, text))

    return links


def extract_text_and_title(html: str) -> tuple[str | None, str]:
    """Extract a title and lightweight body text from HTML."""
    soup = BeautifulSoup(html, "html.parser")

    title = None
    if soup.title and soup.title.string:
        title = soup.title.string.strip()

    # Remove obvious non-content tags before text extraction.
    for tag in soup(["script", "style", "noscript", "header", "footer"]):
        tag.decompose()

    parts = []

    for tag_name in ["h1", "h2", "h3", "p", "li"]:
        for tag in soup.find_all(tag_name):
            text = tag.get_text(" ", strip=True)
            if text:
                parts.append(text)

    raw_text = "\n".join(parts)
    return title, raw_text


def save_raw_html(business_name: str, page_type: str, html: str) -> str:
    """Persist raw HTML for a fetched page and return the saved path."""
    business_slug = slugify(business_name)
    folder = Path("data/raw") / business_slug
    folder.mkdir(parents=True, exist_ok=True)

    filename = f"{page_type}.html"
    path = folder / filename
    path.write_text(html, encoding="utf-8")

    return str(path)


def upsert_page(
    session: Session,
    business_id: int,
    page_type: str,
    url: str,
    title: str | None,
    raw_text: str,
    html_path: str | None,
) -> None:
    """Insert or update one crawled page record for a business."""
    existing = (
        session.query(Page)
        .filter(Page.business_id == business_id, Page.url == url)
        .first()
    )

    if existing:
        existing.page_type = page_type
        existing.title = title
        existing.raw_text = raw_text
        existing.html_path = html_path
    else:
        session.add(
            Page(
                business_id=business_id,
                page_type=page_type,
                url=url,
                title=title,
                raw_text=raw_text,
                html_path=html_path,
            )
        )


def crawl_business_site(session: Session, business: Business) -> dict[str, object]:
    """Fetch a homepage, pick priority internal pages, and persist crawl results."""
    if not business.website:
        return {"success": False, "reason": "No website"}

    home_url = normalize_url(business.website)
    home_html = fetch_html(home_url)

    if not home_html:
        return {"success": False, "reason": "Could not fetch homepage"}

    home_title, home_text = extract_text_and_title(home_html)
    home_html_path = save_raw_html(business.name, "home", home_html)

    upsert_page(
        session=session,
        business_id=business.id,
        page_type="home",
        url=home_url,
        title=home_title,
        raw_text=home_text,
        html_path=home_html_path,
    )

    hrefs = parse_links(home_html)
    candidates = extract_internal_candidate_links(home_url, hrefs)
    selected_pages = pick_priority_pages(home_url, candidates)

    fetched_count = 1

    for page_type, url in selected_pages.items():
        if page_type == "home":
            continue

        html = fetch_html(url)
        if not html:
            continue

        title, raw_text = extract_text_and_title(html)
        html_path = save_raw_html(business.name, page_type, html)

        upsert_page(
            session=session,
            business_id=business.id,
            page_type=page_type,
            url=url,
            title=title,
            raw_text=raw_text,
            html_path=html_path,
        )
        fetched_count += 1

    session.commit()

    return {
        "success": True,
        "reason": None,
        "pages_selected": selected_pages,
        "pages_fetched": fetched_count,
    }
