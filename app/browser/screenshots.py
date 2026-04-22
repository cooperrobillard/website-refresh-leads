"""Homepage screenshot capture helpers."""

from __future__ import annotations

from pathlib import Path

from playwright.sync_api import sync_playwright
from sqlalchemy.orm import Session

from app.models import Artifact, Business, Page


def slugify(value: str) -> str:
    """Create a filesystem-friendly slug for a business name."""
    slug = "".join(c.lower() if c.isalnum() else "-" for c in value)
    parts = [part for part in slug.split("-") if part]
    return "-".join(parts) or "business"


def get_homepage_url(session: Session, business: Business) -> str | None:
    """Prefer the saved home page URL, then fall back to the business website."""
    page = (
        session.query(Page)
        .filter(Page.business_id == business.id, Page.page_type == "home")
        .order_by(Page.id.asc())
        .first()
    )
    if page and page.url:
        return page.url
    return business.website


def upsert_artifact(session: Session, business_id: int, artifact_type: str, file_path: str) -> None:
    """Insert or update a screenshot artifact path for a business."""
    existing = (
        session.query(Artifact)
        .filter(
            Artifact.business_id == business_id,
            Artifact.artifact_type == artifact_type,
        )
        .first()
    )

    if existing:
        existing.file_path = file_path
    else:
        session.add(
            Artifact(
                business_id=business_id,
                artifact_type=artifact_type,
                file_path=file_path,
            )
        )


def capture_screenshot(url: str, output_path: str, mobile: bool = False) -> bool:
    """Capture a single screenshot in either desktop or mobile mode."""
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                if mobile:
                    context = browser.new_context(
                        viewport={"width": 390, "height": 844},
                        is_mobile=True,
                        device_scale_factor=2,
                    )
                else:
                    context = browser.new_context(
                        viewport={"width": 1440, "height": 900},
                        device_scale_factor=1,
                    )

                try:
                    page = context.new_page()
                    page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_timeout(1500)
                    page.screenshot(path=str(output_file), full_page=True)
                finally:
                    context.close()
            finally:
                browser.close()
        return True
    except Exception:
        return False


def capture_homepage_screenshots(session: Session, business: Business) -> dict[str, object]:
    """Capture desktop and mobile homepage screenshots and upsert artifacts."""
    home_url = get_homepage_url(session, business)
    if not home_url:
        return {"success": False, "reason": "No homepage URL"}

    business_slug = slugify(business.name)
    folder = Path("data/screenshots") / business_slug
    folder.mkdir(parents=True, exist_ok=True)

    desktop_path = (folder / "home_desktop.png").as_posix()
    mobile_path = (folder / "home_mobile.png").as_posix()

    desktop_ok = capture_screenshot(home_url, desktop_path, mobile=False)
    mobile_ok = capture_screenshot(home_url, mobile_path, mobile=True)

    if desktop_ok:
        upsert_artifact(session, business.id, "desktop_home_screenshot", desktop_path)

    if mobile_ok:
        upsert_artifact(session, business.id, "mobile_home_screenshot", mobile_path)

    if desktop_ok or mobile_ok:
        session.commit()

    return {
        "success": desktop_ok or mobile_ok,
        "reason": None if (desktop_ok or mobile_ok) else "Could not capture screenshots",
        "homepage_url": home_url,
        "desktop_ok": desktop_ok,
        "mobile_ok": mobile_ok,
        "desktop_path": desktop_path if desktop_ok else None,
        "mobile_path": mobile_path if mobile_ok else None,
    }
