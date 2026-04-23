"""Phase 7 deterministic scoring rubric for lead prioritization."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy.orm import Session

from app.crawl.page_selector import should_skip_link
from app.lead_selection import normalize_website_url
from app.models import Business, Note, Page, Score
from app.scoring.rules import primary_type_blocked, website_looks_blocked


WEIGHTS = {
    "business_legitimacy": 15,
    "website_weakness": 25,
    "conversion_opportunity": 20,
    "trust_packaging": 15,
    "complexity_fit": 15,
    "outreach_viability": 10,
}

STRONG_THRESHOLD = 75
MAYBE_THRESHOLD = 55
MIN_STRONG_LEGITIMACY = 12
MIN_MAYBE_LEGITIMACY = 8
MIN_STRONG_OPPORTUNITY = 18
MIN_REVIEWS_HARD_SKIP = 2


TESTIMONIAL_KEYWORDS = [
    "testimonial",
    "testimonials",
    "review",
    "reviews",
    "what our customers say",
    "what clients say",
]

GALLERY_KEYWORDS = [
    "gallery",
    "project",
    "projects",
    "portfolio",
    "before and after",
    "before-after",
]

HIGH_UPDATE_HINTS = [
    "/blog",
    "/news",
    "/events",
    "/menu",
    "/order-online",
    "/products",
    "/product",
    "/collections",
    "/cart",
    "/checkout",
]

GENERIC_COPY_HINTS = [
    "welcome",
    "quality service",
    "professional service",
    "we are here to help",
    "learn more",
    "contact us today",
]

LEGACY_URL_HINTS = [
    ".aspx",
    ".asp",
    ".php.html",
]

CAMPAIGN_URL_HINTS = [
    "landing-page",
    "gbp-landing",
]


@dataclass
class ScoringResult:
    """Final scoring and note payload for one business."""

    fit_status: str
    skip_reason: str | None
    scores: dict[str, int]
    total_score: int
    confidence: str
    top_issues: list[str]
    quick_summary: str
    teardown_angle: str


def slugify(value: str) -> str:
    """Create a filesystem-friendly slug for report lookups."""
    slug = "".join(c.lower() if c.isalnum() else "-" for c in value)
    parts = [part for part in slug.split("-") if part]
    return "-".join(parts) or "business"


def clamp(value: int, low: int, high: int) -> int:
    """Clamp a score into the allowed range for a dimension."""
    return max(low, min(high, value))


def load_browser_report(business: Business) -> dict[str, object]:
    """Load a saved browser-check report for a business when available."""
    path = Path("data/browser_checks") / f"{slugify(business.name)}.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def get_pages(session: Session, business: Business) -> dict[str, Page]:
    """Return one saved page per page type for a business."""
    rows = (
        session.query(Page)
        .filter(Page.business_id == business.id)
        .order_by(Page.id.asc())
        .all()
    )
    page_map: dict[str, Page] = {}

    for row in rows:
        if row.page_type != "home" and row.url and should_skip_link(row.url):
            continue
        if row.page_type and row.page_type not in page_map:
            page_map[row.page_type] = row

    return page_map


def report_homepage_url(report: dict[str, object]) -> str | None:
    """Return the normalized homepage URL stored in the browser report."""
    homepage_url = report.get("homepage_url")
    if isinstance(homepage_url, str):
        return normalize_website_url(homepage_url)
    return None


def get_homepage_url(
    business: Business,
    page_map: dict[str, Page],
    report: dict[str, object],
) -> str | None:
    """Return the best homepage URL available from crawl, browser checks, or discovery."""
    home_page = page_map.get("home")
    if home_page and home_page.url:
        return normalize_website_url(home_page.url)

    return report_homepage_url(report) or normalize_website_url(business.website)


def homepage_loaded(report: dict[str, object]) -> bool:
    """Return True when the browser report indicates the homepage was reachable."""
    signals = report.get("homepage_signals", {})
    page_loads = report.get("page_loads", {})

    return bool(
        report.get("success") is True
        and (
            signals.get("homepage_loaded", False)
            or page_loads.get("home", False)
        )
    )


def has_homepage_evidence(
    business: Business,
    page_map: dict[str, Page],
    report: dict[str, object],
) -> bool:
    """Return True when the business has crawl or browser evidence for its homepage."""
    if "home" in page_map:
        return True

    return bool(get_homepage_url(business, page_map, report) and homepage_loaded(report))


def combined_text(page_map: dict[str, Page]) -> str:
    """Combine crawled page text into a single lowercased blob."""
    parts = []
    for page in page_map.values():
        if page.raw_text:
            parts.append(page.raw_text.lower())
    return "\n".join(parts)


def homepage_text(page_map: dict[str, Page]) -> str:
    """Return lowercased homepage text when available."""
    home = page_map.get("home")
    if home and home.raw_text:
        return home.raw_text.lower()
    return ""


def has_any(text: str, keywords: list[str]) -> bool:
    """Return True when any keyword appears in the text."""
    return any(keyword in text for keyword in keywords)


def count_relevant_pages(page_map: dict[str, Page]) -> int:
    """Count the small set of page types used by the rubric."""
    return sum(1 for key in ["home", "about", "services", "contact", "gallery", "faq"] if key in page_map)


def known_urls(
    business: Business,
    page_map: dict[str, Page],
    report: dict[str, object],
) -> list[str]:
    """Return the best-known URLs for this business across crawl and browser evidence."""
    urls = [page.url for page in page_map.values() if page.url]
    homepage_url = get_homepage_url(business, page_map, report)
    if homepage_url:
        urls.append(homepage_url)

    seen_urls: set[str] = set()
    deduped_urls: list[str] = []
    for url in urls:
        normalized = normalize_website_url(url)
        if not normalized or normalized in seen_urls:
            continue
        seen_urls.add(normalized)
        deduped_urls.append(normalized)

    return deduped_urls


def opportunity_score(scores: dict[str, int]) -> int:
    """Return the combined redesign-opportunity subtotal."""
    return (
        scores["website_weakness"]
        + scores["conversion_opportunity"]
        + scores["trust_packaging"]
    )


def is_google_sites_site(
    business: Business,
    page_map: dict[str, Page],
    report: dict[str, object],
) -> bool:
    """Return True when the site appears to be hosted on Google Sites."""
    homepage_url = get_homepage_url(business, page_map, report)
    return bool(homepage_url and "sites.google.com" in homepage_url)


def has_legacy_url_pattern(
    business: Business,
    page_map: dict[str, Page],
    report: dict[str, object],
) -> bool:
    """Return True when the site uses obviously legacy URL patterns."""
    urls = " ".join(known_urls(business, page_map, report)).lower()
    return any(hint in urls for hint in LEGACY_URL_HINTS)


def is_campaign_landing_page(
    business: Business,
    page_map: dict[str, Page],
    report: dict[str, object],
) -> bool:
    """Return True when the homepage looks like a campaign or franchise landing page."""
    homepage_url = get_homepage_url(business, page_map, report)
    if not homepage_url:
        return False
    lowered = homepage_url.lower()
    return any(hint in lowered for hint in CAMPAIGN_URL_HINTS)


def detect_hard_skip(
    business: Business,
    page_map: dict[str, Page],
    report: dict[str, object],
) -> str | None:
    """Return a hard-skip reason when the business is clearly out of scope."""
    if not business.website:
        return "No real website"

    if primary_type_blocked(business.primary_type):
        return f"Business type mismatched to offer: {business.primary_type}"

    if website_looks_blocked(business.website):
        return "Website pattern suggests ecommerce or high-update model"

    review_count = business.review_count or 0
    if review_count <= MIN_REVIEWS_HARD_SKIP:
        return f"Weak legitimacy / too few reviews: {review_count}"

    urls = " ".join(url.lower() for url in known_urls(business, page_map, report))
    if has_any(urls, HIGH_UPDATE_HINTS):
        return "Frequent-content-update or ecommerce-heavy site"

    if not has_homepage_evidence(business, page_map, report):
        return "No crawlable homepage found"

    return None


def score_business_legitimacy(
    business: Business,
    page_map: dict[str, Page],
    report: dict[str, object],
) -> int:
    """Score how clearly this looks like a real local service business."""
    score = 0

    if business.website:
        score += 3

    if business.address:
        score += 2

    review_count = business.review_count or 0
    rating = business.rating or 0.0

    if review_count >= 8:
        score += 4
    if review_count >= 20:
        score += 2
    if review_count >= 50:
        score += 2

    if rating >= 4.0:
        score += 2
    if rating >= 4.5:
        score += 1

    if count_relevant_pages(page_map) >= 3:
        score += 2
    elif homepage_loaded(report):
        score += 1

    return clamp(score, 0, WEIGHTS["business_legitimacy"])


def score_website_weakness(
    business: Business,
    page_map: dict[str, Page],
    report: dict[str, object],
) -> int:
    """Score how much obvious website-refresh opportunity is present."""
    score = 0
    home_text = homepage_text(page_map)
    full_text = combined_text(page_map)
    relevant_pages = count_relevant_pages(page_map)

    if "services" not in page_map:
        score += 5
    if "about" not in page_map:
        score += 3
    if "gallery" not in page_map:
        score += 3
    if "faq" not in page_map:
        score += 2

    if len(home_text) < 400:
        score += 6
    elif len(home_text) < 700:
        score += 3

    if len(full_text) < 1200:
        score += 4

    if has_any(home_text, GENERIC_COPY_HINTS):
        score += 2

    if relevant_pages <= 2:
        score += 4
    elif relevant_pages == 3:
        score += 2

    if is_google_sites_site(business, page_map, report):
        score += 6

    if has_legacy_url_pattern(business, page_map, report):
        score += 3

    signals = report.get("homepage_signals", {})
    if signals and not signals.get("homepage_loaded", False):
        score += 4

    return clamp(score, 0, WEIGHTS["website_weakness"])


def score_conversion_opportunity(page_map: dict[str, Page], report: dict) -> int:
    """Score missing or weak conversion paths."""
    score = 0
    signals = report.get("homepage_signals", {})

    if "contact" not in page_map:
        score += 6

    if not signals.get("phone_visible", False) and not signals.get("tel_link_present", False):
        score += 7

    if not signals.get("cta_visible_near_top", False):
        score += 7

    return clamp(score, 0, WEIGHTS["conversion_opportunity"])


def score_trust_packaging(
    business: Business,
    page_map: dict[str, Page],
    report: dict[str, object],
) -> int:
    """Score missing trust and proof elements that help win local work."""
    score = 0
    text = combined_text(page_map)
    home_text = homepage_text(page_map)

    if not has_any(text, TESTIMONIAL_KEYWORDS):
        score += 5

    if "gallery" not in page_map and not has_any(text, GALLERY_KEYWORDS):
        score += 5

    if business.address:
        city_hint = business.address.split(",")[0].lower()
        if city_hint and city_hint not in home_text:
            score += 2

    if "about" not in page_map:
        score += 3

    if count_relevant_pages(page_map) <= 2:
        score += 2

    if is_google_sites_site(business, page_map, report):
        score += 1

    return clamp(score, 0, WEIGHTS["trust_packaging"])


def score_complexity_fit(
    business: Business,
    page_map: dict[str, Page],
    report: dict[str, object],
) -> int:
    """Score how manageable the site looks for a practical refresh project."""
    score = 0
    urls = " ".join(url.lower() for url in known_urls(business, page_map, report))
    relevant_count = count_relevant_pages(page_map)

    if relevant_count >= 3:
        score += 5

    if relevant_count <= 6:
        score += 5
    elif relevant_count <= 8:
        score += 3

    if not has_any(urls, HIGH_UPDATE_HINTS):
        score += 5

    if is_campaign_landing_page(business, page_map, report):
        score -= 8

    return clamp(score, 0, WEIGHTS["complexity_fit"])


def score_outreach_viability(business: Business, page_map: dict[str, Page], report: dict) -> int:
    """Score how reachable and credible the business looks for outreach."""
    score = 0
    signals = report.get("homepage_signals", {})
    page_loads = report.get("page_loads", {})

    if "contact" in page_map:
        score += 4

    if signals.get("phone_visible", False) or signals.get("tel_link_present", False):
        score += 2

    if business.review_count and business.review_count >= 8:
        score += 2

    if business.address:
        score += 1

    if page_loads.get("home", False):
        score += 1

    return clamp(score, 0, WEIGHTS["outreach_viability"])


def build_top_issues(
    business: Business,
    page_map: dict[str, Page],
    report: dict[str, object],
) -> list[str]:
    """Build a short list of concrete site issues to mention in notes."""
    issues = []
    signals = report.get("homepage_signals", {})
    text = combined_text(page_map)

    if not signals.get("cta_visible_near_top", False):
        issues.append("Homepage CTA is weak or buried.")

    if not signals.get("phone_visible", False) and not signals.get("tel_link_present", False):
        issues.append("Phone/contact path is not obvious.")

    if "services" not in page_map:
        issues.append("Services are not clearly packaged on the site.")

    if not has_any(text, TESTIMONIAL_KEYWORDS):
        issues.append("Trust signals and testimonials are limited.")

    if "gallery" not in page_map and not has_any(text, GALLERY_KEYWORDS):
        issues.append("Project proof is underused or hard to find.")

    if count_relevant_pages(page_map) <= 2:
        issues.append("Site structure is thin and key pages are limited.")

    if is_google_sites_site(business, page_map, report) or has_legacy_url_pattern(business, page_map, report):
        issues.append("Site appears to run on a legacy or lower-polish platform.")

    if not issues:
        issues.append("Website looks usable, but the redesign opportunity is less obvious.")

    return issues[:3]


def build_quick_summary(business: Business, fit_status: str, total_score: int) -> str:
    """Build a short summary that matches the final classification."""
    reviews = business.review_count or 0
    rating = business.rating or 0
    if fit_status == "strong":
        tail = "the business looks legitimate and the site shows clear redesign and conversion upside."
    elif fit_status == "maybe":
        tail = "the business looks legitimate and the site shows some refresh opportunity, but the case is less decisive."
    else:
        tail = "the business looks legitimate, but the current evidence suggests limited redesign or outreach upside."

    return f"{business.name} is a {fit_status} lead with a total score of {total_score}. It has {reviews} reviews, a {rating} rating, and {tail}"


def build_teardown_angle(fit_status: str, top_issues: list[str]) -> str:
    """Build a short teardown framing line for notes."""
    if fit_status == "skip":
        return "Lower-priority lead unless stronger conversion or trust gaps are found later."
    if not top_issues:
        return "Focus on a cleaner first impression, stronger contact flow, and clearer trust signals."
    return "Focus the teardown on: " + " ".join(top_issues[:3])


def classify_total_score(total_score: int, scores: dict[str, int]) -> str:
    """Map the numeric score into strong, maybe, or skip."""
    legitimacy = scores["business_legitimacy"]
    redesign_opportunity = opportunity_score(scores)

    if (
        total_score >= STRONG_THRESHOLD
        and legitimacy >= MIN_STRONG_LEGITIMACY
        and redesign_opportunity >= MIN_STRONG_OPPORTUNITY
    ):
        return "strong"

    if total_score >= MAYBE_THRESHOLD and legitimacy >= MIN_MAYBE_LEGITIMACY:
        return "maybe"

    return "skip"


def evaluate_business(session: Session, business: Business) -> ScoringResult:
    """Evaluate one business using the current deterministic rubric."""
    page_map = get_pages(session, business)
    report = load_browser_report(business)

    hard_skip_reason = detect_hard_skip(business, page_map, report)
    if hard_skip_reason:
        return ScoringResult(
            fit_status="skip",
            skip_reason=hard_skip_reason,
            scores={key: 0 for key in WEIGHTS},
            total_score=0,
            confidence="high",
            top_issues=[hard_skip_reason],
            quick_summary=f"{business.name} is a skip lead. {hard_skip_reason}.",
            teardown_angle="Do not prioritize outreach.",
        )

    scores = {
        "business_legitimacy": score_business_legitimacy(business, page_map, report),
        "website_weakness": score_website_weakness(business, page_map, report),
        "conversion_opportunity": score_conversion_opportunity(page_map, report),
        "trust_packaging": score_trust_packaging(business, page_map, report),
        "complexity_fit": score_complexity_fit(business, page_map, report),
        "outreach_viability": score_outreach_viability(business, page_map, report),
    }

    total_score = sum(scores.values())
    fit_status = classify_total_score(total_score, scores)
    top_issues = build_top_issues(business, page_map, report)

    confidence = "medium"
    if not page_map and homepage_loaded(report):
        confidence = "low"
    elif "home" not in page_map and has_homepage_evidence(business, page_map, report):
        confidence = "low"
    elif count_relevant_pages(page_map) >= 4 and report.get("success") is True:
        confidence = "high"
    elif count_relevant_pages(page_map) <= 2 or report.get("success") is False:
        confidence = "low"

    return ScoringResult(
        fit_status=fit_status,
        skip_reason=None if fit_status != "skip" else f"Score below review threshold: {total_score}",
        scores=scores,
        total_score=total_score,
        confidence=confidence,
        top_issues=top_issues,
        quick_summary=build_quick_summary(business, fit_status, total_score),
        teardown_angle=build_teardown_angle(fit_status, top_issues),
    )


def upsert_score_and_note(session: Session, business: Business, result: ScoringResult) -> None:
    """Upsert score and note rows, then sync the business classification."""
    score_row = session.query(Score).filter(Score.business_id == business.id).first()
    if not score_row:
        score_row = Score(business_id=business.id)
        session.add(score_row)

    score_row.business_legitimacy = result.scores["business_legitimacy"]
    score_row.website_weakness = result.scores["website_weakness"]
    score_row.conversion_opportunity = result.scores["conversion_opportunity"]
    score_row.trust_packaging = result.scores["trust_packaging"]
    score_row.complexity_fit = result.scores["complexity_fit"]
    score_row.outreach_viability = result.scores["outreach_viability"]
    score_row.total_score = result.total_score
    score_row.fit_status = result.fit_status
    score_row.confidence = result.confidence

    note_row = session.query(Note).filter(Note.business_id == business.id).first()
    if not note_row:
        note_row = Note(business_id=business.id)
        session.add(note_row)

    note_row.quick_summary = result.quick_summary
    note_row.top_issues = "\n".join(result.top_issues)
    note_row.teardown_angle = result.teardown_angle

    business.fit_status = result.fit_status
    business.skip_reason = result.skip_reason
