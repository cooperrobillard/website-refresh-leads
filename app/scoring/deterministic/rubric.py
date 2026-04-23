"""Phase 7 deterministic scoring rubric for lead prioritization."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy.orm import Session

from app.crawl.page_selector import should_skip_link
from app.lead_selection import normalize_website_url
from app.models import Business, Note, Page, Score
from app.scoring.deterministic.rules import (
    franchise_or_corporate_reason,
    primary_type_blocked,
    website_looks_blocked,
)


WEIGHTS = {
    "business_legitimacy": 15,
    "website_weakness": 18,
    "conversion_opportunity": 16,
    "trust_packaging": 12,
    "complexity_fit": 12,
    "outreach_viability": 12,
    "outreach_story_strength": 15,
}

STRONG_THRESHOLD = 75
MAYBE_THRESHOLD = 55
MIN_STRONG_LEGITIMACY = 12
MIN_MAYBE_LEGITIMACY = 8
MIN_STRONG_OPPORTUNITY = 16
MIN_STRONG_STORY = 9
MIN_MAYBE_STORY = 5
MIN_REVIEWS_HARD_SKIP = 2

EVIDENCE_CAPS = {
    "strong": 100,
    "medium": 72,
    "sparse": 60,
    "minimal": 48,
}

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

VALUE_PROPOSITION_HINTS = [
    "residential",
    "commercial",
    "free estimate",
    "licensed",
    "insured",
    "family owned",
    "locally owned",
    "serving",
    "professional",
    "contractor",
    "services",
]

SERVICE_STRUCTURE_HINTS = [
    "services",
    "our services",
    "what we do",
    "how we help",
    "service area",
]

SECTION_HINTS = [
    "about",
    "services",
    "testimonials",
    "reviews",
    "contact",
    "get started",
    "estimate",
    "why choose",
    "our process",
]

GAP_LABELS = {
    "weak_contact_path": "Phone/contact path is not obvious.",
    "missing_service_story": "Services are not clearly packaged on the site.",
    "thin_trust_signals": "Trust signals and testimonials are limited.",
    "thin_structure": "Site structure is thin and key pages are limited.",
    "thin_home_copy": "Homepage copy is thin and the value proposition is weak.",
    "dated_platform": "Site appears to run on a legacy or lower-polish platform.",
}


@dataclass
class EvidenceAssessment:
    """Evidence quality summary used to cap scores and set confidence."""

    tier: str
    cap: int
    confidence: str


@dataclass
class ScoringResult:
    """Final scoring and note payload for one business."""

    fit_status: str
    skip_reason: str | None
    scores: dict[str, int]
    raw_total_score: int
    total_score: int
    evidence_tier: str
    evidence_cap: int
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


def home_excerpt(page_map: dict[str, Page], limit: int = 800) -> str:
    """Return the first chunk of homepage text for lightweight signal checks."""
    return homepage_text(page_map)[:limit]


def has_any(text: str, keywords: list[str]) -> bool:
    """Return True when any keyword appears in the text."""
    return any(keyword in text for keyword in keywords)


def count_relevant_pages(page_map: dict[str, Page]) -> int:
    """Count the small set of page types used by the rubric."""
    return sum(1 for key in ["home", "about", "services", "contact", "gallery", "faq"] if key in page_map)


def count_pages_with_text(page_map: dict[str, Page], minimum_length: int = 200) -> int:
    """Count pages that have enough saved text to be useful evidence."""
    return sum(1 for page in page_map.values() if page.raw_text and len(page.raw_text.strip()) >= minimum_length)


def total_text_chars(page_map: dict[str, Page]) -> int:
    """Return the total amount of saved crawl text across pages."""
    return sum(len(page.raw_text.strip()) for page in page_map.values() if page.raw_text)


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


def _primary_type_hint(business: Business) -> str:
    """Return the primary type in a readable lowercased form."""
    return (business.primary_type or "").replace("_", " ").strip().lower()


def functional_site_signals(
    business: Business,
    page_map: dict[str, Page],
    report: dict[str, object],
) -> dict[str, bool]:
    """Return a compact set of brochure-site quality signals."""
    home_text = homepage_text(page_map)
    excerpt = home_excerpt(page_map)
    full_text = combined_text(page_map)
    browser_signals = report.get("homepage_signals", {})
    primary_type_hint = _primary_type_hint(business)
    section_hits = sum(1 for hint in SECTION_HINTS if hint in home_text)

    clear_value_prop = bool(
        len(excerpt) >= 220
        and (
            (primary_type_hint and primary_type_hint in excerpt)
            or has_any(excerpt, VALUE_PROPOSITION_HINTS)
        )
    )
    clear_contact_path = bool(
        "contact" in page_map
        or browser_signals.get("phone_visible", False)
        or browser_signals.get("tel_link_present", False)
        or browser_signals.get("cta_visible_near_top", False)
    )
    organized_homepage = bool(
        count_relevant_pages(page_map) >= 4
        or section_hits >= 4
        or (section_hits >= 3 and len(home_text) >= 1100)
    )
    service_structure = bool(
        "services" in page_map
        or has_any(full_text, SERVICE_STRUCTURE_HINTS)
    )
    review_signals = has_any(full_text, TESTIMONIAL_KEYWORDS)
    project_proof = bool("gallery" in page_map or has_any(full_text, GALLERY_KEYWORDS))
    trust_signals = bool(review_signals or project_proof or "about" in page_map)
    brochure_structure = bool(
        count_relevant_pages(page_map) >= 4
        or sum(1 for key in ["about", "services", "contact"] if key in page_map) >= 2
    )

    return {
        "clear_value_prop": clear_value_prop,
        "clear_contact_path": clear_contact_path,
        "organized_homepage": organized_homepage,
        "service_structure": service_structure,
        "review_signals": review_signals,
        "project_proof": project_proof,
        "trust_signals": trust_signals,
        "brochure_structure": brochure_structure,
    }


def legacy_brochure_signals(
    business: Business,
    page_map: dict[str, Page],
    report: dict[str, object],
) -> dict[str, bool]:
    """Return narrow signals for dated but legitimate brochure-style sites."""
    basics = functional_site_signals(business, page_map, report)
    home_text = homepage_text(page_map)
    full_text = combined_text(page_map)
    relevant_pages = count_relevant_pages(page_map)
    useful_pages = count_pages_with_text(page_map)
    review_count = business.review_count or 0

    sparse_brochure_coverage = bool(
        relevant_pages <= 3
        and useful_pages <= 3
        and len(full_text) < 2200
    )
    text_heavy_homepage = bool(
        len(home_text) >= 600
        and relevant_pages <= 3
        and len(full_text) < 2200
        and not basics["organized_homepage"]
    )
    trust_packaging_gap = bool(review_count >= 8 and not basics["trust_signals"])
    dated_brochure_opportunity = bool(
        basics["clear_contact_path"]
        and basics["service_structure"]
        and trust_packaging_gap
        and (text_heavy_homepage or sparse_brochure_coverage)
    )

    return {
        "sparse_brochure_coverage": sparse_brochure_coverage,
        "text_heavy_homepage": text_heavy_homepage,
        "trust_packaging_gap": trust_packaging_gap,
        "dated_brochure_opportunity": dated_brochure_opportunity,
    }


def story_gap_labels(
    business: Business,
    page_map: dict[str, Page],
    report: dict[str, object],
) -> list[str]:
    """Return the concrete story gaps that make outreach persuasive."""
    basics = functional_site_signals(business, page_map, report)
    gaps: list[str] = []

    if not basics["clear_contact_path"]:
        gaps.append("weak_contact_path")
    if not basics["service_structure"]:
        gaps.append("missing_service_story")
    if not basics["trust_signals"]:
        gaps.append("thin_trust_signals")
    if not basics["brochure_structure"]:
        gaps.append("thin_structure")
    if len(homepage_text(page_map)) < 400:
        gaps.append("thin_home_copy")
    if is_google_sites_site(business, page_map, report) or has_legacy_url_pattern(business, page_map, report):
        gaps.append("dated_platform")

    return gaps


def assess_evidence_quality(
    business: Business,
    page_map: dict[str, Page],
    report: dict[str, object],
) -> EvidenceAssessment:
    """Assess evidence quality and return the deterministic score cap."""
    browser_success = report.get("success") is True and homepage_loaded(report)
    home_page = page_map.get("home")
    has_saved_home = bool(home_page and home_page.raw_text)
    relevant_pages = count_relevant_pages(page_map)
    useful_pages = count_pages_with_text(page_map)
    text_chars = total_text_chars(page_map)

    if browser_success and has_saved_home and relevant_pages >= 4 and useful_pages >= 3 and text_chars >= 2500:
        return EvidenceAssessment(tier="strong", cap=EVIDENCE_CAPS["strong"], confidence="high")

    if browser_success and has_saved_home and relevant_pages >= 3 and useful_pages >= 2 and text_chars >= 1200:
        return EvidenceAssessment(tier="medium", cap=EVIDENCE_CAPS["medium"], confidence="medium")

    if (
        has_homepage_evidence(business, page_map, report)
        and (has_saved_home or browser_success)
        and useful_pages >= 1
        and text_chars >= 350
    ):
        return EvidenceAssessment(tier="sparse", cap=EVIDENCE_CAPS["sparse"], confidence="low")

    return EvidenceAssessment(tier="minimal", cap=EVIDENCE_CAPS["minimal"], confidence="low")


def detect_hard_skip(
    business: Business,
    page_map: dict[str, Page],
    report: dict[str, object],
) -> str | None:
    """Return a hard-skip reason when the business is clearly out of scope."""
    if not business.website:
        return "No real website"

    franchise_reason = franchise_or_corporate_reason(
        business_name=business.name,
        website=" ".join(known_urls(business, page_map, report)) or business.website,
        extra_text=combined_text(page_map),
    )
    if franchise_reason:
        return franchise_reason

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

    if is_campaign_landing_page(business, page_map, report):
        return "Corporate or campaign landing page"

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
        score += 1
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
    basics = functional_site_signals(business, page_map, report)
    legacy = legacy_brochure_signals(business, page_map, report)
    browser_signals = report.get("homepage_signals", {})

    if not basics["service_structure"]:
        score += 4
    if "about" not in page_map:
        score += 2
    if not basics["project_proof"]:
        score += 2
    if "faq" not in page_map:
        score += 1

    if len(home_text) < 350:
        score += 4
    elif len(home_text) < 650:
        score += 2

    if len(full_text) < 1400:
        score += 3

    if has_any(home_text, GENERIC_COPY_HINTS):
        score += 2

    if relevant_pages <= 2:
        score += 3
    elif relevant_pages == 3:
        score += 1

    if legacy["text_heavy_homepage"]:
        score += 3

    if legacy["sparse_brochure_coverage"]:
        score += 2

    if legacy["trust_packaging_gap"]:
        score += 2

    if is_google_sites_site(business, page_map, report):
        score += 5

    if has_legacy_url_pattern(business, page_map, report):
        score += 2

    if browser_signals and not browser_signals.get("homepage_loaded", False):
        score += 3

    credits = 0
    if basics["clear_value_prop"]:
        credits += 3
    if basics["clear_contact_path"]:
        credits += 2
    if basics["organized_homepage"]:
        credits += 2
    if basics["service_structure"]:
        credits += 3
    if basics["trust_signals"]:
        credits += 2
    if basics["brochure_structure"]:
        credits += 1 if legacy["sparse_brochure_coverage"] else 3

    score -= credits
    return clamp(score, 0, WEIGHTS["website_weakness"])


def score_conversion_opportunity(
    business: Business,
    page_map: dict[str, Page],
    report: dict[str, object],
) -> int:
    """Score missing or weak conversion paths."""
    score = 0
    basics = functional_site_signals(business, page_map, report)
    browser_signals = report.get("homepage_signals", {})

    if "contact" not in page_map:
        score += 5

    if not browser_signals.get("phone_visible", False) and not browser_signals.get("tel_link_present", False):
        score += 5

    if not browser_signals.get("cta_visible_near_top", False):
        score += 6

    if basics["clear_contact_path"]:
        score -= 4

    if basics["clear_value_prop"] and basics["service_structure"]:
        score -= 1

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
    relevant_pages = count_relevant_pages(page_map)
    basics = functional_site_signals(business, page_map, report)

    if not basics["review_signals"]:
        score += 4

    if not basics["project_proof"]:
        score += 4

    if business.address:
        city_hint = business.address.split(",")[0].lower()
        if city_hint and city_hint not in home_text:
            score += 1

    if "about" not in page_map:
        score += 2

    if relevant_pages <= 2:
        score += 2

    if is_google_sites_site(business, page_map, report):
        score += 1

    if basics["trust_signals"]:
        score -= 2

    if basics["brochure_structure"]:
        score -= 1

    if has_any(text, TESTIMONIAL_KEYWORDS) and has_any(text, GALLERY_KEYWORDS):
        score -= 1

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
    basics = functional_site_signals(business, page_map, report)

    if relevant_count >= 3:
        score += 4

    if relevant_count <= 6:
        score += 4
    elif relevant_count <= 8:
        score += 2

    if not has_any(urls, HIGH_UPDATE_HINTS):
        score += 3

    if basics["brochure_structure"]:
        score += 1

    if is_campaign_landing_page(business, page_map, report):
        score -= 8

    return clamp(score, 0, WEIGHTS["complexity_fit"])


def score_outreach_viability(
    business: Business,
    page_map: dict[str, Page],
    report: dict[str, object],
) -> int:
    """Score how reachable and credible the business looks for outreach."""
    score = 0
    browser_signals = report.get("homepage_signals", {})
    page_loads = report.get("page_loads", {})

    if "contact" in page_map:
        score += 4

    if browser_signals.get("phone_visible", False) or browser_signals.get("tel_link_present", False):
        score += 3

    if business.review_count and business.review_count >= 8:
        score += 2

    if business.review_count and business.review_count >= 20:
        score += 1

    if business.address:
        score += 1

    if page_loads.get("home", False) or homepage_loaded(report):
        score += 1

    return clamp(score, 0, WEIGHTS["outreach_viability"])


def score_outreach_story_strength(
    business: Business,
    page_map: dict[str, Page],
    report: dict[str, object],
) -> int:
    """Score whether the outreach narrative is obvious, fair, and worth sending."""
    score = 0
    gaps = story_gap_labels(business, page_map, report)
    basics = functional_site_signals(business, page_map, report)
    legacy = legacy_brochure_signals(business, page_map, report)

    if len(gaps) >= 2:
        score += 6
    elif len(gaps) == 1:
        score += 3

    if len(gaps) >= 4:
        score += 3
    elif len(gaps) == 3:
        score += 1

    if business.review_count and business.review_count >= 8:
        score += 3

    if business.review_count and business.review_count >= 20:
        score += 1

    if business.address:
        score += 1

    if homepage_loaded(report):
        score += 1

    if legacy["dated_brochure_opportunity"]:
        score += 1

    if basics["brochure_structure"] and basics["clear_contact_path"] and len(gaps) <= 1:
        score -= 2 if legacy["dated_brochure_opportunity"] else 4
    elif basics["brochure_structure"] and len(gaps) == 2:
        score -= 1

    if basics["clear_value_prop"] and basics["service_structure"] and len(gaps) <= 1:
        score -= 1 if legacy["dated_brochure_opportunity"] else 2

    return clamp(score, 0, WEIGHTS["outreach_story_strength"])


def build_top_issues(
    business: Business,
    page_map: dict[str, Page],
    report: dict[str, object],
) -> list[str]:
    """Build a short list of concrete site issues to mention in notes."""
    issues = [GAP_LABELS[label] for label in story_gap_labels(business, page_map, report)]
    legacy = legacy_brochure_signals(business, page_map, report)

    if legacy["dated_brochure_opportunity"]:
        issues.insert(0, "Site covers the basics, but the presentation still feels dated and under-packaged.")

    if not issues:
        issues.append("Website looks usable, but the redesign opportunity is less obvious.")

    return issues[:3]


def build_quick_summary(
    business: Business,
    fit_status: str,
    total_score: int,
    raw_total_score: int,
    evidence_tier: str,
) -> str:
    """Build a short summary that matches the final classification."""
    reviews = business.review_count or 0
    rating = business.rating or 0

    if fit_status == "strong":
        tail = "the business looks legitimate and the outreach story is easy to justify."
    elif fit_status == "maybe":
        tail = "the business looks legitimate and there is some outreach upside, but the case is not decisive."
    else:
        tail = "the current evidence suggests limited or lower-confidence outreach upside."

    cap_note = ""
    if total_score < raw_total_score:
        cap_note = f" Evidence quality is {evidence_tier}, so the score is capped from {raw_total_score} to {total_score}."

    return (
        f"{business.name} is a {fit_status} lead with a total score of {total_score}. "
        f"It has {reviews} reviews, a {rating} rating, and {tail}{cap_note}"
    )


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
    story_strength = scores["outreach_story_strength"]

    if (
        total_score >= STRONG_THRESHOLD
        and legitimacy >= MIN_STRONG_LEGITIMACY
        and redesign_opportunity >= MIN_STRONG_OPPORTUNITY
        and story_strength >= MIN_STRONG_STORY
    ):
        return "strong"

    if (
        total_score >= MAYBE_THRESHOLD
        and legitimacy >= MIN_MAYBE_LEGITIMACY
        and story_strength >= MIN_MAYBE_STORY
    ):
        return "maybe"

    return "skip"


def evaluate_business(session: Session, business: Business) -> ScoringResult:
    """Evaluate one business using the current deterministic rubric."""
    page_map = get_pages(session, business)
    report = load_browser_report(business)
    evidence = assess_evidence_quality(business, page_map, report)

    hard_skip_reason = detect_hard_skip(business, page_map, report)
    if hard_skip_reason:
        return ScoringResult(
            fit_status="skip",
            skip_reason=hard_skip_reason,
            scores={key: 0 for key in WEIGHTS},
            raw_total_score=0,
            total_score=0,
            evidence_tier=evidence.tier,
            evidence_cap=evidence.cap,
            confidence="high",
            top_issues=[hard_skip_reason],
            quick_summary=f"{business.name} is a skip lead. {hard_skip_reason}.",
            teardown_angle="Do not prioritize outreach.",
        )

    scores = {
        "business_legitimacy": score_business_legitimacy(business, page_map, report),
        "website_weakness": score_website_weakness(business, page_map, report),
        "conversion_opportunity": score_conversion_opportunity(business, page_map, report),
        "trust_packaging": score_trust_packaging(business, page_map, report),
        "complexity_fit": score_complexity_fit(business, page_map, report),
        "outreach_viability": score_outreach_viability(business, page_map, report),
        "outreach_story_strength": score_outreach_story_strength(business, page_map, report),
    }

    raw_total_score = sum(scores.values())
    total_score = min(raw_total_score, evidence.cap)
    fit_status = classify_total_score(total_score, scores)
    top_issues = build_top_issues(business, page_map, report)

    skip_reason = None
    if fit_status == "skip":
        if total_score < raw_total_score:
            skip_reason = (
                "Score below review threshold after evidence cap: "
                f"raw={raw_total_score} capped={total_score} tier={evidence.tier}"
            )
        else:
            skip_reason = f"Score below review threshold: {total_score}"

    return ScoringResult(
        fit_status=fit_status,
        skip_reason=skip_reason,
        scores=scores,
        raw_total_score=raw_total_score,
        total_score=total_score,
        evidence_tier=evidence.tier,
        evidence_cap=evidence.cap,
        confidence=evidence.confidence,
        top_issues=top_issues,
        quick_summary=build_quick_summary(
            business,
            fit_status,
            total_score,
            raw_total_score,
            evidence.tier,
        ),
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
    score_row.outreach_story_strength = result.scores["outreach_story_strength"]
    score_row.raw_total_score = result.raw_total_score
    score_row.evidence_tier = result.evidence_tier
    score_row.evidence_cap = result.evidence_cap
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
