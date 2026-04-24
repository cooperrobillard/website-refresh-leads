"""Export a compact review package for current-run lead review."""

from __future__ import annotations

import csv
import json
import shutil
from pathlib import Path
from typing import Any

from sqlalchemy import case
from sqlalchemy.orm import Session

from app.crawl.page_selector import normalize_url, should_skip_link
from app.db import SessionLocal
from app.lead_selection import normalized_website_key
from app.models import Artifact, Business, ModelJudgment, Note, Page, PipelineRun, Score
from app.pipeline_runs import resolve_pipeline_run


EXPORT_ROOT = Path("data/exports")
EXPORT_RUNS_DIR = EXPORT_ROOT / "runs"

PAGE_TYPES = ["home", "about", "services", "contact", "gallery", "faq"]
ARTIFACT_TYPES = {
    "homepage_desktop": "desktop_home_screenshot",
    "homepage_mobile": "mobile_home_screenshot",
}
SCREENSHOT_EXPORT_TYPES = {
    "homepage_desktop": "desktop",
    "homepage_mobile": "mobile",
}
CSV_COLUMNS = [
    "business_id",
    "discovery_run_id",
    "new_this_run",
    "business_name",
    "niche",
    "query_used",
    "location",
    "website",
    "canonical_url",
    "canonical_key",
    "primary_type",
    "google_rating",
    "google_review_count",
    "fit_status",
    "total_score",
    "raw_total_score",
    "confidence",
    "evidence_tier",
    "evidence_cap",
    "pages_captured",
    "screenshots_captured",
    "business_legitimacy",
    "website_weakness",
    "conversion_opportunity",
    "trust_packaging",
    "complexity_fit",
    "outreach_viability",
    "outreach_story_strength",
    "outreach_story_assessment",
    "home_url",
    "about_url",
    "services_url",
    "contact_url",
    "gallery_url",
    "faq_url",
    "desktop_screenshot",
    "mobile_screenshot",
    "top_issues",
    "quick_summary",
    "teardown_angle",
    "skip_reason",
    "final_source",
    "scoring_mode",
    "model_name",
    "prompt_version",
    "response_id",
    "evidence_quality",
    "recommended_action",
    "positive_signals",
    "evidence_warnings",
    "deterministic_fit_status",
    "deterministic_total_score",
    "deterministic_raw_total_score",
    "deterministic_confidence",
]

SCORE_DIMENSIONS = [
    "business_legitimacy",
    "website_weakness",
    "conversion_opportunity",
    "trust_packaging",
    "complexity_fit",
    "outreach_viability",
    "outreach_story_strength",
]


def parse_top_issues(raw_issues: str | None) -> list[str]:
    """Split stored note issues into a clean list."""
    if not raw_issues:
        return []
    return [line.strip() for line in raw_issues.splitlines() if line.strip()]


def parse_multiline_list(raw_text: str | None) -> list[str]:
    """Split stored newline-delimited text into a clean list."""
    if not raw_text:
        return []
    return [line.strip() for line in raw_text.splitlines() if line.strip()]


def recommended_action_from_fit_status(fit_status: str | None) -> str | None:
    """Map a coarse fit status into an export-friendly recommended action."""
    if fit_status == "strong":
        return "review_for_outreach"
    if fit_status == "maybe":
        return "low_priority_review"
    if fit_status == "skip":
        return "skip"
    return None


def sanitize_page_url(page_type: str, url: str | None) -> str | None:
    """Drop obviously bad exported page mappings such as legal/utility pages."""
    if not url:
        return None

    normalized = normalize_url(url)
    if page_type != "home" and should_skip_link(normalized):
        return None

    return normalized


def slugify(value: str) -> str:
    """Create a filesystem-friendly slug for export filenames."""
    slug = "".join(char.lower() if char.isalnum() else "-" for char in value)
    parts = [part for part in slug.split("-") if part]
    return "-".join(parts) or "business"


def ensure_export_root() -> None:
    """Ensure the root export folders exist."""
    EXPORT_RUNS_DIR.mkdir(parents=True, exist_ok=True)


def run_export_directory(run_id: int) -> Path:
    """Return the durable export folder for one pipeline run."""
    ensure_export_root()
    return EXPORT_RUNS_DIR / f"run_{run_id}"


def run_export_paths(run_id: int) -> tuple[Path, Path, Path, Path]:
    """Return the per-run export directory plus its JSON, CSV, and screenshot paths."""
    export_dir = run_export_directory(run_id)
    screenshot_dir = export_dir / "review_screenshots"
    return (
        export_dir,
        export_dir / "review_package.json",
        export_dir / "review_package.csv",
        screenshot_dir,
    )


def build_page_maps(rows: list[Page]) -> dict[int, dict[str, str | None]]:
    """Build one sanitized page map per business."""
    page_maps: dict[int, dict[str, str | None]] = {}

    for row in rows:
        if not row.page_type or row.page_type not in PAGE_TYPES:
            continue

        business_map = page_maps.setdefault(row.business_id, {})
        if row.page_type in business_map:
            continue

        sanitized_url = sanitize_page_url(row.page_type, row.url)
        if sanitized_url:
            business_map[row.page_type] = sanitized_url

    return page_maps


def build_artifact_maps(rows: list[Artifact]) -> dict[int, dict[str, str]]:
    """Build one artifact map per business."""
    artifact_maps: dict[int, dict[str, str]] = {}

    for row in rows:
        if not row.artifact_type or not row.file_path:
            continue

        business_map = artifact_maps.setdefault(row.business_id, {})
        business_map[row.artifact_type] = row.file_path

    return artifact_maps


def current_run_new_businesses_query(session: Session, run_id: int):
    """Return only businesses first admitted in the current run."""
    return session.query(Business).filter(Business.discovery_run_id == run_id)


def captured_page_count(page_map: dict[str, str | None]) -> int:
    """Count exported page URLs for evidence debugging."""
    return sum(1 for page_type in PAGE_TYPES if page_map.get(page_type))


def captured_screenshot_count(artifact_map: dict[str, str]) -> int:
    """Count available homepage screenshots for evidence debugging."""
    return sum(
        1
        for artifact_type in ARTIFACT_TYPES.values()
        if artifact_map.get(artifact_type)
    )


def top_scoring_dimensions(score: Score, limit: int = 3) -> list[dict[str, int | str]]:
    """Return the highest scoring rubric dimensions for quick manual ranking."""
    ranked_dimensions = sorted(
        (
            {"dimension": dimension, "score": getattr(score, dimension) or 0}
            for dimension in SCORE_DIMENSIONS
            if (getattr(score, dimension) or 0) > 0
        ),
        key=lambda row: (-int(row["score"]), str(row["dimension"])),
    )
    return ranked_dimensions[:limit]


def outreach_story_assessment(score_value: int | None) -> str:
    """Return a readable outreach-story label for exported review records."""
    strength = score_value or 0
    if strength >= 9:
        return "strong"
    if strength >= 5:
        return "fair"
    if strength >= 1:
        return "weak"
    return "minimal"


def build_review_record(
    business: Business,
    score: Score,
    note: Note | None,
    page_map: dict[str, str | None],
    artifact_map: dict[str, str],
    current_run_id: int,
) -> dict[str, Any]:
    """Build one exported review record."""
    top_issues = parse_top_issues(note.top_issues if note else None)
    pages_captured = captured_page_count(page_map)
    screenshots_captured = captured_screenshot_count(artifact_map)

    return {
        "business_id": business.id,
        "discovery_run_id": business.discovery_run_id,
        "new_this_run": business.discovery_run_id == current_run_id,
        "business_name": business.name,
        "niche": business.niche,
        "query_used": business.query_used,
        "location": business.address,
        "website": normalize_url(business.website) if business.website else None,
        "canonical_url": normalize_url(business.canonical_url) if business.canonical_url else None,
        "canonical_key": business.canonical_key,
        "primary_type": business.primary_type,
        "google_rating": business.rating,
        "google_review_count": business.review_count,
        "fit_status": score.fit_status,
        "total_score": score.total_score,
        "raw_total_score": score.raw_total_score,
        "confidence": score.confidence,
        "evidence_tier": score.evidence_tier,
        "evidence_cap": score.evidence_cap,
        "pages_captured": pages_captured,
        "screenshots_captured": screenshots_captured,
        "scores": {
            "business_legitimacy": score.business_legitimacy,
            "website_weakness": score.website_weakness,
            "conversion_opportunity": score.conversion_opportunity,
            "trust_packaging": score.trust_packaging,
            "complexity_fit": score.complexity_fit,
            "outreach_viability": score.outreach_viability,
            "outreach_story_strength": score.outreach_story_strength,
        },
        "review_context": {
            "why_it_qualified": note.quick_summary if note else None,
            "top_scoring_dimensions": top_scoring_dimensions(score),
            "evidence": {
                "tier": score.evidence_tier,
                "confidence": score.confidence,
                "cap": score.evidence_cap,
                "raw_total_score": score.raw_total_score,
                "pages_captured": pages_captured,
                "screenshots_captured": screenshots_captured,
            },
            "outreach_story": {
                "strength_score": score.outreach_story_strength,
                "assessment": outreach_story_assessment(score.outreach_story_strength),
                "primary_gaps": top_issues[:2],
            },
        },
        "pages_found": {page_type: page_map.get(page_type) for page_type in PAGE_TYPES},
        "screenshots": {
            "homepage_desktop": artifact_map.get(ARTIFACT_TYPES["homepage_desktop"]),
            "homepage_mobile": artifact_map.get(ARTIFACT_TYPES["homepage_mobile"]),
        },
        "top_issues": top_issues,
        "quick_summary": note.quick_summary if note else None,
        "teardown_angle": note.teardown_angle if note else None,
        "skip_reason": business.skip_reason,
        "final_source": "deterministic",
        "scoring_mode": "deterministic",
        "model_name": None,
        "prompt_version": None,
        "response_id": None,
        "evidence_quality": score.evidence_tier,
        "recommended_action": recommended_action_from_fit_status(score.fit_status),
        "positive_signals": [],
        "evidence_warnings": [],
        "deterministic_compare": None,
    }


def recreate_export_directory(directory: Path) -> None:
    """Reset the export screenshot directory so each run starts clean."""
    if directory.exists():
        shutil.rmtree(directory)
    directory.mkdir(parents=True, exist_ok=True)


def build_export_screenshot_name(
    *,
    business_name: str,
    business_id: int,
    variant: str,
    source_path: str,
    used_filenames: set[str],
) -> str:
    """Build a collision-safe screenshot filename for the export folder."""
    suffix = Path(source_path).suffix or ".png"
    business_slug = slugify(business_name)

    filename = f"{business_slug}_{variant}{suffix}"
    if filename not in used_filenames:
        used_filenames.add(filename)
        return filename

    filename = f"{business_slug}_{business_id}_{variant}{suffix}"
    counter = 2
    while filename in used_filenames:
        filename = f"{business_slug}_{business_id}_{variant}_{counter}{suffix}"
        counter += 1

    used_filenames.add(filename)
    return filename


def copy_screenshot_for_export(
    *,
    business_name: str,
    business_id: int,
    variant: str,
    source_path: str | None,
    export_dir: Path,
    used_filenames: set[str],
) -> str | None:
    """Copy one screenshot into the export folder when the source file exists."""
    if not source_path:
        return None

    source = Path(source_path)
    if not source.exists():
        return None

    filename = build_export_screenshot_name(
        business_name=business_name,
        business_id=business_id,
        variant=variant,
        source_path=source_path,
        used_filenames=used_filenames,
    )
    destination = export_dir / filename

    try:
        shutil.copy2(source, destination)
    except OSError:
        return None

    return destination.as_posix()


def collect_export_screenshots(records: list[dict[str, Any]], screenshot_dir: Path) -> int:
    """Copy screenshots for exported leads into a clean export bundle."""
    recreate_export_directory(screenshot_dir)

    copied_count = 0
    used_filenames: set[str] = set()

    for record in records:
        export_screenshots: dict[str, str | None] = {}

        for screenshot_key, variant in SCREENSHOT_EXPORT_TYPES.items():
            export_path = copy_screenshot_for_export(
                business_name=record["business_name"],
                business_id=record["business_id"],
                variant=variant,
                source_path=record["screenshots"].get(screenshot_key),
                export_dir=screenshot_dir,
                used_filenames=used_filenames,
            )
            export_screenshots[screenshot_key] = export_path
            if export_path:
                copied_count += 1

        record["export_screenshots"] = export_screenshots

    return copied_count


def write_csv(records: list[dict[str, Any]], output_path: Path) -> None:
    """Write the flat CSV export for manual scanning."""
    with output_path.open("w", newline="", encoding="utf-8") as file_handle:
        writer = csv.writer(file_handle)
        writer.writerow(CSV_COLUMNS)

        for record in records:
            writer.writerow(
                [
                    record["business_id"],
                    record["discovery_run_id"],
                    record["new_this_run"],
                    record["business_name"],
                    record["niche"],
                    record["query_used"],
                    record["location"],
                    record["website"],
                    record["canonical_url"],
                    record["canonical_key"],
                    record["primary_type"],
                    record["google_rating"],
                    record["google_review_count"],
                    record["fit_status"],
                    record["total_score"],
                    record["raw_total_score"],
                    record["confidence"],
                    record["evidence_tier"],
                    record["evidence_cap"],
                    record["pages_captured"],
                    record["screenshots_captured"],
                    record["scores"]["business_legitimacy"],
                    record["scores"]["website_weakness"],
                    record["scores"]["conversion_opportunity"],
                    record["scores"]["trust_packaging"],
                    record["scores"]["complexity_fit"],
                    record["scores"]["outreach_viability"],
                    record["scores"]["outreach_story_strength"],
                    record["review_context"]["outreach_story"]["assessment"],
                    record["pages_found"]["home"],
                    record["pages_found"]["about"],
                    record["pages_found"]["services"],
                    record["pages_found"]["contact"],
                    record["pages_found"]["gallery"],
                    record["pages_found"]["faq"],
                    record["screenshots"]["homepage_desktop"],
                    record["screenshots"]["homepage_mobile"],
                    " | ".join(record["top_issues"]),
                    record["quick_summary"],
                    record["teardown_angle"],
                    record["skip_reason"],
                    record.get("final_source"),
                    record.get("scoring_mode"),
                    record.get("model_name"),
                    record.get("prompt_version"),
                    record.get("response_id"),
                    record.get("evidence_quality"),
                    record.get("recommended_action"),
                    " | ".join(record.get("positive_signals", [])),
                    " | ".join(record.get("evidence_warnings", [])),
                    (record.get("deterministic_compare") or {}).get("fit_status"),
                    (record.get("deterministic_compare") or {}).get("total_score"),
                    (record.get("deterministic_compare") or {}).get("raw_total_score"),
                    (record.get("deterministic_compare") or {}).get("confidence"),
                ]
            )


def dedupe_scored_rows(
    rows: list[tuple[Business, Score, Note | None]],
) -> tuple[list[tuple[Business, Score, Note | None]], int]:
    """Keep one canonical scored row per normalized website key."""
    grouped_rows: dict[str, list[tuple[Business, Score, Note | None]]] = {}

    for business, score, note in rows:
        website_key = business.canonical_key or normalized_website_key(business.website) or f"business:{business.id}"
        grouped_rows.setdefault(website_key, []).append((business, score, note))

    deduped_rows: list[tuple[Business, Score, Note | None]] = []
    for row_group in grouped_rows.values():
        canonical_row = max(
            row_group,
            key=lambda row: (
                row[0].review_count or 0,
                row[1].total_score or 0,
                -row[0].id,
            ),
        )
        deduped_rows.append(canonical_row)

    deduped_rows.sort(
        key=lambda row: (
            0 if row[1].fit_status == "strong" else 1 if row[1].fit_status == "maybe" else 2,
            -(row[1].total_score or 0),
            -(row[0].review_count or 0),
            row[0].name.lower(),
        )
    )

    return deduped_rows, len(rows) - len(deduped_rows)


def dedupe_model_rows(
    rows: list[tuple[Business, ModelJudgment, Score | None]],
) -> tuple[list[tuple[Business, ModelJudgment, Score | None]], int]:
    """Keep one canonical model-judged row per normalized website key."""
    grouped_rows: dict[str, list[tuple[Business, ModelJudgment, Score | None]]] = {}

    for business, judgment, score in rows:
        website_key = business.canonical_key or normalized_website_key(business.website) or f"business:{business.id}"
        grouped_rows.setdefault(website_key, []).append((business, judgment, score))

    deduped_rows: list[tuple[Business, ModelJudgment, Score | None]] = []
    for row_group in grouped_rows.values():
        canonical_row = max(
            row_group,
            key=lambda row: (
                row[0].review_count or 0,
                row[1].website_weakness or 0,
                row[1].outreach_story_strength or 0,
                -row[0].id,
            ),
        )
        deduped_rows.append(canonical_row)

    status_priority = {"strong": 0, "maybe": 1, "skip": 2}
    confidence_priority = {"high": 0, "medium": 1, "low": 2}
    deduped_rows.sort(
        key=lambda row: (
            status_priority.get(row[1].fit_status or "", 3),
            confidence_priority.get(row[1].confidence or "", 3),
            -(row[1].website_weakness or 0),
            -(row[1].outreach_story_strength or 0),
            -(row[0].review_count or 0),
            row[0].name.lower(),
        )
    )

    return deduped_rows, len(rows) - len(deduped_rows)


def build_model_review_record(
    *,
    business: Business,
    judgment: ModelJudgment,
    page_map: dict[str, str | None],
    artifact_map: dict[str, str],
    current_run_id: int,
    scoring_mode: str,
    deterministic_score: Score | None = None,
) -> dict[str, Any]:
    """Build one exported review record from a stored model judgment."""
    top_issues = parse_multiline_list(judgment.top_issues)
    positive_signals = parse_multiline_list(judgment.positive_signals)
    evidence_warnings = parse_multiline_list(judgment.evidence_warnings)
    pages_captured = captured_page_count(page_map)
    screenshots_captured = captured_screenshot_count(artifact_map)
    deterministic_compare = None

    if deterministic_score is not None:
        deterministic_compare = {
            "fit_status": deterministic_score.fit_status,
            "total_score": deterministic_score.total_score,
            "raw_total_score": deterministic_score.raw_total_score,
            "confidence": deterministic_score.confidence,
            "evidence_tier": deterministic_score.evidence_tier,
        }

    return {
        "business_id": business.id,
        "discovery_run_id": business.discovery_run_id,
        "new_this_run": business.discovery_run_id == current_run_id,
        "business_name": business.name,
        "niche": business.niche,
        "query_used": business.query_used,
        "location": business.address,
        "website": normalize_url(business.website) if business.website else None,
        "canonical_url": normalize_url(business.canonical_url) if business.canonical_url else None,
        "canonical_key": business.canonical_key,
        "primary_type": business.primary_type,
        "google_rating": business.rating,
        "google_review_count": business.review_count,
        "fit_status": judgment.fit_status,
        "total_score": None,
        "raw_total_score": None,
        "confidence": judgment.confidence,
        "evidence_tier": judgment.evidence_quality,
        "evidence_cap": None,
        "evidence_quality": judgment.evidence_quality,
        "pages_captured": pages_captured,
        "screenshots_captured": screenshots_captured,
        "scores": {
            "business_legitimacy": judgment.business_legitimacy,
            "website_weakness": judgment.website_weakness,
            "conversion_opportunity": None,
            "trust_packaging": None,
            "complexity_fit": None,
            "outreach_viability": None,
            "outreach_story_strength": judgment.outreach_story_strength,
        },
        "review_context": {
            "why_it_qualified": judgment.short_reasoning,
            "top_scoring_dimensions": [
                {"dimension": "website_weakness", "score": judgment.website_weakness or 0},
                {"dimension": "outreach_story_strength", "score": judgment.outreach_story_strength or 0},
                {"dimension": "business_legitimacy", "score": judgment.business_legitimacy or 0},
            ],
            "evidence": {
                "tier": judgment.evidence_quality,
                "confidence": judgment.confidence,
                "cap": None,
                "raw_total_score": None,
                "pages_captured": pages_captured,
                "screenshots_captured": screenshots_captured,
                "warnings": evidence_warnings,
            },
            "outreach_story": {
                "strength_score": judgment.outreach_story_strength,
                "assessment": outreach_story_assessment(judgment.outreach_story_strength),
                "primary_gaps": top_issues[:2],
            },
            "model_judgment": {
                "recommended_action": judgment.recommended_action,
                "positive_signals": positive_signals,
                "evidence_warnings": evidence_warnings,
            },
        },
        "pages_found": {page_type: page_map.get(page_type) for page_type in PAGE_TYPES},
        "screenshots": {
            "homepage_desktop": artifact_map.get(ARTIFACT_TYPES["homepage_desktop"]),
            "homepage_mobile": artifact_map.get(ARTIFACT_TYPES["homepage_mobile"]),
        },
        "top_issues": top_issues,
        "quick_summary": judgment.short_reasoning,
        "teardown_angle": judgment.short_teardown_angle,
        "skip_reason": business.skip_reason if business.skip_reason else (judgment.short_reasoning if judgment.fit_status == "skip" else None),
        "final_source": "model_judgment",
        "scoring_mode": scoring_mode,
        "model_name": judgment.model_name,
        "prompt_version": judgment.prompt_version,
        "response_id": judgment.response_id,
        "recommended_action": judgment.recommended_action,
        "positive_signals": positive_signals,
        "evidence_warnings": evidence_warnings,
        "deterministic_compare": deterministic_compare,
    }


def pipeline_run_scoring_mode(session: Session, run_id: int) -> str:
    """Return the scoring mode recorded for one pipeline run."""
    pipeline_run = session.get(PipelineRun, run_id)
    if pipeline_run is None:
        raise ValueError(f"Pipeline run not found: {run_id}")
    return pipeline_run.scoring_mode


def export_review_package(
    limit: int = 20,
    include_maybe: bool = True,
    fallback_to_skips: bool = True,
    run_id: int | None = None,
) -> list[dict[str, Any]]:
    """Export current-run leads using the run's configured final judgment source."""
    deterministic_status_order = case(
        (Score.fit_status == "strong", 0),
        (Score.fit_status == "maybe", 1),
        else_=2,
    )
    model_status_order = case(
        (ModelJudgment.fit_status == "strong", 0),
        (ModelJudgment.fit_status == "maybe", 1),
        else_=2,
    )

    with SessionLocal() as session:
        current_run_id, _ = resolve_pipeline_run(session, run_id)
        scoring_mode = pipeline_run_scoring_mode(session, current_run_id)
        export_dir, json_path, csv_path, screenshot_dir = run_export_paths(current_run_id)
        export_dir.mkdir(parents=True, exist_ok=True)

        records: list[dict[str, Any]]
        duplicate_count = 0

        if scoring_mode == "deterministic":
            query = (
                current_run_new_businesses_query(session, current_run_id)
                .with_entities(Business, Score, Note)
                .join(Score, Score.business_id == Business.id)
                .outerjoin(Note, Note.business_id == Business.id)
            )

            if include_maybe:
                query = query.filter(Score.fit_status.in_(["strong", "maybe"]))
            else:
                query = query.filter(Score.fit_status == "strong")

            rows = (
                query.order_by(
                    deterministic_status_order,
                    Score.total_score.desc(),
                    Business.review_count.desc(),
                    Business.name.asc(),
                )
                .all()
            )
            rows, duplicate_count = dedupe_scored_rows(rows)
            rows = rows[:limit]

            if not rows and fallback_to_skips:
                fallback_rows = (
                    current_run_new_businesses_query(session, current_run_id)
                    .with_entities(Business, Score, Note)
                    .join(Score, Score.business_id == Business.id)
                    .outerjoin(Note, Note.business_id == Business.id)
                    .filter(Score.fit_status == "skip")
                    .order_by(Score.total_score.desc(), Business.review_count.desc(), Business.name.asc())
                    .all()
                )
                fallback_rows, fallback_duplicate_count = dedupe_scored_rows(fallback_rows)
                rows = fallback_rows[:limit]
                duplicate_count += fallback_duplicate_count
                if rows:
                    print("No current-run strong/maybe leads found. Exporting current-run skip leads as fallback.")

            business_ids = [business.id for business, _, _ in rows]
        else:
            query = (
                current_run_new_businesses_query(session, current_run_id)
                .with_entities(Business, ModelJudgment, Score)
                .join(ModelJudgment, ModelJudgment.business_id == Business.id)
                .outerjoin(Score, Score.business_id == Business.id)
                .filter(ModelJudgment.pipeline_run_id == current_run_id)
                .filter(ModelJudgment.judgment_mode == scoring_mode)
            )

            if include_maybe:
                query = query.filter(ModelJudgment.fit_status.in_(["strong", "maybe"]))
            else:
                query = query.filter(ModelJudgment.fit_status == "strong")

            rows = (
                query.order_by(
                    model_status_order,
                    ModelJudgment.website_weakness.desc(),
                    ModelJudgment.outreach_story_strength.desc(),
                    Business.review_count.desc(),
                    Business.name.asc(),
                )
                .all()
            )
            rows, duplicate_count = dedupe_model_rows(rows)
            rows = rows[:limit]

            if not rows and fallback_to_skips:
                fallback_rows = (
                    current_run_new_businesses_query(session, current_run_id)
                    .with_entities(Business, ModelJudgment, Score)
                    .join(ModelJudgment, ModelJudgment.business_id == Business.id)
                    .outerjoin(Score, Score.business_id == Business.id)
                    .filter(ModelJudgment.pipeline_run_id == current_run_id)
                    .filter(ModelJudgment.judgment_mode == scoring_mode)
                    .filter(ModelJudgment.fit_status == "skip")
                    .order_by(
                        ModelJudgment.confidence.asc(),
                        Business.review_count.desc(),
                        Business.name.asc(),
                    )
                    .all()
                )
                fallback_rows, fallback_duplicate_count = dedupe_model_rows(fallback_rows)
                rows = fallback_rows[:limit]
                duplicate_count += fallback_duplicate_count
                if rows:
                    print("No current-run strong/maybe model judgments found. Exporting current-run skip leads as fallback.")

            business_ids = [business.id for business, _, _ in rows]

        page_rows = (
            session.query(Page)
            .filter(Page.business_id.in_(business_ids))
            .order_by(Page.business_id.asc(), Page.id.asc())
            .all()
        ) if business_ids else []
        artifact_rows = (
            session.query(Artifact)
            .filter(Artifact.business_id.in_(business_ids))
            .order_by(Artifact.business_id.asc(), Artifact.id.asc())
            .all()
        ) if business_ids else []

        page_maps = build_page_maps(page_rows)
        artifact_maps = build_artifact_maps(artifact_rows)

        if scoring_mode == "deterministic":
            records = [
                build_review_record(
                    business=business,
                    score=score,
                    note=note,
                    page_map=page_maps.get(business.id, {}),
                    artifact_map=artifact_maps.get(business.id, {}),
                    current_run_id=current_run_id,
                )
                for business, score, note in rows
            ]
        else:
            records = [
                build_model_review_record(
                    business=business,
                    judgment=judgment,
                    page_map=page_maps.get(business.id, {}),
                    artifact_map=artifact_maps.get(business.id, {}),
                    current_run_id=current_run_id,
                    scoring_mode=scoring_mode,
                    deterministic_score=score,
                )
                for business, judgment, score in rows
            ]
        copied_screenshot_count = collect_export_screenshots(records, screenshot_dir)

        json_path.write_text(json.dumps(records, indent=2), encoding="utf-8")

        write_csv(records, csv_path)

        strong_count = sum(1 for record in records if record["fit_status"] == "strong")
        maybe_count = sum(1 for record in records if record["fit_status"] == "maybe")

        print(f"Run {current_run_id}: exporting current-run new candidates only")
        print(f"Scoring mode: {scoring_mode}")
        print(f"Exported {len(records)} leads")
        if duplicate_count:
            print(f"Skipped {duplicate_count} duplicate website entr{'y' if duplicate_count == 1 else 'ies'}")
        print(f"Strong: {strong_count}")
        print(f"Maybe: {maybe_count}")
        print(f"Run export folder: {export_dir}")
        print(f"JSON: {json_path}")
        print(f"CSV:  {csv_path}")
        print(f"Copied screenshots: {copied_screenshot_count}")
        print(f"Screenshot export folder: {screenshot_dir}")

        return records


if __name__ == "__main__":
    from app.schema import ensure_database_schema

    ensure_database_schema()
    export_review_package(limit=20, include_maybe=True)
