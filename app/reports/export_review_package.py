"""Export a compact review package for manual lead review."""

from __future__ import annotations

import csv
import json
import shutil
from pathlib import Path
from typing import Any

from sqlalchemy import case

from app.crawl.page_selector import normalize_url, should_skip_link
from app.db import SessionLocal
from app.lead_selection import normalized_website_key
from app.models import Artifact, Business, Note, Page, Score
from app.pipeline_runs import businesses_for_run_query, resolve_pipeline_run


EXPORT_DIR = Path("data/exports")
EXPORT_DIR.mkdir(parents=True, exist_ok=True)
EXPORT_SCREENSHOT_DIR = EXPORT_DIR / "review_screenshots"

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
    "business_name",
    "niche",
    "location",
    "website",
    "primary_type",
    "google_rating",
    "google_review_count",
    "fit_status",
    "total_score",
    "confidence",
    "business_legitimacy",
    "website_weakness",
    "conversion_opportunity",
    "trust_packaging",
    "complexity_fit",
    "outreach_viability",
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
]


def parse_top_issues(raw_issues: str | None) -> list[str]:
    """Split stored note issues into a clean list."""
    if not raw_issues:
        return []
    return [line.strip() for line in raw_issues.splitlines() if line.strip()]


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


def build_review_record(
    business: Business,
    score: Score,
    note: Note | None,
    page_map: dict[str, str | None],
    artifact_map: dict[str, str],
) -> dict[str, Any]:
    """Build one exported review record."""
    top_issues = parse_top_issues(note.top_issues if note else None)

    return {
        "business_id": business.id,
        "business_name": business.name,
        "niche": business.niche,
        "location": business.address,
        "website": normalize_url(business.website) if business.website else None,
        "primary_type": business.primary_type,
        "google_rating": business.rating,
        "google_review_count": business.review_count,
        "fit_status": score.fit_status,
        "total_score": score.total_score,
        "confidence": score.confidence,
        "scores": {
            "business_legitimacy": score.business_legitimacy,
            "website_weakness": score.website_weakness,
            "conversion_opportunity": score.conversion_opportunity,
            "trust_packaging": score.trust_packaging,
            "complexity_fit": score.complexity_fit,
            "outreach_viability": score.outreach_viability,
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


def collect_export_screenshots(records: list[dict[str, Any]]) -> int:
    """Copy screenshots for exported leads into a clean export bundle."""
    recreate_export_directory(EXPORT_SCREENSHOT_DIR)

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
                export_dir=EXPORT_SCREENSHOT_DIR,
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
                    record["business_name"],
                    record["niche"],
                    record["location"],
                    record["website"],
                    record["primary_type"],
                    record["google_rating"],
                    record["google_review_count"],
                    record["fit_status"],
                    record["total_score"],
                    record["confidence"],
                    record["scores"]["business_legitimacy"],
                    record["scores"]["website_weakness"],
                    record["scores"]["conversion_opportunity"],
                    record["scores"]["trust_packaging"],
                    record["scores"]["complexity_fit"],
                    record["scores"]["outreach_viability"],
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


def export_review_package(
    limit: int = 20,
    include_maybe: bool = True,
    fallback_to_skips: bool = True,
    run_id: int | None = None,
) -> list[dict[str, Any]]:
    """Export strong leads, plus maybe leads when requested, to JSON and CSV."""
    status_order = case(
        (Score.fit_status == "strong", 0),
        (Score.fit_status == "maybe", 1),
        else_=2,
    )

    with SessionLocal() as session:
        current_run = resolve_pipeline_run(session, run_id)
        query = (
            businesses_for_run_query(session, current_run)
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
                status_order,
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
                businesses_for_run_query(session, current_run)
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
                print("No strong/maybe leads found. Exporting top scored skip leads as fallback.")

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

        records = [
            build_review_record(
                business=business,
                score=score,
                note=note,
                page_map=page_maps.get(business.id, {}),
                artifact_map=artifact_maps.get(business.id, {}),
            )
            for business, score, note in rows
        ]
        copied_screenshot_count = collect_export_screenshots(records)

        json_path = EXPORT_DIR / "review_package.json"
        json_path.write_text(json.dumps(records, indent=2), encoding="utf-8")

        csv_path = EXPORT_DIR / "review_package.csv"
        write_csv(records, csv_path)

        strong_count = sum(1 for record in records if record["fit_status"] == "strong")
        maybe_count = sum(1 for record in records if record["fit_status"] == "maybe")

        print(f"Run {current_run.id}: exported {len(records)} leads")
        if duplicate_count:
            print(f"Skipped {duplicate_count} duplicate website entr{'y' if duplicate_count == 1 else 'ies'}")
        print(f"Strong: {strong_count}")
        print(f"Maybe: {maybe_count}")
        print(f"JSON: {json_path}")
        print(f"CSV:  {csv_path}")
        print(f"Copied screenshots: {copied_screenshot_count}")
        print(f"Screenshot export folder: {EXPORT_SCREENSHOT_DIR}")

        return records


if __name__ == "__main__":
    from app.schema import ensure_database_schema

    ensure_database_schema()
    export_review_package(limit=20, include_maybe=True)
