"""Aggregate per-run review exports into one batch-level review package."""

from __future__ import annotations

import copy
import csv
import json
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from app.reports.export_review_package import (
    CSV_COLUMNS,
    EXPORT_ROOT,
    EXPORT_RUNS_DIR,
    SCREENSHOT_EXPORT_TYPES,
    copy_screenshot_for_export,
    slugify,
)


EXPORT_BATCHES_DIR = EXPORT_ROOT / "batches"
BATCH_COMBINED_CSV_COLUMNS = [
    "source_run_id",
    "source_query",
    "source_niche",
    "source_scoring_mode",
    *CSV_COLUMNS,
]
BATCH_SUMMARY_COLUMNS = [
    "query",
    "niche",
    "run_id",
    "inserted_new",
    "exported_leads",
    "strong_count",
    "maybe_count",
    "skip_count",
    "scoring_mode",
    "export_folder_path",
    "included_in_batch",
    "reason_excluded",
]


@dataclass
class RunBatchExport:
    """Collected export state for one run inside a query-file batch."""

    run_id: int
    query: str
    niche: str
    scoring_mode: str
    inserted_new: int
    records: list[dict[str, Any]]
    export_dir: Path


@dataclass
class BatchExportResult:
    """Returned details for one completed batch export."""

    batch_id: str
    export_dir: Path
    json_path: Path
    csv_path: Path
    summary_path: Path
    screenshot_dir: Path
    metadata_path: Path
    included_run_count: int
    excluded_run_count: int
    exported_lead_count: int
    deleted_run_export_dirs: list[str]
    failed_cleanup_run_export_dirs: list[str]


def ensure_batch_export_root() -> None:
    """Ensure the batch export root exists."""
    EXPORT_BATCHES_DIR.mkdir(parents=True, exist_ok=True)


def batch_export_directory(batch_id: str) -> Path:
    """Return the final batch export directory for one batch id."""
    ensure_batch_export_root()
    return EXPORT_BATCHES_DIR / batch_id


def batch_export_paths(batch_id: str) -> tuple[Path, Path, Path, Path, Path]:
    """Return the primary files inside one batch export directory."""
    export_dir = batch_export_directory(batch_id)
    return (
        export_dir / "combined_review_package.json",
        export_dir / "combined_review_package.csv",
        export_dir / "batch_summary.csv",
        export_dir / "review_screenshots",
        export_dir / "batch_metadata.json",
    )


def build_batch_id(
    *,
    query_file: str | None = None,
    current_time: datetime | None = None,
) -> str:
    """Build a simple local batch id from time plus query file stem."""
    timestamp = (current_time or datetime.now()).strftime("%Y%m%d_%H%M%S_%f")
    source_label = slugify(Path(query_file).stem) if query_file else "query-file"
    return f"batch_{timestamp}_{source_label}"


def fit_status_counts(records: list[dict[str, Any]]) -> tuple[int, int, int]:
    """Count exported records by fit status."""
    strong_count = sum(1 for record in records if record.get("fit_status") == "strong")
    maybe_count = sum(1 for record in records if record.get("fit_status") == "maybe")
    skip_count = sum(1 for record in records if record.get("fit_status") == "skip")
    return strong_count, maybe_count, skip_count


def build_batch_summary_rows(run_exports: list[RunBatchExport]) -> list[dict[str, Any]]:
    """Build one batch-summary row per run from the current batch invocation."""
    rows: list[dict[str, Any]] = []

    for run_export in run_exports:
        strong_count, maybe_count, skip_count = fit_status_counts(run_export.records)
        included_in_batch = bool(run_export.records)

        rows.append(
            {
                "query": run_export.query,
                "niche": run_export.niche,
                "run_id": run_export.run_id,
                "inserted_new": run_export.inserted_new,
                "exported_leads": len(run_export.records),
                "strong_count": strong_count,
                "maybe_count": maybe_count,
                "skip_count": skip_count,
                "scoring_mode": run_export.scoring_mode,
                "export_folder_path": run_export.export_dir.as_posix(),
                "included_in_batch": "true" if included_in_batch else "false",
                "reason_excluded": None if included_in_batch else "empty export",
            }
        )

    return rows


def copy_batch_screenshots(
    *,
    batch_records: list[dict[str, Any]],
    screenshot_dir: Path,
) -> int:
    """Copy included-run screenshots into the batch-local screenshot folder."""
    screenshot_dir.mkdir(parents=True, exist_ok=True)
    used_filenames: set[str] = set()
    copied_count = 0

    for record in batch_records:
        export_screenshots: dict[str, str | None] = {}

        for screenshot_key, variant in SCREENSHOT_EXPORT_TYPES.items():
            source_path = (
                (record.get("export_screenshots") or {}).get(screenshot_key)
                or (record.get("screenshots") or {}).get(screenshot_key)
            )
            export_path = copy_screenshot_for_export(
                business_name=record["business_name"],
                business_id=record["business_id"],
                variant=f"run{record['source_run_id']}_{variant}",
                source_path=source_path,
                export_dir=screenshot_dir,
                used_filenames=used_filenames,
            )
            export_screenshots[screenshot_key] = export_path
            if export_path:
                copied_count += 1

        record["export_screenshots"] = export_screenshots

    return copied_count


def _csv_screenshot_path(record: dict[str, Any], screenshot_key: str) -> str | None:
    """Prefer bundled export screenshots for CSV review output."""
    export_screenshots = record.get("export_screenshots") or {}
    if export_screenshots.get(screenshot_key):
        return export_screenshots[screenshot_key]

    screenshots = record.get("screenshots") or {}
    return screenshots.get(screenshot_key)


def write_batch_combined_csv(records: list[dict[str, Any]], output_path: Path) -> None:
    """Write one flat CSV across all included runs in the batch."""
    with output_path.open("w", newline="", encoding="utf-8") as file_handle:
        writer = csv.writer(file_handle)
        writer.writerow(BATCH_COMBINED_CSV_COLUMNS)

        for record in records:
            writer.writerow(
                [
                    record.get("source_run_id"),
                    record.get("source_query"),
                    record.get("source_niche"),
                    record.get("source_scoring_mode"),
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
                    _csv_screenshot_path(record, "homepage_desktop"),
                    _csv_screenshot_path(record, "homepage_mobile"),
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


def write_batch_summary_csv(summary_rows: list[dict[str, Any]], output_path: Path) -> None:
    """Write the batch-summary CSV for run-level review."""
    with output_path.open("w", newline="", encoding="utf-8") as file_handle:
        writer = csv.DictWriter(file_handle, fieldnames=BATCH_SUMMARY_COLUMNS)
        writer.writeheader()
        writer.writerows(summary_rows)


def batch_records_for_export(run_exports: list[RunBatchExport]) -> list[dict[str, Any]]:
    """Return copied, annotated records for only the non-empty runs."""
    combined_records: list[dict[str, Any]] = []

    for run_export in run_exports:
        if not run_export.records:
            continue

        for record in run_export.records:
            combined_record = copy.deepcopy(record)
            combined_record["source_run_id"] = run_export.run_id
            combined_record["source_query"] = run_export.query
            combined_record["source_niche"] = run_export.niche
            combined_record["source_scoring_mode"] = run_export.scoring_mode
            combined_record["source_export_folder_path"] = run_export.export_dir.as_posix()
            combined_records.append(combined_record)

    return combined_records


def _safe_run_export_directory(path: Path) -> bool:
    """Return true only for batch-cleanup targets inside the run export root."""
    try:
        resolved_root = EXPORT_RUNS_DIR.resolve()
        resolved_path = path.resolve()
        resolved_path.relative_to(resolved_root)
    except (OSError, ValueError):
        return False

    return resolved_path.name.startswith("run_")


def cleanup_run_export_directories(run_exports: list[RunBatchExport]) -> tuple[list[str], list[str]]:
    """Delete only the current batch's per-run export directories."""
    deleted_paths: list[str] = []
    failed_paths: list[str] = []

    for run_export in run_exports:
        export_dir = run_export.export_dir
        if not export_dir.exists():
            continue
        if not _safe_run_export_directory(export_dir):
            failed_paths.append(export_dir.as_posix())
            continue

        try:
            shutil.rmtree(export_dir)
        except OSError:
            failed_paths.append(export_dir.as_posix())
            continue

        deleted_paths.append(export_dir.as_posix())

    return deleted_paths, failed_paths


def export_batch_review_package(
    *,
    run_exports: list[RunBatchExport],
    query_file: str | None = None,
    batch_id: str | None = None,
) -> BatchExportResult:
    """Write one combined review package for a query-file batch."""
    if not run_exports:
        raise ValueError("Batch export requires at least one run export.")

    resolved_batch_id = batch_id or build_batch_id(query_file=query_file)
    export_dir = batch_export_directory(resolved_batch_id)
    if export_dir.exists():
        raise FileExistsError(f"Batch export folder already exists: {export_dir}")

    json_path, csv_path, summary_path, screenshot_dir, metadata_path = batch_export_paths(resolved_batch_id)
    temp_dir = export_dir.with_name(f"{export_dir.name}_tmp")

    if temp_dir.exists():
        shutil.rmtree(temp_dir)

    summary_rows = build_batch_summary_rows(run_exports)
    combined_records = batch_records_for_export(run_exports)

    try:
        temp_dir.mkdir(parents=True, exist_ok=False)
        temp_json_path = temp_dir / json_path.name
        temp_csv_path = temp_dir / csv_path.name
        temp_summary_path = temp_dir / summary_path.name
        temp_screenshot_dir = temp_dir / screenshot_dir.name

        copied_screenshot_count = copy_batch_screenshots(
            batch_records=combined_records,
            screenshot_dir=temp_screenshot_dir,
        )
        temp_json_path.write_text(json.dumps(combined_records, indent=2), encoding="utf-8")
        write_batch_combined_csv(combined_records, temp_csv_path)
        write_batch_summary_csv(summary_rows, temp_summary_path)

        temp_dir.replace(export_dir)
    except Exception:
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)
        raise

    deleted_paths, failed_cleanup_paths = cleanup_run_export_directories(run_exports)

    metadata = {
        "batch_id": resolved_batch_id,
        "query_file": query_file,
        "created_at": datetime.now().isoformat(),
        "total_runs": len(run_exports),
        "included_runs": [row["run_id"] for row in summary_rows if row["included_in_batch"] == "true"],
        "excluded_runs": [row["run_id"] for row in summary_rows if row["included_in_batch"] == "false"],
        "exported_lead_count": len(combined_records),
        "copied_screenshot_count": copied_screenshot_count,
        "deleted_run_export_dirs": deleted_paths,
        "failed_cleanup_run_export_dirs": failed_cleanup_paths,
    }
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    print(f"Batch export created: {export_dir}")
    print(f"Included runs: {metadata['included_runs']}")
    print(f"Excluded runs: {metadata['excluded_runs']}")
    print(f"Exported leads: {len(combined_records)}")
    print(f"Copied screenshots: {copied_screenshot_count}")
    print(f"Batch summary CSV: {summary_path}")
    print(f"Combined JSON: {json_path}")
    print(f"Combined CSV: {csv_path}")
    if deleted_paths:
        print(f"Deleted per-run export folders: {len(deleted_paths)}")
    if failed_cleanup_paths:
        print(f"Warning: failed to delete {len(failed_cleanup_paths)} per-run export folder(s)")

    return BatchExportResult(
        batch_id=resolved_batch_id,
        export_dir=export_dir,
        json_path=json_path,
        csv_path=csv_path,
        summary_path=summary_path,
        screenshot_dir=screenshot_dir,
        metadata_path=metadata_path,
        included_run_count=len(metadata["included_runs"]),
        excluded_run_count=len(metadata["excluded_runs"]),
        exported_lead_count=len(combined_records),
        deleted_run_export_dirs=deleted_paths,
        failed_cleanup_run_export_dirs=failed_cleanup_paths,
    )
