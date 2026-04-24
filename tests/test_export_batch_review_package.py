"""Tests for query-file batch review exports."""

from __future__ import annotations

import csv
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from app.reports import export_batch_review_package as batch_module


def build_record(
    *,
    business_id: int,
    run_id: int,
    business_name: str,
    query: str,
    niche: str,
    desktop_path: str,
    mobile_path: str,
    fit_status: str = "strong",
) -> dict[str, object]:
    """Build a compact review record fixture."""
    return {
        "business_id": business_id,
        "discovery_run_id": run_id,
        "new_this_run": True,
        "business_name": business_name,
        "niche": niche,
        "query_used": query,
        "location": "Lowell, MA",
        "website": f"https://{business_id}.example.com/",
        "canonical_url": f"https://{business_id}.example.com/",
        "canonical_key": f"{business_id}.example.com",
        "primary_type": "painter",
        "google_rating": 4.8,
        "google_review_count": 24,
        "fit_status": fit_status,
        "total_score": 62,
        "raw_total_score": 62,
        "confidence": "high",
        "evidence_tier": "medium",
        "evidence_cap": 72,
        "pages_captured": 2,
        "screenshots_captured": 2,
        "scores": {
            "business_legitimacy": 14,
            "website_weakness": 8,
            "conversion_opportunity": 6,
            "trust_packaging": 6,
            "complexity_fit": 12,
            "outreach_viability": 10,
            "outreach_story_strength": 8,
        },
        "review_context": {
            "why_it_qualified": "Legitimate local business with a clear refresh story.",
            "top_scoring_dimensions": [
                {"dimension": "business_legitimacy", "score": 14},
            ],
            "evidence": {
                "tier": "medium",
                "confidence": "high",
                "cap": 72,
                "raw_total_score": 62,
                "pages_captured": 2,
                "screenshots_captured": 2,
            },
            "outreach_story": {
                "strength_score": 8,
                "assessment": "fair",
                "primary_gaps": ["Thin trust signals"],
            },
        },
        "pages_found": {
            "home": f"https://{business_id}.example.com/",
            "about": f"https://{business_id}.example.com/about",
            "services": None,
            "contact": None,
            "gallery": None,
            "faq": None,
        },
        "screenshots": {
            "homepage_desktop": desktop_path,
            "homepage_mobile": mobile_path,
        },
        "export_screenshots": {
            "homepage_desktop": desktop_path,
            "homepage_mobile": mobile_path,
        },
        "top_issues": ["Thin trust signals"],
        "quick_summary": "Legitimate local business with visible refresh upside.",
        "teardown_angle": "Lead with the trust gap and dated presentation.",
        "skip_reason": None,
        "final_source": "deterministic",
        "scoring_mode": "deterministic",
        "model_name": None,
        "prompt_version": None,
        "response_id": None,
        "evidence_quality": "medium",
        "recommended_action": "review_for_outreach",
        "positive_signals": [],
        "evidence_warnings": [],
        "deterministic_compare": None,
    }


class ExportBatchReviewPackageTests(unittest.TestCase):
    """Verify batch exports aggregate current query-file run outputs safely."""

    def test_batch_export_combines_non_empty_runs_and_deletes_current_run_folders(self) -> None:
        with TemporaryDirectory() as temp_dir:
            export_root = Path(temp_dir) / "exports"
            export_runs_dir = export_root / "runs"
            export_batches_dir = export_root / "batches"
            export_runs_dir.mkdir(parents=True, exist_ok=True)
            export_batches_dir.mkdir(parents=True, exist_ok=True)

            run_101_dir = export_runs_dir / "run_101"
            run_102_dir = export_runs_dir / "run_102"
            run_103_dir = export_runs_dir / "run_103"
            for run_dir in [run_101_dir, run_102_dir, run_103_dir]:
                screenshot_dir = run_dir / "review_screenshots"
                screenshot_dir.mkdir(parents=True, exist_ok=True)
                (run_dir / "review_package.json").write_text("[]", encoding="utf-8")
                (run_dir / "review_package.csv").write_text("", encoding="utf-8")

            run_101_desktop = run_101_dir / "review_screenshots" / "desktop.png"
            run_101_mobile = run_101_dir / "review_screenshots" / "mobile.png"
            run_103_desktop = run_103_dir / "review_screenshots" / "desktop.png"
            run_103_mobile = run_103_dir / "review_screenshots" / "mobile.png"
            for path in [run_101_desktop, run_101_mobile, run_103_desktop, run_103_mobile]:
                path.write_bytes(b"image")

            run_exports = [
                batch_module.RunBatchExport(
                    run_id=101,
                    query="painters lowell ma",
                    niche="painters",
                    scoring_mode="deterministic",
                    inserted_new=3,
                    records=[
                        build_record(
                            business_id=1,
                            run_id=101,
                            business_name="Acme Painting",
                            query="painters lowell ma",
                            niche="painters",
                            desktop_path=run_101_desktop.as_posix(),
                            mobile_path=run_101_mobile.as_posix(),
                            fit_status="strong",
                        )
                    ],
                    export_dir=run_101_dir,
                ),
                batch_module.RunBatchExport(
                    run_id=102,
                    query="painters dracut ma",
                    niche="painters",
                    scoring_mode="deterministic",
                    inserted_new=0,
                    records=[],
                    export_dir=run_102_dir,
                ),
                batch_module.RunBatchExport(
                    run_id=103,
                    query="painters chelmsford ma",
                    niche="painters",
                    scoring_mode="deterministic",
                    inserted_new=2,
                    records=[
                        build_record(
                            business_id=2,
                            run_id=103,
                            business_name="Acme Painting",
                            query="painters chelmsford ma",
                            niche="painters",
                            desktop_path=run_103_desktop.as_posix(),
                            mobile_path=run_103_mobile.as_posix(),
                            fit_status="maybe",
                        )
                    ],
                    export_dir=run_103_dir,
                ),
            ]

            with patch.object(batch_module, "EXPORT_RUNS_DIR", export_runs_dir), patch.object(
                batch_module,
                "EXPORT_BATCHES_DIR",
                export_batches_dir,
            ):
                result = batch_module.export_batch_review_package(
                    run_exports=run_exports,
                    query_file="prompts/queries.txt",
                    batch_id="batch_test_queries",
                )

            self.assertEqual(result.included_run_count, 2)
            self.assertEqual(result.excluded_run_count, 1)
            self.assertEqual(result.exported_lead_count, 2)

            combined_records = json.loads(result.json_path.read_text(encoding="utf-8"))
            self.assertEqual([row["source_run_id"] for row in combined_records], [101, 103])
            self.assertEqual(
                [row["source_query"] for row in combined_records],
                ["painters lowell ma", "painters chelmsford ma"],
            )

            screenshot_files = sorted(path.name for path in result.screenshot_dir.iterdir())
            self.assertEqual(
                screenshot_files,
                [
                    "acme-painting_run101_desktop.png",
                    "acme-painting_run101_mobile.png",
                    "acme-painting_run103_desktop.png",
                    "acme-painting_run103_mobile.png",
                ],
            )

            with result.summary_path.open(newline="", encoding="utf-8") as file_handle:
                summary_rows = list(csv.DictReader(file_handle))

            self.assertEqual(len(summary_rows), 3)
            self.assertEqual(summary_rows[1]["run_id"], "102")
            self.assertEqual(summary_rows[1]["included_in_batch"], "false")
            self.assertEqual(summary_rows[1]["reason_excluded"], "empty export")

            self.assertFalse(run_101_dir.exists())
            self.assertFalse(run_102_dir.exists())
            self.assertFalse(run_103_dir.exists())

            metadata = json.loads(result.metadata_path.read_text(encoding="utf-8"))
            self.assertEqual(metadata["deleted_run_export_dirs"], result.deleted_run_export_dirs)
            self.assertEqual(metadata["failed_cleanup_run_export_dirs"], [])

    def test_batch_export_failure_keeps_per_run_export_folders(self) -> None:
        with TemporaryDirectory() as temp_dir:
            export_root = Path(temp_dir) / "exports"
            export_runs_dir = export_root / "runs"
            export_batches_dir = export_root / "batches"
            export_runs_dir.mkdir(parents=True, exist_ok=True)
            export_batches_dir.mkdir(parents=True, exist_ok=True)

            run_dir = export_runs_dir / "run_201"
            screenshot_dir = run_dir / "review_screenshots"
            screenshot_dir.mkdir(parents=True, exist_ok=True)
            desktop_path = screenshot_dir / "desktop.png"
            mobile_path = screenshot_dir / "mobile.png"
            desktop_path.write_bytes(b"desktop")
            mobile_path.write_bytes(b"mobile")

            run_export = batch_module.RunBatchExport(
                run_id=201,
                query="painters lowell ma",
                niche="painters",
                scoring_mode="deterministic",
                inserted_new=1,
                records=[
                    build_record(
                        business_id=9,
                        run_id=201,
                        business_name="Keep My Folder Painting",
                        query="painters lowell ma",
                        niche="painters",
                        desktop_path=desktop_path.as_posix(),
                        mobile_path=mobile_path.as_posix(),
                    )
                ],
                export_dir=run_dir,
            )

            with patch.object(batch_module, "EXPORT_RUNS_DIR", export_runs_dir), patch.object(
                batch_module,
                "EXPORT_BATCHES_DIR",
                export_batches_dir,
            ), patch.object(
                batch_module,
                "write_batch_combined_csv",
                side_effect=RuntimeError("csv write failed"),
            ):
                with self.assertRaisesRegex(RuntimeError, "csv write failed"):
                    batch_module.export_batch_review_package(
                        run_exports=[run_export],
                        query_file="prompts/queries.txt",
                        batch_id="batch_failure_case",
                    )

            self.assertTrue(run_dir.exists())
            self.assertFalse((export_batches_dir / "batch_failure_case").exists())


if __name__ == "__main__":
    unittest.main()
