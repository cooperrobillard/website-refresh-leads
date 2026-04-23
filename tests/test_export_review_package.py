"""Focused tests for current-run review package exports."""

from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import Artifact, Business, Note, Page, Score
from app.pipeline_runs import create_pipeline_run
from app.reports import export_review_package as export_module


class ExportReviewPackageTests(unittest.TestCase):
    """Verify exports stay aligned with current-run new candidates."""

    def setUp(self) -> None:
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(bind=self.engine)
        self.Session = sessionmaker(bind=self.engine, autoflush=False, autocommit=False)

    def tearDown(self) -> None:
        self.engine.dispose()

    def test_export_only_includes_current_run_new_candidates(self) -> None:
        with TemporaryDirectory() as temp_dir:
            export_runs_dir = Path(temp_dir) / "exports" / "runs"
            export_runs_dir.mkdir(parents=True, exist_ok=True)

            with self.Session() as session:
                first_run_id = create_pipeline_run(session, query="painters lowell ma", niche="painters")
                second_run_id = create_pipeline_run(
                    session,
                    query="painters lowell ma",
                    niche="painters",
                    allow_revisit=True,
                )

                old_business = Business(
                    name="Old Acme Painting",
                    niche="painters",
                    query_used="painters lowell ma",
                    website="https://old-acme.example.com",
                    canonical_url="https://old-acme.example.com",
                    canonical_key="old-acme.example.com",
                    address="Lowell, MA",
                    primary_type="painter",
                    rating=4.9,
                    review_count=31,
                    fit_status="strong",
                    discovery_run_id=first_run_id,
                    last_seen_run_id=second_run_id,
                    eligible_for_revisit=True,
                )
                new_business = Business(
                    name="New Acme Painting",
                    niche="painters",
                    query_used="painters lowell ma",
                    website="https://new-acme.example.com",
                    canonical_url="https://new-acme.example.com",
                    canonical_key="new-acme.example.com",
                    address="Lowell, MA",
                    primary_type="painter",
                    rating=4.7,
                    review_count=18,
                    fit_status="maybe",
                    discovery_run_id=second_run_id,
                )
                session.add_all([old_business, new_business])
                session.flush()

                session.add_all(
                    [
                        Score(
                            business_id=old_business.id,
                            business_legitimacy=15,
                            website_weakness=6,
                            conversion_opportunity=5,
                            trust_packaging=5,
                            complexity_fit=12,
                            outreach_viability=12,
                            outreach_story_strength=10,
                            raw_total_score=65,
                            evidence_tier="strong",
                            evidence_cap=100,
                            total_score=65,
                            fit_status="strong",
                            confidence="high",
                        ),
                        Score(
                            business_id=new_business.id,
                            business_legitimacy=14,
                            website_weakness=4,
                            conversion_opportunity=3,
                            trust_packaging=4,
                            complexity_fit=12,
                            outreach_viability=11,
                            outreach_story_strength=7,
                            raw_total_score=55,
                            evidence_tier="medium",
                            evidence_cap=72,
                            total_score=55,
                            fit_status="maybe",
                            confidence="medium",
                        ),
                        Note(
                            business_id=old_business.id,
                            quick_summary="Old lead from a prior run.",
                            top_issues="Legacy issue",
                            teardown_angle="Not relevant for this export.",
                        ),
                        Note(
                            business_id=new_business.id,
                            quick_summary="Legitimate local business with fair refresh upside.",
                            top_issues="Thin trust signals\nDated brochure presentation",
                            teardown_angle="Lead with the outdated presentation and weak trust packaging.",
                        ),
                        Page(
                            business_id=new_business.id,
                            page_type="home",
                            url="https://new-acme.example.com",
                            raw_text="New Acme Painting serves Lowell homeowners.",
                        ),
                        Page(
                            business_id=new_business.id,
                            page_type="contact",
                            url="https://new-acme.example.com/contact",
                            raw_text="Call us today for a free estimate.",
                        ),
                    ]
                )
                session.commit()

            with patch.object(export_module, "SessionLocal", self.Session), patch.object(
                export_module,
                "EXPORT_RUNS_DIR",
                export_runs_dir,
            ):
                records = export_module.export_review_package(
                    limit=10,
                    include_maybe=True,
                    run_id=second_run_id,
                )

            self.assertEqual(len(records), 1)
            record = records[0]
            self.assertEqual(record["business_name"], "New Acme Painting")
            self.assertEqual(record["discovery_run_id"], second_run_id)
            self.assertTrue(record["new_this_run"])
            self.assertEqual(record["query_used"], "painters lowell ma")
            self.assertEqual(record["canonical_url"], "https://new-acme.example.com/")
            self.assertEqual(record["canonical_key"], "new-acme.example.com")
            self.assertEqual(record["raw_total_score"], 55)
            self.assertEqual(record["evidence_tier"], "medium")
            self.assertEqual(record["evidence_cap"], 72)
            self.assertEqual(record["scores"]["outreach_story_strength"], 7)
            self.assertEqual(record["pages_captured"], 2)
            self.assertEqual(record["screenshots_captured"], 0)
            self.assertEqual(
                record["review_context"]["top_scoring_dimensions"][0]["dimension"],
                "business_legitimacy",
            )
            self.assertEqual(record["review_context"]["evidence"]["tier"], "medium")
            self.assertEqual(record["review_context"]["outreach_story"]["assessment"], "fair")
            self.assertEqual(
                record["review_context"]["outreach_story"]["primary_gaps"],
                ["Thin trust signals", "Dated brochure presentation"],
            )

            run_export_dir = export_runs_dir / f"run_{second_run_id}"
            json_path = run_export_dir / "review_package.json"
            csv_path = run_export_dir / "review_package.csv"
            screenshot_dir = run_export_dir / "review_screenshots"
            self.assertTrue(run_export_dir.exists())
            self.assertTrue(json_path.exists())
            self.assertTrue(csv_path.exists())
            self.assertTrue(screenshot_dir.exists())
            exported_records = json.loads(json_path.read_text(encoding="utf-8"))
            self.assertEqual([row["business_name"] for row in exported_records], ["New Acme Painting"])

    def test_export_skip_fallback_stays_with_current_run_candidates(self) -> None:
        with TemporaryDirectory() as temp_dir:
            export_runs_dir = Path(temp_dir) / "exports" / "runs"
            export_runs_dir.mkdir(parents=True, exist_ok=True)

            with self.Session() as session:
                first_run_id = create_pipeline_run(session, query="painters lowell ma", niche="painters")
                second_run_id = create_pipeline_run(session, query="painters lowell ma", niche="painters")

                prior_business = Business(
                    name="Prior Strong Lead",
                    niche="painters",
                    query_used="painters lowell ma",
                    website="https://prior-strong.example.com",
                    canonical_url="https://prior-strong.example.com",
                    canonical_key="prior-strong.example.com",
                    address="Lowell, MA",
                    primary_type="painter",
                    rating=4.9,
                    review_count=40,
                    fit_status="strong",
                    discovery_run_id=first_run_id,
                )
                current_skip = Business(
                    name="Current Skip Lead",
                    niche="painters",
                    query_used="painters lowell ma",
                    website="https://current-skip.example.com",
                    canonical_url="https://current-skip.example.com",
                    canonical_key="current-skip.example.com",
                    address="Lowell, MA",
                    primary_type="painter",
                    rating=4.6,
                    review_count=12,
                    fit_status="skip",
                    skip_reason="Score below review threshold: 42",
                    discovery_run_id=second_run_id,
                )
                session.add_all([prior_business, current_skip])
                session.flush()

                session.add_all(
                    [
                        Score(
                            business_id=prior_business.id,
                            business_legitimacy=15,
                            website_weakness=8,
                            conversion_opportunity=7,
                            trust_packaging=6,
                            complexity_fit=12,
                            outreach_viability=12,
                            outreach_story_strength=10,
                            raw_total_score=70,
                            evidence_tier="strong",
                            evidence_cap=100,
                            total_score=70,
                            fit_status="strong",
                            confidence="high",
                        ),
                        Score(
                            business_id=current_skip.id,
                            business_legitimacy=12,
                            website_weakness=3,
                            conversion_opportunity=2,
                            trust_packaging=4,
                            complexity_fit=12,
                            outreach_viability=9,
                            outreach_story_strength=0,
                            raw_total_score=42,
                            evidence_tier="medium",
                            evidence_cap=72,
                            total_score=42,
                            fit_status="skip",
                            confidence="medium",
                        ),
                    ]
                )
                session.commit()

            with patch.object(export_module, "SessionLocal", self.Session), patch.object(
                export_module,
                "EXPORT_RUNS_DIR",
                export_runs_dir,
            ):
                records = export_module.export_review_package(
                    limit=10,
                    include_maybe=True,
                    fallback_to_skips=True,
                    run_id=second_run_id,
                )

            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["business_name"], "Current Skip Lead")
            self.assertEqual(records[0]["fit_status"], "skip")

    def test_exports_for_two_runs_use_different_folders_and_keep_screenshots(self) -> None:
        with TemporaryDirectory() as temp_dir:
            export_runs_dir = Path(temp_dir) / "exports" / "runs"
            export_runs_dir.mkdir(parents=True, exist_ok=True)
            source_dir = Path(temp_dir) / "source_screens"
            source_dir.mkdir(parents=True, exist_ok=True)

            desktop_source = source_dir / "desktop.png"
            mobile_source = source_dir / "mobile.png"
            desktop_source.write_bytes(b"desktop")
            mobile_source.write_bytes(b"mobile")

            with self.Session() as session:
                first_run_id = create_pipeline_run(session, query="painters lowell ma", niche="painters")
                second_run_id = create_pipeline_run(session, query="painters chelmsford ma", niche="painters")

                first_business = Business(
                    name="Acme Painting",
                    niche="painters",
                    query_used="painters lowell ma",
                    website="https://acme-lowell.example.com",
                    canonical_url="https://acme-lowell.example.com",
                    canonical_key="acme-lowell.example.com",
                    address="Lowell, MA",
                    primary_type="painter",
                    rating=4.8,
                    review_count=21,
                    fit_status="maybe",
                    discovery_run_id=first_run_id,
                )
                second_business = Business(
                    name="Acme Painting",
                    niche="painters",
                    query_used="painters chelmsford ma",
                    website="https://acme-chelmsford.example.com",
                    canonical_url="https://acme-chelmsford.example.com",
                    canonical_key="acme-chelmsford.example.com",
                    address="Chelmsford, MA",
                    primary_type="painter",
                    rating=4.7,
                    review_count=19,
                    fit_status="maybe",
                    discovery_run_id=second_run_id,
                )
                session.add_all([first_business, second_business])
                session.flush()

                for business in [first_business, second_business]:
                    session.add(
                        Score(
                            business_id=business.id,
                            business_legitimacy=14,
                            website_weakness=4,
                            conversion_opportunity=3,
                            trust_packaging=4,
                            complexity_fit=12,
                            outreach_viability=10,
                            outreach_story_strength=7,
                            raw_total_score=54,
                            evidence_tier="medium",
                            evidence_cap=72,
                            total_score=54,
                            fit_status="maybe",
                            confidence="medium",
                        )
                    )
                    session.add(
                        Artifact(
                            business_id=business.id,
                            artifact_type="desktop_home_screenshot",
                            file_path=desktop_source.as_posix(),
                        )
                    )
                    session.add(
                        Artifact(
                            business_id=business.id,
                            artifact_type="mobile_home_screenshot",
                            file_path=mobile_source.as_posix(),
                        )
                    )
                session.commit()

            with patch.object(export_module, "SessionLocal", self.Session), patch.object(
                export_module,
                "EXPORT_RUNS_DIR",
                export_runs_dir,
            ):
                first_records = export_module.export_review_package(limit=10, include_maybe=True, run_id=first_run_id)
                second_records = export_module.export_review_package(limit=10, include_maybe=True, run_id=second_run_id)

            self.assertEqual(len(first_records), 1)
            self.assertEqual(len(second_records), 1)

            first_dir = export_runs_dir / f"run_{first_run_id}"
            second_dir = export_runs_dir / f"run_{second_run_id}"
            self.assertNotEqual(first_dir, second_dir)
            self.assertTrue((first_dir / "review_package.json").exists())
            self.assertTrue((second_dir / "review_package.json").exists())

            first_screenshot_dir = first_dir / "review_screenshots"
            second_screenshot_dir = second_dir / "review_screenshots"
            self.assertTrue(first_screenshot_dir.exists())
            self.assertTrue(second_screenshot_dir.exists())

            first_files = sorted(path.name for path in first_screenshot_dir.iterdir())
            second_files = sorted(path.name for path in second_screenshot_dir.iterdir())
            self.assertEqual(first_files, ["acme-painting_desktop.png", "acme-painting_mobile.png"])
            self.assertEqual(second_files, ["acme-painting_desktop.png", "acme-painting_mobile.png"])
            self.assertTrue((first_screenshot_dir / "acme-painting_desktop.png").exists())
            self.assertTrue((second_screenshot_dir / "acme-painting_desktop.png").exists())


if __name__ == "__main__":
    unittest.main()
