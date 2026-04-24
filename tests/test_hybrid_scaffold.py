"""Focused tests for the preservation-first hybrid scaffold."""

from __future__ import annotations

import unittest
from collections import Counter
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.judging.package_builder import build_business_judging_package, post_browser_evidence_gate
from app.judging.runner import run_final_judgment
from app.models import Artifact, Business, Page, PipelineRun
from app.pipeline_runs import create_pipeline_run
from app.scoring.run_prefilter import run_prefilter


class HybridScaffoldTests(unittest.TestCase):
    """Verify the new scaffold keeps deterministic behavior while decoupling prefiltering."""

    def setUp(self) -> None:
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(bind=self.engine)
        self.Session = sessionmaker(bind=self.engine, autoflush=False, autocommit=False)

    def tearDown(self) -> None:
        self.engine.dispose()

    def test_pipeline_run_defaults_to_model_judge(self) -> None:
        with self.Session() as session:
            default_run_id = create_pipeline_run(session, query="painters lowell ma", niche="painters")
            compare_run_id = create_pipeline_run(
                session,
                query="painters lowell ma",
                niche="painters",
                scoring_mode="compare",
            )

            default_run = session.get(PipelineRun, default_run_id)
            compare_run = session.get(PipelineRun, compare_run_id)

            self.assertIsNotNone(default_run)
            self.assertIsNotNone(compare_run)
            assert default_run is not None
            assert compare_run is not None
            self.assertEqual(default_run.scoring_mode, "model_judge")
            self.assertEqual(compare_run.scoring_mode, "compare")

    def test_prefilter_updates_prefilter_fields_without_overwriting_final_status(self) -> None:
        with self.Session() as session:
            run_id = create_pipeline_run(session, query="painters lowell ma", niche="painters")
            business = Business(
                name="Acme Painting",
                website="https://acme.example.com",
                primary_type="painter",
                review_count=15,
                rating=4.8,
                discovery_run_id=run_id,
                fit_status="strong",
                skip_reason="Existing final judgment should remain untouched",
            )
            session.add(business)
            session.commit()

        with patch("app.scoring.deterministic.prefilter.SessionLocal", self.Session):
            counts = run_prefilter(run_id=run_id)

        self.assertEqual(counts, Counter({"strong": 1}))

        with self.Session() as session:
            refreshed = session.query(Business).one()
            self.assertEqual(refreshed.prefilter_status, "strong")
            self.assertIsNone(refreshed.prefilter_reason)
            self.assertEqual(refreshed.fit_status, "strong")
            self.assertEqual(
                refreshed.skip_reason,
                "Existing final judgment should remain untouched",
            )

    def test_final_judgment_dispatches_by_scoring_mode(self) -> None:
        with patch(
            "app.judging.runner.run_deterministic_scoring",
            return_value=Counter({"strong": 1}),
        ) as deterministic_mock, patch(
            "app.judging.runner.run_model_judging",
            return_value=Counter({"maybe": 1}),
        ) as model_mock, patch(
            "app.judging.runner.run_compare_mode",
            return_value=Counter({"skip": 1}),
        ) as compare_mock:
            deterministic_result = run_final_judgment(run_id=11, scoring_mode="deterministic")
            model_result = run_final_judgment(run_id=12, scoring_mode="model_judge")
            compare_result = run_final_judgment(run_id=13, scoring_mode="compare")

        self.assertEqual(deterministic_result["strong"], 1)
        self.assertEqual(model_result["maybe"], 1)
        self.assertEqual(compare_result["skip"], 1)
        deterministic_mock.assert_called_once_with(run_id=11)
        model_mock.assert_any_call(run_id=12, finalize_business=True, judgment_mode="model_judge")
        compare_mock.assert_called_once_with(run_id=13)

    def test_prefilter_skips_duplicate_canonical_website_after_first_business(self) -> None:
        with self.Session() as session:
            run_id = create_pipeline_run(session, query="painters lowell ma", niche="painters")
            session.add_all(
                [
                    Business(
                        name="Acme Painting",
                        website="https://acme.example.com",
                        primary_type="painter",
                        discovery_run_id=run_id,
                    ),
                    Business(
                        name="Acme Painting North",
                        website="https://www.acme.example.com/about",
                        primary_type="painter",
                        discovery_run_id=run_id,
                    ),
                ]
            )
            session.commit()

        with patch("app.scoring.deterministic.prefilter.SessionLocal", self.Session):
            counts = run_prefilter(run_id=run_id)

        self.assertEqual(counts["strong"], 1)
        self.assertEqual(counts["skip"], 1)

        with self.Session() as session:
            rows = session.query(Business).order_by(Business.id.asc()).all()
            self.assertEqual(rows[0].prefilter_status, "strong")
            self.assertEqual(rows[1].prefilter_status, "skip")
            self.assertIn("Duplicate website", rows[1].prefilter_reason or "")

    def test_judging_package_is_compact_and_structured(self) -> None:
        with TemporaryDirectory() as temp_dir:
            desktop_path = Path(temp_dir) / "desktop.png"
            mobile_path = Path(temp_dir) / "mobile.png"
            desktop_path.write_bytes(b"desktop")
            mobile_path.write_bytes(b"mobile")

            with self.Session() as session:
                run_id = create_pipeline_run(session, query="painters lowell ma", niche="painters")
                business = Business(
                    name="Acme Painting",
                    website="https://acme.example.com",
                    canonical_url="https://acme.example.com",
                    primary_type="painter",
                    rating=4.7,
                    review_count=19,
                    niche="painters",
                    query_used="painters lowell ma",
                    address="Lowell, MA",
                    discovery_run_id=run_id,
                    prefilter_status="strong",
                )
                session.add(business)
                session.flush()

                session.add_all(
                    [
                        Page(
                            business_id=business.id,
                            page_type="home",
                            url="https://acme.example.com",
                            raw_text=("Home copy " * 120),
                        ),
                        Page(
                            business_id=business.id,
                            page_type="services",
                            url="https://acme.example.com/services",
                            raw_text=("Services copy " * 80),
                        ),
                        Page(
                            business_id=business.id,
                            page_type="contact",
                            url="https://acme.example.com/contact",
                            raw_text="Call us today.",
                        ),
                        Artifact(
                            business_id=business.id,
                            artifact_type="desktop_home_screenshot",
                            file_path=desktop_path.as_posix(),
                        ),
                        Artifact(
                            business_id=business.id,
                            artifact_type="mobile_home_screenshot",
                            file_path=mobile_path.as_posix(),
                        ),
                    ]
                )
                session.commit()

                with patch(
                    "app.judging.package_builder._load_browser_report",
                    return_value={
                        "homepage_signals": {
                            "homepage_loaded": True,
                            "phone_visible": True,
                            "tel_link_present": True,
                            "cta_visible_near_top": False,
                        },
                        "page_loads": {"home": True, "services": True},
                    },
                ):
                    package = build_business_judging_package(
                        session,
                        business=business,
                        pipeline_run_id=run_id,
                    )

        self.assertEqual(package.business_name, "Acme Painting")
        self.assertEqual(package.canonical_url, "https://acme.example.com/")
        self.assertEqual(package.pages_captured_count, 3)
        self.assertEqual(package.screenshots_captured_count, 2)
        self.assertTrue(package.evidence_summary["has_desktop_screenshot"])
        self.assertTrue(package.evidence_summary["has_service_text"])
        self.assertEqual(package.pages_found["services"], "https://acme.example.com/services")
        self.assertLessEqual(len(package.text_excerpts["home"] or ""), 360)
        self.assertTrue(package.diagnostics["phone_visible"])
        self.assertFalse(package.diagnostics["franchise_flag"])
        self.assertIsNone(post_browser_evidence_gate(package))

    def test_post_browser_evidence_gate_skips_when_no_usable_evidence_exists(self) -> None:
        with self.Session() as session:
            run_id = create_pipeline_run(session, query="painters lowell ma", niche="painters")
            business = Business(
                name="Evidence Gap Painting",
                website="https://evidence-gap.example.com",
                canonical_url="https://evidence-gap.example.com",
                primary_type="painter",
                discovery_run_id=run_id,
                prefilter_status="strong",
            )
            session.add(business)
            session.commit()

            with patch(
                "app.judging.package_builder._load_browser_report",
                return_value={
                    "homepage_signals": {
                        "homepage_loaded": False,
                        "phone_visible": False,
                        "tel_link_present": False,
                        "cta_visible_near_top": False,
                    },
                    "page_loads": {"home": False},
                },
            ):
                package = build_business_judging_package(
                    session,
                    business=business,
                    pipeline_run_id=run_id,
                )

        reason = post_browser_evidence_gate(package)
        self.assertIsNotNone(reason)
        assert reason is not None
        self.assertIn("Homepage could not be loaded", reason)


if __name__ == "__main__":
    unittest.main()
