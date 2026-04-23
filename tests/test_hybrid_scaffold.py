"""Focused tests for the preservation-first hybrid scaffold."""

from __future__ import annotations

import unittest
from collections import Counter
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.judging.runner import run_final_judgment
from app.models import Business, PipelineRun
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
        model_mock.assert_called_once_with(run_id=12, finalize_business=True, judgment_mode="primary")
        compare_mock.assert_called_once_with(run_id=13)


if __name__ == "__main__":
    unittest.main()
