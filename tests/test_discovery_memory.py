"""Focused tests for cross-run canonical discovery memory."""

from __future__ import annotations

import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.db import Base
from app.discovery.places import upsert_businesses
from app.models import Business, PipelineRun
from app.pipeline_runs import businesses_for_run_query, create_pipeline_run


class DiscoveryMemoryTests(unittest.TestCase):
    """Verify canonical-site exclusion and revisit plumbing."""

    def setUp(self) -> None:
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(bind=self.engine)
        self.Session = sessionmaker(bind=self.engine, autoflush=False, autocommit=False)

    def tearDown(self) -> None:
        self.engine.dispose()

    def test_default_cross_run_exclusion_skips_existing_canonical_sites(self) -> None:
        with self.Session() as session:
            first_run = create_pipeline_run(session, query="painters lowell ma", niche="painters")
            first_counts = upsert_businesses(
                session=session,
                places=[
                    {
                        "id": "place-1",
                        "displayName": {"text": "Acme Painting"},
                        "websiteUri": "https://www.acmepainting.com/services?utm_source=maps",
                        "formattedAddress": "Lowell, MA",
                        "primaryType": "painter",
                        "rating": 4.8,
                        "userRatingCount": 19,
                    }
                ],
                niche="painters",
                query_used="painters lowell ma",
                current_run=first_run,
            )
            self.assertEqual(first_counts["inserted"], 1)

            second_run = create_pipeline_run(session, query="painters lowell ma", niche="painters")
            second_counts = upsert_businesses(
                session=session,
                places=[
                    {
                        "id": "place-2",
                        "displayName": {"text": "Acme Painting LLC"},
                        "websiteUri": "http://acmepainting.com/about#team",
                        "formattedAddress": "Lowell, MA",
                        "primaryType": "painter",
                        "rating": 4.9,
                        "userRatingCount": 22,
                    }
                ],
                niche="painters",
                query_used="painters lowell ma",
                current_run=second_run,
            )

            self.assertEqual(second_counts["inserted"], 0)
            self.assertEqual(second_counts["skipped_existing_processed"], 1)

            businesses = session.query(Business).order_by(Business.id.asc()).all()
            self.assertEqual(len(businesses), 1)
            self.assertEqual(businesses[0].discovery_run_id, first_run.id)
            self.assertEqual(businesses[0].canonical_key, "acmepainting.com")
            self.assertEqual(businesses[0].last_seen_run_id, second_run.id)

    def test_allow_revisit_only_re_admits_eligible_businesses(self) -> None:
        with self.Session() as session:
            first_run = create_pipeline_run(session, query="painters lowell ma", niche="painters")
            upsert_businesses(
                session=session,
                places=[
                    {
                        "id": "place-1",
                        "displayName": {"text": "Acme Painting"},
                        "websiteUri": "https://acmepainting.com",
                        "formattedAddress": "Lowell, MA",
                        "primaryType": "painter",
                        "rating": 4.8,
                        "userRatingCount": 19,
                    }
                ],
                niche="painters",
                query_used="painters lowell ma",
                current_run=first_run,
            )

            business = session.query(Business).first()
            self.assertIsNotNone(business)
            assert business is not None
            business.eligible_for_revisit = True
            session.commit()

            revisit_run = create_pipeline_run(
                session,
                query="painters lowell ma",
                niche="painters",
                allow_revisit=True,
            )
            counts = upsert_businesses(
                session=session,
                places=[
                    {
                        "id": "place-1",
                        "displayName": {"text": "Acme Painting"},
                        "websiteUri": "https://www.acmepainting.com/contact",
                        "formattedAddress": "Lowell, MA",
                        "primaryType": "painter",
                        "rating": 4.8,
                        "userRatingCount": 20,
                    }
                ],
                niche="painters",
                query_used="painters lowell ma",
                current_run=revisit_run,
            )

            self.assertEqual(counts["skipped_existing_processed"], 0)
            scoped_businesses = businesses_for_run_query(session, revisit_run).all()
            self.assertEqual([row.id for row in scoped_businesses], [business.id])


if __name__ == "__main__":
    unittest.main()
