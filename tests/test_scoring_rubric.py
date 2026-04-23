"""Focused tests for the deterministic scoring rubric."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from app.models import Business, Page
from app.scoring.rules import passes_basic_filters
from app.scoring.rubric import (
    evaluate_business,
    score_outreach_story_strength,
    score_website_weakness,
)


def build_report(
    *,
    homepage_loaded: bool = True,
    phone_visible: bool = False,
    tel_link_present: bool = False,
    cta_visible_near_top: bool = False,
) -> dict[str, object]:
    """Build a minimal browser report fixture."""
    return {
        "success": True,
        "homepage_signals": {
            "homepage_loaded": homepage_loaded,
            "phone_visible": phone_visible,
            "tel_link_present": tel_link_present,
            "cta_visible_near_top": cta_visible_near_top,
        },
        "page_loads": {"home": homepage_loaded},
    }


class ScoringRubricTests(unittest.TestCase):
    """Verify the new scoring behavior stays deterministic and practical."""

    def test_prefilter_skips_known_franchise_brand(self) -> None:
        business = Business(
            name="CertaPro Painters of Andover, MA",
            website="https://certapro.com/andover/landing-page/residential-gbp-landing",
            primary_type="painter",
            review_count=24,
            rating=4.7,
        )

        result = passes_basic_filters(business)

        self.assertEqual(result.fit_status, "skip")
        self.assertIsNotNone(result.reason)
        assert result.reason is not None
        self.assertIn("Franchise", result.reason)

    def test_structured_brochure_site_gets_lower_weakness_score(self) -> None:
        business = Business(
            name="Acme Painting",
            website="https://acmepainting.com",
            primary_type="painter",
            review_count=25,
            rating=4.8,
            address="Lowell, MA",
        )
        report = build_report(
            homepage_loaded=True,
            phone_visible=True,
            tel_link_present=True,
            cta_visible_near_top=True,
        )

        thin_page_map = {
            "home": Page(
                page_type="home",
                url="https://acmepainting.com",
                raw_text="Welcome to Acme Painting. Call us today for professional service.",
            )
        }
        structured_page_map = {
            "home": Page(
                page_type="home",
                url="https://acmepainting.com",
                raw_text=(
                    "Acme Painting provides residential and commercial painting services in Lowell. "
                    "Get a free estimate, review our process, and see why local homeowners trust our team. "
                    "Services About Testimonials Contact Why Choose Us Our Process "
                )
                * 6,
            ),
            "services": Page(
                page_type="services",
                url="https://acmepainting.com/services",
                raw_text="Interior painting exterior painting cabinet painting deck staining service areas.",
            ),
            "about": Page(
                page_type="about",
                url="https://acmepainting.com/about",
                raw_text="Family owned Lowell painting contractor serving local homeowners and businesses.",
            ),
            "contact": Page(
                page_type="contact",
                url="https://acmepainting.com/contact",
                raw_text="Call us today or request a quote for your next painting project.",
            ),
            "gallery": Page(
                page_type="gallery",
                url="https://acmepainting.com/gallery",
                raw_text="Project gallery before and after work customer reviews and testimonials.",
            ),
        }

        thin_score = score_website_weakness(business, thin_page_map, report)
        structured_score = score_website_weakness(business, structured_page_map, report)

        self.assertLess(structured_score, thin_score)

    def test_sparse_evidence_caps_high_raw_score(self) -> None:
        business = Business(
            name="Acme Painting",
            website="https://acmepainting.com",
            primary_type="painter",
            review_count=28,
            rating=4.8,
            address="Lowell, MA",
        )
        sparse_page_map = {
            "home": Page(
                page_type="home",
                url="https://acmepainting.com",
                raw_text=(
                    "Acme Painting serves Lowell homeowners and businesses. "
                    "Residential and commercial painting. Free estimates available now. "
                )
                * 4,
            )
        }
        sparse_report = build_report(
            homepage_loaded=True,
            phone_visible=False,
            tel_link_present=False,
            cta_visible_near_top=False,
        )

        with patch("app.scoring.rubric.get_pages", return_value=sparse_page_map), patch(
            "app.scoring.rubric.load_browser_report",
            return_value=sparse_report,
        ):
            result = evaluate_business(session=None, business=business)

        self.assertEqual(result.evidence_tier, "sparse")
        self.assertEqual(result.evidence_cap, 60)
        self.assertGreater(result.raw_total_score, result.total_score)
        self.assertLessEqual(result.total_score, 60)
        self.assertNotEqual(result.fit_status, "strong")

    def test_legacy_brochure_site_surfaces_as_maybe(self) -> None:
        business = Business(
            name="Acme Painting",
            website="https://acmepainting.com",
            primary_type="painter",
            review_count=23,
            rating=4.8,
            address="Lowell, MA",
        )
        report = build_report(
            homepage_loaded=True,
            phone_visible=True,
            tel_link_present=True,
            cta_visible_near_top=True,
        )
        legacy_page_map = {
            "home": Page(
                page_type="home",
                url="https://acmepainting.com",
                raw_text=(
                    "Your local residential painter. Contact Acme Painting for a free estimate. "
                    "Home Services Contact Estimates. We serve Lowell, Dracut, Chelmsford, and nearby towns. "
                    "Interior painting, ceiling painting, trim painting, and exterior touch-ups for homeowners. "
                )
                * 3,
            ),
            "services": Page(
                page_type="services",
                url="https://acmepainting.com/services",
                raw_text=(
                    "Interior painting exterior painting trim painting cabinet painting. "
                    "Service area Lowell Dracut Chelmsford Westford."
                )
                * 2,
            ),
            "contact": Page(
                page_type="contact",
                url="https://acmepainting.com/contact",
                raw_text="Call today for a free estimate and contact our Lowell painting company.",
            ),
        }
        polished_page_map = {
            "home": Page(
                page_type="home",
                url="https://acmepainting.com",
                raw_text=(
                    "Acme Painting provides residential and commercial painting services in Lowell. "
                    "Get a free estimate, review our process, and see why local homeowners trust our team. "
                    "Services About Testimonials Contact Why Choose Us Our Process "
                )
                * 6,
            ),
            "services": Page(
                page_type="services",
                url="https://acmepainting.com/services",
                raw_text="Interior painting exterior painting cabinet painting deck staining service areas.",
            ),
            "about": Page(
                page_type="about",
                url="https://acmepainting.com/about",
                raw_text="Family owned Lowell painting contractor serving local homeowners and businesses.",
            ),
            "contact": Page(
                page_type="contact",
                url="https://acmepainting.com/contact",
                raw_text="Call us today or request a quote for your next painting project.",
            ),
            "gallery": Page(
                page_type="gallery",
                url="https://acmepainting.com/gallery",
                raw_text="Project gallery before and after work customer reviews and testimonials.",
            ),
        }

        legacy_weakness = score_website_weakness(business, legacy_page_map, report)
        polished_weakness = score_website_weakness(business, polished_page_map, report)
        legacy_story = score_outreach_story_strength(business, legacy_page_map, report)
        polished_story = score_outreach_story_strength(business, polished_page_map, report)

        self.assertGreaterEqual(legacy_weakness, 3)
        self.assertGreater(legacy_weakness, polished_weakness)
        self.assertGreater(legacy_story, polished_story)

        with patch("app.scoring.rubric.get_pages", return_value=legacy_page_map), patch(
            "app.scoring.rubric.load_browser_report",
            return_value=report,
        ):
            result = evaluate_business(session=None, business=business)

        self.assertEqual(result.fit_status, "maybe")
        self.assertGreater(result.scores["website_weakness"], 0)
        self.assertGreaterEqual(result.scores["outreach_story_strength"], 7)


if __name__ == "__main__":
    unittest.main()
