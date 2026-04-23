"""Focused tests for canonical website normalization."""

from __future__ import annotations

import unittest

from app.canonical_sites import canonical_website_key, canonical_website_url, normalize_website_url


class CanonicalSitesTests(unittest.TestCase):
    """Verify canonical normalization stays deterministic."""

    def test_normalize_website_url_strips_noise(self) -> None:
        self.assertEqual(
            normalize_website_url("HTTP://www.Example.com/services/?utm_source=test#hero"),
            "https://example.com/services",
        )

    def test_regular_hosts_use_host_level_identity(self) -> None:
        self.assertEqual(
            canonical_website_key("https://www.example.com/about/team?ref=1"),
            "example.com",
        )
        self.assertEqual(
            canonical_website_url("https://www.example.com/about/team?ref=1"),
            "https://example.com/",
        )

    def test_google_sites_preserves_meaningful_path_prefix(self) -> None:
        url = "https://sites.google.com/view/acme-painting/home?authuser=0#section"
        self.assertEqual(canonical_website_key(url), "sites.google.com/view/acme-painting")
        self.assertEqual(canonical_website_url(url), "https://sites.google.com/view/acme-painting")

    def test_certapro_preserves_first_tenant_segment(self) -> None:
        url = "https://certapro.com/andover/landing-page/residential-gbp-landing"
        self.assertEqual(canonical_website_key(url), "certapro.com/andover")
        self.assertEqual(canonical_website_url(url), "https://certapro.com/andover")


if __name__ == "__main__":
    unittest.main()
