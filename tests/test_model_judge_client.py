"""Focused tests for the OpenAI model-judge client."""

from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from app.judging.client import ModelJudgeClient
from app.judging.prompting import build_prompt
from app.judging.schemas import BusinessJudgingPackage


class _FakeResponse:
    def __init__(self) -> None:
        self.output_text = (
            '{"fit_status":"strong","confidence":"high","evidence_quality":"medium",'
            '"business_legitimacy":8,"website_weakness":7,"outreach_story_strength":9,'
            '"recommended_action":"review_for_outreach","top_issues":["Thin trust signals"],'
            '"short_teardown_angle":"Lead with the trust gap and dated first impression.",'
            '"short_reasoning":"Legitimate local service business with a clear outreach angle.",'
            '"evidence_warnings":["Only a few pages were captured"],'
            '"positive_signals":["Legitimate local service business"]}'
        )

    def model_dump(self, mode: str = "json") -> dict[str, object]:
        return {
            "id": "resp_test_123",
            "model": "gpt-5.4-mini",
            "output": [],
        }


class _FakeResponsesAPI:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return _FakeResponse()


class _FakeOpenAIClient:
    def __init__(self) -> None:
        self.responses = _FakeResponsesAPI()


class ModelJudgeClientTests(unittest.TestCase):
    """Verify the OpenAI client builds multimodal requests and parses output."""

    def test_judge_builds_strict_multimodal_request_and_parses_response(self) -> None:
        with TemporaryDirectory() as temp_dir:
            desktop_path = Path(temp_dir) / "desktop.png"
            mobile_path = Path(temp_dir) / "mobile.png"
            desktop_path.write_bytes(b"desktop")
            mobile_path.write_bytes(b"mobile")

            package = BusinessJudgingPackage(
                business_id=1,
                pipeline_run_id=99,
                business_name="Acme Painting",
                website="https://acme.example.com/",
                canonical_url="https://acme.example.com/",
                niche="painters",
                query_used="painters lowell ma",
                location="Lowell, MA",
                primary_type="painter",
                google_rating=4.7,
                google_review_count=18,
                browser_homepage_signals={"homepage_loaded": True},
                page_load_map={"home": True},
                pages_found={"home": "https://acme.example.com/"},
                pages_captured_count=2,
                screenshots_captured_count=2,
                text_excerpts={"home": "Acme Painting serves Lowell homeowners."},
                screenshot_paths={
                    "desktop": desktop_path.as_posix(),
                    "mobile": mobile_path.as_posix(),
                },
                evidence_summary={"has_desktop_screenshot": True},
                diagnostics={"homepage_loaded": True},
            )
            prompt = build_prompt(package)
            fake_sdk_client = _FakeOpenAIClient()
            client = ModelJudgeClient(sdk_client=fake_sdk_client)

            outcome = client.judge(package, prompt, judgment_mode="model_judge")

        self.assertEqual(outcome.response_id, "resp_test_123")
        self.assertEqual(outcome.fit_status, "strong")
        self.assertEqual(outcome.website_weakness, 7)
        self.assertEqual(outcome.positive_signals, ["Legitimate local service business"])

        call = fake_sdk_client.responses.calls[0]
        self.assertEqual(call["model"], "gpt-5.4-mini")
        self.assertEqual(call["text"]["format"]["type"], "json_schema")
        self.assertTrue(call["text"]["format"]["strict"])
        user_content = call["input"][1]["content"]
        image_items = [item for item in user_content if item["type"] == "input_image"]
        self.assertEqual(len(image_items), 2)
        self.assertTrue(all(item["image_url"].startswith("data:image/png;base64,") for item in image_items))


if __name__ == "__main__":
    unittest.main()
