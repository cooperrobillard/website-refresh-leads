"""OpenAI Responses API client for multimodal lead judgment."""

from __future__ import annotations

import base64
import json
import mimetypes
from pathlib import Path
from typing import Any

from app.config import OPENAI_API_KEY, OPENAI_MODEL
from app.judging.schemas import BusinessJudgingPackage, ModelJudgeOutcome, PromptBundle, model_judgment_json_schema


def _load_openai_client_class():
    """Import the OpenAI SDK lazily so tests do not require the package."""
    try:
        from openai import OpenAI
    except ImportError as exc:
        raise RuntimeError(
            "The `openai` package is not installed. Run `pip install -r requirements.txt`."
        ) from exc

    return OpenAI


def _image_data_url(path: str) -> str | None:
    """Convert a local screenshot into a data URL for Responses image input."""
    file_path = Path(path)
    if not file_path.exists():
        return None

    mime_type, _ = mimetypes.guess_type(file_path.name)
    if not mime_type:
        mime_type = "image/png"

    encoded = base64.b64encode(file_path.read_bytes()).decode("ascii")
    return f"data:{mime_type};base64,{encoded}"


def _response_to_dict(response: Any) -> dict[str, Any]:
    """Convert an SDK response object into a serializable dict when possible."""
    if isinstance(response, dict):
        return response
    if hasattr(response, "model_dump"):
        return response.model_dump(mode="json")
    if hasattr(response, "to_dict"):
        return response.to_dict()
    return {"repr": repr(response)}


def _response_output_text(response: Any, raw_response: dict[str, Any]) -> str:
    """Extract the JSON text from a Responses API result."""
    output_text = getattr(response, "output_text", None)
    if isinstance(output_text, str) and output_text.strip():
        return output_text

    output_items = raw_response.get("output", [])
    for output_item in output_items:
        content_items = output_item.get("content", [])
        for content_item in content_items:
            if content_item.get("type") == "output_text":
                text = content_item.get("text")
                if isinstance(text, str) and text.strip():
                    return text
            if content_item.get("type") == "refusal":
                raise RuntimeError(f"Model judge refusal: {content_item.get('refusal') or 'Unknown refusal'}")

    raise RuntimeError("Responses API returned no structured output text.")


def _clamp_score(value: Any) -> int:
    """Clamp numeric rubric-style scores into the 0-10 range."""
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = 0
    return max(0, min(10, parsed))


class ModelJudgeClient:
    """Thin OpenAI Responses API client for multimodal lead judging."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        model_name: str | None = None,
        sdk_client: Any | None = None,
    ) -> None:
        self.api_key = api_key or OPENAI_API_KEY
        self.model_name = model_name or OPENAI_MODEL
        self._sdk_client = sdk_client

    def _client(self):
        """Return a configured OpenAI SDK client."""
        if self._sdk_client is not None:
            return self._sdk_client
        if not self.api_key:
            raise RuntimeError("OPENAI_API_KEY is not configured.")
        openai_client_class = _load_openai_client_class()
        self._sdk_client = openai_client_class(api_key=self.api_key)
        return self._sdk_client

    def _build_input(self, package: BusinessJudgingPackage, prompt: PromptBundle) -> list[dict[str, Any]]:
        """Build the Responses API multimodal input payload."""
        user_content: list[dict[str, Any]] = [
            {
                "type": "input_text",
                "text": prompt.user_prompt,
            }
        ]

        for label, path in [
            ("desktop homepage screenshot", package.screenshot_paths.get("desktop")),
            ("mobile homepage screenshot", package.screenshot_paths.get("mobile")),
        ]:
            if not path:
                continue

            data_url = _image_data_url(path)
            if not data_url:
                continue

            user_content.append(
                {
                    "type": "input_text",
                    "text": f"Attached image: {label}.",
                }
            )
            user_content.append(
                {
                    "type": "input_image",
                    "image_url": data_url,
                    "detail": "low",
                }
            )

        return [
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": prompt.system_prompt,
                    }
                ],
            },
            {
                "role": "user",
                "content": user_content,
            },
        ]

    def judge(
        self,
        package: BusinessJudgingPackage,
        prompt: PromptBundle,
        *,
        judgment_mode: str,
    ) -> ModelJudgeOutcome:
        """Call the OpenAI Responses API and return one parsed model judgment."""
        response = self._client().responses.create(
            model=self.model_name,
            input=self._build_input(package, prompt),
            text={"format": model_judgment_json_schema()},
            max_output_tokens=700,
        )
        raw_response = _response_to_dict(response)
        output_text = _response_output_text(response, raw_response)

        try:
            parsed = json.loads(output_text)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Responses API returned invalid JSON for business {package.business_name}."
            ) from exc

        return ModelJudgeOutcome(
            model_name=str(raw_response.get("model") or self.model_name),
            prompt_version=prompt.prompt_version,
            response_id=raw_response.get("id"),
            judgment_mode=judgment_mode,
            fit_status=str(parsed["fit_status"]),
            confidence=str(parsed["confidence"]),
            evidence_quality=str(parsed["evidence_quality"]),
            business_legitimacy=_clamp_score(parsed["business_legitimacy"]),
            website_weakness=_clamp_score(parsed["website_weakness"]),
            outreach_story_strength=_clamp_score(parsed["outreach_story_strength"]),
            recommended_action=str(parsed["recommended_action"]),
            top_issues=[str(item) for item in parsed.get("top_issues", [])[:3]],
            short_teardown_angle=str(parsed.get("short_teardown_angle") or ""),
            short_reasoning=str(parsed.get("short_reasoning") or ""),
            evidence_warnings=[str(item) for item in parsed.get("evidence_warnings", [])[:5]],
            positive_signals=[str(item) for item in parsed.get("positive_signals", [])[:5]],
            raw_json=raw_response,
        )
