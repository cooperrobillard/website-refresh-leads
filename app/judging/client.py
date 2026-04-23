"""Placeholder model client for the hybrid judgment path."""

from __future__ import annotations

from app.judging.schemas import BusinessJudgingPackage, ModelJudgeOutcome, PromptBundle


class ModelJudgeClient:
    """Future model client stub.

    Returning ``None`` keeps the pipeline local-first until an API client is wired.
    """

    model_name = "scaffold-not-configured"

    def judge(
        self,
        package: BusinessJudgingPackage,
        prompt: PromptBundle,
        *,
        judgment_mode: str,
    ) -> ModelJudgeOutcome | None:
        """Return a model judgment when a live client is configured."""
        _ = package
        _ = prompt
        _ = judgment_mode
        return None

