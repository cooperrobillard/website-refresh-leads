"""Database models for the local MVP schema."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


class PipelineRun(Base):
    """A single pipeline execution for one query/niche pair."""

    __tablename__ = "pipeline_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    query: Mapped[str] = mapped_column(String(255), nullable=False)
    niche: Mapped[str] = mapped_column(String(100), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    allow_revisit: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    run_label: Mapped[str | None] = mapped_column(String(255), nullable=True)
    scoring_mode: Mapped[str] = mapped_column(String(30), default="model_judge", nullable=False)

    discovered_businesses: Mapped[list["Business"]] = relationship(
        back_populates="discovery_run",
        foreign_keys="Business.discovery_run_id",
    )
    model_judgments: Mapped[list["ModelJudgment"]] = relationship(back_populates="pipeline_run")


class Business(Base):
    """A candidate business lead and its top-level metadata."""

    __tablename__ = "businesses"
    __table_args__ = (
        UniqueConstraint("canonical_key", name="uq_businesses_canonical_key"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    place_id: Mapped[str | None] = mapped_column(String(255), unique=True, nullable=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    niche: Mapped[str | None] = mapped_column(String(100), nullable=True)
    query_used: Mapped[str | None] = mapped_column(String(255), nullable=True)

    website: Mapped[str | None] = mapped_column(String(500), nullable=True)
    canonical_key: Mapped[str | None] = mapped_column(String(500), nullable=True)
    canonical_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    address: Mapped[str | None] = mapped_column(String(500), nullable=True)
    primary_type: Mapped[str | None] = mapped_column(String(100), nullable=True)

    rating: Mapped[float | None] = mapped_column(Float, nullable=True)
    review_count: Mapped[int | None] = mapped_column(Integer, nullable=True)

    prefilter_status: Mapped[str | None] = mapped_column(String(20), nullable=True)
    prefilter_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    fit_status: Mapped[str | None] = mapped_column(String(20), nullable=True)
    skip_reason: Mapped[str | None] = mapped_column(Text, nullable=True)

    discovery_run_id: Mapped[int | None] = mapped_column(
        ForeignKey("pipeline_runs.id"),
        index=True,
        nullable=True,
    )
    first_seen_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_seen_run_id: Mapped[int | None] = mapped_column(ForeignKey("pipeline_runs.id"), nullable=True)
    eligible_for_revisit: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    discovery_run: Mapped["PipelineRun | None"] = relationship(
        back_populates="discovered_businesses",
        foreign_keys=[discovery_run_id],
    )
    pages: Mapped[list["Page"]] = relationship(back_populates="business", cascade="all, delete-orphan")
    artifacts: Mapped[list["Artifact"]] = relationship(back_populates="business", cascade="all, delete-orphan")
    score: Mapped["Score | None"] = relationship(back_populates="business", uselist=False, cascade="all, delete-orphan")
    note: Mapped["Note | None"] = relationship(back_populates="business", uselist=False, cascade="all, delete-orphan")
    model_judgments: Mapped[list["ModelJudgment"]] = relationship(
        back_populates="business",
        cascade="all, delete-orphan",
    )


class Page(Base):
    """A crawled page associated with a business website."""

    __tablename__ = "pages"
    __table_args__ = (
        UniqueConstraint("business_id", "url", name="uq_pages_business_url"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    business_id: Mapped[int] = mapped_column(ForeignKey("businesses.id"), nullable=False)

    page_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    url: Mapped[str] = mapped_column(String(500), nullable=False)
    title: Mapped[str | None] = mapped_column(String(500), nullable=True)
    raw_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    html_path: Mapped[str | None] = mapped_column(String(500), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    business: Mapped["Business"] = relationship(back_populates="pages")


class Artifact(Base):
    """A stored artifact such as a screenshot or downloaded asset."""

    __tablename__ = "artifacts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    business_id: Mapped[int] = mapped_column(ForeignKey("businesses.id"), nullable=False)

    artifact_type: Mapped[str] = mapped_column(String(100), nullable=False)
    file_path: Mapped[str] = mapped_column(String(500), nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    business: Mapped["Business"] = relationship(back_populates="artifacts")


class Score(Base):
    """A simple one-to-one score record for a business."""

    __tablename__ = "scores"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    business_id: Mapped[int] = mapped_column(ForeignKey("businesses.id"), unique=True, nullable=False)

    business_legitimacy: Mapped[int | None] = mapped_column(Integer, nullable=True)
    website_weakness: Mapped[int | None] = mapped_column(Integer, nullable=True)
    conversion_opportunity: Mapped[int | None] = mapped_column(Integer, nullable=True)
    trust_packaging: Mapped[int | None] = mapped_column(Integer, nullable=True)
    complexity_fit: Mapped[int | None] = mapped_column(Integer, nullable=True)
    outreach_viability: Mapped[int | None] = mapped_column(Integer, nullable=True)
    outreach_story_strength: Mapped[int | None] = mapped_column(Integer, nullable=True)

    raw_total_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    evidence_tier: Mapped[str | None] = mapped_column(String(20), nullable=True)
    evidence_cap: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total_score: Mapped[int | None] = mapped_column(Integer, nullable=True)
    fit_status: Mapped[str | None] = mapped_column(String(20), nullable=True)
    confidence: Mapped[str | None] = mapped_column(String(20), nullable=True)

    business: Mapped["Business"] = relationship(back_populates="score")


class Note(Base):
    """Manual or generated notes about a business lead."""

    __tablename__ = "notes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    business_id: Mapped[int] = mapped_column(ForeignKey("businesses.id"), unique=True, nullable=False)

    quick_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    top_issues: Mapped[str | None] = mapped_column(Text, nullable=True)
    teardown_angle: Mapped[str | None] = mapped_column(Text, nullable=True)
    final_memo: Mapped[str | None] = mapped_column(Text, nullable=True)

    business: Mapped["Business"] = relationship(back_populates="note")


class ModelJudgment(Base):
    """Compact storage for model-driven lead judgments and fallback scaffolding."""

    __tablename__ = "model_judgments"
    __table_args__ = (
        UniqueConstraint(
            "business_id",
            "pipeline_run_id",
            "judgment_mode",
            name="uq_model_judgments_business_run_mode",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    business_id: Mapped[int] = mapped_column(ForeignKey("businesses.id"), nullable=False)
    pipeline_run_id: Mapped[int] = mapped_column(ForeignKey("pipeline_runs.id"), nullable=False)

    model_name: Mapped[str | None] = mapped_column(String(100), nullable=True)
    prompt_version: Mapped[str | None] = mapped_column(String(50), nullable=True)
    response_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    judgment_mode: Mapped[str] = mapped_column(String(30), nullable=False)
    fit_status: Mapped[str | None] = mapped_column(String(20), nullable=True)
    confidence: Mapped[str | None] = mapped_column(String(20), nullable=True)
    evidence_quality: Mapped[str | None] = mapped_column(String(20), nullable=True)
    business_legitimacy: Mapped[str | None] = mapped_column(String(20), nullable=True)
    website_weakness: Mapped[str | None] = mapped_column(String(20), nullable=True)
    outreach_story_strength: Mapped[str | None] = mapped_column(String(20), nullable=True)
    recommended_action: Mapped[str | None] = mapped_column(String(50), nullable=True)
    top_issues: Mapped[str | None] = mapped_column(Text, nullable=True)
    short_teardown_angle: Mapped[str | None] = mapped_column(Text, nullable=True)
    short_reasoning: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    business: Mapped["Business"] = relationship(back_populates="model_judgments")
    pipeline_run: Mapped["PipelineRun"] = relationship(back_populates="model_judgments")
