"""Helpers for creating pipeline runs and scoping work to one run."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import and_, or_
from sqlalchemy.orm import Query, Session

from app.db import SessionLocal
from app.models import Business, PipelineRun


def create_pipeline_run(
    session: Session,
    *,
    query: str,
    niche: str,
    allow_revisit: bool = False,
    run_label: str | None = None,
) -> PipelineRun:
    """Insert and return one pipeline run row."""
    run = PipelineRun(
        query=query,
        niche=niche,
        allow_revisit=allow_revisit,
        run_label=run_label,
    )
    session.add(run)
    session.commit()
    session.refresh(run)
    return run


def start_pipeline_run(
    *,
    query: str,
    niche: str,
    allow_revisit: bool = False,
    run_label: str | None = None,
) -> PipelineRun:
    """Create a pipeline run using a short-lived session."""
    with SessionLocal() as session:
        return create_pipeline_run(
            session,
            query=query,
            niche=niche,
            allow_revisit=allow_revisit,
            run_label=run_label,
        )


def resolve_pipeline_run(session: Session, run_id: int | None = None) -> PipelineRun:
    """Return a specific run or the most recent run when no id is provided."""
    if run_id is not None:
        run = session.get(PipelineRun, run_id)
        if run is None:
            raise ValueError(f"Pipeline run not found: {run_id}")
        return run

    run = (
        session.query(PipelineRun)
        .order_by(PipelineRun.started_at.desc(), PipelineRun.id.desc())
        .first()
    )
    if run is None:
        raise ValueError("No pipeline runs found. Run discovery first.")
    return run


def businesses_for_run_query(session: Session, run: PipelineRun) -> Query[Business]:
    """Return the default business scope for one pipeline run."""
    if not run.allow_revisit:
        return session.query(Business).filter(Business.discovery_run_id == run.id)

    return session.query(Business).filter(
        or_(
            Business.discovery_run_id == run.id,
            and_(
                Business.last_seen_run_id == run.id,
                Business.eligible_for_revisit.is_(True),
            ),
        )
    )


def finish_pipeline_run(run_id: int) -> None:
    """Mark a pipeline run as finished when it still exists."""
    with SessionLocal() as session:
        run = session.get(PipelineRun, run_id)
        if run is None:
            return

        run.finished_at = datetime.utcnow()
        session.commit()
