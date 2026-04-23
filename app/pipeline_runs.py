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
) -> int:
    """Insert one pipeline run row and return its id."""
    run = PipelineRun(
        query=query,
        niche=niche,
        allow_revisit=allow_revisit,
        run_label=run_label,
    )
    session.add(run)
    session.flush()
    run_id = run.id
    session.commit()
    return run_id


def start_pipeline_run(
    *,
    query: str,
    niche: str,
    allow_revisit: bool = False,
    run_label: str | None = None,
) -> int:
    """Create a pipeline run using a short-lived session and return its id."""
    with SessionLocal() as session:
        return create_pipeline_run(
            session,
            query=query,
            niche=niche,
            allow_revisit=allow_revisit,
            run_label=run_label,
        )


def resolve_pipeline_run(session: Session, run_id: int | None = None) -> tuple[int, bool]:
    """Return the resolved run id and its allow_revisit setting."""
    if run_id is not None:
        row = (
            session.query(PipelineRun.id, PipelineRun.allow_revisit)
            .filter(PipelineRun.id == run_id)
            .first()
        )
        if row is None:
            raise ValueError(f"Pipeline run not found: {run_id}")
        return int(row[0]), bool(row[1])

    row = (
        session.query(PipelineRun.id, PipelineRun.allow_revisit)
        .order_by(PipelineRun.started_at.desc(), PipelineRun.id.desc())
        .first()
    )
    if row is None:
        raise ValueError("No pipeline runs found. Run discovery first.")
    return int(row[0]), bool(row[1])


def businesses_for_run_query(session: Session, run_id: int, allow_revisit: bool) -> Query[Business]:
    """Return the default business scope for one pipeline run."""
    if not allow_revisit:
        return session.query(Business).filter(Business.discovery_run_id == run_id)

    return session.query(Business).filter(
        or_(
            Business.discovery_run_id == run_id,
            and_(
                Business.last_seen_run_id == run_id,
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
