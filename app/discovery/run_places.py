"""Run Places discovery queries and persist normalized business rows."""

from __future__ import annotations

import argparse
from typing import Any

from app.db import SessionLocal
from app.discovery.places import search_places_text, upsert_businesses
from app.pipeline_runs import create_pipeline_run, finish_pipeline_run
from app.models import PipelineRun
from app.schema import ensure_database_schema


def positive_int(value: str) -> int:
    """Parse a positive integer argument."""
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be 1 or greater")
    return parsed


def run_places_query(
    query: str,
    niche: str,
    page_size: int = 10,
    max_pages: int = 1,
    run_id: int | None = None,
) -> dict[str, int]:
    """Run a Places query across one or more result pages."""
    total_places = 0
    total_inserted = 0
    total_updated = 0
    total_skipped_existing = 0
    page_token: str | None = None
    page_number = 0
    created_run_locally = run_id is None

    with SessionLocal() as session:
        if run_id is None:
            current_run = create_pipeline_run(
                session,
                query=query,
                niche=niche,
            )
        else:
            current_run = session.get(PipelineRun, run_id)
            if current_run is None:
                raise ValueError(f"Pipeline run not found: {run_id}")

        while page_number < max_pages:
            page_number += 1
            result: dict[str, Any] = search_places_text(
                query=query,
                page_size=page_size,
                page_token=page_token,
            )
            places = result.get("places", [])

            counts = upsert_businesses(
                session=session,
                places=places,
                niche=niche,
                query_used=query,
                current_run=current_run,
            )

            total_places += len(places)
            total_inserted += counts["inserted"]
            total_updated += counts["updated_metadata"]
            total_skipped_existing += counts["skipped_existing_processed"]

            print(
                f"Page {page_number}: found={len(places)}"
            )
            print(
                f"  inserted_new={counts['inserted']} "
                f"updated_metadata={counts['updated_metadata']} "
                f"skipped_existing_processed={counts['skipped_existing_processed']}"
            )

            page_token = result.get("nextPageToken")
            if not page_token:
                break

    if created_run_locally:
        finish_pipeline_run(current_run.id)

    print(
        f"Done: run_id={current_run.id} pages={page_number} found={total_places}"
    )
    print(
        f"  inserted_new={total_inserted} "
        f"updated_metadata={total_updated} "
        f"skipped_existing_processed={total_skipped_existing}"
    )

    return {
        "run_id": current_run.id,
        "pages_fetched": page_number,
        "places_found": total_places,
        "inserted": total_inserted,
        "updated_metadata": total_updated,
        "skipped_existing_processed": total_skipped_existing,
    }


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the Places runner."""
    parser = argparse.ArgumentParser(description="Run Google Places discovery.")
    parser.add_argument("--query", required=True, help="Search query to send to Places.")
    parser.add_argument("--niche", required=True, help="Niche label to store on discovered businesses.")
    parser.add_argument(
        "--page-size",
        type=positive_int,
        default=10,
        help="Number of Places results to request per page. Default: 10.",
    )
    parser.add_argument(
        "--max-pages",
        type=positive_int,
        default=1,
        help="Maximum number of Places result pages to fetch. Default: 1.",
    )
    parser.add_argument(
        "--allow-revisit",
        action="store_true",
        help=(
            "Allow businesses already marked eligible_for_revisit to re-enter the run. "
            "Default: false."
        ),
    )
    return parser.parse_args()


def main() -> None:
    ensure_database_schema()
    args = parse_args()
    with SessionLocal() as session:
        current_run = create_pipeline_run(
            session,
            query=args.query,
            niche=args.niche,
            allow_revisit=args.allow_revisit,
        )

    print(f"Pipeline run {current_run.id} | allow_revisit={current_run.allow_revisit}")

    try:
        run_places_query(
            query=args.query,
            niche=args.niche,
            page_size=args.page_size,
            max_pages=args.max_pages,
            run_id=current_run.id,
        )
    finally:
        finish_pipeline_run(current_run.id)


if __name__ == "__main__":
    main()
