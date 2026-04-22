"""Run Places discovery queries and persist normalized business rows."""

from __future__ import annotations

import argparse
from typing import Any

from app.db import SessionLocal
from app.discovery.places import search_places_text, upsert_businesses


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
) -> dict[str, int]:
    """Run a Places query across one or more result pages."""
    total_places = 0
    total_inserted = 0
    total_updated = 0
    page_token: str | None = None
    page_number = 0

    with SessionLocal() as session:
        while page_number < max_pages:
            page_number += 1
            result: dict[str, Any] = search_places_text(
                query=query,
                page_size=page_size,
                page_token=page_token,
            )
            places = result.get("places", [])

            inserted, updated = upsert_businesses(
                session=session,
                places=places,
                niche=niche,
                query_used=query,
            )

            total_places += len(places)
            total_inserted += inserted
            total_updated += updated

            print(
                f"Page {page_number}: "
                f"found={len(places)} inserted={inserted} updated={updated}"
            )

            page_token = result.get("nextPageToken")
            if not page_token:
                break

    print(
        f"Done: pages={page_number} found={total_places} "
        f"inserted={total_inserted} updated={total_updated}"
    )

    return {
        "pages_fetched": page_number,
        "places_found": total_places,
        "inserted": total_inserted,
        "updated": total_updated,
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_places_query(
        query=args.query,
        niche=args.niche,
        page_size=args.page_size,
        max_pages=args.max_pages,
    )


if __name__ == "__main__":
    main()
