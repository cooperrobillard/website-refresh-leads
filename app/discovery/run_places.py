"""Run a Places search query and persist normalized business rows."""

from __future__ import annotations

from app.db import SessionLocal
from app.discovery.places import search_places_text, upsert_businesses


def main() -> None:
    query = "painters lowell ma"
    niche = "painters"

    with SessionLocal() as session:
        result = search_places_text(query=query, page_size=10)
        places = result.get("places", [])

        inserted, updated = upsert_businesses(
            session=session,
            places=places,
            niche=niche,
            query_used=query,
        )

        print(f"Found {len(places)} places")
        print(f"Inserted: {inserted}")
        print(f"Updated: {updated}")

        next_page_token = result.get("nextPageToken")
        if next_page_token:
            print("Next page token found.")


if __name__ == "__main__":
    main()
