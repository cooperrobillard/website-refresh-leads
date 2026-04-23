"""Initialize the local SQLite database and create all tables."""

from __future__ import annotations

import argparse

from app.schema import ensure_database_schema, reset_sqlite_database


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for database initialization."""
    parser = argparse.ArgumentParser(description="Initialize the local SQLite database.")
    parser.add_argument(
        "--reset",
        action="store_true",
        help=(
            "Delete the existing local SQLite database before recreating the schema. "
            "Useful after schema changes when you want a clean history."
        ),
    )
    return parser.parse_args()

def main() -> None:
    """Create all registered database tables and apply lightweight upgrades."""
    args = parse_args()
    if args.reset:
        database_path = reset_sqlite_database()
        print(f"Deleted database: {database_path}")

    ensure_database_schema()
    print("Database initialized.")


if __name__ == "__main__":
    main()
