"""Initialize the local SQLite database and create all tables."""

from __future__ import annotations

from app import models  # noqa: F401
from app.db import Base, engine


def main() -> None:
    """Create all registered database tables."""
    Base.metadata.create_all(bind=engine)
    print("Database initialized.")


if __name__ == "__main__":
    main()
