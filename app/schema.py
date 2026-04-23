"""Pragmatic local schema helpers for the SQLite pipeline database."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import text

from app.canonical_sites import canonical_website_key, canonical_website_url
from app.config import DATABASE_URL
from app.db import Base, SessionLocal, engine


BUSINESS_COLUMN_STATEMENTS = {
    "canonical_key": "ALTER TABLE businesses ADD COLUMN canonical_key VARCHAR(500)",
    "canonical_url": "ALTER TABLE businesses ADD COLUMN canonical_url VARCHAR(500)",
    "discovery_run_id": (
        "ALTER TABLE businesses ADD COLUMN discovery_run_id "
        "INTEGER REFERENCES pipeline_runs(id)"
    ),
    "first_seen_at": "ALTER TABLE businesses ADD COLUMN first_seen_at DATETIME",
    "last_seen_at": "ALTER TABLE businesses ADD COLUMN last_seen_at DATETIME",
    "last_seen_run_id": (
        "ALTER TABLE businesses ADD COLUMN last_seen_run_id "
        "INTEGER REFERENCES pipeline_runs(id)"
    ),
    "eligible_for_revisit": (
        "ALTER TABLE businesses ADD COLUMN eligible_for_revisit BOOLEAN NOT NULL DEFAULT 0"
    ),
}


def _is_sqlite_database() -> bool:
    """Return True when the configured database is a local SQLite file."""
    return DATABASE_URL.startswith("sqlite:///")


def _table_exists(connection, table_name: str) -> bool:
    """Return True when the named table exists."""
    row = connection.execute(
        text(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' AND name = :table_name"
        ),
        {"table_name": table_name},
    ).first()
    return row is not None


def _column_names(connection, table_name: str) -> set[str]:
    """Return the current column names for one SQLite table."""
    rows = connection.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
    return {row[1] for row in rows}


def _index_columns(connection, index_name: str) -> list[str]:
    """Return the ordered column list for one SQLite index."""
    rows = connection.execute(text(f"PRAGMA index_info({index_name})")).fetchall()
    return [row[2] for row in rows]


def _has_index_for_column(connection, table_name: str, column_name: str, *, unique: bool) -> bool:
    """Return True when an index already covers the requested column."""
    index_rows = connection.execute(text(f"PRAGMA index_list({table_name})")).fetchall()

    for row in index_rows:
        index_name = row[1]
        is_unique = bool(row[2])
        if is_unique != unique:
            continue
        if _index_columns(connection, index_name) == [column_name]:
            return True

    return False


def _ensure_sqlite_business_columns() -> None:
    """Add missing SQLite business columns for lightweight schema upgrades."""
    with engine.begin() as connection:
        if not _table_exists(connection, "businesses"):
            return

        existing_columns = _column_names(connection, "businesses")
        for column_name, statement in BUSINESS_COLUMN_STATEMENTS.items():
            if column_name not in existing_columns:
                connection.execute(text(statement))


def _ensure_sqlite_indexes() -> None:
    """Create the small set of indexes needed by the run-scoped pipeline."""
    with engine.begin() as connection:
        if not _table_exists(connection, "businesses"):
            return

        if not _has_index_for_column(connection, "businesses", "canonical_key", unique=True):
            connection.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS ix_businesses_canonical_key "
                    "ON businesses (canonical_key)"
                )
            )

        if not _has_index_for_column(connection, "businesses", "discovery_run_id", unique=False):
            connection.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_businesses_discovery_run_id "
                    "ON businesses (discovery_run_id)"
                )
            )


def _backfill_existing_businesses() -> dict[str, int]:
    """Populate canonical fields for legacy businesses when possible."""
    from app.models import Business

    touched = 0
    legacy_duplicate_sites = 0

    with SessionLocal() as session:
        businesses = (
            session.query(Business)
            .order_by(Business.created_at.asc(), Business.id.asc())
            .all()
        )
        claimed_keys: set[str] = set()

        for business in businesses:
            changed = False
            derived_canonical_key = canonical_website_key(business.website)
            derived_canonical_url = canonical_website_url(business.website)

            if business.canonical_url != derived_canonical_url:
                business.canonical_url = derived_canonical_url
                changed = True

            if business.first_seen_at is None:
                business.first_seen_at = business.created_at
                changed = True

            if business.last_seen_at is None:
                business.last_seen_at = business.first_seen_at or business.created_at
                changed = True

            if business.canonical_key:
                if business.canonical_key in claimed_keys:
                    business.canonical_key = None
                    legacy_duplicate_sites += 1
                    changed = True
                else:
                    claimed_keys.add(business.canonical_key)
            elif derived_canonical_key and derived_canonical_key not in claimed_keys:
                business.canonical_key = derived_canonical_key
                claimed_keys.add(derived_canonical_key)
                changed = True
            elif derived_canonical_key:
                legacy_duplicate_sites += 1

            if changed:
                touched += 1

        if touched:
            session.commit()

    return {
        "touched": touched,
        "legacy_duplicate_sites": legacy_duplicate_sites,
    }


def ensure_database_schema() -> None:
    """Create tables and apply the small SQLite upgrades this MVP needs."""
    from app import models  # noqa: F401

    Base.metadata.create_all(bind=engine)

    if not _is_sqlite_database():
        return

    _ensure_sqlite_business_columns()
    backfill_result = _backfill_existing_businesses()
    _ensure_sqlite_indexes()

    if backfill_result["touched"]:
        print(
            "Schema update: "
            f"backfilled {backfill_result['touched']} existing business entr"
            f"{'y' if backfill_result['touched'] == 1 else 'ies'}."
        )
        if backfill_result["legacy_duplicate_sites"]:
            print(
                "Schema note: "
                f"{backfill_result['legacy_duplicate_sites']} legacy duplicate canonical site entr"
                f"{'y was' if backfill_result['legacy_duplicate_sites'] == 1 else 'ies were'} "
                "left without a canonical key. "
                "If you want a completely clean history after this schema change, "
                "do a one-time database reset."
            )


def reset_sqlite_database() -> Path:
    """Delete the local SQLite database file and sidecars."""
    if not _is_sqlite_database():
        raise ValueError("--reset is only supported for local SQLite databases.")

    database_path = Path(DATABASE_URL.removeprefix("sqlite:///"))
    for suffix in ["", "-shm", "-wal"]:
        candidate = Path(f"{database_path}{suffix}")
        if candidate.exists():
            candidate.unlink()

    return database_path
