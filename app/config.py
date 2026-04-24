"""Environment-based configuration for the project."""

from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()

GOOGLE_PLACES_API_KEY: str = os.getenv("GOOGLE_PLACES_API_KEY", "")
DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///data/leads.db")
OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-5.4-mini")
