"""Minimal environment-based configuration for the project."""

from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()

GOOGLE_PLACES_API_KEY: str = os.getenv("GOOGLE_PLACES_API_KEY", "")
DB_PATH: str = os.getenv("DB_PATH", "sqlite:///leads.db")
