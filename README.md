# website-refresh-leads

`website-refresh-leads` is a local MVP for discovering and evaluating small-business websites that may be strong candidates for website refresh or redesign services.

## V1 Goal

Build a lightweight pipeline that can surface potential leads, gather basic site evidence, apply a simple scoring rubric, and export results for manual review.

## Current Status

Phase 2 foundation in progress. The project now includes the initial database setup, core SQLAlchemy models, and a simple database initialization script, while the real discovery, crawling, browser automation, and scoring logic are still placeholders.

## Planned Workflow

1. Discovery: find candidate businesses and websites.
2. Crawl: fetch site pages and identify relevant URLs.
3. Screenshots / Checks: capture site visuals and run basic site-quality checks.
4. Scoring: apply a rubric to evaluate refresh potential.
5. Export / Review: export lead data and generate review summaries.

## Getting Started

1. Create a virtual environment.
2. Install dependencies from `requirements.txt`.
3. Copy `.env.example` to `.env` and fill in any needed values.
4. Run `python -m app.main`.

## Environment

The project loads environment variables from `.env` via `python-dotenv`. The example file is `.env.example`, and the main database setting is:

```env
DATABASE_URL=sqlite:///data/leads.db
```

This keeps the local SQLite database inside the existing `data/` directory.

## Initialize the Database

After installing dependencies and setting up `.env`, create the local database tables with:

```bash
python -m app.init_db
```
