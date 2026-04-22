# website-refresh-leads

`website-refresh-leads` is a local MVP for discovering and evaluating small-business websites that may be strong candidates for website refresh or redesign services.

## V1 Goal

Build a lightweight local pipeline that can surface potential leads, gather site evidence, apply deterministic scoring, and export a compact review package for manual outreach review.

## Current Status

The repo is now a script-driven MVP for repeated weekly lead runs. Discovery, prefiltering, crawl, browser checks, scoring, and review-package export are all wired together for local use.

## Workflow

1. Discovery: find candidate businesses and websites.
2. Prefilter: mark obvious `strong`, `maybe`, and `skip` leads.
3. Crawl: fetch core site pages and save raw HTML.
4. Screenshots / Checks: capture homepage screenshots and browser signals.
5. Scoring: apply the rubric and store notes.
6. Export / Review: create a compact shortlist package for manual review.

## Setup

1. Create and activate a virtual environment.
2. Install dependencies.
3. Install Playwright browsers.
4. Copy `.env.example` to `.env` and fill in your Places API key.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install
cp .env.example .env
```

## Database Init

Create the local SQLite database and tables:

```bash
python -m app.init_db
```

The project reads configuration from `.env` via `python-dotenv`. The main database setting is:

```bash
DATABASE_URL=sqlite:///data/leads.db
```

## Single-Query Usage

Run the full pipeline for one query:

```bash
python -m app.main --query "painters lowell ma" --niche painters
```

Optional discovery controls:

```bash
python -m app.main \
  --query "painters lowell ma" \
  --niche painters \
  --page-size 10 \
  --max-pages 2
```

## Multi-Query Usage

Run multiple queries from a plain text file:

```bash
python -m app.main --query-file prompts/weekly_queries.txt --niche painters
```

The query file supports:

- One query per line, using the CLI `--niche` as the shared niche
- Or `query | niche` per line when different niches are needed

Example:

```text
painters lowell ma
painters chelmsford ma
pressure washing nashua nh | pressure_washing
```

## Discovery-Only Usage

If you want to run discovery by itself:

```bash
python -m app.discovery.run_places \
  --query "painters lowell ma" \
  --niche painters \
  --page-size 10 \
  --max-pages 2
```

## Output Files

The pipeline writes local artifacts to:

- `data/leads.db`: SQLite database
- `data/raw/`: raw HTML captured during crawl
- `data/screenshots/`: desktop and mobile homepage screenshots
- `data/browser_checks/`: JSON browser-check reports
- `data/exports/review_package.csv`: flat shortlist export
- `data/exports/review_package.json`: structured shortlist export

The review package includes:

- business info and review counts
- final `fit_status`, score, and confidence
- per-dimension score breakdown
- selected page URLs
- screenshot paths
- top issues, quick summary, teardown angle, and skip reason
