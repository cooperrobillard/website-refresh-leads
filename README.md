# website-refresh-leads

`website-refresh-leads` is a local MVP for discovering and evaluating small-business websites that may be strong candidates for website refresh or redesign services.

## V1 Goal

Build a lightweight local pipeline that can surface potential leads, gather site evidence, run a preserved deterministic path or an OpenAI-powered model-judge path, and export a compact review package for manual outreach review.

## Current Status

The repo is now a script-driven MVP for repeated weekly lead runs. Discovery, prefiltering, crawl, browser checks, final judgment, and review-package export are all wired together for local use.

The architecture is now preservation-first hybrid:

- deterministic prefiltering remains the lightweight admission gate
- deterministic rubric scoring is preserved and still runnable
- `model_judge` is the new default scoring mode and intended primary direction
- `model_judge` now uses the OpenAI Responses API with compact multimodal evidence and strict structured output
- `compare` mode preserves deterministic scoring while exporting model judgment as the primary review output

Canonical website memory is now durable across runs. By default, if a canonical website was surfaced in any prior run, future runs skip it even when the prior lead was weak or only partially evidenced.

## Workflow

1. Discovery: find candidate businesses and websites.
2. Prefilter: mark obvious `strong`, `maybe`, and `skip` admission outcomes.
3. Crawl: fetch core site pages and save raw HTML.
4. Screenshots / Checks: capture homepage screenshots and browser signals.
5. Final Judgment: run the selected scoring mode and store notes.
6. Export / Review: create a compact shortlist package for current-run manual review.

The deterministic rubric still uses the evidence the repo already collects. If crawl coverage is partial but browser validation still confirms the homepage is reachable, the lead can still be scored with lower confidence instead of automatically collapsing to zero.

## Setup

1. Create and activate a virtual environment.
2. Install dependencies.
3. Install Playwright browsers.
4. Copy `.env.example` to `.env` and fill in your Places API key and local OpenAI key.

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
python3 -m app.init_db
```

If you already have an older local database from before the canonical-memory schema change, the code will try to backfill it automatically. For the cleanest path after this upgrade, a one-time reset is acceptable:

```bash
python3 -m app.init_db --reset
```

The project reads configuration from `.env` via `python-dotenv`. The main database setting is:

```bash
DATABASE_URL=sqlite:///data/leads.db
```

OpenAI model judging is configured the same way:

```bash
OPENAI_API_KEY=
OPENAI_MODEL=gpt-5.4-mini
```

## Single-Query Usage

Run the full pipeline for one query:

```bash
python3 -m app.main --query "painters lowell ma" --niche painters
```

The main runner now supports:

- `--scoring-mode model_judge` (default)
- `--scoring-mode deterministic`
- `--scoring-mode compare`

Mode behavior:

- `model_judge`: uses GPT-5.4 mini through the Responses API and exports `ModelJudgment` rows as the primary review package
- `deterministic`: preserves the old deterministic scoring and export path
- `compare`: runs deterministic scoring plus model judging, then exports model judgment with deterministic comparison fields

Optional discovery controls:

```bash
python3 -m app.main \
  --query "painters lowell ma" \
  --niche painters \
  --scoring-mode model_judge \
  --page-size 10 \
  --max-pages 2
```

Default duplicate handling is strict across runs at the canonical website level. A later revisit path is plumbed through `--allow-revisit`, but revisits still require the stored business row to be explicitly marked `eligible_for_revisit` first.

Default exports are also strict: each `PipelineRun` writes its own review package under `data/exports/runs/run_<run_id>/`, and those exports only include businesses first admitted in that run. Older leads do not resurface in the default review package unless a dedicated export override is added later.

## Multi-Query Usage

Run multiple queries from a plain text file:

```bash
python3 -m app.main --query-file prompts/queries.txt --niche painters
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
python3 -m app.discovery.run_places \
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
- `data/exports/runs/run_<run_id>/review_package.csv`: flat shortlist export for one run
- `data/exports/runs/run_<run_id>/review_package.json`: structured shortlist export for one run
- `data/exports/runs/run_<run_id>/review_screenshots/`: copied screenshots bundled with that run's review package

The review package includes current-run new candidates only. Within that scope it includes:

- business info and review counts
- run/debug fields such as `query_used`, canonical URL/key, and `discovery_run_id`
- final `fit_status`, confidence, evidence quality, and recommended action
- model-judgment fields such as website weakness, outreach-story strength, positive signals, and evidence warnings
- deterministic comparison fields when the run used `--scoring-mode compare`
- per-dimension score breakdown when the run used deterministic scoring
- compact review context for manual ranking: why it qualified, evidence strength, and outreach-story strength
- selected page URLs
- screenshot paths
- top issues, quick summary, teardown angle, and skip reason

If a run produces zero `strong` or `maybe` leads, the exporter automatically falls back to the top scored `skip` leads from that same run only, so prior-run leads still do not reappear by default.

For a single query run, check the `run_<run_id>` folder printed at the end of the export step. For a `--query-file` batch, each query gets its own sibling folder under `data/exports/runs/`, so earlier run packages and screenshots are preserved instead of being overwritten by later runs.
