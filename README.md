# website-refresh-leads

`website-refresh-leads` is a local MVP for discovering and evaluating small-business websites that may be strong candidates for website refresh or redesign services.

## V1 Goal

Build a lightweight pipeline that can surface potential leads, gather basic site evidence, apply a simple scoring rubric, and export results for manual review.

## Current Status

Phase 1 scaffold only. The project structure, module boundaries, and base configuration are in place, but the real integrations and pipeline logic have not been implemented yet.

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
