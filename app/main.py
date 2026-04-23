"""Command-line runner for the full local lead-generation pipeline."""

from __future__ import annotations

import argparse
from pathlib import Path

from app.browser.run_browser_checks import run_browser_validation
from app.crawl.run_crawl import run_crawl
from app.discovery.run_places import positive_int, run_places_query
from app.pipeline_runs import finish_pipeline_run, start_pipeline_run
from app.reports.export_review_package import export_review_package
from app.scoring.run_prefilter import run_prefilter
from app.scoring.run_scoring import run_scoring
from app.schema import ensure_database_schema


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the full pipeline runner."""
    parser = argparse.ArgumentParser(description="Run the website refresh leads pipeline.")
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--query", help="Single discovery query to run.")
    source_group.add_argument(
        "--query-file",
        help="Plain text file of queries to run. Use one query per line or `query | niche`.",
    )
    parser.add_argument(
        "--niche",
        help="Niche label for the query. Required for --query and used as the fallback for --query-file.",
    )
    parser.add_argument(
        "--page-size",
        type=positive_int,
        default=10,
        help="Number of Places results to request per page. Default: 10.",
    )
    parser.add_argument(
        "--max-pages",
        type=positive_int,
        default=1,
        help="Maximum number of Places result pages to fetch per query. Default: 1.",
    )
    parser.add_argument(
        "--allow-revisit",
        action="store_true",
        help=(
            "Allow businesses already marked eligible_for_revisit to re-enter the run. "
            "Default: false."
        ),
    )
    return parser.parse_args()


def load_query_jobs(
    query: str | None,
    niche: str | None,
    query_file: str | None,
) -> list[tuple[str, str]]:
    """Build the list of query/niche jobs to run."""
    if query:
        if not niche:
            raise ValueError("--niche is required when using --query")
        return [(query, niche)]

    if not query_file:
        raise ValueError("Provide either --query or --query-file")

    jobs: list[tuple[str, str]] = []
    file_path = Path(query_file)
    if not file_path.exists():
        raise ValueError(f"Query file not found: {query_file}")

    for line_number, raw_line in enumerate(file_path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if "|" in line:
            query_text, niche_text = [part.strip() for part in line.split("|", 1)]
        else:
            query_text = line
            niche_text = niche or ""

        if not query_text:
            raise ValueError(f"Missing query on line {line_number} in {query_file}")
        if not niche_text:
            raise ValueError(
                f"Missing niche on line {line_number} in {query_file}. "
                "Provide `query | niche` or pass --niche."
            )

        jobs.append((query_text, niche_text))

    if not jobs:
        raise ValueError(f"No queries found in {query_file}")

    return jobs


def run_pipeline_for_query(
    query: str,
    niche: str,
    page_size: int,
    max_pages: int,
    allow_revisit: bool = False,
) -> None:
    """Run the full sequential pipeline for one query/niche pair."""
    print(f"Query: {query}")
    print(f"Niche: {niche}")
    current_run = start_pipeline_run(
        query=query,
        niche=niche,
        allow_revisit=allow_revisit,
    )
    print(f"Run ID: {current_run.id} | allow_revisit={current_run.allow_revisit}")

    try:
        print("\n[1/6] Discovery")
        run_places_query(
            query=query,
            niche=niche,
            page_size=page_size,
            max_pages=max_pages,
            run_id=current_run.id,
        )

        print("\n[2/6] Prefilter")
        run_prefilter(run_id=current_run.id)

        print("\n[3/6] Crawl")
        run_crawl(run_id=current_run.id)

        print("\n[4/6] Browser Checks")
        run_browser_validation(run_id=current_run.id)

        print("\n[5/6] Scoring")
        run_scoring(run_id=current_run.id)

        print("\n[6/6] Export Review Package")
        export_review_package(limit=20, include_maybe=True, run_id=current_run.id)
    finally:
        finish_pipeline_run(current_run.id)


def main() -> None:
    """Run the local lead-generation pipeline from the command line."""
    args = parse_args()
    ensure_database_schema()
    try:
        jobs = load_query_jobs(args.query, args.niche, args.query_file)
    except ValueError as exc:
        raise SystemExit(f"Error: {exc}") from exc

    print(f"Starting website refresh leads pipeline for {len(jobs)} quer{'y' if len(jobs) == 1 else 'ies'}...")

    for index, (query, niche) in enumerate(jobs, start=1):
        print("\n" + "=" * 72)
        print(f"Pipeline {index}/{len(jobs)}")
        print("=" * 72)
        run_pipeline_for_query(
            query=query,
            niche=niche,
            page_size=args.page_size,
            max_pages=args.max_pages,
            allow_revisit=args.allow_revisit,
        )

    print("\nPipeline run complete.")


if __name__ == "__main__":
    main()
