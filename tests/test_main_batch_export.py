"""Tests for batch-export orchestration in app.main."""

from __future__ import annotations

import argparse
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from app import main as main_module
from app.reports.export_batch_review_package import RunBatchExport


class MainBatchExportTests(unittest.TestCase):
    """Verify query-file runs trigger one batch export while single-query runs do not."""

    def test_query_file_run_triggers_batch_export(self) -> None:
        with TemporaryDirectory() as temp_dir:
            query_file = Path(temp_dir) / "queries.txt"
            query_file.write_text("painters lowell ma\npainters dracut ma\n", encoding="utf-8")

            first_export = RunBatchExport(
                run_id=1,
                query="painters lowell ma",
                niche="painters",
                scoring_mode="model_judge",
                inserted_new=2,
                records=[],
                export_dir=Path(temp_dir) / "exports" / "runs" / "run_1",
            )
            second_export = RunBatchExport(
                run_id=2,
                query="painters dracut ma",
                niche="painters",
                scoring_mode="model_judge",
                inserted_new=1,
                records=[],
                export_dir=Path(temp_dir) / "exports" / "runs" / "run_2",
            )

            args = argparse.Namespace(
                query=None,
                query_file=query_file.as_posix(),
                niche="painters",
                page_size=10,
                max_pages=1,
                allow_revisit=False,
                scoring_mode="model_judge",
            )

            with patch.object(main_module, "parse_args", return_value=args), patch.object(
                main_module,
                "ensure_database_schema",
            ), patch.object(
                main_module,
                "run_pipeline_for_query",
                side_effect=[first_export, second_export],
            ) as run_pipeline_mock, patch.object(
                main_module,
                "export_batch_review_package",
            ) as batch_export_mock:
                main_module.main()

            self.assertEqual(run_pipeline_mock.call_count, 2)
            batch_export_mock.assert_called_once()
            self.assertEqual(
                batch_export_mock.call_args.kwargs["run_exports"],
                [first_export, second_export],
            )
            self.assertEqual(
                batch_export_mock.call_args.kwargs["query_file"],
                query_file.as_posix(),
            )

    def test_single_query_run_keeps_per_run_behavior(self) -> None:
        args = argparse.Namespace(
            query="painters lowell ma",
            query_file=None,
            niche="painters",
            page_size=10,
            max_pages=1,
            allow_revisit=False,
            scoring_mode="model_judge",
        )

        single_export = RunBatchExport(
            run_id=11,
            query="painters lowell ma",
            niche="painters",
            scoring_mode="model_judge",
            inserted_new=3,
            records=[],
            export_dir=Path("data/exports/runs/run_11"),
        )

        with patch.object(main_module, "parse_args", return_value=args), patch.object(
            main_module,
            "ensure_database_schema",
        ), patch.object(
            main_module,
            "run_pipeline_for_query",
            return_value=single_export,
        ) as run_pipeline_mock, patch.object(
            main_module,
            "export_batch_review_package",
        ) as batch_export_mock:
            main_module.main()

        run_pipeline_mock.assert_called_once()
        batch_export_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
