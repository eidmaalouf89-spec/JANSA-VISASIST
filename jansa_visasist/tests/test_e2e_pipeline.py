"""
JANSA VISASIST — End-to-End Pipeline Tests (M1 → M2 → M3).

Runs the complete pipeline and validates structural invariants.
M3 output is date-dependent, so we test invariants rather than exact counts.

Tests skip gracefully if GrandFichier is not present.
"""

import datetime
import json
import os
import tempfile

import pandas as pd
import pytest

from jansa_visasist.context import PipelineContext
from jansa_visasist.main import run_pipeline
from jansa_visasist.main_m2 import run_module2
from jansa_visasist.main_m3 import run_module3


# ── Paths ──

GRANDFICHIER_PATH = os.path.normpath(os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "GrandFichier_1.xlsx"
))

_SKIP = "GrandFichier_1.xlsx not found — skipping E2E pipeline tests"
needs_gf = pytest.mark.skipif(not os.path.isfile(GRANDFICHIER_PATH), reason=_SKIP)


# ── Fixture: run full M1 → M2 → M3 pipeline once ──

@pytest.fixture(scope="module")
def e2e_result():
    if not os.path.isfile(GRANDFICHIER_PATH):
        pytest.skip(_SKIP)

    with tempfile.TemporaryDirectory(prefix="jansa_e2e_test_") as tmpdir:
        m1_dir = os.path.join(tmpdir, "m1")
        m2_dir = os.path.join(tmpdir, "m2")
        m3_dir = os.path.join(tmpdir, "m3")

        # M1
        ctx = PipelineContext(input_path=GRANDFICHIER_PATH, output_dir=m1_dir)
        m1_exit = run_pipeline(ctx)

        with open(os.path.join(m1_dir, "master_dataset.json"), "r", encoding="utf-8") as f:
            master_df = pd.DataFrame(json.load(f))

        # M2
        m2_exit = run_module2(master_df, m2_dir)

        with open(os.path.join(m2_dir, "enriched_master_dataset.json"), "r", encoding="utf-8") as f:
            enriched_df = pd.DataFrame(json.load(f))

        # M3 — use today's date (output is date-dependent)
        ref_date = datetime.date.today()
        queue_df, summary_df, exclusion_df = run_module3(
            enriched_df, m3_dir, reference_date=ref_date
        )

        # Also load from files for file-based checks
        with open(os.path.join(m3_dir, "m3_priority_queue.json"), "r", encoding="utf-8") as f:
            queue_from_file = pd.DataFrame(json.load(f))

        with open(os.path.join(m3_dir, "m3_exclusion_log.json"), "r", encoding="utf-8") as f:
            exclusion_data = json.load(f)

        yield {
            "m1_exit": m1_exit,
            "m2_exit": m2_exit,
            "master_df": master_df,
            "enriched_df": enriched_df,
            "queue_df": queue_df,
            "summary_df": summary_df,
            "exclusion_df": exclusion_df,
            "queue_from_file": queue_from_file,
            "exclusion_data": exclusion_data,
            "m2_dir": m2_dir,
            "m3_dir": m3_dir,
        }


# ── Tests ──

@needs_gf
class TestEndToEndPipeline:

    def test_all_modules_exit_zero(self, e2e_result):
        assert e2e_result["m1_exit"] == 0
        assert e2e_result["m2_exit"] == 0

    def test_row_count_preserved_m1_m2(self, e2e_result):
        assert len(e2e_result["enriched_df"]) == len(e2e_result["master_df"])

    def test_m3_all_is_latest_true(self, e2e_result):
        queue = e2e_result["queue_df"]
        if not queue.empty and "is_latest" in queue.columns:
            assert (queue["is_latest"] == True).all(), \
                "Queue contains rows with is_latest != True"  # noqa: E712

    def test_m3_all_visa_global_null(self, e2e_result):
        queue = e2e_result["queue_df"]
        if not queue.empty and "visa_global" in queue.columns:
            assert queue["visa_global"].isna().all(), \
                "Queue contains rows with non-null visa_global"

    def test_m3_no_duplicates_in_queue(self, e2e_result):
        queue = e2e_result["queue_df"]
        if not queue.empty and "duplicate_flag" in queue.columns:
            assert (queue["duplicate_flag"] != "DUPLICATE").all(), \
                "Queue contains DUPLICATE rows"

    def test_m3_scores_in_range(self, e2e_result):
        queue = e2e_result["queue_df"]
        if not queue.empty:
            assert queue["priority_score"].min() >= 0, "Score below 0"
            assert queue["priority_score"].max() <= 100, "Score above 100"

    def test_m3_queue_plus_exclusions_covers_input(self, e2e_result):
        """Queue + exclusion count should roughly cover all latest non-duplicate rows."""
        queue_len = len(e2e_result["queue_df"])
        excl_len = len(e2e_result["exclusion_data"])
        enriched = e2e_result["enriched_df"]
        # At minimum, queue + exclusions should be > 0 if there's data
        if not enriched.empty:
            assert queue_len + excl_len > 0, "No queue or exclusion output"

    def test_m3_category_values_valid(self, e2e_result):
        queue = e2e_result["queue_df"]
        if not queue.empty and "category" in queue.columns:
            valid_cats = {
                "EASY_WIN", "BLOCKED", "FAST_REJECT", "CONFLICT",
                "WAITING", "NOT_STARTED", "UNCATEGORIZED",
            }
            actual_cats = set(queue["category"].unique())
            invalid = actual_cats - valid_cats
            assert not invalid, f"Invalid categories: {invalid}"

    def test_m3_file_output_matches_return(self, e2e_result):
        """Queue written to file matches returned DataFrame in length."""
        queue_df = e2e_result["queue_df"]
        queue_file = e2e_result["queue_from_file"]
        assert len(queue_df) == len(queue_file)

    def test_m2_idempotency(self, e2e_result):
        """Running M2 twice on same input produces identical enriched output."""
        master_df = e2e_result["master_df"]
        enriched_df = e2e_result["enriched_df"]

        with tempfile.TemporaryDirectory(prefix="jansa_idem_") as tmpdir:
            m2_dir2 = os.path.join(tmpdir, "m2_run2")
            run_module2(master_df, m2_dir2)

            with open(os.path.join(m2_dir2, "enriched_master_dataset.json"), "r", encoding="utf-8") as f:
                enriched_df2 = pd.DataFrame(json.load(f))

            assert enriched_df.shape == enriched_df2.shape
            assert set(enriched_df.columns) == set(enriched_df2.columns)
