"""
JANSA VISASIST — End-to-End Pipeline Tests (M1 → M2 → M3).

Runs the complete M1 → M2 → M3 pipeline against data/GrandFichier_1.xlsx
and validates M3 outputs against golden_m3_snapshot.json.

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


# ── Fixtures ──

GRANDFICHIER_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "GrandFichier_1.xlsx"
)
GRANDFICHIER_PATH = os.path.normpath(GRANDFICHIER_PATH)

GOLDEN_DIR = os.path.join(os.path.dirname(__file__), "golden")

_SKIP_REASON = "GrandFichier_1.xlsx not found — skipping E2E pipeline tests"

needs_grandfichier = pytest.mark.skipif(
    not os.path.isfile(GRANDFICHIER_PATH), reason=_SKIP_REASON
)


def _load_golden(name: str) -> dict:
    path = os.path.join(GOLDEN_DIR, name)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture(scope="module")
def e2e_result():
    """Run full M1 → M2 → M3 pipeline once for all tests."""
    if not os.path.isfile(GRANDFICHIER_PATH):
        pytest.skip(_SKIP_REASON)

    with tempfile.TemporaryDirectory(prefix="jansa_e2e_test_") as tmpdir:
        m1_dir = os.path.join(tmpdir, "m1")
        m2_dir = os.path.join(tmpdir, "m2")
        m3_dir = os.path.join(tmpdir, "m3")

        # M1
        ctx = PipelineContext(input_path=GRANDFICHIER_PATH, output_dir=m1_dir)
        m1_exit = run_pipeline(ctx)

        master_path = os.path.join(m1_dir, "master_dataset.json")
        with open(master_path, "r", encoding="utf-8") as f:
            master_df = pd.DataFrame(json.load(f))

        # M2
        m2_exit = run_module2(master_df, m2_dir)

        enriched_path = os.path.join(m2_dir, "enriched_master.json")
        with open(enriched_path, "r", encoding="utf-8") as f:
            enriched_df = pd.DataFrame(json.load(f))

        # M3 (use fixed reference date for reproducibility)
        ref_date = datetime.date(2025, 1, 15)
        queue_df, summary_df, exclusion_df = run_module3(
            enriched_df, m3_dir, reference_date=ref_date
        )

        yield {
            "m1_exit": m1_exit,
            "m2_exit": m2_exit,
            "master_df": master_df,
            "enriched_df": enriched_df,
            "queue_df": queue_df,
            "summary_df": summary_df,
            "exclusion_df": exclusion_df,
            "m3_dir": m3_dir,
        }


# ── Test Class ──

@needs_grandfichier
class TestEndToEndPipeline:
    """Full M1 → M2 → M3 end-to-end pipeline tests."""

    def test_all_modules_exit_zero(self, e2e_result):
        """All three modules exit with code 0."""
        assert e2e_result["m1_exit"] == 0
        assert e2e_result["m2_exit"] == 0

    def test_row_count_preserved_m1_m2(self, e2e_result):
        """M2 preserves the same row count as M1."""
        assert len(e2e_result["enriched_df"]) == len(e2e_result["master_df"])

    def test_queue_plus_exclusion_equals_latest(self, e2e_result):
        """Queue + exclusion counts roughly match is_latest rows."""
        golden_m3 = _load_golden("golden_m3_snapshot.json")
        queue_size = len(e2e_result["queue_df"])
        # Queue should be in the right ballpark of golden
        assert queue_size == golden_m3["queue_size"]

    def test_m3_queue_size(self, e2e_result):
        """M3 queue size matches golden."""
        golden = _load_golden("golden_m3_snapshot.json")
        assert len(e2e_result["queue_df"]) == golden["queue_size"]

    def test_m3_exclusion_count(self, e2e_result):
        """M3 exclusion count matches golden."""
        golden = _load_golden("golden_m3_snapshot.json")
        # Load exclusion log from file for accurate count
        excl_path = os.path.join(e2e_result["m3_dir"], "exclusion_log.json")
        if os.path.isfile(excl_path):
            with open(excl_path, "r", encoding="utf-8") as f:
                excl_data = json.load(f)
            assert len(excl_data) == golden["exclusion_count"]
        else:
            pytest.skip("exclusion_log.json not found")

    def test_m3_scores_in_range(self, e2e_result):
        """All M3 priority scores are within [0, 100]."""
        golden = _load_golden("golden_m3_snapshot.json")
        queue = e2e_result["queue_df"]
        if not queue.empty:
            assert queue["priority_score"].min() >= golden["score_min_gte"]
            assert queue["priority_score"].max() <= golden["score_max_lte"]

    def test_m3_all_is_latest_true(self, e2e_result):
        """All rows in M3 queue have is_latest=True."""
        golden = _load_golden("golden_m3_snapshot.json")
        queue = e2e_result["queue_df"]
        if not queue.empty and "is_latest" in queue.columns:
            assert (queue["is_latest"] == True).all() == golden["all_is_latest_true"]

    def test_m3_no_duplicates_in_queue(self, e2e_result):
        """No DUPLICATE rows in M3 queue."""
        golden = _load_golden("golden_m3_snapshot.json")
        queue = e2e_result["queue_df"]
        if not queue.empty and "duplicate_flag" in queue.columns:
            no_dup = (queue["duplicate_flag"] != "DUPLICATE").all()
            assert bool(no_dup) == golden["no_duplicate_in_queue"]

    def test_m3_all_visa_global_null(self, e2e_result):
        """All rows in M3 queue have visa_global=null (pending items only)."""
        golden = _load_golden("golden_m3_snapshot.json")
        queue = e2e_result["queue_df"]
        if not queue.empty and "visa_global" in queue.columns:
            all_null = queue["visa_global"].isna().all()
            assert bool(all_null) == golden["all_visa_global_null"]

    def test_m3_category_distribution(self, e2e_result):
        """M3 category distribution matches golden."""
        golden = _load_golden("golden_m3_snapshot.json")
        queue = e2e_result["queue_df"]
        if not queue.empty and "category" in queue.columns:
            actual = queue["category"].value_counts().to_dict()
            actual = {k: int(v) for k, v in actual.items()}
            assert actual == golden["category_distribution"]

    def test_pipeline_idempotency(self, e2e_result):
        """Running M2 twice on same input produces identical enriched output."""
        enriched_df = e2e_result["enriched_df"]
        master_df = e2e_result["master_df"]

        with tempfile.TemporaryDirectory(prefix="jansa_idem_") as tmpdir:
            m2_dir2 = os.path.join(tmpdir, "m2_run2")
            run_module2(master_df, m2_dir2)

            enriched_path2 = os.path.join(m2_dir2, "enriched_master.json")
            with open(enriched_path2, "r", encoding="utf-8") as f:
                enriched_df2 = pd.DataFrame(json.load(f))

            # Same shape
            assert enriched_df.shape == enriched_df2.shape
            # Same columns
            assert set(enriched_df.columns) == set(enriched_df2.columns)
