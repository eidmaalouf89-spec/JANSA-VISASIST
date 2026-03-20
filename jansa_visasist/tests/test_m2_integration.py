"""
JANSA VISASIST — Module 2 Integration Tests.

Runs M1 → M2 pipeline against data/GrandFichier_1.xlsx
and compares M2 outputs to golden_m2_snapshot.json.

Tests skip gracefully if GrandFichier is not present.
"""

import json
import os
import tempfile

import pandas as pd
import pytest

from jansa_visasist.context import PipelineContext
from jansa_visasist.main import run_pipeline
from jansa_visasist.main_m2 import run_module2
from jansa_visasist.config_m2 import UNPARSEABLE_PREFIX


# ── Paths ──

GRANDFICHIER_PATH = os.path.normpath(os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "GrandFichier_1.xlsx"
))
GOLDEN_DIR = os.path.join(os.path.dirname(__file__), "golden")

_SKIP = "GrandFichier_1.xlsx not found — skipping M2 integration tests"
needs_gf = pytest.mark.skipif(not os.path.isfile(GRANDFICHIER_PATH), reason=_SKIP)


def _load_golden(name: str) -> dict:
    with open(os.path.join(GOLDEN_DIR, name), "r", encoding="utf-8") as f:
        return json.load(f)


# ── Fixture: run M1 then M2 once ──

@pytest.fixture(scope="module")
def m2_result():
    if not os.path.isfile(GRANDFICHIER_PATH):
        pytest.skip(_SKIP)

    with tempfile.TemporaryDirectory(prefix="jansa_m2_test_") as tmpdir:
        m1_dir = os.path.join(tmpdir, "m1")
        m2_dir = os.path.join(tmpdir, "m2")

        # M1
        ctx = PipelineContext(input_path=GRANDFICHIER_PATH, output_dir=m1_dir)
        run_pipeline(ctx)

        with open(os.path.join(m1_dir, "master_dataset.json"), "r", encoding="utf-8") as f:
            master_df = pd.DataFrame(json.load(f))

        # M2
        exit_code = run_module2(master_df, m2_dir)

        with open(os.path.join(m2_dir, "enriched_master_dataset.json"), "r", encoding="utf-8") as f:
            enriched_df = pd.DataFrame(json.load(f))

        with open(os.path.join(m2_dir, "linking_anomalies.json"), "r", encoding="utf-8") as f:
            anomalies = json.load(f)

        yield {
            "exit_code": exit_code,
            "enriched_df": enriched_df,
            "anomalies": anomalies,
            "master_df": master_df,
            "m2_dir": m2_dir,
        }


# ── Tests ──

@needs_gf
class TestM2Integration:

    def test_exit_code_zero(self, m2_result):
        assert m2_result["exit_code"] == 0

    def test_total_rows_preserved(self, m2_result):
        golden = _load_golden("golden_m2_snapshot.json")
        assert len(m2_result["enriched_df"]) == golden["total_rows"]

    def test_duplicate_flag_distribution(self, m2_result):
        golden = _load_golden("golden_m2_snapshot.json")
        df = m2_result["enriched_df"]
        actual = {k: int(v) for k, v in df["duplicate_flag"].value_counts().to_dict().items()}
        assert actual == golden["duplicate_flag_distribution"]

    def test_total_anomalies(self, m2_result):
        golden = _load_golden("golden_m2_snapshot.json")
        assert len(m2_result["anomalies"]) == golden["total_anomalies"]

    def test_anomaly_type_distribution(self, m2_result):
        golden = _load_golden("golden_m2_snapshot.json")
        actual = {}
        for a in m2_result["anomalies"]:
            t = a.get("anomaly_type", "UNKNOWN")
            actual[t] = actual.get(t, 0) + 1
        assert actual == golden["anomaly_type_distribution"]

    def test_cross_lot_rows(self, m2_result):
        golden = _load_golden("golden_m2_snapshot.json")
        df = m2_result["enriched_df"]
        actual = int(df["is_cross_lot"].sum()) if "is_cross_lot" in df.columns else 0
        assert actual == golden["cross_lot_rows"]

    def test_is_latest_count(self, m2_result):
        golden = _load_golden("golden_m2_snapshot.json")
        df = m2_result["enriched_df"]
        actual = int(df["is_latest"].sum()) if "is_latest" in df.columns else 0
        assert actual == golden["is_latest_count"]

    def test_unparseable_count(self, m2_result):
        golden = _load_golden("golden_m2_snapshot.json")
        df = m2_result["enriched_df"]
        actual = int(df["doc_family_key"].str.startswith(UNPARSEABLE_PREFIX).sum())
        assert actual == golden["unparseable_count"]

    def test_m2_columns_added(self, m2_result):
        df = m2_result["enriched_df"]
        expected_cols = [
            "doc_family_key", "doc_version_key", "ind_sort_order",
            "previous_version_key", "is_latest", "revision_count",
            "is_cross_lot", "cross_lot_sheets", "duplicate_flag",
        ]
        for col in expected_cols:
            assert col in df.columns, f"Missing M2 column: {col}"

    def test_row_count_matches_m1(self, m2_result):
        """M2 must not add or remove rows from M1."""
        assert len(m2_result["enriched_df"]) == len(m2_result["master_df"])

    def test_gp2_cross_lot_sheets_null_for_non_cross_lot(self, m2_result):
        df = m2_result["enriched_df"]
        if "is_cross_lot" in df.columns and "cross_lot_sheets" in df.columns:
            non_cross = df[df["is_cross_lot"] == False]  # noqa: E712
            assert non_cross["cross_lot_sheets"].isna().all(), \
                "GP2 violation: non-cross-lot rows have non-null cross_lot_sheets"
