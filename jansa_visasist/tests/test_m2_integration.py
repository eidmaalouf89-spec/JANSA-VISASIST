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


# ── Fixtures ──

GRANDFICHIER_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "GrandFichier_1.xlsx"
)
GRANDFICHIER_PATH = os.path.normpath(GRANDFICHIER_PATH)

GOLDEN_DIR = os.path.join(os.path.dirname(__file__), "golden")

_SKIP_REASON = "GrandFichier_1.xlsx not found — skipping M2 integration tests"

needs_grandfichier = pytest.mark.skipif(
    not os.path.isfile(GRANDFICHIER_PATH), reason=_SKIP_REASON
)


def _load_golden(name: str) -> dict:
    path = os.path.join(GOLDEN_DIR, name)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture(scope="module")
def m2_result():
    """Run M1 then M2 pipeline once for all tests in this module."""
    if not os.path.isfile(GRANDFICHIER_PATH):
        pytest.skip(_SKIP_REASON)

    with tempfile.TemporaryDirectory(prefix="jansa_m2_test_") as tmpdir:
        m1_dir = os.path.join(tmpdir, "m1")
        m2_dir = os.path.join(tmpdir, "m2")

        # M1
        ctx = PipelineContext(input_path=GRANDFICHIER_PATH, output_dir=m1_dir)
        run_pipeline(ctx)

        # Load M1 output
        master_path = os.path.join(m1_dir, "master_dataset.json")
        with open(master_path, "r", encoding="utf-8") as f:
            master_df = pd.DataFrame(json.load(f))

        # M2
        exit_code = run_module2(master_df, m2_dir)

        # Load M2 output
        enriched_path = os.path.join(m2_dir, "enriched_master.json")
        with open(enriched_path, "r", encoding="utf-8") as f:
            enriched_df = pd.DataFrame(json.load(f))

        anomalies_path = os.path.join(m2_dir, "anomalies.json")
        with open(anomalies_path, "r", encoding="utf-8") as f:
            anomalies = json.load(f)

        yield {
            "exit_code": exit_code,
            "enriched_df": enriched_df,
            "anomalies": anomalies,
        }


# ── Test Class ──

@needs_grandfichier
class TestM2Integration:
    """M2 end-to-end pipeline integration tests against golden snapshots."""

    def test_exit_code_zero(self, m2_result):
        """Pipeline exits with code 0."""
        assert m2_result["exit_code"] == 0

    def test_total_rows_preserved(self, m2_result):
        """M2 does not add or remove rows."""
        golden = _load_golden("golden_m2_snapshot.json")
        assert len(m2_result["enriched_df"]) == golden["total_rows"]

    def test_duplicate_flag_distribution(self, m2_result):
        """Duplicate flag distribution matches golden."""
        golden = _load_golden("golden_m2_snapshot.json")
        df = m2_result["enriched_df"]
        actual = df["duplicate_flag"].value_counts().to_dict()
        actual = {k: int(v) for k, v in actual.items()}
        assert actual == golden["duplicate_flag_distribution"]

    def test_anomaly_count(self, m2_result):
        """Total anomaly count matches golden."""
        golden = _load_golden("golden_m2_snapshot.json")
        assert len(m2_result["anomalies"]) == golden["anomaly_count"]

    def test_anomaly_type_distribution(self, m2_result):
        """Anomaly type distribution matches golden."""
        golden = _load_golden("golden_m2_snapshot.json")
        actual = {}
        for a in m2_result["anomalies"]:
            t = a.get("anomaly_type", "UNKNOWN")
            actual[t] = actual.get(t, 0) + 1
        assert actual == golden["anomaly_type_distribution"]

    def test_cross_lot_count(self, m2_result):
        """Cross-lot family count matches golden."""
        golden = _load_golden("golden_m2_snapshot.json")
        df = m2_result["enriched_df"]
        actual = int(df["is_cross_lot"].sum()) if "is_cross_lot" in df.columns else 0
        assert actual == golden["cross_lot_count"]

    def test_family_count_gte(self, m2_result):
        """Family count is at least as large as golden threshold."""
        golden = _load_golden("golden_m2_snapshot.json")
        df = m2_result["enriched_df"]
        family_count = int(df["doc_family_key"].nunique())
        assert family_count >= golden["family_count_gte"]

    def test_is_latest_count_gte(self, m2_result):
        """is_latest=True count is at least as large as golden threshold."""
        golden = _load_golden("golden_m2_snapshot.json")
        df = m2_result["enriched_df"]
        latest_count = int(df["is_latest"].sum()) if "is_latest" in df.columns else 0
        assert latest_count >= golden["is_latest_true_count_gte"]

    def test_unparseable_count_lte(self, m2_result):
        """UNPARSEABLE rows do not exceed golden threshold."""
        golden = _load_golden("golden_m2_snapshot.json")
        df = m2_result["enriched_df"]
        unp = int(df["doc_family_key"].str.startswith(UNPARSEABLE_PREFIX).sum())
        assert unp <= golden["unparseable_count_lte"]

    def test_m2_columns_added(self, m2_result):
        """All expected M2-derived columns exist."""
        golden = _load_golden("golden_m2_snapshot.json")
        df = m2_result["enriched_df"]
        for col in golden["columns_added"]:
            assert col in df.columns, f"Missing M2 column: {col}"

    def test_gp2_cross_lot_sheets_null_for_non_cross_lot(self, m2_result):
        """GP2: non-cross-lot rows have cross_lot_sheets=None."""
        df = m2_result["enriched_df"]
        if "is_cross_lot" in df.columns and "cross_lot_sheets" in df.columns:
            non_cross = df[df["is_cross_lot"] == False]
            assert non_cross["cross_lot_sheets"].isna().all(), \
                "GP2 violation: non-cross-lot rows have non-null cross_lot_sheets"
