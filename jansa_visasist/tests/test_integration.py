"""
JANSA VISASIST — Module 1 Integration Tests.

Runs the full M1 pipeline against data/GrandFichier_1.xlsx
and compares outputs to golden snapshots.

Tests skip gracefully if GrandFichier is not present.
"""

import json
import os
import tempfile

import pandas as pd
import pytest

from jansa_visasist.context import PipelineContext
from jansa_visasist.main import run_pipeline


# ── Fixtures ──

GRANDFICHIER_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "GrandFichier_1.xlsx"
)
GRANDFICHIER_PATH = os.path.normpath(GRANDFICHIER_PATH)

GOLDEN_DIR = os.path.join(os.path.dirname(__file__), "golden")

_SKIP_REASON = "GrandFichier_1.xlsx not found — skipping integration tests"

needs_grandfichier = pytest.mark.skipif(
    not os.path.isfile(GRANDFICHIER_PATH), reason=_SKIP_REASON
)


def _load_golden(name: str) -> dict:
    path = os.path.join(GOLDEN_DIR, name)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture(scope="module")
def m1_result():
    """Run M1 pipeline once for all tests in this module."""
    if not os.path.isfile(GRANDFICHIER_PATH):
        pytest.skip(_SKIP_REASON)

    with tempfile.TemporaryDirectory(prefix="jansa_m1_test_") as tmpdir:
        ctx = PipelineContext(input_path=GRANDFICHIER_PATH, output_dir=tmpdir)
        exit_code = run_pipeline(ctx)

        # Load master dataset
        master_path = os.path.join(tmpdir, "master_dataset.json")
        with open(master_path, "r", encoding="utf-8") as f:
            records = json.load(f)
        master_df = pd.DataFrame(records)

        # Load validation report
        val_path = os.path.join(tmpdir, "validation_report.json")
        with open(val_path, "r", encoding="utf-8") as f:
            validation = json.load(f)

        yield {
            "exit_code": exit_code,
            "ctx": ctx,
            "master_df": master_df,
            "validation": validation,
        }


# ── Test Class ──

@needs_grandfichier
class TestM1Integration:
    """M1 end-to-end pipeline integration tests against golden snapshots."""

    def test_exit_code_zero(self, m1_result):
        """Pipeline exits with code 0."""
        assert m1_result["exit_code"] == 0

    def test_total_rows(self, m1_result):
        """Total row count matches golden snapshot."""
        golden = _load_golden("golden_snapshot.json")
        assert len(m1_result["master_df"]) == golden["total_rows"]

    def test_sheets_count(self, m1_result):
        """Number of processed sheets matches golden."""
        golden = _load_golden("golden_snapshot.json")
        assert m1_result["ctx"].sheets_processed == golden["sheets_count"]

    def test_row_quality_distribution(self, m1_result):
        """Row quality distribution matches golden."""
        golden = _load_golden("golden_snapshot.json")
        df = m1_result["master_df"]
        actual = df["row_quality"].value_counts().to_dict()
        actual = {k: int(v) for k, v in actual.items()}
        assert actual == golden["row_quality_distribution"]

    def test_visa_global_distribution(self, m1_result):
        """Visa global distribution matches golden."""
        golden = _load_golden("golden_snapshot.json")
        df = m1_result["master_df"]
        actual = df["visa_global"].fillna("null").value_counts().to_dict()
        actual = {k: int(v) for k, v in actual.items()}
        assert actual == golden["visa_global_distribution"]

    def test_no_null_documents(self, m1_result):
        """No null document values (post-validation)."""
        golden = _load_golden("golden_snapshot.json")
        df = m1_result["master_df"]
        null_count = int(df["document"].isna().sum())
        assert null_count == golden["has_document_null_count"]

    def test_ind_null_count_bounded(self, m1_result):
        """Null IND count does not exceed golden threshold."""
        golden = _load_golden("golden_snapshot.json")
        df = m1_result["master_df"]
        null_count = int(df["ind"].isna().sum())
        assert null_count <= golden["has_ind_null_count_lte"]

    def test_source_sheets_unique(self, m1_result):
        """Unique source_sheet count matches golden."""
        golden = _load_golden("golden_snapshot.json")
        df = m1_result["master_df"]
        assert int(df["source_sheet"].nunique()) == golden["source_sheets_unique_count"]

    def test_required_columns_present(self, m1_result):
        """All golden-listed columns exist in master dataset."""
        golden = _load_golden("golden_snapshot.json")
        df = m1_result["master_df"]
        for col in golden["columns_present"]:
            assert col in df.columns, f"Missing column: {col}"

    def test_row_id_unique(self, m1_result):
        """Every row_id is unique."""
        df = m1_result["master_df"]
        assert df["row_id"].is_unique, "Duplicate row_id values found"

    def test_log_total_entries(self, m1_result):
        """Import log entry count matches golden."""
        golden = _load_golden("golden_log_summary.json")
        assert len(m1_result["ctx"].import_log) == golden["total_entries"]

    def test_log_severity_distribution(self, m1_result):
        """Log severity distribution matches golden."""
        golden = _load_golden("golden_log_summary.json")
        actual = {}
        for entry in m1_result["ctx"].import_log:
            sev = entry.severity
            actual[sev] = actual.get(sev, 0) + 1
        assert actual == golden["severity_distribution"]

    def test_validation_total_checks(self, m1_result):
        """Number of validation checks matches golden."""
        golden = _load_golden("golden_validation.json")
        assert len(m1_result["validation"]) == golden["total_checks"]

    def test_validation_passed_count(self, m1_result):
        """Number of passed validation checks matches golden."""
        golden = _load_golden("golden_validation.json")
        passed = sum(
            1 for v in m1_result["validation"].values()
            if v.get("passed", False)
        )
        assert passed == golden["passed_checks"]

    def test_validation_date_sanity_fails(self, m1_result):
        """The date_sanity check fails as recorded in golden."""
        golden = _load_golden("golden_validation.json")
        if "date_sanity" in golden.get("checks", {}):
            expected_pass = golden["checks"]["date_sanity"]["passed"]
            actual = m1_result["validation"].get("date_sanity", {}).get("passed")
            assert actual == expected_pass

    def test_gp2_no_empty_strings_in_nullables(self, m1_result):
        """GP2 enforcement: nullable fields contain None, never empty string."""
        df = m1_result["master_df"]
        nullable_cols = ["visa_global", "date_diffusion", "date_reception",
                         "date_contractuelle_visa", "ind"]
        for col in nullable_cols:
            if col in df.columns:
                empty_count = (df[col] == "").sum()
                assert empty_count == 0, f"GP2 violation: {col} has {empty_count} empty strings"
