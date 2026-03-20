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


# ── Paths ──

GRANDFICHIER_PATH = os.path.normpath(os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "GrandFichier_1.xlsx"
))
GOLDEN_DIR = os.path.join(os.path.dirname(__file__), "golden")

_SKIP = "GrandFichier_1.xlsx not found — skipping integration tests"
needs_gf = pytest.mark.skipif(not os.path.isfile(GRANDFICHIER_PATH), reason=_SKIP)


def _load_golden(name: str) -> dict:
    with open(os.path.join(GOLDEN_DIR, name), "r", encoding="utf-8") as f:
        return json.load(f)


# ── Fixture: run M1 once ──

@pytest.fixture(scope="module")
def m1_result():
    if not os.path.isfile(GRANDFICHIER_PATH):
        pytest.skip(_SKIP)

    with tempfile.TemporaryDirectory(prefix="jansa_m1_test_") as tmpdir:
        ctx = PipelineContext(input_path=GRANDFICHIER_PATH, output_dir=tmpdir)
        exit_code = run_pipeline(ctx)

        with open(os.path.join(tmpdir, "master_dataset.json"), "r", encoding="utf-8") as f:
            records = json.load(f)
        master_df = pd.DataFrame(records)

        with open(os.path.join(tmpdir, "validation_report.json"), "r", encoding="utf-8") as f:
            validation = json.load(f)

        yield {
            "exit_code": exit_code,
            "ctx": ctx,
            "master_df": master_df,
            "validation": validation,
        }


# ── Tests ──

@needs_gf
class TestM1Integration:

    def test_exit_code_zero(self, m1_result):
        assert m1_result["exit_code"] == 0

    def test_total_rows(self, m1_result):
        golden = _load_golden("golden_snapshot.json")
        assert len(m1_result["master_df"]) == golden["total_rows"]

    def test_sheets_count(self, m1_result):
        golden = _load_golden("golden_snapshot.json")
        assert m1_result["ctx"].sheets_processed == golden["sheets_count"]

    def test_row_quality_distribution(self, m1_result):
        golden = _load_golden("golden_snapshot.json")
        df = m1_result["master_df"]
        actual = {k: int(v) for k, v in df["row_quality"].value_counts().to_dict().items()}
        assert actual == golden["row_quality_distribution"]

    def test_visa_global_distribution(self, m1_result):
        golden = _load_golden("golden_snapshot.json")
        df = m1_result["master_df"]
        actual = {k: int(v) for k, v in df["visa_global"].dropna().value_counts().to_dict().items()}
        assert actual == golden["visa_global_distribution"]

    def test_document_null_count(self, m1_result):
        golden = _load_golden("golden_snapshot.json")
        df = m1_result["master_df"]
        assert int(df["document"].isna().sum()) == golden["document_nulls"]

    def test_ind_null_count(self, m1_result):
        golden = _load_golden("golden_snapshot.json")
        df = m1_result["master_df"]
        assert int(df["ind"].isna().sum()) == golden["ind_nulls"]

    def test_source_sheets_unique(self, m1_result):
        golden = _load_golden("golden_snapshot.json")
        df = m1_result["master_df"]
        assert int(df["source_sheet"].nunique()) == golden["sheets_count"]

    def test_required_columns_present(self, m1_result):
        df = m1_result["master_df"]
        required = [
            "document", "document_raw", "ind", "visa_global",
            "source_sheet", "source_row", "row_id",
            "row_quality", "assigned_approvers",
        ]
        for col in required:
            assert col in df.columns, f"Missing column: {col}"

    def test_row_id_unique(self, m1_result):
        df = m1_result["master_df"]
        assert df["row_id"].is_unique, "Duplicate row_id values found"

    def test_log_total_entries(self, m1_result):
        golden = _load_golden("golden_log_summary.json")
        assert len(m1_result["ctx"].import_log) == golden["total_entries"]

    def test_log_severity_distribution(self, m1_result):
        golden = _load_golden("golden_log_summary.json")
        actual = {}
        for entry in m1_result["ctx"].import_log:
            actual[entry.severity] = actual.get(entry.severity, 0) + 1
        assert actual == golden["severity_distribution"]

    def test_validation_checks_match_golden(self, m1_result):
        golden_val = _load_golden("golden_validation.json")
        actual_val = m1_result["validation"]
        for check_name, expected in golden_val.items():
            assert check_name in actual_val, f"Missing validation check: {check_name}"
            assert actual_val[check_name]["passed"] == expected["passed"], \
                f"Validation check '{check_name}': expected passed={expected['passed']}, got {actual_val[check_name]['passed']}"

    def test_gp2_no_empty_strings(self, m1_result):
        """GP2 enforcement: nullable fields contain None, never empty string."""
        df = m1_result["master_df"]
        nullable_cols = [
            "visa_global", "date_diffusion", "date_reception",
            "date_contractuelle_visa", "ind",
        ]
        for col in nullable_cols:
            if col in df.columns:
                empty_count = int((df[col] == "").sum())
                assert empty_count == 0, f"GP2 violation: {col} has {empty_count} empty strings"
