"""
Module 3 Integration Tests.
Load actual M2 output -> run full M3 pipeline -> verify all constraints.
"""

import json
import os
import tempfile

import pytest
import numpy as np
import pandas as pd

from jansa_visasist.main_m3 import run_module3, _load_enriched_dataset
from jansa_visasist.config_m3 import VALID_CATEGORIES, VALID_CONSENSUS_TYPES, SCORE_MIN, SCORE_MAX


# ── Path resolution ──
_PROJECT_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
M2_OUTPUT_JSON = os.path.join(_PROJECT_ROOT, "output", "m2", "enriched_master_dataset.json")


@pytest.fixture(scope="module")
def m3_result():
    """Run M3 pipeline once on actual M2 output."""
    if not os.path.exists(M2_OUTPUT_JSON):
        pytest.skip(f"M2 output not found at {M2_OUTPUT_JSON}")

    enriched_df = _load_enriched_dataset(M2_OUTPUT_JSON)
    tmpdir = tempfile.mkdtemp(prefix="jansa_m3_")
    queue_df, summary_df, exclusion_df = run_module3(enriched_df, tmpdir)

    return queue_df, summary_df, exclusion_df, tmpdir, enriched_df


class TestM3Integration:
    def test_queue_not_empty(self, m3_result):
        queue_df, _, _, _, _ = m3_result
        assert len(queue_df) > 0

    def test_row_count_preserved(self, m3_result):
        queue_df, _, exclusion_df, _, enriched_df = m3_result
        assert len(queue_df) + len(exclusion_df) == len(enriched_df)

    def test_queue_columns_present(self, m3_result):
        queue_df, _, _, _, _ = m3_result
        required_cols = [
            "days_since_diffusion", "days_until_deadline", "is_overdue",
            "days_overdue", "has_deadline", "approver_response_summary",
            "total_assigned", "replied", "pending",
            "approvers_vso", "approvers_vao", "approvers_ref", "approvers_hm",
            "relevant_approvers", "missing_approvers", "blocking_approvers",
            "consensus_type", "category", "priority_score",
        ]
        for col in required_cols:
            assert col in queue_df.columns, f"Missing column: {col}"

    def test_all_categories_valid(self, m3_result):
        queue_df, _, _, _, _ = m3_result
        assert queue_df["category"].isin(VALID_CATEGORIES).all()

    def test_all_consensus_valid(self, m3_result):
        queue_df, _, _, _, _ = m3_result
        assert queue_df["consensus_type"].isin(VALID_CONSENSUS_TYPES).all()

    def test_every_row_has_category(self, m3_result):
        queue_df, _, _, _, _ = m3_result
        assert queue_df["category"].notna().all()

    def test_scores_in_range(self, m3_result):
        queue_df, _, _, _, _ = m3_result
        assert queue_df["priority_score"].between(SCORE_MIN, SCORE_MAX).all()

    def test_no_latest_false_in_queue(self, m3_result):
        queue_df, _, _, _, _ = m3_result
        if "is_latest" in queue_df.columns:
            assert (queue_df["is_latest"] == True).all()  # noqa

    def test_no_duplicate_in_queue(self, m3_result):
        queue_df, _, _, _, _ = m3_result
        if "duplicate_flag" in queue_df.columns:
            assert (queue_df["duplicate_flag"] != "DUPLICATE").all()

    def test_no_resolved_visa_in_queue(self, m3_result):
        queue_df, _, _, _, _ = m3_result
        assert queue_df["visa_global"].isna().all()

    def test_category_totals_match_queue(self, m3_result):
        queue_df, summary_df, _, _, _ = m3_result
        cat_total = summary_df.loc[
            summary_df["group_type"] == "category", "count"
        ].sum()
        assert cat_total == len(queue_df)

    def test_blocked_has_revision_gt_1(self, m3_result):
        queue_df, _, _, _, _ = m3_result
        blocked = queue_df[queue_df["category"] == "BLOCKED"]
        if len(blocked) > 0:
            assert (blocked["revision_count"] > 1).all()
            assert (blocked["consensus_type"] == "ALL_REJECT").all()

    def test_fast_reject_has_revision_le_1(self, m3_result):
        queue_df, _, _, _, _ = m3_result
        fr = queue_df[queue_df["category"] == "FAST_REJECT"]
        if len(fr) > 0:
            assert (fr["revision_count"] <= 1).all()
            assert (fr["consensus_type"] == "ALL_REJECT").all()

    def test_sorted_by_priority(self, m3_result):
        queue_df, _, _, _, _ = m3_result
        if len(queue_df) < 2:
            return
        scores = queue_df["priority_score"].values
        # First score >= second score (descending)
        assert scores[0] >= scores[1]

    def test_approver_counts_are_integers(self, m3_result):
        queue_df, _, _, _, _ = m3_result
        int_cols = ["total_assigned", "replied", "pending",
                    "approvers_vso", "approvers_vao", "approvers_ref",
                    "approvers_hm", "relevant_approvers"]
        for col in int_cols:
            # Check all values are int-like
            for val in queue_df[col]:
                assert isinstance(val, (int, np.integer)), \
                    f"{col} has non-int value: {val} ({type(val)})"

    def test_missing_approvers_are_lists(self, m3_result):
        queue_df, _, _, _, _ = m3_result
        for col in ["missing_approvers", "blocking_approvers"]:
            for val in queue_df[col]:
                assert isinstance(val, list), f"{col} has non-list: {val}"

    def test_summary_has_three_group_types(self, m3_result):
        _, summary_df, _, _, _ = m3_result
        group_types = set(summary_df["group_type"])
        assert "category" in group_types
        assert "lot" in group_types
        assert "sheet" in group_types

    def test_output_files_created(self, m3_result):
        _, _, _, tmpdir, _ = m3_result
        expected = [
            "m3_priority_queue.json",
            "m3_priority_queue.csv",
            "m3_category_summary.json",
            "m3_category_summary.csv",
            "m3_exclusion_log.json",
            "m3_exclusion_log.csv",
            "m3_pipeline_report.json",
        ]
        for fname in expected:
            assert os.path.exists(os.path.join(tmpdir, fname)), f"Missing: {fname}"

    def test_pipeline_report_structure(self, m3_result):
        _, _, _, tmpdir, _ = m3_result
        with open(os.path.join(tmpdir, "m3_pipeline_report.json"), encoding="utf-8") as f:
            report = json.load(f)
        assert "module" in report
        assert report["module"] == "module_3"
        assert "input_rows" in report
        assert "pending_count" in report
        assert "category_distribution" in report

    def test_relevant_equals_total_minus_hm(self, m3_result):
        """relevant_approvers must equal total_assigned - approvers_hm."""
        queue_df, _, _, _, _ = m3_result
        computed = queue_df["total_assigned"] - queue_df["approvers_hm"]
        assert (queue_df["relevant_approvers"] == computed).all()

    def test_not_started_has_zero_relevant_replies(self, m3_result):
        """NOT_STARTED rows must have no relevant approver replies."""
        queue_df, _, _, _, _ = m3_result
        not_started = queue_df[queue_df["consensus_type"] == "NOT_STARTED"]
        if len(not_started) > 0:
            # All NOT_STARTED rows: relevant_approvers - pending_among_relevant == 0
            # (pending among relevant = relevant - replied_among_relevant)
            # Equivalent: approvers_vso + approvers_vao + approvers_ref == 0
            driving_count = (not_started["approvers_vso"]
                           + not_started["approvers_vao"]
                           + not_started["approvers_ref"])
            assert (driving_count == 0).all(), \
                "NOT_STARTED rows have non-zero driving replies"
