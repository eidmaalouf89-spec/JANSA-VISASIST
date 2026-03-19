"""
Module 3 Unit Tests.
Tests each pipeline step with synthetic DataFrames.
"""

import datetime

import pytest
import pandas as pd
import numpy as np

from jansa_visasist.pipeline.m3.filtering import (
    _parse_assigned_approvers,
    _is_all_hm,
    validate_and_prepare,
    filter_to_pending_scope,
    Module3InputError,
)
from jansa_visasist.pipeline.m3.time_metrics import add_time_metrics
from jansa_visasist.pipeline.m3.approver_analysis import (
    _analyze_single_row,
    add_approver_analysis,
)
from jansa_visasist.pipeline.m3.consensus import _determine_consensus, add_consensus_type
from jansa_visasist.pipeline.m3.categories import _determine_category, add_categories
from jansa_visasist.pipeline.m3.scoring import _compute_score, add_priority_scores
from jansa_visasist.pipeline.m3.summaries import (
    sort_and_finalize,
    build_category_summaries,
)
from jansa_visasist.context_m3 import Module3Context
from jansa_visasist.config_m3 import (
    OVERDUE_MAX_POINTS, OVERDUE_CAP_DAYS,
    PROXIMITY_3D_POINTS, PROXIMITY_7D_POINTS, PROXIMITY_14D_POINTS,
    COMPLETENESS_ALL_APPROVE, COMPLETENESS_ALL_REJECT, COMPLETENESS_MIXED,
    REVISION_DEPTH_HIGH, REVISION_DEPTH_MED,
    MISSING_DEADLINE_PENALTY,
)


# ──────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────

def _make_base_row(**overrides):
    """Build a minimal valid row dict for testing."""
    row = {
        "row_id": "test_0_1",
        "source_sheet": "LOT_TEST",
        "source_row": 1,
        "document": "P17T2TEST",
        "document_raw": "P17T2TEST",
        "doc_family_key": "P17T2TEST",
        "doc_version_key": "P17T2TEST::A::LOT_TEST",
        "lot": "T001",
        "ind": "A",
        "ind_sort_order": 1,
        "revision_count": 1,
        "is_latest": True,
        "duplicate_flag": "UNIQUE",
        "visa_global": None,
        "visa_global_raw": None,
        "date_diffusion": "2024-06-01",
        "date_diffusion_raw": "2024-06-01",
        "date_reception": None,
        "date_contractuelle_visa": "2024-06-15",
        "date_contractuelle_visa_raw": "2024-06-15",
        "assigned_approvers": ["MOEX_GEMO", "ARCHI_MOX"],
        "MOEX_GEMO_statut": None,
        "MOEX_GEMO_statut_raw": None,
        "MOEX_GEMO_date": None,
        "MOEX_GEMO_date_raw": None,
        "MOEX_GEMO_n": None,
        "ARCHI_MOX_statut": None,
        "ARCHI_MOX_statut_raw": None,
        "ARCHI_MOX_date": None,
        "ARCHI_MOX_date_raw": None,
        "ARCHI_MOX_n": None,
    }
    row.update(overrides)
    return row


def _make_df(rows):
    """Build DataFrame from list of row dicts."""
    return pd.DataFrame(rows)


def _make_ctx(ref_date=None):
    if ref_date is None:
        ref_date = datetime.date(2024, 7, 1)
    return Module3Context(output_dir="/tmp/test_m3", reference_date=ref_date)


# ──────────────────────────────────────────────────
# Test: parse_assigned_approvers
# ──────────────────────────────────────────────────

class TestParseAssignedApprovers:
    def test_json_string(self):
        result = _parse_assigned_approvers('["A", "B"]')
        assert result == ["A", "B"]

    def test_semicolon_string(self):
        result = _parse_assigned_approvers("A;B;C")
        assert result == ["A", "B", "C"]

    def test_list_passthrough(self):
        result = _parse_assigned_approvers(["X", "Y"])
        assert result == ["X", "Y"]

    def test_null(self):
        assert _parse_assigned_approvers(None) == []
        assert _parse_assigned_approvers(float('nan')) == []

    def test_empty_string(self):
        assert _parse_assigned_approvers("") == []


# ──────────────────────────────────────────────────
# Test: Filtering (Step 1)
# ──────────────────────────────────────────────────

class TestFiltering:
    def test_excludes_not_latest(self):
        rows = [_make_base_row(is_latest=False)]
        df = _make_df(rows)
        ctx = _make_ctx()
        df = validate_and_prepare(df, ctx)
        pending, excl = filter_to_pending_scope(df, ctx)
        assert len(pending) == 0
        assert len(excl) == 1
        assert ctx.exclusion_log[0].exclusion_reason == "NOT_LATEST"

    def test_excludes_duplicate(self):
        rows = [_make_base_row(duplicate_flag="DUPLICATE")]
        df = _make_df(rows)
        ctx = _make_ctx()
        df = validate_and_prepare(df, ctx)
        pending, excl = filter_to_pending_scope(df, ctx)
        assert len(pending) == 0
        assert ctx.exclusion_log[0].exclusion_reason == "DUPLICATE"

    def test_excludes_resolved_visa(self):
        rows = [_make_base_row(visa_global="VSO")]
        df = _make_df(rows)
        ctx = _make_ctx()
        df = validate_and_prepare(df, ctx)
        pending, excl = filter_to_pending_scope(df, ctx)
        assert len(pending) == 0
        assert ctx.exclusion_log[0].exclusion_reason == "VISA_RESOLVED"

    def test_excludes_hm_visa(self):
        rows = [_make_base_row(visa_global="HM")]
        df = _make_df(rows)
        ctx = _make_ctx()
        df = validate_and_prepare(df, ctx)
        pending, excl = filter_to_pending_scope(df, ctx)
        assert len(pending) == 0
        assert ctx.exclusion_log[0].exclusion_reason == "VISA_HM"

    def test_excludes_all_hm_approvers(self):
        rows = [_make_base_row(MOEX_GEMO_statut="HM", ARCHI_MOX_statut="HM")]
        df = _make_df(rows)
        ctx = _make_ctx()
        df = validate_and_prepare(df, ctx)
        pending, excl = filter_to_pending_scope(df, ctx)
        assert len(pending) == 0
        assert ctx.exclusion_log[0].exclusion_reason == "ALL_APPROVERS_HM"

    def test_keeps_pending_row(self):
        rows = [_make_base_row()]
        df = _make_df(rows)
        ctx = _make_ctx()
        df = validate_and_prepare(df, ctx)
        pending, excl = filter_to_pending_scope(df, ctx)
        assert len(pending) == 1
        assert len(excl) == 0

    def test_row_count_preserved(self):
        rows = [
            _make_base_row(row_id="r1"),
            _make_base_row(row_id="r2", is_latest=False),
            _make_base_row(row_id="r3", visa_global="REF"),
        ]
        df = _make_df(rows)
        ctx = _make_ctx()
        df = validate_and_prepare(df, ctx)
        pending, excl = filter_to_pending_scope(df, ctx)
        assert len(pending) + len(excl) == 3


# ──────────────────────────────────────────────────
# Test: Time Metrics (Step 2)
# ──────────────────────────────────────────────────

class TestTimeMetrics:
    def test_overdue(self):
        rows = [_make_base_row(date_contractuelle_visa="2024-06-20")]
        df = _make_df(rows)
        ctx = _make_ctx(datetime.date(2024, 7, 1))
        df = validate_and_prepare(df, ctx)
        pending, _ = filter_to_pending_scope(df, ctx)
        pending = add_time_metrics(pending, datetime.date(2024, 7, 1))
        assert pending.iloc[0]["is_overdue"] == True  # noqa
        assert pending.iloc[0]["days_overdue"] == 11

    def test_not_overdue(self):
        rows = [_make_base_row(date_contractuelle_visa="2024-07-10")]
        df = _make_df(rows)
        ctx = _make_ctx(datetime.date(2024, 7, 1))
        df = validate_and_prepare(df, ctx)
        pending, _ = filter_to_pending_scope(df, ctx)
        pending = add_time_metrics(pending, datetime.date(2024, 7, 1))
        assert pending.iloc[0]["is_overdue"] == False  # noqa
        assert pending.iloc[0]["days_until_deadline"] == 9

    def test_no_deadline(self):
        rows = [_make_base_row(date_contractuelle_visa=None)]
        df = _make_df(rows)
        ctx = _make_ctx()
        df = validate_and_prepare(df, ctx)
        pending, _ = filter_to_pending_scope(df, ctx)
        pending = add_time_metrics(pending, datetime.date(2024, 7, 1))
        assert pending.iloc[0]["has_deadline"] == False  # noqa
        assert pending.iloc[0]["is_overdue"] == False  # noqa

    def test_null_diffusion(self):
        rows = [_make_base_row(date_diffusion=None)]
        df = _make_df(rows)
        ctx = _make_ctx()
        df = validate_and_prepare(df, ctx)
        pending, _ = filter_to_pending_scope(df, ctx)
        pending = add_time_metrics(pending, datetime.date(2024, 7, 1))
        assert pending.iloc[0]["days_since_diffusion"] is None


# ──────────────────────────────────────────────────
# Test: Consensus (Step 4)
# ──────────────────────────────────────────────────

class TestConsensus:
    """
    _determine_consensus(replied_among_relevant, pending_among_relevant,
                         relevant_count, ref_count, vso_vao_count)
    """
    def test_not_started(self):
        # 0 relevant replied, 2 pending, 2 relevant
        assert _determine_consensus(0, 2, 2, 0, 0) == "NOT_STARTED"

    def test_not_started_zero_pending(self):
        # Edge: 0 relevant replied, 0 pending, 0 relevant → NOT_STARTED then ALL_HM
        # But 0 replied wins first
        assert _determine_consensus(0, 0, 0, 0, 0) == "NOT_STARTED"

    def test_not_started_with_hm_replies(self):
        """
        BUG FIX: When HM approvers have replied but all relevant approvers
        are still pending, the consensus should be NOT_STARTED (not INCOMPLETE).
        """
        # 2 relevant approvers, 0 have replied among relevant, 2 pending
        assert _determine_consensus(0, 2, 2, 0, 0) == "NOT_STARTED"

    def test_incomplete(self):
        # 1 relevant replied, 1 still pending, 2 relevant
        assert _determine_consensus(1, 1, 2, 0, 1) == "INCOMPLETE"

    def test_all_hm_safety_net(self):
        """
        ALL_HM safety net at Rule 3. In practice, all-HM rows are excluded
        in Step 1. If relevant_count=0, then replied_among_relevant=0 and
        Rule 1 fires first (→ NOT_STARTED). Rule 3 is reachable only in
        theoretically impossible states (replied > 0 but relevant = 0).
        We still test it directly to verify the safety net code path.
        """
        # Impossible state: 1 replied among relevant but 0 relevant
        # Rule 1 skipped (1 > 0), Rule 2 skipped (0 not > 0), Rule 3 fires
        assert _determine_consensus(1, 0, 0, 0, 0) == "ALL_HM"

    def test_all_hm_realistic(self):
        """
        Realistic all-HM scenario: relevant=0, replied=0, pending=0.
        Rule 1 fires → NOT_STARTED (this is correct per spec; the row
        should have been excluded in Step 1 anyway).
        """
        assert _determine_consensus(0, 0, 0, 0, 0) == "NOT_STARTED"

    def test_mixed(self):
        assert _determine_consensus(3, 0, 3, 1, 2) == "MIXED"

    def test_all_reject(self):
        assert _determine_consensus(2, 0, 2, 2, 0) == "ALL_REJECT"

    def test_all_approve(self):
        assert _determine_consensus(2, 0, 2, 0, 2) == "ALL_APPROVE"

    def test_non_driving_fallback(self):
        # All replied but no VSO/VAO/REF — only SUS/DEF
        # replied_among_relevant=2, pending=0, relevant=2, ref=0, vso_vao=0
        assert _determine_consensus(2, 0, 2, 0, 0) == "INCOMPLETE"


# ──────────────────────────────────────────────────
# Test: Categories (Step 5)
# ──────────────────────────────────────────────────

class TestCategories:
    def test_easy_win(self):
        assert _determine_category("ALL_APPROVE", 1) == "EASY_WIN_APPROVE"

    def test_blocked(self):
        assert _determine_category("ALL_REJECT", 3) == "BLOCKED"

    def test_fast_reject(self):
        assert _determine_category("ALL_REJECT", 1) == "FAST_REJECT"

    def test_fast_reject_zero(self):
        assert _determine_category("ALL_REJECT", 0) == "FAST_REJECT"

    def test_conflict(self):
        assert _determine_category("MIXED", 1) == "CONFLICT"

    def test_waiting(self):
        assert _determine_category("INCOMPLETE", 1) == "WAITING"

    def test_not_started(self):
        assert _determine_category("NOT_STARTED", 1) == "NOT_STARTED"


# ──────────────────────────────────────────────────
# Test: Scoring (Step 6)
# ──────────────────────────────────────────────────

class TestScoring:
    def test_overdue_0_days(self):
        score = _compute_score(True, 0, True, -0, "INCOMPLETE", 1)
        assert score == 0.0  # 0 overdue days → 0 overdue points, no proximity, no completeness

    def test_overdue_15_days(self):
        score = _compute_score(True, 15, True, -15, "ALL_APPROVE", 1)
        expected_overdue = (15 / OVERDUE_CAP_DAYS) * OVERDUE_MAX_POINTS  # 20.0
        expected = expected_overdue + COMPLETENESS_ALL_APPROVE  # 20 + 20 = 40
        assert abs(score - expected) < 0.01

    def test_overdue_capped_at_30(self):
        score = _compute_score(True, 45, True, -45, "INCOMPLETE", 1)
        assert abs(score - OVERDUE_MAX_POINTS) < 0.01

    def test_proximity_3d(self):
        score = _compute_score(False, 0, True, 2, "INCOMPLETE", 1)
        assert abs(score - PROXIMITY_3D_POINTS) < 0.01

    def test_proximity_7d(self):
        score = _compute_score(False, 0, True, 5, "INCOMPLETE", 1)
        assert abs(score - PROXIMITY_7D_POINTS) < 0.01

    def test_proximity_14d(self):
        score = _compute_score(False, 0, True, 10, "INCOMPLETE", 1)
        assert abs(score - PROXIMITY_14D_POINTS) < 0.01

    def test_no_proximity_beyond_14d(self):
        score = _compute_score(False, 0, True, 20, "INCOMPLETE", 1)
        assert score == 0.0

    def test_overdue_no_proximity(self):
        """Overdue and proximity are mutually exclusive."""
        score_overdue = _compute_score(True, 5, True, -5, "INCOMPLETE", 1)
        # Should get overdue points but NOT proximity
        expected_overdue = (5 / OVERDUE_CAP_DAYS) * OVERDUE_MAX_POINTS
        assert abs(score_overdue - expected_overdue) < 0.01

    def test_completeness_all_approve(self):
        score = _compute_score(False, 0, True, 20, "ALL_APPROVE", 1)
        assert abs(score - COMPLETENESS_ALL_APPROVE) < 0.01

    def test_completeness_all_reject(self):
        score = _compute_score(False, 0, True, 20, "ALL_REJECT", 1)
        assert abs(score - COMPLETENESS_ALL_REJECT) < 0.01

    def test_revision_depth_high(self):
        score = _compute_score(False, 0, True, 20, "INCOMPLETE", 5)
        assert abs(score - REVISION_DEPTH_HIGH) < 0.01

    def test_revision_depth_med(self):
        score = _compute_score(False, 0, True, 20, "INCOMPLETE", 2)
        assert abs(score - REVISION_DEPTH_MED) < 0.01

    def test_missing_deadline_penalty(self):
        score = _compute_score(False, 0, False, None, "INCOMPLETE", 1)
        assert abs(score - 0.0) < 0.01  # -10 clamped to 0

    def test_score_clamped_to_0(self):
        score = _compute_score(False, 0, False, None, "INCOMPLETE", 1)
        assert score >= 0

    def test_score_max_theoretical(self):
        # Max: 40 overdue + 0 proximity + 20 completeness + 5 revision = 65
        score = _compute_score(True, 30, True, -30, "ALL_APPROVE", 5)
        assert abs(score - 65.0) < 0.01


# ──────────────────────────────────────────────────
# Test: Summaries (Step 7)
# ──────────────────────────────────────────────────

class TestSummaries:
    def test_category_totals_match(self):
        rows = [
            _make_base_row(row_id="r1"),
            _make_base_row(row_id="r2"),
            _make_base_row(row_id="r3"),
        ]
        df = _make_df(rows)
        ctx = _make_ctx()
        df = validate_and_prepare(df, ctx)
        pending, _ = filter_to_pending_scope(df, ctx)
        pending = add_time_metrics(pending, ctx.reference_date)
        pending = add_approver_analysis(pending, ctx)
        pending = add_consensus_type(pending)
        pending = add_categories(pending)
        pending = add_priority_scores(pending)
        pending = sort_and_finalize(pending)
        summary = build_category_summaries(pending)

        cat_total = summary.loc[summary["group_type"] == "category", "count"].sum()
        assert cat_total == len(pending)
