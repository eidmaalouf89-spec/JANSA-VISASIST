"""
Module 4 Contract Tests — Schema Lock Verification
Tests that M4 outputs conform to the locked schema contract in m4_schema.py.

These tests fail if:
- a column is missing from a DataFrame schema
- a field is renamed
- enum values drift
- block_status is missing
- lifecycle_state is invalid
- return signature changes
"""

import datetime
from unittest.mock import patch

import pandas as pd
import pytest

from jansa_visasist.schemas.m4_schema import (
    ANALYSIS_RESULT_REQUIRED_KEYS,
    ANALYSIS_RESULT_BLOCK_NAMES,
    VALID_BLOCK_STATUS,
    VALID_LIFECYCLE_STATES,
    VALID_AGREEMENT_TYPES,
    VALID_BLOCKING_PATTERNS,
    VALID_DEADLINE_STATUSES,
    VALID_AGE_BRACKETS,
    G1_COLUMNS,
    G2_COLUMNS,
    G3_COLUMNS,
    G4_COLUMNS,
    G1_SCHEMA,
    G2_SCHEMA,
    G3_SCHEMA,
    G4_SCHEMA,
    SCHEMA_REGISTRY,
    validate_analysis_result_schema,
    validate_dataframe_schema,
    enforce_column_order,
    empty_dataframe,
    M4SchemaValidationError,
)


# ============================================================================
# FIXTURES
# ============================================================================

def _make_valid_block(status="OK"):
    """Minimal valid A-block dict."""
    return {"block_status": status}


def _make_valid_analysis_result(row_id="test_001"):
    """Minimal valid analysis_result dict passing all schema checks."""
    return {
        "row_id": row_id,
        "agreement": {
            "block_status": "OK",
            "agreement_type": "FULL_APPROVAL",
            "approve_count": 3,
            "reject_count": 0,
            "pending_count": 0,
            "hm_count": 0,
            "non_classifiable_count": 0,
            "approve_list": ["A", "B", "C"],
            "reject_list": [],
            "pending_list": [],
            "non_classifiable_list": [],
            "agreement_detail": "test",
            "consensus_match": True,
        },
        "conflict": {"block_status": "OK", "conflict_detected": False,
                      "conflict_severity": None, "majority_position": None,
                      "approvers_against_majority": [], "conflict_detail": None},
        "missing": {"block_status": "OK", "missing_approvers": [],
                     "total_missing": 0, "worst_urgency": None,
                     "critical_missing": [], "missing_summary": "test"},
        "blocking": {"block_status": "OK", "is_blocked": False,
                      "blocking_pattern": "NOT_BLOCKED", "blocking_approvers": [],
                      "is_systemic_blocker": [], "consecutive_rejections": None,
                      "blocking_detail": "test"},
        "delta": {"block_status": "OK", "has_previous": False,
                   "previous_ind": None, "visa_global_change": None,
                   "approver_changes": [], "total_changed": 0,
                   "new_responses": 0, "lost_responses": 0, "reversals": 0,
                   "delta_summary": "test"},
        "time": {"block_status": "OK", "days_since_diffusion": 5,
                  "days_until_deadline": 10, "is_overdue": False,
                  "days_overdue": 0, "has_deadline": True,
                  "deadline_status": "COMFORTABLE", "age_bracket": "FRESH",
                  "time_summary": "test"},
        "lifecycle_state": "READY_TO_ISSUE",
        "analysis_degraded": False,
        "failed_blocks": [],
    }


def _make_g1_row():
    """Minimal valid G1 row dict."""
    return {
        "approver_key": "MOEX_GEMO",
        "display_name": "MOEX GEMO",
        "total_latest_assigned": 10,
        "total_responded": 8,
        "total_blocking": 2,
        "blocking_rate": 0.2,
        "avg_response_days": 5.5,
        "is_systemic_blocker": False,
        "blocked_families": [],
        "severity": "LOW",
    }


def _make_g2_row():
    """Minimal valid G2 row dict."""
    return {
        "doc_family_key": "DOC_001",
        "source_sheet": "LOT1",
        "document": "DOC_001",
        "titre": "Test Doc",
        "is_looping": False,
        "loop_length": 0,
        "loop_start_ind": None,
        "loop_end_ind": None,
        "persistent_blockers": [],
        "latest_visa_global": None,
    }


def _make_g3_row():
    """Minimal valid G3 row dict."""
    return {
        "row_id": "test_001",
        "risk_score": 0,
        "is_high_risk": False,
        "contributing_factors": [],
        "factor_details": [],
    }


def _make_g4_row():
    """Minimal valid G4 row dict."""
    return {
        "source_sheet": "LOT1",
        "total_documents": 10,
        "total_pending": 3,
        "total_overdue": 1,
        "total_high_risk": 0,
        "category_distribution": {},
        "approval_rate": 0.7,
        "avg_priority_score": 5.0,
        "avg_days_pending": 12.0,
        "is_high_risk_cluster": False,
        "health_score": 85.0,
    }


# ============================================================================
# 1. test_analysis_result_required_keys
# ============================================================================

class TestAnalysisResultRequiredKeys:
    """All required top-level keys must be present."""

    def test_valid_result_passes(self):
        result = _make_valid_analysis_result()
        errors = validate_analysis_result_schema(result)
        assert errors == [], f"Valid result should have no errors: {errors}"

    def test_missing_row_id(self):
        result = _make_valid_analysis_result()
        del result["row_id"]
        errors = validate_analysis_result_schema(result)
        assert any("row_id" in e for e in errors)

    def test_missing_lifecycle_state(self):
        result = _make_valid_analysis_result()
        del result["lifecycle_state"]
        errors = validate_analysis_result_schema(result)
        assert any("lifecycle_state" in e for e in errors)

    def test_missing_analysis_degraded(self):
        result = _make_valid_analysis_result()
        del result["analysis_degraded"]
        errors = validate_analysis_result_schema(result)
        assert any("analysis_degraded" in e for e in errors)

    def test_missing_failed_blocks(self):
        result = _make_valid_analysis_result()
        del result["failed_blocks"]
        errors = validate_analysis_result_schema(result)
        assert any("failed_blocks" in e for e in errors)

    @pytest.mark.parametrize("block_name", list(ANALYSIS_RESULT_BLOCK_NAMES))
    def test_missing_block(self, block_name):
        result = _make_valid_analysis_result()
        del result[block_name]
        errors = validate_analysis_result_schema(result)
        assert any(block_name in e for e in errors)

    def test_required_keys_match_schema_constant(self):
        """ANALYSIS_RESULT_REQUIRED_KEYS must cover the exact 10 keys."""
        expected = {
            "row_id", "agreement", "conflict", "missing", "blocking",
            "delta", "time", "lifecycle_state", "analysis_degraded", "failed_blocks",
        }
        assert set(ANALYSIS_RESULT_REQUIRED_KEYS) == expected


# ============================================================================
# 2. test_analysis_result_block_status_valid
# ============================================================================

class TestAnalysisResultBlockStatus:
    """Every A-block must have block_status ∈ VALID_BLOCK_STATUS."""

    def test_ok_status_passes(self):
        result = _make_valid_analysis_result()
        errors = validate_analysis_result_schema(result)
        assert errors == []

    def test_failed_status_passes(self):
        result = _make_valid_analysis_result()
        result["agreement"]["block_status"] = "FAILED"
        errors = validate_analysis_result_schema(result)
        # FAILED is valid, but we get a consistency error because
        # analysis_degraded=False while a block has FAILED status
        block_status_errors = [e for e in errors if "block_status" in e and "invalid" in e.lower()]
        assert block_status_errors == []

    def test_invalid_block_status(self):
        result = _make_valid_analysis_result()
        result["agreement"]["block_status"] = "PARTIAL"
        errors = validate_analysis_result_schema(result)
        assert any("invalid block_status" in e.lower() for e in errors)

    def test_missing_block_status(self):
        result = _make_valid_analysis_result()
        del result["agreement"]["block_status"]
        errors = validate_analysis_result_schema(result)
        assert any("missing block_status" in e.lower() for e in errors)

    def test_valid_block_status_set(self):
        """VALID_BLOCK_STATUS must be exactly {"OK", "FAILED"}."""
        assert VALID_BLOCK_STATUS == frozenset({"OK", "FAILED"})


# ============================================================================
# 3. test_analysis_result_lifecycle_valid
# ============================================================================

class TestAnalysisResultLifecycle:
    """lifecycle_state must be in VALID_LIFECYCLE_STATES."""

    def test_valid_states(self):
        for state in VALID_LIFECYCLE_STATES:
            result = _make_valid_analysis_result()
            result["lifecycle_state"] = state
            errors = validate_analysis_result_schema(result)
            lifecycle_errors = [e for e in errors if "lifecycle_state" in e]
            assert lifecycle_errors == [], f"State {state} should be valid"

    def test_invalid_state(self):
        result = _make_valid_analysis_result()
        result["lifecycle_state"] = "INVENTED_STATE"
        errors = validate_analysis_result_schema(result)
        assert any("lifecycle_state" in e for e in errors)

    def test_lifecycle_enum_exact(self):
        """Exact 8 lifecycle states."""
        expected = {
            "NOT_STARTED", "WAITING_RESPONSES", "READY_TO_ISSUE",
            "READY_TO_REJECT", "NEEDS_ARBITRATION", "CHRONIC_BLOCKED",
            "ON_HOLD", "EXCLUDED",
        }
        assert VALID_LIFECYCLE_STATES == frozenset(expected)


# ============================================================================
# 4. test_analysis_result_failed_blocks_consistency
# ============================================================================

class TestAnalysisResultFailedBlocksConsistency:
    """failed_blocks must be list, analysis_degraded must be bool, consistency check."""

    def test_failed_blocks_is_list(self):
        result = _make_valid_analysis_result()
        result["failed_blocks"] = "A1"  # not a list
        errors = validate_analysis_result_schema(result)
        assert any("not a list" in e for e in errors)

    def test_analysis_degraded_is_bool(self):
        result = _make_valid_analysis_result()
        result["analysis_degraded"] = 1  # not bool
        errors = validate_analysis_result_schema(result)
        assert any("not bool" in e for e in errors)

    def test_failed_blocks_nonempty_but_degraded_false(self):
        result = _make_valid_analysis_result()
        result["failed_blocks"] = ["A1"]
        result["analysis_degraded"] = False
        errors = validate_analysis_result_schema(result)
        assert any("analysis_degraded is False" in e for e in errors)

    def test_consistent_degraded_true_with_failures(self):
        result = _make_valid_analysis_result()
        result["failed_blocks"] = ["A3"]
        result["analysis_degraded"] = True
        result["missing"]["block_status"] = "FAILED"
        errors = validate_analysis_result_schema(result)
        # Should pass — consistent
        consistency_errors = [e for e in errors if "analysis_degraded" in e]
        assert consistency_errors == []


# ============================================================================
# 5. test_g1_columns_exact
# ============================================================================

class TestG1ColumnsExact:
    """G1 DataFrame must have exactly the expected columns in canonical order."""

    def test_exact_columns(self):
        expected = (
            "approver_key", "display_name", "total_latest_assigned",
            "total_responded", "total_blocking", "blocking_rate",
            "avg_response_days", "is_systemic_blocker", "blocked_families",
            "severity",
        )
        assert G1_COLUMNS == expected

    def test_valid_dataframe_passes(self):
        df = pd.DataFrame([_make_g1_row()])
        df = enforce_column_order(df, "G1")
        errors = validate_dataframe_schema(df, "G1")
        assert errors == [], f"Valid G1 should pass: {errors}"

    def test_missing_column_detected(self):
        row = _make_g1_row()
        del row["severity"]
        df = pd.DataFrame([row])
        errors = validate_dataframe_schema(df, "G1")
        assert any("severity" in e for e in errors)

    def test_column_order_enforced(self):
        row = _make_g1_row()
        df = pd.DataFrame([row])
        # Scramble column order
        df = df[list(reversed(df.columns))]
        df = enforce_column_order(df, "G1")
        assert list(df.columns) == list(G1_COLUMNS)

    def test_schema_version_in_registry(self):
        assert "G1" in SCHEMA_REGISTRY
        _, _, version = SCHEMA_REGISTRY["G1"]
        assert version == "1.0.0"


# ============================================================================
# 6. test_g2_columns_exact
# ============================================================================

class TestG2ColumnsExact:
    """G2 DataFrame must have exactly the expected columns."""

    def test_exact_columns(self):
        expected = (
            "doc_family_key", "source_sheet", "document", "titre",
            "is_looping", "loop_length", "loop_start_ind", "loop_end_ind",
            "persistent_blockers", "latest_visa_global",
        )
        assert G2_COLUMNS == expected

    def test_valid_dataframe_passes(self):
        df = pd.DataFrame([_make_g2_row()])
        df = enforce_column_order(df, "G2")
        errors = validate_dataframe_schema(df, "G2")
        assert errors == [], f"Valid G2 should pass: {errors}"

    def test_missing_identifier_is_fatal(self):
        row = _make_g2_row()
        del row["doc_family_key"]
        df = pd.DataFrame([row])
        errors = validate_dataframe_schema(df, "G2")
        assert any("required identifier column" in e for e in errors)


# ============================================================================
# 7. test_g3_columns_exact
# ============================================================================

class TestG3ColumnsExact:
    """G3 DataFrame must have exactly the expected columns."""

    def test_exact_columns(self):
        expected = (
            "row_id", "risk_score", "is_high_risk",
            "contributing_factors", "factor_details",
        )
        assert G3_COLUMNS == expected

    def test_valid_dataframe_passes(self):
        df = pd.DataFrame([_make_g3_row()])
        df = enforce_column_order(df, "G3")
        errors = validate_dataframe_schema(df, "G3")
        assert errors == [], f"Valid G3 should pass: {errors}"

    def test_missing_column_detected(self):
        row = _make_g3_row()
        del row["risk_score"]
        df = pd.DataFrame([row])
        errors = validate_dataframe_schema(df, "G3")
        assert any("risk_score" in e for e in errors)


# ============================================================================
# 8. test_g4_columns_exact
# ============================================================================

class TestG4ColumnsExact:
    """G4 DataFrame must have exactly the expected columns."""

    def test_exact_columns(self):
        expected = (
            "source_sheet", "total_documents", "total_pending",
            "total_overdue", "total_high_risk", "category_distribution",
            "approval_rate", "avg_priority_score", "avg_days_pending",
            "is_high_risk_cluster", "health_score",
        )
        assert G4_COLUMNS == expected

    def test_valid_dataframe_passes(self):
        df = pd.DataFrame([_make_g4_row()])
        df = enforce_column_order(df, "G4")
        errors = validate_dataframe_schema(df, "G4")
        assert errors == [], f"Valid G4 should pass: {errors}"

    def test_missing_column_detected(self):
        row = _make_g4_row()
        del row["health_score"]
        df = pd.DataFrame([row])
        errors = validate_dataframe_schema(df, "G4")
        assert any("health_score" in e for e in errors)

    def test_empty_dataframe_has_correct_columns(self):
        df = empty_dataframe("G4")
        assert list(df.columns) == list(G4_COLUMNS)
        assert len(df) == 0


# ============================================================================
# 9. test_run_module4_return_signature
# ============================================================================

class TestRunModule4ReturnSignature:
    """run_module4 must return a 5-element tuple with correct types."""

    def test_empty_queue_return_types(self):
        """With empty M3 queue, run_module4 should return quickly with empty but valid outputs."""
        from jansa_visasist.pipeline.module4 import (
            run_module4,
            M1_REQUIRED_COLS,
            M2_EXTRA_COLS,
            M3_EXTRA_COLS,
        )

        m1_cols = list(M1_REQUIRED_COLS)
        m2_cols = list(M1_REQUIRED_COLS | M2_EXTRA_COLS)
        m3_cols = list(M3_EXTRA_COLS)

        m1 = pd.DataFrame(columns=m1_cols)
        m2 = pd.DataFrame(columns=m2_cols)
        m3 = pd.DataFrame(columns=m3_cols)

        result = run_module4(m1, m2, m3, datetime.date.today())

        # Must be 5-element tuple
        assert isinstance(result, tuple), f"Expected tuple, got {type(result)}"
        assert len(result) == 5, f"Expected 5 elements, got {len(result)}"

        per_item, g1, g2, g3, g4 = result

        # Type checks
        assert isinstance(per_item, list)
        assert isinstance(g1, pd.DataFrame)
        assert isinstance(g2, pd.DataFrame)
        assert isinstance(g3, pd.DataFrame)
        assert isinstance(g4, pd.DataFrame)

        # Empty queue -> empty results
        assert len(per_item) == 0

        # G1–G4 DataFrames have canonical columns
        assert list(g1.columns) == list(G1_COLUMNS)
        assert list(g2.columns) == list(G2_COLUMNS)
        assert list(g3.columns) == list(G3_COLUMNS)
        assert list(g4.columns) == list(G4_COLUMNS)


# ============================================================================
# ADDITIONAL: Schema module self-consistency
# ============================================================================

class TestSchemaRegistryConsistency:
    """SCHEMA_REGISTRY must be internally consistent."""

    @pytest.mark.parametrize("name", ["G1", "G2", "G3", "G4"])
    def test_registry_entry_structure(self, name):
        assert name in SCHEMA_REGISTRY
        schema, columns, version = SCHEMA_REGISTRY[name]
        assert isinstance(schema, dict)
        assert isinstance(columns, tuple)
        assert isinstance(version, str)
        # Columns must match schema keys
        assert tuple(schema.keys()) == columns

    @pytest.mark.parametrize("name", ["G1", "G2", "G3", "G4"])
    def test_empty_dataframe_columns(self, name):
        df = empty_dataframe(name)
        _, columns, _ = SCHEMA_REGISTRY[name]
        assert list(df.columns) == list(columns)
        assert len(df) == 0
